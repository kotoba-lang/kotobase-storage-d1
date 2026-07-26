# kotobase-storage-d1

Cloudflare D1 storage adapter and Datomic-shaped Worker deployment.

D1 is SQLite-backed and is not PostgreSQL. This repository validates the same
Kotobase immutable-block/mutable-ref semantics on D1, with real signed CACAO
authentication and deny-by-default tenant/capability authorization.

The Worker has no custom route and does not modify production kotobase.net.

## Datomic API

The Clojure/ClojureScript compatibility namespace uses Datomic's argument
order and query grammar:

```clojure
(require '[kotobase.datomic :as d])

(d/transact conn
  {:tx-data [[:db/add "alice" :person/name "Alice"]
             [:db/add "alice" :person/role "admin"]]})

(d/q '[:find ?e ?name
       :in $ ?role
       :where
       [?e :person/role ?role]
       [?e :person/name ?name]]
     (d/db conn)
     "admin")

(d/pull (d/db conn) [:person/name :person/role] "alice")
(d/datoms (d/db conn) {:index :eavt :components ["alice"]})
```

The Worker transports those values as EDN so keywords, symbols, sets, query
vectors, and pull selectors are not weakened into an ad-hoc JSON query DSL:

- `POST /v1/transact` — `{:tx-data [...]}`
- `POST /v1/reindex` — `{}` (rebuild the D1 projection from canonical blocks)
- `POST /v1/q` — `{:query [:find ...] :args [...]}`
- `POST /v1/pull` — `{:selector [...] :eid ...}`
- `POST /v1/datoms` — `{:index :eavt :components [...]}`
- `POST /v1/tx-range` — `{:start 0 :end nil :limit 100}`
- `POST /v1/listeners/register` — `{:op :register :consumer "worker-a" :since 0}`
- `POST /v1/listeners/poll` — `{:op :poll :consumer "worker-a" :limit 100}`
- `POST /v1/listeners/ack` — `{:op :ack :consumer "worker-a" :t 42}`
- `GET /v1/admin/status`
- `GET /v1/head`
- `GET /v1/basis` — `{:basis-cid ... :basis-t ...}`

`q`, `pull`, and `datoms` accept one optional immutable database selector in
their EDN envelope: `:as-of t`, `:since t`, or `:history true`. The Datalog
query itself remains ordinary Datomic syntax.

Every call carries the database ref in `x-kotobase-ref` and a fresh signed
CACAO in `authorization`. Query text is data parsed by the ClojureScript EDN
reader; it is never evaluated as code. Predicate/function clauses use the
engine's fixed whitelist.

`x-kotobase-ref` accepts either the literal ref name
(`kotobase/db/<tenant_did>/<db_name>`) or its content-addressed CID
(`graphCidFromName` from `gftdcojp/net-kotobase`'s `kotobase.graph`,
identical to net-kotobase's own edge derivation). A successful `/v1/transact`
records `(cid, ref_name)` into `kotobase_graph_cid_index`, so a later
`q`/`pull`/`datoms`/`head` call bearing only the CID resolves back to the ref
name a prior transact already established — a CID nothing has ever
transacted into 404s as `UnknownGraphCid` rather than being treated as a
fresh/empty graph. This exists because net-kotobase's own client only ever
sends the CID for reads (never the literal name); see
`gftdcojp/net-kotobase`'s ADR-2607260940.

Verified boundaries:

- generic signed Ed25519 `did:key` CACAO authentication;
- expiry and issued-at checks plus D1-backed nonce replay rejection;
- capability and tenant-scoped, deny-by-default authorization;
- immutable block collision rejection;
- mutable ref genesis/update compare-and-set;
- persisted authn/authz decision evidence without storing credentials.
- full engine transactions over multiple immutable IPLD blocks;
- Datomic vector queries including `:in`, relation/scalar/collection/tuple
  result shapes and `:keys`/`:strs`/`:syms`;
- pull selectors and EAVT/AEVT/AVET/VAET datom access.

## SQLite/D1 query projection

The immutable block/ref model remains the portable source of truth, but D1
queries no longer need to hydrate the complete block chain. Migration
`0003_datomic_projection.sql` adds:

- a current-datom table whose primary key is the EAVT arrangement;
- covering AEVT, AVET, and VAET indexes;
- append-only transaction/datom history;
- a projection head, which must exactly match the canonical ref head before
  the SQL fast path may be read;
- incrementally refreshed per-attribute cardinality materialization.

Projection mutations and the mutable-ref compare-and-set are published in one
atomic D1 `batch()`. If a database predates the projection or its head is
stale, query/pull/datoms safely use the canonical hydrate path.

`POST /v1/reindex` hydrates the canonical head and atomically replaces current
datoms, schema, uniqueness, and attribute-statistics arrangements only if the
ref still names that head. A concurrent transaction produces
`:reason :head-changed` instead of publishing a mixed basis.

## Datomic schema enforcement

Schema entity maps use Datomic's ordinary transaction form:

```clojure
{:db/id :account/email
 :db/ident :account/email
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/one
 :db/unique :db.unique/identity}
```

The D1 adapter validates supported `:db/valueType` declarations before the
canonical transaction, expands cardinality-one replacement into explicit
retractions, and maintains a unique-value arrangement whose primary key rejects
cross-entity collisions in the same atomic ref-CAS batch. Schema validation is
tied to the canonical head; a concurrent schema change forces the transaction
to retry validation.

Tuple attributes use the ordinary Datomic declarations `:db/tupleAttrs`,
`:db/tupleTypes`, or `:db/tupleType`. Composite tuple values are derived and
updated atomically with their constituent attributes, so a
`:db.unique/identity` composite can enforce multi-attribute identity and serve
as a lookup ref.

Persisted transaction functions use `:db.type/fn` and `:db/fn`, but the stored
body is a bounded, declarative `kotobase/tx-ir-v1` template rather than
arbitrary Clojure source. This preserves Datomic invocation syntax while
keeping Worker execution deterministic and free of `eval`.

Bulk datom additions, retractions, history, and touched-attribute refreshes use
set-based JSON/SQL statements rather than one D1 request per datom. Transactions
against a ref with no installed schema keep a zero-lookup preparation path;
schema-aware transactions fetch only touched cardinality/uniqueness keys.

The SQL fast path currently covers positive triple-clause Datalog, scalar
inputs, relation/scalar/collection/tuple result shapes, result maps, and
single-variable `count`. Flat forward pull and all four current datom index
orders are also indexed. Rules, negation, `or`, and function clauses retain
the canonical engine fallback.

Run the correctness suite against a local or remote endpoint:

```bash
KOTOBASE_D1_URL=http://127.0.0.1:8787 node scripts/verify.mjs
```

Run the same suite plus 300/3,000/15,000-datom performance probes:

```bash
KOTOBASE_D1_URL=http://127.0.0.1:8787 npm run benchmark
```

The benchmark validates every point, count, and join result before recording
latency; it does not treat a fast error or empty response as a measurement.

Migration `0006_datomic_admin.sql` adds an atomic transaction outbox and
durable consumer cursors. In-process users can use `d/listen`; D1 consumers use
the register/poll/ack endpoints, which survive Worker restarts. The
administration endpoint reports canonical/projection heads, counts, listener
state, and latest transaction sequence. This is database administration, not a
claim to reproduce Datomic Cloud's deployment control plane. See
`docs/datomic-coverage.md` for the exact boundary.

The deploy shell uses the same CACAO SIWE canonicalization as
`gftdcojp/net-kotobase`. Its authn/authz decision shapes follow
`kotoba-lang/authentication` and `kotoba-lang/authorization`.
