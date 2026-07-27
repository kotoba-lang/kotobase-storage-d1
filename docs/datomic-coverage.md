# D1-backed Datomic API coverage

## Maturity (2026-07-27)

| Layer | Status | Evidence |
|---|---|---|
| Library `kotobase.datomic.client` | **Client API beta** | Matches published `datomic.client.api` var names + arg-maps; 80 engine assertions green |
| HTTP `/api/*` | **Client API beta** | EDN, **not XRPC**; paths named after Client API ops |
| HTTP `/v1/*` | Legacy alias | Same handlers; keep for existing callers |
| Cognitect `client-cloud` proprietary wire | **Not claimed** | Requires Transit golden capture (ADR-2606201800 residue) |
| Production SLO / load gate | **Open** | Benchmark harness exists; permanent multi-tenant route still optional |

## Compatibility rule

**Use `kotobase.datomic.client` as the drop-in for `datomic.client.api`.**
It exposes the same public surface (client / connect / create-database /
list-databases / q / qseq / pull / datoms / seek-datoms / rseek-datoms /
index-range / index-pull / transact / with / with-db / as-of / since /
history / sync / tx-range / db-stats). `administer-system` is unsupported
(Cloud control plane).

`kotobase.datomic` remains the lower-level grammar facade. The Worker
carries EDN over HTTP. There is **no XRPC** on this surface and no second
JSON query language for an LLM or application to learn.

### HTTP Client API (preferred)

```
POST /api/q          body: {:query ... :args [...]}   ; omit db from :args
POST /api/transact   body: {:tx-data [...]}
POST /api/pull       body: {:selector ... :eid ...}
POST /api/datoms     body: {:index :eavt :components [...]}
POST /api/tx-range   body: {:start 0 :end nil}
POST /api/db         → basis handle
```

Headers: `Content-Type: application/edn`, `Authorization: CACAO …`,
`x-kotobase-ref: kotobase/db/<tenant>/<db-name>` (or `X-Datomic-DB-Name`).

Implemented syntax:

- `(d/db conn)`
- `(d/transact conn {:tx-data [...]})`
- schema entity maps using `:db/ident`, `:db/valueType`,
  `:db/cardinality`, and `:db/unique`
- `[:db/add e a v]`, `[:db/retract e a v]`,
  `[:db/retractEntity e]`
- entity maps such as `{:db/id e :person/name "Alice"}`
- `(d/q query db & inputs)`
- vector and map query forms
- `:find`, `:in`, `:where`, `:with`, `:keys`, `:strs`, `:syms`
- relation, scalar (`.`), collection (`...`), and tuple find shapes
- joins, negation, `or`, `or-join`, whitelisted predicates/functions,
  aggregates, and engine rules
- `(d/pull db selector eid)`
- `(d/datoms db {:index ... :components ... :limit ...})`
- EAVT, AEVT, AVET, and VAET index selection
- authenticated `POST /v1/reindex` from the canonical CID head
- immutable database values via `db`, `basis-t`, `as-of`, `since`, and
  `history`
- negative tempids and `resolve-tempid`
- lookup refs in entity and ref-value positions
- `:db.unique/identity` upsert
- `with`, `pull-many`, `entity`, `touch`, `entid`, `ident`, `seek-datoms`,
  and `index-range`
- `:db.fn/cas`, `:db.fn/retractAttribute`, `:db.fn/retractEntity`, and
  application-registered pure transaction functions
- homogeneous, heterogeneous, and composite tuple attributes via
  `:db/tupleType`, `:db/tupleTypes`, and `:db/tupleAttrs`
- persisted `:db.type/fn` transaction functions in the bounded
  `kotobase/tx-ir-v1` declarative format
- `tx-range`, in-process `listen` / `unlisten`, and D1 durable
  register/poll/ack listener cursors
- D1 database administration status for canonical/projection health and counts

## Deliberate differences from Datomic

Kotobase is not the Datomic product and does not pretend otherwise:

- basis and transaction identity are immutable CIDs, not numeric basis-t and
  transaction entity ids;
- entity ids allocated for tempids are stable Kotobase strings rather than
  Datomic partition-encoded longs;
- trusted host transaction functions remain available, while persisted
  functions are a portable declarative transaction IR; arbitrary persisted
  Clojure source is not evaluated inside a Worker;
- Kotobase supplies transaction reports, durable D1 listeners, and database
  health administration, but Datomic Cloud's deployment/topology control plane
  is outside this storage adapter;
- values in the current underlying peer engine retain its existing wire
  representation rules; Datomic's complete scalar/schema semantics are not
  claimed;
- schema enforcement covers string, boolean, long, double, keyword, ref,
  instant, UUID, symbol, function, and tuple values. Installed schema remains
  immutable; schema alteration is not part of the D1 facade.

The D1 HTTP facade accepts `:as-of`, `:since`, or `:history true` in the EDN
envelope for `q`, `pull`, and `datoms`; `GET /v1/basis` returns the immutable
`:basis-cid` and numeric `:basis-t`.

## Verified D1 path

The same verification script targets local workerd+D1 or a remote Cloudflare
D1 Worker. The current feature matrix is exercised locally end to end:

1. signed CACAO authentication;
2. capability and tenant-ref authorization;
3. multi-block IPLD transaction and ref CAS;
4. relation, input, scalar, collection, and aggregate queries;
5. pull;
6. EAVT and AVET datom access;
7. retraction followed by a query observing the new basis;
8. persisted head resolution;
9. tempid allocation, identity upsert, lookup refs, and compare-and-swap;
10. as-of, since, and assertion/retraction history reads;
11. composite tuple identity and lookup-ref upsert;
12. persisted declarative transaction functions;
13. durable transaction range, listener register/poll/ack, and administration
    status;
14. missing auth, wrong capability, cross-tenant access, replay, stale CAS,
    CID collision, uniqueness, and value-type rejection.

## Indexed SQLite/D1 execution

The D1 adapter maintains a rebuildable query projection at the same CAS
boundary as the canonical mutable ref:

- EAVT primary arrangement;
- covering AEVT, AVET, and VAET indexes;
- current datoms plus append-only transaction history;
- an attribute-cardinality materialized view refreshed only for attributes
  touched by a transaction (or all attributes after `retractEntity`);
- projection-head equality as the admission check for every indexed read.

SQL compilation is intentionally a semantics-preserving fast subset. Positive
triple clauses, scalar inputs, joins, standard result shapes, named result
maps, and a single `count` aggregate use SQLite. Flat forward pull and
EAVT/AEVT/AVET/VAET `datoms` access use the same projection. Unsupported
grammar and stale/missing projections use the canonical immutable-block
hydrate path.

An operator can explicitly rebuild a missing/stale projection with
`POST /v1/reindex`. Rebuild insertion is chunked, but projection publication is
one D1 transaction conditional on the canonical ref still naming the hydrated
head. Reindex reconstructs current state and schema-derived arrangements;
append-only pre-projection datom history cannot be inferred from a current
snapshot. Canonical `d/tx-range` can still read transaction blocks from the
immutable chain; the D1 durable outbox begins when migration 0006 is active.

This keeps compatibility monotonic: adding an optimization cannot turn a
previously valid Datomic query into an invalid one, and a partially migrated
database cannot return projected data from the wrong basis.
