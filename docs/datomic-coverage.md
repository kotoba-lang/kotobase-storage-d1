# D1-backed Datomic API coverage

## Compatibility rule

`kotobase.datomic` uses Datomic's function argument order and EDN grammar.
The Worker carries that EDN unchanged across HTTP. There is no second JSON
query language for an LLM or application to learn.

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

## Deliberate differences from Datomic

Kotobase is not the Datomic product and does not pretend otherwise:

- basis and transaction identity are immutable CIDs, not numeric basis-t and
  transaction entity ids;
- entity ids allocated for tempids are stable Kotobase strings rather than
  Datomic partition-encoded longs;
- application transaction functions are registered as trusted host functions;
  arbitrary persisted Clojure source is not evaluated inside a Worker;
- listeners and Datomic's deployment/transactor administration are outside the
  provider-neutral database API;
- values in the current underlying peer engine retain its existing wire
  representation rules; Datomic's complete scalar/schema semantics are not
  claimed;
- schema enforcement currently covers string, boolean, long, double, keyword,
  ref, instant, UUID, and symbol values. Datomic tuple attributes and schema
  alteration are not yet part of the D1 facade.

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
11. missing auth, wrong capability, cross-tenant access, replay, stale CAS,
    and CID collision rejection.

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
append-only pre-projection history cannot be inferred from a current snapshot.

This keeps compatibility monotonic: adding an optimization cannot turn a
previously valid Datomic query into an invalid one, and a partially migrated
database cannot return projected data from the wrong basis.
