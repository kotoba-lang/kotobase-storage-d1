# D1-backed Datomic API coverage

## Compatibility rule

`kotobase.datomic` uses Datomic's function argument order and EDN grammar.
The Worker carries that EDN unchanged across HTTP. There is no second JSON
query language for an LLM or application to learn.

Implemented syntax:

- `(d/db conn)`
- `(d/transact conn {:tx-data [...]})`
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

## Deliberate differences from Datomic

Kotobase is not the Datomic product and does not pretend otherwise:

- basis and transaction identity are immutable CIDs, not numeric basis-t and
  transaction entity ids;
- a Kotobase database handle resolves its ref at operation time instead of
  representing a cached immutable Datomic `Db` value;
- tempid allocation/resolution and lookup refs are not implemented;
- transaction functions, listeners, sync, log, index-range, seek-datoms,
  entity/touch, and pull-many are not public compatibility functions;
- values in the current underlying peer engine retain its existing wire
  representation rules; Datomic's complete scalar/schema semantics are not
  claimed;
- schema installation and as-of/since/history primitives exist below the
  facade but are not yet exposed through the D1 compatibility API.

## Verified D1 path

Both local workerd+D1 and remote Cloudflare D1 verification cover:

1. signed CACAO authentication;
2. capability and tenant-ref authorization;
3. multi-block IPLD transaction and ref CAS;
4. relation, input, scalar, collection, and aggregate queries;
5. pull;
6. EAVT and AVET datom access;
7. retraction followed by a query observing the new basis;
8. persisted head resolution;
9. missing auth, wrong capability, cross-tenant access, replay, stale CAS,
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

This keeps compatibility monotonic: adding an optimization cannot turn a
previously valid Datomic query into an invalid one, and a partially migrated
database cannot return projected data from the wrong basis.
