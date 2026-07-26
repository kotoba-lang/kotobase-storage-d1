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
- `POST /v1/q` — `{:query [:find ...] :args [...]}`
- `POST /v1/pull` — `{:selector [...] :eid ...}`
- `POST /v1/datoms` — `{:index :eavt :components [...]}`
- `GET /v1/head`

Every call carries the database ref in `x-kotobase-ref` and a fresh signed
CACAO in `authorization`. Query text is data parsed by the ClojureScript EDN
reader; it is never evaluated as code. Predicate/function clauses use the
engine's fixed whitelist.

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

This is syntax-compatible rather than a claim of complete Datomic product
compatibility. Kotobase uses CID heads instead of numeric basis-t/tx ids and
does not implement Datomic's transactor administration, tempid allocation,
listeners, or log API. See `docs/datomic-coverage.md` for the exact boundary.

The deploy shell uses the same CACAO SIWE canonicalization as
`gftdcojp/net-kotobase`. Its authn/authz decision shapes follow
`kotoba-lang/authentication` and `kotoba-lang/authorization`.
