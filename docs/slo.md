# D1 Datomic Client API — load / SLO gates

**Status**: qualification harness (not a contractual multi-tenant SLA).
**Host**: `https://kotobase-storage-d1.aozora.app`
**Runner**: `npm run slo:qualify`

## Targets (v1 gates)

| Metric | Gate | Notes |
|---|---|---|
| point query p50 (1k entities / 3k datoms) | ≤ 80 ms | projection path |
| count query p50 (1k entities) | ≤ 100 ms | projection path |
| join+count p50 (1k entities) | ≤ 150 ms | projection path |
| transact wall (100 entities batch) | ≤ 1500 ms | multi-block + CAS |
| cold reindex (1k entities) | ≤ 5000 ms | rebuild projection then query |
| error rate in suite | 0 | any non-2xx fails gate |

Override via env:

```bash
KOTOBASE_D1_URL=https://kotobase-storage-d1.aozora.app \
KOTOBASE_SLO_POINT_P50_MS=80 \
KOTOBASE_SLO_COUNT_P50_MS=100 \
KOTOBASE_SLO_JOIN_P50_MS=150 \
KOTOBASE_SLO_TX_MS=1500 \
KOTOBASE_SLO_REINDEX_MS=5000 \
npm run slo:qualify
```

## Cold / index-pruned read policy

1. Prefer SQL projection when `projection.head_cid == refs.cid`.
2. On **stale projection** (`:reason :stale-projection`), Worker **rebuilds once**
   from the canonical CID head, then retries the fast path.
3. If the query is outside the SQL subset (`:unsupported-query`), fall back to
   full hydrate without rebuild.
4. Time-travel (`:as-of` / `:since` / `:history`) always hydrates (no projection).

## What this does *not* claim

- Multi-region p99 SLOs
- Capacity for 10^6+ datoms without further sharding
- Cognitect Cloud wire latency parity
