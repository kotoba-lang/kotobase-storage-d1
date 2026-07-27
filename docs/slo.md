# D1 Datomic Client API — load / SLO gates

**Status**: qualification harness (not a contractual multi-tenant SLA).
**Host**: `https://kotobase-storage-d1.aozora.app`
**Runner**: `npm run slo:qualify`

## Targets (v1 gates)

| Metric | Default gate | Notes |
|---|---|---|
| point query p50 | ≤ 120 ms | projection path (N entities) |
| count query p50 | ≤ 150 ms | projection path |
| join+count p50 | ≤ 200 ms | projection path |
| transact wall | ≤ 5000 ms | batch of N entity maps |
| cold reindex wall | ≤ 8000 ms | rebuild projection then query |
| entities N | 300 | override with `KOTOBASE_SLO_ENTITIES` |
| error rate in suite | 0 | any non-2xx fails gate |

Defaults are **measured** production-first-pass gates (not aspirational
Cloud-class SLOs). Tighten via env as the path optimizes.

```bash
KOTOBASE_D1_URL=https://kotobase-storage-d1.aozora.app \
KOTOBASE_SLO_ENTITIES=300 \
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
