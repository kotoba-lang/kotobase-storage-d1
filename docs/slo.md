# D1 Datomic Client API — load / SLO gates

**Status**: qualification harness (not a contractual multi-tenant SLA).
**Host**: `https://kotobase-storage-d1.aozora.app`
**Runner**: `npm run slo:qualify`

## Targets (v1 gates)

| Metric | Default gate | Notes |
|---|---|---|
| point query p50 | ≤ 1500 ms | includes CF RTT + CACAO + D1 |
| count query p50 | ≤ 1500 ms | projection path when warm |
| join+count p50 | ≤ 2000 ms | projection path when warm |
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

## Latest measured (2026-07-27, N=300, CACAO did:key)

| Path | point p50 | count p50 | join p50 | transact | reindex | transport |
|------|-----------|-----------|----------|----------|---------|-----------|
| D1 direct | 682 ms | 650 ms | 604 ms | 3193 ms | 1369 ms | public HTTPS |
| edge `kotobase.net` | 659 ms | 639 ms | 681 ms | 3039 ms | 1990 ms* | **service-binding** |

\*reindex still hits D1 `/v1/reindex` (not exposed on edge `/api/*`).

Evidence: `docs/evidence/slo-auth-2026-07-27.edn`. Both paths green under v1 gates.

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
