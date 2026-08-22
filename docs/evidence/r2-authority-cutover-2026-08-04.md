# R2 authority cutover evidence — 2026-08-04

No tenant identifiers, ref names, or CIDs are included in this record.

## Decision

- Mutable graph-head authority: Cloudflare R2 object ETag CAS.
- Canonical immutable blocks: R2 namespace `kotobase/datomic/v2/production/canonical`.
- D1 role after cutover: rebuildable query projection and rollback mirror; it is
  not consulted as the head authority.
- Read path during the v2 grace period: bounded SQL-compatible reads use the D1
  projection only when its projected head equals the mirrored head. Unsupported
  or temporal reads fall back to the R2 canonical engine.
- GC remains disabled (`KOTOBASE_R2_GC=0`).

## Production versions

- Backfill Worker: `c7fbab25-4f3b-4e38-88b7-4a1972c3b117`, deployed
  2026-08-04T05:31:41.138Z.
- R2 authority candidate: `7a46e445-7273-462d-be0e-ed9872757d83`, deployed
  2026-08-04T06:16:04.155Z.
- Gateway authority cutover: `fce70115-abd8-431e-b9a4-c60e9207704a`, deployed
  2026-08-04T06:26:43.803Z.
- Current gateway observability revision: `0b803383-5f01-40ca-9332-5a6b395d1e09`;
  `/health` explicitly reports `head_authority=r2-etag-cas`.
- Gateway binding: `KOTOBASE_D1` (stable legacy binding name) ->
  `kotobase-storage-r2-candidate`.
- Immediate rollback target: service `kotobase-storage-d1`.

## Backfill and physical parity

The first qualification correctly failed because the D1 API represented BLOBs
as number arrays and the first copier wrote zero-byte R2 objects. D1 remained
authoritative and no user traffic read those objects. The resumable repair
overwrote only those known zero-byte placeholders and restarted qualification.

Final repaired cutoff:

- blocks: 18,111 / 18,111
- canonical bytes: 206,903,168 / 206,903,168
- refs: 137 / 137, exact CID and revision
- result: pass

After disabling repair mode, normal tail catch-up also passed. Following the
multi-region and gateway smoke writes, the later tail qualification observed:

- blocks: 18,114 / 18,114
- canonical bytes: 206,913,887 / 206,913,887
- refs: 141 / 141
- result: pass

State is checkpointed after each bounded page with R2 ETag CAS. A deliberately
overlapping invocation converged on one checkpoint. Actual SQL-variable and
waitUntil/CPU termination events resumed from the last durable cursor without
resetting authority or losing copied objects.

## Semantic shadow parity

- graph refs checked: 127
- matched heads: 127
- failures: 0
- stale D1 projection count: 2 (canonical R2 head still matched)
- large projection qualified by R2 head + exact D1 projected head + physical
  block parity: 1
- result: pass

Direct full-chain canonical hydration measured p50 2,522 ms, p95 37,182 ms,
and p99 89,276 ms. This failed the operational latency gate, so the gateway was
not cut over to that read path. The candidate was changed to keep R2 as head
authority while serving bounded current-basis reads from the rebuildable D1
projection. Merkle-LSM v2 remains the path that removes this projection
dependency.

## Latency and multi-region drills

Each run transacted a fresh synthetic graph, verified its count through the R2
authority endpoint, then read the same ref through the D1 rollback endpoint.

NRT candidate, 100 entities / 8 samples:

- transaction: 2,155.1 ms
- R2-authority projection path p95: point 796.6 ms, count 746.2 ms, join 800.9 ms
- D1 rollback p95: point 1,392.5 ms, count 834.7 ms, join 806.1 ms
- result: pass (5,000 ms gate)

IAD GitHub runner, 100 entities / 8 samples:

- transaction: 4,942.3 ms
- R2-authority projection path p95: point 1,763.8 ms, count 2,085.4 ms,
  join 1,711.0 ms
- D1 rollback p95: point 1,731.8 ms, count 2,421.3 ms, join 2,518.6 ms
- result: pass (5,000 ms gate)
- GitHub Actions run: 30883786652

Production gateway after cutover, NRT, 30 entities / 4 samples:

- transaction: 2,535.9 ms
- gateway R2-authority p95: point 711.4 ms, count 629.1 ms, join 749.6 ms
- D1 rollback p95: point 870.7 ms, count 974.7 ms, join 871.7 ms
- result: pass (5,000 ms gate)

## Crash and rollback drills

- A semantic parity invocation was forcibly terminated by the runtime after its
  waitUntil budget. Checkpoint 85/85 survived and the following invocation
  resumed at 89/89 with no failures.
- Candidate version `18ffb22b-2dd2-41ea-96fd-90af1af9e501` was rolled back to
  `bda9987a-d174-4c68-8b2d-c0d5ec9b38b9`, health was green and checkpoint
  11/11 remained. Restoring `18ffb22b-2dd2-41ea-96fd-90af1af9e501` retained
  the same state.
- R2 authority transactions retry the D1 mirror three times. An unrepaired
  mismatch leaves a durable `rollback-mirror-pending` marker; health currently
  reports `rollback_mirror_ready=true`.

## Cost gate

Measured canonical payload is approximately 0.207 GB and the initial namespace
contains about 18.3k block/ref/state objects. At standard R2 rates, storage is
about USD 0.0031/month before the account-wide free tier. The failed first copy
plus repair used roughly 36.2k block PUTs, approximately USD 0.163 at the linear
Class-A rate before the account-wide monthly free tier. Egress is free. Actual
invoice impact depends on other account usage and Cloudflare billing-unit
rounding.

Pricing source: https://developers.cloudflare.com/r2/pricing/

## Grace and GC gate

Authority grace began at 2026-08-04T06:26:43.803Z. The configured grace is
86,400 seconds, so the earliest time-based eligibility is
2026-08-05T06:26:43.803Z (2026-08-05 15:26:43.803 JST).

GC MUST remain disabled until all of the following are true:

1. the full grace interval has actually elapsed;
2. physical tail parity and rollback-mirror health remain green;
3. the v2 Merkle-LSM projection has run for at least the same grace interval;
4. rollback no longer depends on any object proposed for collection.

This evidence does not authorize early GC and does not claim the elapsed-time
gate has passed.
