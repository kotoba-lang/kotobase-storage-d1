-- Tenant-scope the block store.
--
-- `kotobase_blocks.cid` was a GLOBAL primary key while refs were per-DID
-- (authorization requires resource to sit under `kotobase/db/<iss>/`), so the
-- block store was the one shared surface underneath an otherwise isolated
-- tenant model. Two consequences, both reachable by any caller holding any
-- keypair:
--
--   1. Integrity. Writes are `ON CONFLICT(cid) DO NOTHING` and reads return
--      whatever bytes are stored WITHOUT re-deriving the CID from them. So
--      whoever writes a CID first owns it globally: an attacker who learns a
--      CID a victim's graph will reference can pre-store different bytes
--      under it, and the victim's own reads then return the attacker's
--      content. CIDs are not secret -- every transact/datoms response
--      returns them.
--
--   2. Availability. The victim's honest write of that CID becomes a no-op
--      (DO NOTHING), the read-back comparison fails, and they get a
--      permanent CidCollision 409 for a block they legitimately own.
--
-- Scoping the key by principal closes both without changing the endpoint's
-- contract: `/v1/commit` keys stay opaque client-chosen strings (the
-- verification suite uses "cid-a"/"cid-b"), and content-addressing proper
-- stays the engine's business on the transact path. Two tenants may now hold
-- the same key with different bytes, which is correct -- they are different
-- blocks.
--
-- The projection tables (0003) are already keyed by ref_name, and ref names
-- are the tenant-prefixed resource authorization checks, so they needed no
-- change.
--
-- Legacy rows are carried over under principal '' rather than being
-- attributed to a guess. No authenticated principal is ever '' (a did:key is
-- non-empty), so pre-migration blocks become unreachable rather than
-- silently owned by someone. This database is the disposable verification
-- one; a production migration would need a real backfill of block ownership
-- first.

CREATE TABLE IF NOT EXISTS kotobase_blocks_tenant (
  principal   TEXT NOT NULL,
  cid         TEXT NOT NULL,
  bytes       BLOB NOT NULL,
  byte_length INTEGER NOT NULL DEFAULT 0,
  created_at  INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (principal, cid)
);

INSERT OR IGNORE INTO kotobase_blocks_tenant
  (principal, cid, bytes, byte_length, created_at)
  SELECT '', cid, bytes, byte_length, created_at FROM kotobase_blocks;

DROP TABLE kotobase_blocks;

ALTER TABLE kotobase_blocks_tenant RENAME TO kotobase_blocks;
