-- Reverse index: content-addressed graph CID -> the literal ref name it was
-- derived from (`kotobase/db/<tenant_did>/<db_name>`). A CID is a one-way
-- hash of the ref name, so a caller holding only the CID (as net-kotobase's
-- own kotobase-client does for q/pull/datoms -- it never sends the literal
-- db_name for reads) cannot be looked up without this table. Populated on
-- every successful commit (POST /v1/transact) that establishes or updates a
-- ref by its literal name; read endpoints resolve a CID-shaped
-- `x-kotobase-ref` through this table before touching storage.

CREATE TABLE IF NOT EXISTS kotobase_graph_cid_index (
  cid        TEXT PRIMARY KEY,
  ref_name   TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_kotobase_graph_cid_index_ref_name
  ON kotobase_graph_cid_index (ref_name);
