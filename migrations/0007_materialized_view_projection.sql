-- Fast-path metadata for named materialized views.
--
-- Immutable IPLD view blocks remain authoritative. This table only projects
-- the authenticated fold declaration so /v1/view can select the same
-- current-state attributes from kotobase_datoms_current without rehydrating
-- the full block graph. The canonical fold still persists the portable IPLD
-- view block; the D1 declaration becomes readable first.

CREATE TABLE IF NOT EXISTS kotobase_view_specs (
  ref_name   TEXT NOT NULL,
  view_name  TEXT NOT NULL,
  spec_edn   TEXT NOT NULL,
  attrs_json TEXT NOT NULL,
  basis_cid  TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (ref_name, view_name)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_kotobase_view_specs_ref
  ON kotobase_view_specs(ref_name, view_name, basis_cid);
