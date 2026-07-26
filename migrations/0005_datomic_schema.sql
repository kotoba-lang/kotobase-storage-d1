-- Datomic schema projection and uniqueness arrangement.
--
-- Schema is itself stored as ordinary immutable datoms. These tables are the
-- rebuildable, current-basis execution form used to validate transactions.

CREATE TABLE IF NOT EXISTS kotobase_schema (
  ref_name    TEXT NOT NULL,
  a_edn       TEXT NOT NULL,
  value_type  TEXT NOT NULL,
  cardinality TEXT NOT NULL,
  unique_kind TEXT,
  basis_cid   TEXT NOT NULL,
  PRIMARY KEY (ref_name, a_edn)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS kotobase_unique_values (
  ref_name TEXT NOT NULL,
  a_edn    TEXT NOT NULL,
  v_edn    TEXT NOT NULL,
  e_edn    TEXT NOT NULL,
  basis_cid TEXT NOT NULL,
  PRIMARY KEY (ref_name, a_edn, v_edn)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_kotobase_unique_entity
  ON kotobase_unique_values(ref_name, e_edn, a_edn, v_edn);
