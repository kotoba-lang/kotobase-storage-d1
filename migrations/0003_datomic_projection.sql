-- Query projection for the Datomic-shaped API.
--
-- Immutable blocks remain the portable source of truth.  These tables are a
-- transactionally published, rebuildable SQLite projection of the current
-- basis, history, and the first incremental materialized view (attribute
-- cardinality).

CREATE TABLE IF NOT EXISTS kotobase_projection (
  ref_name   TEXT PRIMARY KEY,
  head_cid   TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS kotobase_datoms_current (
  ref_name TEXT NOT NULL,
  e_edn    TEXT NOT NULL,
  a_edn    TEXT NOT NULL,
  v_edn    TEXT NOT NULL,
  tx_cid   TEXT NOT NULL,
  PRIMARY KEY (ref_name, e_edn, a_edn, v_edn)
) WITHOUT ROWID;

-- The primary key is EAVT.  The remaining covering indexes keep every value
-- needed by d/datoms and the SQL Datalog compiler in the index leaf.
CREATE INDEX IF NOT EXISTS idx_kotobase_datoms_aevt
  ON kotobase_datoms_current
  (ref_name, a_edn, e_edn, v_edn, tx_cid);

CREATE INDEX IF NOT EXISTS idx_kotobase_datoms_avet
  ON kotobase_datoms_current
  (ref_name, a_edn, v_edn, e_edn, tx_cid);

CREATE INDEX IF NOT EXISTS idx_kotobase_datoms_vaet
  ON kotobase_datoms_current
  (ref_name, v_edn, a_edn, e_edn, tx_cid);

CREATE TABLE IF NOT EXISTS kotobase_datom_history (
  ref_name TEXT NOT NULL,
  tx_cid   TEXT NOT NULL,
  ordinal  INTEGER NOT NULL,
  added    INTEGER NOT NULL CHECK (added IN (0, 1)),
  e_edn    TEXT NOT NULL,
  a_edn    TEXT,
  v_edn    TEXT,
  PRIMARY KEY (ref_name, tx_cid, ordinal)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_kotobase_history_eav
  ON kotobase_datom_history
  (ref_name, e_edn, a_edn, v_edn, tx_cid);

-- RisingWave-style first materialization: maintained only for attributes
-- touched by a transaction, rather than recomputing the whole database.
CREATE TABLE IF NOT EXISTS kotobase_attribute_stats (
  ref_name   TEXT NOT NULL,
  a_edn      TEXT NOT NULL,
  datom_count INTEGER NOT NULL,
  basis_cid  TEXT NOT NULL,
  PRIMARY KEY (ref_name, a_edn)
) WITHOUT ROWID;
