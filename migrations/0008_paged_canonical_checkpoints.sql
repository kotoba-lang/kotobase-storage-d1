-- Bounded-memory canonical checkpoints for D1-backed graphs.
--
-- Legacy IPLD fold rebuilds all four indexes in one Worker invocation and
-- exceeds the fixed Worker heap for the production AppView graph. These
-- tables let a durable Workflow copy a stable projection head in keyset-
-- paginated chunks, hash every page, then atomically publish the generation
-- only if the projection head did not change while it was being copied.

CREATE TABLE IF NOT EXISTS kotobase_canonical_checkpoint_pages (
  ref_name    TEXT NOT NULL,
  generation  TEXT NOT NULL,
  page_no     INTEGER NOT NULL,
  first_e_edn TEXT NOT NULL,
  first_a_edn TEXT NOT NULL,
  first_v_edn TEXT NOT NULL,
  last_e_edn  TEXT NOT NULL,
  last_a_edn  TEXT NOT NULL,
  last_v_edn  TEXT NOT NULL,
  row_count   INTEGER NOT NULL,
  sha256      TEXT NOT NULL,
  rows_json   TEXT NOT NULL,
  created_at  INTEGER NOT NULL,
  PRIMARY KEY (ref_name, generation, page_no)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS kotobase_canonical_checkpoints (
  ref_name     TEXT PRIMARY KEY,
  generation   TEXT NOT NULL,
  source_head  TEXT NOT NULL,
  page_count   INTEGER NOT NULL,
  row_count    INTEGER NOT NULL,
  completed_at INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS kotobase_compaction_jobs (
  generation   TEXT PRIMARY KEY,
  ref_name     TEXT NOT NULL,
  source_head  TEXT NOT NULL,
  status       TEXT NOT NULL,
  page_count   INTEGER NOT NULL DEFAULT 0,
  row_count    INTEGER NOT NULL DEFAULT 0,
  error        TEXT,
  started_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL,
  completed_at INTEGER
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_kotobase_compaction_jobs_ref
  ON kotobase_compaction_jobs(ref_name, started_at DESC);

