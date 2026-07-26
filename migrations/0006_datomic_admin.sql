-- Datomic tuple metadata, durable transaction notifications, and listener
-- cursors. The immutable IPLD chain remains authoritative; these tables are
-- rebuildable/operational projections.

ALTER TABLE kotobase_schema ADD COLUMN tuple_attrs_edn TEXT;
ALTER TABLE kotobase_schema ADD COLUMN tuple_types_edn TEXT;
ALTER TABLE kotobase_schema ADD COLUMN tuple_type_edn TEXT;

CREATE TABLE IF NOT EXISTS kotobase_tx_outbox (
  ref_name    TEXT    NOT NULL,
  t           INTEGER NOT NULL,
  tx_cid      TEXT    NOT NULL,
  payload_edn TEXT    NOT NULL,
  created_at  INTEGER NOT NULL,
  PRIMARY KEY (ref_name, t),
  UNIQUE (ref_name, tx_cid)
);

CREATE INDEX IF NOT EXISTS idx_kotobase_tx_outbox_ref_created
  ON kotobase_tx_outbox(ref_name, created_at, t);

CREATE TABLE IF NOT EXISTS kotobase_listener_cursor (
  ref_name   TEXT    NOT NULL,
  consumer   TEXT    NOT NULL,
  next_t     INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (ref_name, consumer)
);

CREATE INDEX IF NOT EXISTS idx_kotobase_listener_cursor_updated
  ON kotobase_listener_cursor(updated_at);
