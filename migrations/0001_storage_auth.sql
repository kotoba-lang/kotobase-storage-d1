CREATE TABLE IF NOT EXISTS kotobase_blocks (
  cid TEXT PRIMARY KEY,
  bytes BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS kotobase_refs (
  name TEXT PRIMARY KEY,
  cid TEXT NOT NULL,
  revision INTEGER NOT NULL DEFAULT 1,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS auth_nonces (
  principal TEXT NOT NULL,
  nonce TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  PRIMARY KEY (principal, nonce)
);

CREATE TABLE IF NOT EXISTS auth_decisions (
  request_id TEXT PRIMARY KEY,
  principal TEXT,
  kind TEXT NOT NULL,
  decision TEXT NOT NULL,
  reason TEXT,
  action TEXT,
  resource TEXT,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_auth_decisions_principal_time
  ON auth_decisions(principal, created_at);
