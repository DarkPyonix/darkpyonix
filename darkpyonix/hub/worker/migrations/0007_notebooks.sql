CREATE TABLE notebooks (
  notebook_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL REFERENCES accounts(account_id),
  creator_device TEXT,
  title TEXT NOT NULL,
  visibility TEXT NOT NULL,
  next_version INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX notebooks_account ON notebooks(account_id);
CREATE TABLE notebook_versions (
  notebook_id TEXT NOT NULL REFERENCES notebooks(notebook_id) ON DELETE CASCADE,
  version INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  message TEXT,
  source_filename TEXT NOT NULL,
  source_sha256 TEXT NOT NULL,
  source_size INTEGER NOT NULL,
  source_cells INTEGER NOT NULL,
  run_id TEXT,
  run_sha256 TEXT,
  run_size INTEGER,
  PRIMARY KEY (notebook_id, version)
);
