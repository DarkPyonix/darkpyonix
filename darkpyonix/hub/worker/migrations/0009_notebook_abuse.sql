-- FR-H19: operator takedown and reader reports.
ALTER TABLE notebooks ADD COLUMN taken_down INTEGER NOT NULL DEFAULT 0;
ALTER TABLE notebooks ADD COLUMN taken_down_at INTEGER;
-- No foreign key: a report outlives the notebook it names (the operator keeps the evidence).
-- ip_hash is the SHA-256 of the reporter's address, enough to count reports per address per hour.
CREATE TABLE notebook_reports (
  report_id INTEGER PRIMARY KEY AUTOINCREMENT,
  notebook_id TEXT NOT NULL,
  reporter_account TEXT,
  ip_hash TEXT NOT NULL,
  reason TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE INDEX notebook_reports_ip ON notebook_reports(ip_hash, created_at);
CREATE INDEX notebook_reports_notebook ON notebook_reports(notebook_id);
