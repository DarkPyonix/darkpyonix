-- FR-H9: bumped on every change visible in the account's device list (not last_seen alone);
-- GET /v1/devices answers with ETag W/"v<devices_version>".
ALTER TABLE accounts ADD COLUMN devices_version INTEGER NOT NULL DEFAULT 0;
