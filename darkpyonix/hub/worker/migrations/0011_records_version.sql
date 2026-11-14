-- FR-H2 (#132): bumped with every accepted PUT /pkarr of the account's devices;
-- GET /pkarr/watch re-reads this one row while it waits.
ALTER TABLE accounts ADD COLUMN records_version INTEGER NOT NULL DEFAULT 0;
