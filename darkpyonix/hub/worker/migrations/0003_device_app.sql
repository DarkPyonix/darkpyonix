-- FR-H10: what the device says it runs, as validated JSON ({kind, version, services}); NULL = unknown.
ALTER TABLE devices ADD COLUMN app TEXT;
