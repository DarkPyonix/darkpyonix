-- NFR-H2: a read-only resolve token per device, for iroh's PkarrResolver query string.
-- Stored as SHA-256 hex like every other token; NULL until issued.
ALTER TABLE devices ADD COLUMN resolve_token_hash TEXT;
CREATE UNIQUE INDEX devices_by_resolve_token ON devices(resolve_token_hash);
