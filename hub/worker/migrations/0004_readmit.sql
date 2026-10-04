-- FR-H11: until when the account owner lets a removed key start a device link again (unix
-- seconds); cleared when the key is restored. NULL = not re-admitted.
ALTER TABLE devices ADD COLUMN readmit_until INTEGER;
