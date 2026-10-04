-- FR-H1 (Ember INTENT D3): an account has at most one active main server.
--
-- Accounts that already have several keep the oldest; the others are removed the way a
-- replacement removes them: revoked, their names moved to the kept one, their shares and
-- address records dropped. Rows are only ever compared with the kept (never updated) row,
-- so the order SQLite visits them in does not matter.
UPDATE devices SET revoked_at = CAST(strftime('%s', 'now') AS INTEGER), online = 0
WHERE role = 'main_server' AND revoked_at IS NULL AND EXISTS (
    SELECT 1 FROM devices AS kept
    WHERE kept.account_id = devices.account_id AND kept.role = 'main_server' AND kept.revoked_at IS NULL
      AND (kept.created_at < devices.created_at
           OR (kept.created_at = devices.created_at AND kept.endpoint_id < devices.endpoint_id))
);
UPDATE names SET endpoint_id = (
    SELECT kept.endpoint_id FROM devices AS kept
    WHERE kept.account_id = names.account_id AND kept.role = 'main_server' AND kept.revoked_at IS NULL
)
WHERE endpoint_id IN (SELECT endpoint_id FROM devices WHERE revoked_at IS NOT NULL) AND EXISTS (
    SELECT 1 FROM devices AS kept
    WHERE kept.account_id = names.account_id AND kept.role = 'main_server' AND kept.revoked_at IS NULL
);
DELETE FROM shares WHERE endpoint_id IN (SELECT endpoint_id FROM devices WHERE revoked_at IS NOT NULL);
DELETE FROM records WHERE endpoint_id IN (SELECT endpoint_id FROM devices WHERE revoked_at IS NOT NULL);
-- Device lists may have changed (FR-H9); waking every waiter once is harmless.
UPDATE accounts SET devices_version = devices_version + 1;

CREATE UNIQUE INDEX one_main_server_per_account ON devices(account_id)
    WHERE role = 'main_server' AND revoked_at IS NULL;

-- The current main server a main_server link was approved to replace (FR-H1); NULL = none.
ALTER TABLE device_links ADD COLUMN replace_endpoint_id TEXT;
