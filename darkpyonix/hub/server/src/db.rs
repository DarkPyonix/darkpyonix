//! SQLite storage. Every function locks the connection for one short, synchronous
//! operation and returns owned values, so no lock is ever held across an `.await`.

use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
};

use rusqlite::{params, Connection, OptionalExtension};

const SCHEMA: &str = "
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS accounts (
    account_id  TEXT PRIMARY KEY,
    token_hash  TEXT NOT NULL UNIQUE,
    created_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS challenges (
    challenge   TEXT PRIMARY KEY,
    account_id  TEXT NOT NULL REFERENCES accounts(account_id),
    expires_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS devices (
    endpoint_id TEXT PRIMARY KEY,
    account_id  TEXT NOT NULL REFERENCES accounts(account_id),
    name        TEXT NOT NULL,
    role        TEXT NOT NULL,
    token_hash  TEXT NOT NULL UNIQUE,
    created_at  INTEGER NOT NULL,
    last_seen   INTEGER,
    revoked_at  INTEGER
);
CREATE TABLE IF NOT EXISTS records (
    endpoint_id  TEXT PRIMARY KEY REFERENCES devices(endpoint_id),
    packet       BLOB NOT NULL,
    timestamp_us INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS shares (
    share_id    TEXT PRIMARY KEY,
    endpoint_id TEXT NOT NULL REFERENCES devices(endpoint_id),
    created_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS relay_passes (
    token_hash  TEXT PRIMARY KEY,
    share_id    TEXT NOT NULL,
    expires_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS names (
    name        TEXT PRIMARY KEY,
    endpoint_id TEXT NOT NULL REFERENCES devices(endpoint_id),
    account_id  TEXT NOT NULL REFERENCES accounts(account_id),
    created_at  INTEGER NOT NULL
);
";

pub(crate) type DbResult<T> = Result<T, rusqlite::Error>;

pub(crate) struct Db(Mutex<Connection>);

/// A device row.
#[derive(Debug, Clone)]
pub(crate) struct DeviceRow {
    pub endpoint_id: String,
    pub account_id: String,
    pub name: String,
    pub role: String,
    pub created_at: i64,
    pub last_seen: Option<i64>,
    pub revoked: bool,
}

/// A name row.
#[derive(Debug, Clone)]
pub(crate) struct NameRow {
    pub name: String,
    pub endpoint_id: String,
    pub account_id: String,
}

/// Who presented a token.
#[derive(Debug, Clone)]
pub(crate) enum TokenOwner {
    Account {
        account_id: String,
    },
    Device {
        account_id: String,
        endpoint_id: String,
        role: String,
    },
}

/// Outcome of inserting a share or a name that may already exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Claim {
    Created,
    AlreadyYours,
    Taken,
}

fn device_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeviceRow> {
    let revoked_at: Option<i64> = row.get(6)?;
    Ok(DeviceRow {
        endpoint_id: row.get(0)?,
        account_id: row.get(1)?,
        name: row.get(2)?,
        role: row.get(3)?,
        created_at: row.get(4)?,
        last_seen: row.get(5)?,
        revoked: revoked_at.is_some(),
    })
}

const DEVICE_COLUMNS: &str =
    "endpoint_id, account_id, name, role, created_at, last_seen, revoked_at";

impl Db {
    pub(crate) fn open(path: &Path) -> DbResult<Self> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self(Mutex::new(conn)))
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    // ---- accounts and tokens ----

    pub(crate) fn insert_account(
        &self,
        account_id: &str,
        token_hash: &str,
        now: i64,
    ) -> DbResult<()> {
        self.conn().execute(
            "INSERT INTO accounts (account_id, token_hash, created_at) VALUES (?1, ?2, ?3)",
            params![account_id, token_hash, now],
        )?;
        Ok(())
    }

    pub(crate) fn token_owner(&self, token_hash: &str) -> DbResult<Option<TokenOwner>> {
        let conn = self.conn();
        let account: Option<String> = conn
            .query_row(
                "SELECT account_id FROM accounts WHERE token_hash = ?1",
                params![token_hash],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(account_id) = account {
            return Ok(Some(TokenOwner::Account { account_id }));
        }
        conn.query_row(
            "SELECT account_id, endpoint_id, role FROM devices
             WHERE token_hash = ?1 AND revoked_at IS NULL",
            params![token_hash],
            |row| {
                Ok(TokenOwner::Device {
                    account_id: row.get(0)?,
                    endpoint_id: row.get(1)?,
                    role: row.get(2)?,
                })
            },
        )
        .optional()
    }

    // ---- challenges ----

    pub(crate) fn insert_challenge(
        &self,
        challenge: &str,
        account_id: &str,
        expires_at: i64,
        now: i64,
    ) -> DbResult<()> {
        let conn = self.conn();
        conn.execute("DELETE FROM challenges WHERE expires_at < ?1", params![now])?;
        conn.execute(
            "INSERT INTO challenges (challenge, account_id, expires_at) VALUES (?1, ?2, ?3)",
            params![challenge, account_id, expires_at],
        )?;
        Ok(())
    }

    /// Removes the challenge and returns its expiry if it belonged to `account_id`.
    pub(crate) fn take_challenge(
        &self,
        challenge: &str,
        account_id: &str,
    ) -> DbResult<Option<i64>> {
        let conn = self.conn();
        let found: Option<(String, i64)> = conn
            .query_row(
                "SELECT account_id, expires_at FROM challenges WHERE challenge = ?1",
                params![challenge],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        conn.execute(
            "DELETE FROM challenges WHERE challenge = ?1",
            params![challenge],
        )?;
        Ok(found.and_then(|(owner, expires_at)| (owner == account_id).then_some(expires_at)))
    }

    // ---- devices ----

    pub(crate) fn device_exists_any_state(&self, endpoint_id: &str) -> DbResult<bool> {
        let n: i64 = self.conn().query_row(
            "SELECT COUNT(*) FROM devices WHERE endpoint_id = ?1",
            params![endpoint_id],
            |row| row.get(0),
        )?;
        Ok(n > 0)
    }

    pub(crate) fn insert_device(&self, device: &DeviceRow, token_hash: &str) -> DbResult<()> {
        self.conn().execute(
            "INSERT INTO devices (endpoint_id, account_id, name, role, token_hash, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                device.endpoint_id,
                device.account_id,
                device.name,
                device.role,
                token_hash,
                device.created_at
            ],
        )?;
        Ok(())
    }

    pub(crate) fn device(&self, endpoint_id: &str) -> DbResult<Option<DeviceRow>> {
        self.conn()
            .query_row(
                &format!("SELECT {DEVICE_COLUMNS} FROM devices WHERE endpoint_id = ?1"),
                params![endpoint_id],
                device_from_row,
            )
            .optional()
    }

    /// The device if it is active and belongs to `account_id`.
    pub(crate) fn account_device(
        &self,
        account_id: &str,
        endpoint_id: &str,
    ) -> DbResult<Option<DeviceRow>> {
        Ok(self
            .device(endpoint_id)?
            .filter(|d| !d.revoked && d.account_id == account_id))
    }

    pub(crate) fn device_active(&self, endpoint_id: &str) -> DbResult<bool> {
        Ok(self.device(endpoint_id)?.is_some_and(|d| !d.revoked))
    }

    pub(crate) fn list_devices(&self, account_id: &str) -> DbResult<Vec<DeviceRow>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(&format!(
            "SELECT {DEVICE_COLUMNS} FROM devices
             WHERE account_id = ?1 AND revoked_at IS NULL ORDER BY rowid"
        ))?;
        let rows = stmt.query_map(params![account_id], device_from_row)?;
        rows.collect()
    }

    /// Revokes the device and drops what it published. Returns false if it was not an
    /// active device of the account.
    pub(crate) fn revoke_device(
        &self,
        account_id: &str,
        endpoint_id: &str,
        now: i64,
    ) -> DbResult<bool> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let changed = tx.execute(
            "UPDATE devices SET revoked_at = ?3
             WHERE endpoint_id = ?1 AND account_id = ?2 AND revoked_at IS NULL",
            params![endpoint_id, account_id, now],
        )?;
        if changed > 0 {
            tx.execute(
                "DELETE FROM records WHERE endpoint_id = ?1",
                params![endpoint_id],
            )?;
            tx.execute(
                "DELETE FROM relay_passes WHERE share_id IN
                 (SELECT share_id FROM shares WHERE endpoint_id = ?1)",
                params![endpoint_id],
            )?;
            tx.execute(
                "DELETE FROM shares WHERE endpoint_id = ?1",
                params![endpoint_id],
            )?;
        }
        tx.commit()?;
        Ok(changed > 0)
    }

    pub(crate) fn touch_device(&self, endpoint_id: &str, now: i64) -> DbResult<()> {
        self.conn().execute(
            "UPDATE devices SET last_seen = ?2 WHERE endpoint_id = ?1",
            params![endpoint_id, now],
        )?;
        Ok(())
    }

    // ---- address records ----

    pub(crate) fn record_timestamp(&self, endpoint_id: &str) -> DbResult<Option<i64>> {
        self.conn()
            .query_row(
                "SELECT timestamp_us FROM records WHERE endpoint_id = ?1",
                params![endpoint_id],
                |row| row.get(0),
            )
            .optional()
    }

    pub(crate) fn put_record(
        &self,
        endpoint_id: &str,
        packet: &[u8],
        timestamp_us: i64,
    ) -> DbResult<()> {
        self.conn().execute(
            "INSERT INTO records (endpoint_id, packet, timestamp_us) VALUES (?1, ?2, ?3)
             ON CONFLICT(endpoint_id) DO UPDATE SET packet = ?2, timestamp_us = ?3",
            params![endpoint_id, packet, timestamp_us],
        )?;
        Ok(())
    }

    /// The full signed packet bytes (public key, signature, timestamp, DNS packet).
    pub(crate) fn record(&self, endpoint_id: &str) -> DbResult<Option<Vec<u8>>> {
        self.conn()
            .query_row(
                "SELECT packet FROM records WHERE endpoint_id = ?1",
                params![endpoint_id],
                |row| row.get(0),
            )
            .optional()
    }

    // ---- shares and guest relay passes ----

    pub(crate) fn claim_share(
        &self,
        share_id: &str,
        endpoint_id: &str,
        now: i64,
    ) -> DbResult<Claim> {
        let conn = self.conn();
        let owner: Option<String> = conn
            .query_row(
                "SELECT endpoint_id FROM shares WHERE share_id = ?1",
                params![share_id],
                |row| row.get(0),
            )
            .optional()?;
        match owner {
            Some(owner) if owner == endpoint_id => Ok(Claim::AlreadyYours),
            Some(_) => Ok(Claim::Taken),
            None => {
                conn.execute(
                    "INSERT INTO shares (share_id, endpoint_id, created_at) VALUES (?1, ?2, ?3)",
                    params![share_id, endpoint_id, now],
                )?;
                Ok(Claim::Created)
            }
        }
    }

    /// The hosting device of a share, if the share exists and its device is active.
    pub(crate) fn share_host(&self, share_id: &str) -> DbResult<Option<DeviceRow>> {
        let host: Option<String> = self
            .conn()
            .query_row(
                "SELECT endpoint_id FROM shares WHERE share_id = ?1",
                params![share_id],
                |row| row.get(0),
            )
            .optional()?;
        match host {
            Some(endpoint_id) => Ok(self.device(&endpoint_id)?.filter(|d| !d.revoked)),
            None => Ok(None),
        }
    }

    pub(crate) fn delete_share(&self, share_id: &str) -> DbResult<()> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM relay_passes WHERE share_id = ?1",
            params![share_id],
        )?;
        conn.execute("DELETE FROM shares WHERE share_id = ?1", params![share_id])?;
        Ok(())
    }

    pub(crate) fn insert_pass(
        &self,
        token_hash: &str,
        share_id: &str,
        expires_at: i64,
        now: i64,
    ) -> DbResult<()> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM relay_passes WHERE expires_at < ?1",
            params![now],
        )?;
        conn.execute(
            "INSERT INTO relay_passes (token_hash, share_id, expires_at) VALUES (?1, ?2, ?3)",
            params![token_hash, share_id, expires_at],
        )?;
        Ok(())
    }

    pub(crate) fn pass_valid(&self, token_hash: &str, now: i64) -> DbResult<bool> {
        let n: i64 = self.conn().query_row(
            "SELECT COUNT(*) FROM relay_passes WHERE token_hash = ?1 AND expires_at >= ?2",
            params![token_hash, now],
            |row| row.get(0),
        )?;
        Ok(n > 0)
    }

    // ---- names ----

    pub(crate) fn claim_name(
        &self,
        name: &str,
        endpoint_id: &str,
        account_id: &str,
        now: i64,
    ) -> DbResult<Claim> {
        let conn = self.conn();
        let owner: Option<String> = conn
            .query_row(
                "SELECT endpoint_id FROM names WHERE name = ?1",
                params![name],
                |row| row.get(0),
            )
            .optional()?;
        match owner {
            Some(owner) if owner == endpoint_id => Ok(Claim::AlreadyYours),
            Some(_) => Ok(Claim::Taken),
            None => {
                conn.execute(
                    "INSERT INTO names (name, endpoint_id, account_id, created_at)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![name, endpoint_id, account_id, now],
                )?;
                Ok(Claim::Created)
            }
        }
    }

    pub(crate) fn name(&self, name: &str) -> DbResult<Option<NameRow>> {
        self.conn()
            .query_row(
                "SELECT name, endpoint_id, account_id FROM names WHERE name = ?1",
                params![name],
                |row| {
                    Ok(NameRow {
                        name: row.get(0)?,
                        endpoint_id: row.get(1)?,
                        account_id: row.get(2)?,
                    })
                },
            )
            .optional()
    }

    pub(crate) fn list_names(&self, account_id: &str) -> DbResult<Vec<NameRow>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT name, endpoint_id, account_id FROM names WHERE account_id = ?1 ORDER BY name",
        )?;
        let rows = stmt.query_map(params![account_id], |row| {
            Ok(NameRow {
                name: row.get(0)?,
                endpoint_id: row.get(1)?,
                account_id: row.get(2)?,
            })
        })?;
        rows.collect()
    }

    pub(crate) fn delete_name(&self, name: &str) -> DbResult<()> {
        self.conn()
            .execute("DELETE FROM names WHERE name = ?1", params![name])?;
        Ok(())
    }

    /// Names held by a device (to release when it is removed).
    pub(crate) fn names_of_device(&self, endpoint_id: &str) -> DbResult<Vec<String>> {
        let conn = self.conn();
        let mut stmt = conn.prepare("SELECT name FROM names WHERE endpoint_id = ?1")?;
        let rows = stmt.query_map(params![endpoint_id], |row| row.get(0))?;
        rows.collect()
    }
}
