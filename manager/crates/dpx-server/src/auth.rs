//! Manager tokens and permissions (SPEC FR-A2, FR-A3, FR-M4).
//!
//! The master token is random per manager process unless configured. Share tokens exist only
//! on a dedicated manager; they are stored as SHA-256 hashes in `<home>/manager.db` and shown
//! once, at creation.

use std::path::Path;
use std::sync::Mutex;

use rand::RngCore;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::util;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Permission {
    Viewer1 = 1,
    Viewer2 = 2,
    Viewer3 = 3,
    /// `viewer3` plus cell edits and locks (SPEC FR-S8).
    Editor = 4,
    Admin = 5,
}

impl Permission {
    pub fn as_str(self) -> &'static str {
        match self {
            Permission::Viewer1 => "viewer1",
            Permission::Viewer2 => "viewer2",
            Permission::Viewer3 => "viewer3",
            Permission::Editor => "editor",
            Permission::Admin => "admin",
        }
    }
    pub fn parse_share(s: &str) -> Option<Self> {
        match s {
            "viewer1" => Some(Permission::Viewer1),
            "viewer2" => Some(Permission::Viewer2),
            "viewer3" => Some(Permission::Viewer3),
            "editor" => Some(Permission::Editor),
            _ => None,
        }
    }
}

/// Who is calling: the master token (`admin`) or a share bound to one kernel.
#[derive(Debug, Clone)]
pub struct Principal {
    pub permission: Permission,
    pub kernel_id: Option<String>,
    pub share_id: Option<String>,
    /// The share's label (shown as the user name of its clients, FR-S4).
    pub label: Option<String>,
}

impl Principal {
    pub fn admin() -> Self {
        Self { permission: Permission::Admin, kernel_id: None, share_id: None, label: None }
    }
    pub fn at_least(&self, p: Permission) -> bool {
        self.permission >= p
    }
    pub fn can_see(&self, kernel_id: &str) -> bool {
        self.kernel_id.as_deref().is_none_or(|k| k == kernel_id)
    }
}

pub fn sha256_hex(s: &str) -> String {
    hex::encode(Sha256::digest(s.as_bytes()))
}

pub fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// A share as listed (`Share` schema). The token itself is never stored.
#[derive(Debug, Clone, Serialize)]
pub struct Share {
    pub share_id: String,
    pub kernel_id: String,
    pub permission: String,
    pub label: Option<String>,
    pub created_at: String,
    pub expires_at: Option<String>,
}

/// SQLite share store of a dedicated manager (`<home>/manager.db`).
pub struct ShareStore {
    conn: Mutex<Connection>,
}

impl ShareStore {
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let conn = Connection::open(path).map_err(std::io::Error::other)?;
        util::chmod_private(path);
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS shares (
                 share_id      TEXT PRIMARY KEY,
                 kernel_id     TEXT NOT NULL,
                 permission    TEXT NOT NULL,
                 label         TEXT,
                 created_at    TEXT NOT NULL,
                 expires_at    TEXT,
                 expires_unix  INTEGER,
                 token_sha256  TEXT NOT NULL UNIQUE
             );
             CREATE INDEX IF NOT EXISTS shares_kernel ON shares(kernel_id);",
        )
        .map_err(std::io::Error::other)?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn authenticate(&self, token: &str) -> Option<Principal> {
        let digest = sha256_hex(token);
        let row: Option<(String, String, String, Option<i64>, Option<String>)> = self
            .lock()
            .query_row(
                "SELECT share_id, kernel_id, permission, expires_unix, label FROM shares WHERE token_sha256 = ?1",
                params![digest],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()
            .ok()
            .flatten();
        let (share_id, kernel_id, permission, expires, label) = row?;
        if let Some(t) = expires {
            if t <= util::unix_now() {
                return None;
            }
        }
        Some(Principal {
            permission: Permission::parse_share(&permission)?,
            kernel_id: Some(kernel_id),
            share_id: Some(share_id),
            label,
        })
    }

    pub fn list(&self, kernel_id: &str) -> rusqlite::Result<Vec<Share>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT share_id, kernel_id, permission, label, created_at, expires_at
             FROM shares WHERE kernel_id = ?1 ORDER BY created_at, share_id",
        )?;
        let rows = stmt.query_map(params![kernel_id], |r| {
            Ok(Share {
                share_id: r.get(0)?,
                kernel_id: r.get(1)?,
                permission: r.get(2)?,
                label: r.get(3)?,
                created_at: r.get(4)?,
                expires_at: r.get(5)?,
            })
        })?;
        rows.collect()
    }

    /// Creates a share and returns it with its token (the only time the token is visible).
    pub fn create(
        &self,
        kernel_id: &str,
        permission: Permission,
        label: Option<String>,
        expires: Option<time::OffsetDateTime>,
    ) -> rusqlite::Result<(Share, String)> {
        let share = Share {
            share_id: format!("s_{}", random_hex(8)),
            kernel_id: kernel_id.to_string(),
            permission: permission.as_str().to_string(),
            label,
            created_at: util::now_iso(),
            expires_at: expires.map(util::iso),
        };
        let token = random_hex(32);
        self.lock().execute(
            "INSERT INTO shares (share_id, kernel_id, permission, label, created_at, expires_at,
                                 expires_unix, token_sha256)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                share.share_id,
                share.kernel_id,
                share.permission,
                share.label,
                share.created_at,
                share.expires_at,
                expires.map(|t| t.unix_timestamp()),
                sha256_hex(&token)
            ],
        )?;
        Ok((share, token))
    }

    pub fn revoke(&self, kernel_id: &str, share_id: &str) -> rusqlite::Result<bool> {
        let n = self.lock().execute(
            "DELETE FROM shares WHERE share_id = ?1 AND kernel_id = ?2",
            params![share_id, kernel_id],
        )?;
        Ok(n > 0)
    }
}

/// Token check for every request: master first (constant time), then shares.
pub struct Auth {
    master_sha256: [u8; 32],
    pub shares: Option<ShareStore>,
}

impl Auth {
    pub fn new(master_token: &str, shares: Option<ShareStore>) -> Self {
        Self { master_sha256: Sha256::digest(master_token.as_bytes()).into(), shares }
    }

    pub fn authenticate(&self, token: Option<&str>) -> Option<Principal> {
        let token = token.filter(|t| !t.is_empty())?;
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        if ct_eq(&digest, &self.master_sha256) {
            return Some(Principal::admin());
        }
        self.shares.as_ref()?.authenticate(token)
    }
}
