//! Manager tokens, password and permissions (SPEC FR-A2, FR-A3, FR-A4, FR-M4).
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

/// One of the five capabilities of SPEC FR-A3 (INTENT D18). They have no order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    Read,
    History,
    Execute,
    Edit,
    Manage,
}

impl Capability {
    pub const ALL: [Capability; 5] =
        [Capability::Read, Capability::History, Capability::Execute, Capability::Edit, Capability::Manage];

    pub fn as_str(self) -> &'static str {
        match self {
            Capability::Read => "read",
            Capability::History => "history",
            Capability::Execute => "execute",
            Capability::Edit => "edit",
            Capability::Manage => "manage",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.as_str() == s)
    }

    fn bit(self) -> u8 {
        1 << (self as u8)
    }
}

/// A set of capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Caps(u8);

impl Caps {
    pub fn all() -> Self {
        Caps(Capability::ALL.iter().fold(0, |acc, c| acc | c.bit()))
    }

    /// `None` when a name is unknown (tier names included) or the list is empty.
    pub fn from_names<S: AsRef<str>>(names: &[S]) -> Option<Self> {
        if names.is_empty() {
            return None;
        }
        let mut bits = 0u8;
        for n in names {
            bits |= Capability::parse(n.as_ref())?.bit();
        }
        Some(Caps(bits))
    }

    pub fn has(self, c: Capability) -> bool {
        self.0 & c.bit() != 0
    }

    /// Names in the canonical order of [`CAPABILITIES`].
    pub fn names(self) -> Vec<&'static str> {
        Capability::ALL.iter().filter(|c| self.has(**c)).map(|c| c.as_str()).collect()
    }

    pub fn joined(self, sep: &str) -> String {
        self.names().join(sep)
    }
}

/// The five capabilities of SPEC FR-A3, in their canonical order.
pub const CAPABILITIES: [&str; 5] = ["read", "history", "execute", "edit", "manage"];

/// `read+history` to `["read", "history"]`; `None` unless the names are known, unique and in
/// the canonical order of [`CAPABILITIES`] (SPEC FR-A4).
pub fn parse_token_type(s: &str) -> Option<Vec<String>> {
    let names: Vec<&str> = s.split('+').collect();
    let mut last: Option<usize> = None;
    for n in &names {
        let i = CAPABILITIES.iter().position(|c| c == n)?;
        if last.is_some_and(|l| i <= l) {
            return None;
        }
        last = Some(i);
    }
    Some(names.into_iter().map(String::from).collect())
}

/// Who is calling: the master token (all capabilities) or a token bound to one kernel.
#[derive(Debug, Clone)]
pub struct Principal {
    pub caps: Caps,
    pub kernel_id: Option<String>,
    pub share_id: Option<String>,
    /// The share's label (shown as the user name of its clients, FR-S4).
    pub label: Option<String>,
    /// The manager's master token (FR-A4).
    pub master: bool,
    /// A kernel's initial token (FR-A6): it may set the first manager password (FR-A4).
    pub initial: bool,
}

impl Principal {
    pub fn admin() -> Self {
        Self { caps: Caps::all(), kernel_id: None, share_id: None, label: None, master: true, initial: false }
    }
    pub fn has(&self, c: Capability) -> bool {
        self.caps.has(c)
    }
    pub fn can_see(&self, kernel_id: &str) -> bool {
        self.kernel_id.as_deref().is_none_or(|k| k == kernel_id)
    }
}

/// `read+history` as stored in `manager.db`; `None` unless every name is a capability.
fn parse_caps(s: &str) -> Option<Caps> {
    let names: Vec<&str> = s.split('+').collect();
    Caps::from_names(&names)
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

// ------------------------------------------------------------------ password (SPEC FR-A4)

const PBKDF2_ITERATIONS: u32 = 120_000;
const PASSWORD_SCHEME: &str = "pbkdf2-sha256";

struct HmacSha256 {
    inner: Sha256,
    outer: Sha256,
}

impl HmacSha256 {
    fn new(key: &[u8]) -> Self {
        let mut k = [0u8; 64];
        if key.len() > 64 {
            k[..32].copy_from_slice(&Sha256::digest(key));
        } else {
            k[..key.len()].copy_from_slice(key);
        }
        let mut ipad = [0x36u8; 64];
        let mut opad = [0x5cu8; 64];
        for ((ip, op), kb) in ipad.iter_mut().zip(opad.iter_mut()).zip(k.iter()) {
            *ip ^= kb;
            *op ^= kb;
        }
        let mut inner = Sha256::new();
        inner.update(ipad);
        let mut outer = Sha256::new();
        outer.update(opad);
        Self { inner, outer }
    }

    fn mac(&self, msg: &[u8]) -> [u8; 32] {
        let mut i = self.inner.clone();
        i.update(msg);
        let digest = i.finalize();
        let mut o = self.outer.clone();
        o.update(digest);
        o.finalize().into()
    }
}

/// PBKDF2-HMAC-SHA256 with one 32-byte output block (RFC 8018).
fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let prf = HmacSha256::new(password);
    let mut first = salt.to_vec();
    first.extend_from_slice(&1u32.to_be_bytes());
    let mut u = prf.mac(&first);
    let mut t = u;
    for _ in 1..iterations {
        u = prf.mac(&u);
        for (a, b) in t.iter_mut().zip(u.iter()) {
            *a ^= *b;
        }
    }
    t
}

/// `pbkdf2-sha256$<iterations>$<salt hex>$<hash hex>`: a salted slow hash (SPEC FR-A4).
pub fn hash_password(password: &str) -> String {
    let mut salt = [0u8; 16];
    rand::rng().fill_bytes(&mut salt);
    let dk = pbkdf2_sha256(password.as_bytes(), &salt, PBKDF2_ITERATIONS);
    format!("{PASSWORD_SCHEME}${PBKDF2_ITERATIONS}${}${}", hex::encode(salt), hex::encode(dk))
}

pub fn check_password(password: &str, stored: &str) -> bool {
    let parts: Vec<&str> = stored.split('$').collect();
    if parts.len() != 4 || parts[0] != PASSWORD_SCHEME {
        return false;
    }
    let (Ok(iterations), Ok(salt), Ok(want)) = (parts[1].parse::<u32>(), hex::decode(parts[2]), hex::decode(parts[3]))
    else {
        return false;
    };
    if iterations == 0 {
        return false;
    }
    ct_eq(&pbkdf2_sha256(password.as_bytes(), &salt, iterations), &want)
}

/// How a token looked up against this manager's own records.
pub enum Lookup {
    Found(Principal),
    /// A manager shared token that was reset (`401 token_revoked`).
    Revoked,
    Unknown,
}

/// Result of checking a password against the manager's.
#[derive(Debug, PartialEq, Eq)]
pub enum PasswordCheck {
    NotSet,
    Ok,
    Wrong,
}

/// A share as listed (`Share` schema). The token itself is never stored.
#[derive(Debug, Clone, Serialize)]
pub struct Share {
    pub share_id: String,
    pub kernel_id: String,
    pub capabilities: Vec<String>,
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
             CREATE INDEX IF NOT EXISTS shares_kernel ON shares(kernel_id);
             CREATE TABLE IF NOT EXISTS manager_auth (
                 key   TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS manager_shared (
                 share_id      TEXT PRIMARY KEY,
                 token_type    TEXT NOT NULL,
                 capabilities  TEXT NOT NULL,
                 token_sha256  TEXT NOT NULL UNIQUE,
                 created_at    TEXT NOT NULL,
                 revoked_at    TEXT
             );",
        )
        .map_err(std::io::Error::other)?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn authenticate(&self, token: &str) -> Option<Principal> {
        let digest = sha256_hex(token);
        type ShareRow = (String, String, String, Option<i64>, Option<String>);
        let row: Option<ShareRow> = self
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
            caps: parse_caps(&permission)?,
            kernel_id: Some(kernel_id),
            share_id: Some(share_id),
            label,
            master: false,
            initial: false,
        })
    }

    fn auth_get(&self, key: &str) -> Option<String> {
        self.lock()
            .query_row("SELECT value FROM manager_auth WHERE key = ?1", params![key], |r| r.get(0))
            .optional()
            .ok()
            .flatten()
    }

    fn auth_put(&self, key: &str, value: &str) -> rusqlite::Result<()> {
        self.lock().execute(
            "INSERT INTO manager_auth (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    /// A manager shared token (FR-A4): found, reset (revoked) or unknown.
    pub fn shared_lookup(&self, token: &str) -> Lookup {
        let digest = sha256_hex(token);
        let row: Option<(String, String, Option<String>)> = self
            .lock()
            .query_row(
                "SELECT share_id, capabilities, revoked_at FROM manager_shared WHERE token_sha256 = ?1",
                params![digest],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .ok()
            .flatten();
        let Some((share_id, caps, revoked)) = row else { return Lookup::Unknown };
        if revoked.is_some() {
            return Lookup::Revoked;
        }
        let Some(caps) = parse_caps(&caps) else { return Lookup::Unknown };
        Lookup::Found(Principal {
            caps,
            kernel_id: None,
            share_id: Some(share_id),
            label: None,
            master: false,
            initial: false,
        })
    }

    /// Replaces the manager shared token of one type: the old one is marked revoked, a new one is
    /// stored hashed. Returns `(share_id, token, created_at)`.
    /// Share ids of the active shared token of `token_type` (before a reset revokes it).
    pub fn active_shared_ids(&self, token_type: &str) -> rusqlite::Result<Vec<String>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT share_id FROM manager_shared WHERE token_type = ?1 AND revoked_at IS NULL",
        )?;
        let rows = stmt.query_map(params![token_type], |r| r.get::<_, String>(0))?;
        rows.collect()
    }

    pub fn reset_shared(&self, token_type: &str) -> rusqlite::Result<(String, String, String)> {
        let share_id = format!("s_{}", random_hex(8));
        let token = random_hex(32);
        let created = util::now_iso();
        let conn = self.lock();
        conn.execute(
            "UPDATE manager_shared SET revoked_at = ?2 WHERE token_type = ?1 AND revoked_at IS NULL",
            params![token_type, created],
        )?;
        conn.execute(
            "INSERT INTO manager_shared (share_id, token_type, capabilities, token_sha256, created_at)
             VALUES (?1, ?2, ?2, ?3, ?4)",
            params![share_id, token_type, sha256_hex(&token), created],
        )?;
        Ok((share_id, token, created))
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
                capabilities: r.get::<_, String>(2)?.split('+').map(String::from).collect(),
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
        caps: Caps,
        label: Option<String>,
        expires: Option<time::OffsetDateTime>,
    ) -> rusqlite::Result<(Share, String)> {
        let share = Share {
            share_id: format!("s_{}", random_hex(8)),
            kernel_id: kernel_id.to_string(),
            capabilities: caps.names().into_iter().map(String::from).collect(),
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
                caps.joined("+"),
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

/// The master token: its hash, and its value while this process knows it.
struct Master {
    sha256: [u8; 32],
    plain: Option<String>,
}

/// Token check for every request: master first (constant time), then manager shared tokens,
/// then per-kernel shares. Also holds the manager's one password (SPEC FR-A4).
pub struct Auth {
    master: Mutex<Master>,
    pub shares: Option<ShareStore>,
}

impl Auth {
    /// A master token reset through the API is kept as a hash in `manager.db` and wins over
    /// `master_token` after a restart (its value is then unknown until the next reset).
    pub fn new(master_token: &str, shares: Option<ShareStore>) -> Self {
        let stored = shares
            .as_ref()
            .and_then(|s| s.auth_get("master_sha256"))
            .and_then(|h| hex::decode(h).ok())
            .and_then(|b| <[u8; 32]>::try_from(b).ok());
        let master = match stored {
            Some(sha256) => Master { sha256, plain: None },
            None => Master { sha256: Sha256::digest(master_token.as_bytes()).into(), plain: Some(master_token.to_string()) },
        };
        Self { master: Mutex::new(master), shares }
    }

    fn master(&self) -> std::sync::MutexGuard<'_, Master> {
        self.master.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn lookup(&self, token: Option<&str>) -> Lookup {
        let Some(token) = token.filter(|t| !t.is_empty()) else { return Lookup::Unknown };
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        if ct_eq(&digest, &self.master().sha256) {
            return Lookup::Found(Principal::admin());
        }
        let Some(shares) = self.shares.as_ref() else { return Lookup::Unknown };
        match shares.shared_lookup(token) {
            Lookup::Unknown => match shares.authenticate(token) {
                Some(p) => Lookup::Found(p),
                None => Lookup::Unknown,
            },
            other => other,
        }
    }

    pub fn password_set(&self) -> bool {
        self.shares.as_ref().is_some_and(|s| s.auth_get("password").is_some())
    }

    /// Slow (PBKDF2): call from `spawn_blocking`.
    pub fn check_password(&self, password: &str) -> PasswordCheck {
        match self.shares.as_ref().and_then(|s| s.auth_get("password")) {
            None => PasswordCheck::NotSet,
            Some(stored) if check_password(password, &stored) => PasswordCheck::Ok,
            Some(_) => PasswordCheck::Wrong,
        }
    }

    /// Slow (PBKDF2): call from `spawn_blocking`.
    pub fn set_password(&self, password: &str) -> std::io::Result<()> {
        let shares = self.shares.as_ref().ok_or_else(|| std::io::Error::other("no manager.db"))?;
        shares.auth_put("password", &hash_password(password)).map_err(std::io::Error::other)
    }

    /// The master token for `GET /api/auth`. When only its hash is known (after a restart that
    /// followed a reset), a new master token is issued, because a hash cannot be shown.
    pub fn login_master(&self) -> std::io::Result<String> {
        if let Some(p) = self.master().plain.clone() {
            return Ok(p);
        }
        self.rotate_master()
    }

    /// Issues a new master token; the old one stops working.
    pub fn rotate_master(&self) -> std::io::Result<String> {
        let shares = self.shares.as_ref().ok_or_else(|| std::io::Error::other("no manager.db"))?;
        let token = random_hex(32);
        let sha256: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        shares.auth_put("master_sha256", &hex::encode(sha256)).map_err(std::io::Error::other)?;
        let mut m = self.master();
        m.sha256 = sha256;
        m.plain = Some(token.clone());
        Ok(token)
    }
}
