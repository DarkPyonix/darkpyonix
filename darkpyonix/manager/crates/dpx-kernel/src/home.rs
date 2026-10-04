//! The runtime home (`DARKPYONIX_HOME`, default `~/.darkpyonix`), PROTOCOL §1.
//! Mirrors `darkpyonix/kernel/_home.py`.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use rand::RngCore;
use sha2::{Digest, Sha256};

pub const USER_KEY_BYTES: usize = 32;

/// `DARKPYONIX_HOME` or `~/.darkpyonix`, made absolute.
pub fn default_home() -> PathBuf {
    let raw = match std::env::var_os("DARKPYONIX_HOME") {
        Some(v) if !v.is_empty() => PathBuf::from(v),
        _ => home_dir().join(".darkpyonix"),
    };
    crate::ident::abspath(&raw)
}

fn home_dir() -> PathBuf {
    #[cfg(windows)]
    let var = std::env::var_os("USERPROFILE");
    #[cfg(not(windows))]
    let var = std::env::var_os("HOME");
    var.map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."))
}

/// `<home>/<name>`, created with mode 0700 (and the home) when missing.
pub fn subdir(home: &Path, name: &str) -> io::Result<PathBuf> {
    let path = home.join(name);
    create_dir_private(&path)?;
    Ok(path)
}

pub fn create_dir_private(path: &Path) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)
}

pub fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::thread_rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

fn create_new_private(path: &Path) -> io::Result<fs::File> {
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)
}

/// Read `<home>/user.key`, creating it atomically and race-safely on first use (FR-A1):
/// write a private temp file, then `link()` it into place; the loser of a race uses the
/// winner's key.
pub fn user_key(home: &Path) -> io::Result<Vec<u8>> {
    let path = home.join("user.key");
    match fs::read(&path) {
        Ok(key) if key.len() == USER_KEY_BYTES => return Ok(key),
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    create_dir_private(home)?;
    let mut candidate = vec![0u8; USER_KEY_BYTES];
    rand::thread_rng().fill_bytes(&mut candidate);
    let tmp = home.join(format!("user.key.{}.tmp", random_hex(4)));
    {
        let mut f = create_new_private(&tmp)?;
        f.write_all(&candidate)?;
        f.sync_all()?;
    }
    let linked = fs::hard_link(&tmp, &path);
    let _ = fs::remove_file(&tmp);
    match linked {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    fs::read(&path)
}

/// `hex(SHA-256(user.key))[:16]` (PROTOCOL §2.2).
pub fn user_tag(key: &[u8]) -> String {
    hex::encode(Sha256::digest(key))[..16].to_string()
}
