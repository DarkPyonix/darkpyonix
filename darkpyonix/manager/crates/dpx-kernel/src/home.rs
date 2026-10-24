//! The runtime home (`DARKPYONIX_HOME`, default `~/.darkpyonix`), PROTOCOL §1.
//! Mirrors `darkpyonix/kernel/_home.py`.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use rand::RngCore;
use sha2::{Digest, Sha256};

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

/// The OS account that PROTOCOL §2.2 hashes: `uid:<n>` (POSIX).
///
/// Windows needs the SID (`sid:S-1-5-...`) and its named pipe; that is a follow-up of #55.
pub fn account() -> io::Result<String> {
    #[cfg(unix)]
    {
        // SAFETY: getuid has no preconditions and cannot fail.
        Ok(format!("uid:{}", unsafe { libc::getuid() }))
    }
    #[cfg(not(unix))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the OS account (SID) is not implemented on this platform yet",
        ))
    }
}

/// `hex(SHA-256(account))[:16]` (PROTOCOL §2.2). No key file is read or created.
pub fn user_tag() -> io::Result<String> {
    Ok(hex::encode(Sha256::digest(account()?.as_bytes()))[..16].to_string())
}
