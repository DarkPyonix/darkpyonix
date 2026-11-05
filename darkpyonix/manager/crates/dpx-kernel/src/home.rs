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

/// The OS account that PROTOCOL §2.2 hashes: `uid:<n>` (POSIX) or `sid:<SID>` (Windows).
pub fn account() -> io::Result<String> {
    #[cfg(unix)]
    {
        // SAFETY: getuid has no preconditions and cannot fail.
        Ok(format!("uid:{}", unsafe { libc::getuid() }))
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt;
        use std::os::windows::io::{FromRawHandle, OwnedHandle};
        use std::ptr::null_mut;
        use windows_sys::Win32::Foundation::LocalFree;
        use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
        use windows_sys::Win32::Security::{
            GetTokenInformation, TokenUser, TOKEN_QUERY, TOKEN_USER,
        };
        use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

        let mut token = null_mut();
        // SAFETY: The pseudo-handle is valid and token points to writable handle storage.
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: OpenProcessToken returned an owned, valid handle.
        let token = unsafe { OwnedHandle::from_raw_handle(token) };
        use std::os::windows::io::AsRawHandle;

        let mut size = 0u32;
        // SAFETY: The null buffer requests the required size, written to size.
        unsafe { GetTokenInformation(token.as_raw_handle(), TokenUser, null_mut(), 0, &mut size) };
        if size == 0 {
            return Err(io::Error::last_os_error());
        }
        // Use pointer-aligned storage for TOKEN_USER and the SID it references.
        let mut buffer = vec![
            0usize;
            (size as usize + std::mem::size_of::<usize>() - 1)
                / std::mem::size_of::<usize>()
        ];
        // SAFETY: The buffer is aligned and has at least size bytes; token is valid.
        if unsafe {
            GetTokenInformation(
                token.as_raw_handle(),
                TokenUser,
                buffer.as_mut_ptr().cast(),
                size,
                &mut size,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: A successful TokenUser query initialized a TOKEN_USER in the buffer.
        let user = unsafe { &*(buffer.as_ptr().cast::<TOKEN_USER>()) };
        let mut sid = null_mut();
        // SAFETY: The SID belongs to the live token information buffer and sid is writable.
        if unsafe { ConvertSidToStringSidW(user.User.Sid, &mut sid) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut len = 0;
        // SAFETY: ConvertSidToStringSidW returned a null-terminated UTF-16 string.
        while unsafe { *sid.add(len) } != 0 {
            len += 1;
        }
        // SAFETY: The allocation contains len UTF-16 units before its terminator.
        let text = std::ffi::OsString::from_wide(unsafe { std::slice::from_raw_parts(sid, len) });
        // SAFETY: The string was allocated by ConvertSidToStringSidW and is no longer used.
        unsafe { LocalFree(sid.cast()) };
        Ok(format!("sid:{}", text.to_string_lossy()))
    }
    #[cfg(not(any(unix, windows)))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the OS account (SID) is not implemented on this platform yet",
        ))
    }
}

#[cfg(all(test, windows))]
mod tests {
    #[test]
    fn test_fr_a1_windows_account_is_a_sid_string() {
        assert!(super::account().unwrap().starts_with("sid:S-1-5-"));
    }
}

/// `hex(SHA-256(account))[:16]` (PROTOCOL §2.2). No key file is read or created.
pub fn user_tag() -> io::Result<String> {
    Ok(hex::encode(Sha256::digest(account()?.as_bytes()))[..16].to_string())
}
