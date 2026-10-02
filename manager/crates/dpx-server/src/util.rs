//! Small helpers: time stamps, id patterns, private files.

use std::io::Write;
use std::path::Path;

use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

pub fn iso(t: OffsetDateTime) -> String {
    let t = t.to_offset(time::UtcOffset::UTC).replace_nanosecond(0).unwrap_or(t);
    t.format(&Rfc3339).unwrap_or_default()
}

pub fn now_iso() -> String {
    iso(OffsetDateTime::now_utc())
}

pub fn unix_now() -> i64 {
    OffsetDateTime::now_utc().unix_timestamp()
}

pub fn parse_datetime(s: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(s, &Rfc3339).ok()
}

fn hex_lower(s: &str) -> bool {
    s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// `^k_[0-9a-f]{20}$`
pub fn is_kernel_id(s: &str) -> bool {
    s.len() == 22 && s.starts_with("k_") && hex_lower(&s[2..])
}

/// `^s_[0-9a-f]{16}$`
pub fn is_share_id(s: &str) -> bool {
    s.len() == 18 && s.starts_with("s_") && hex_lower(&s[2..])
}

/// `^[0-9]{8}-[0-9]{6}-[0-9a-f]{4}$`
pub fn is_run_id(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 20
        && b[..8].iter().all(u8::is_ascii_digit)
        && b[8] == b'-'
        && b[9..15].iter().all(u8::is_ascii_digit)
        && b[15] == b'-'
        && hex_lower(&s[16..])
}

#[cfg(unix)]
pub fn chmod_private(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
pub fn chmod_private(_path: &Path) {}

/// Creates `dir` (0700 on Unix) if missing.
pub fn ensure_private_dir(dir: &Path) -> std::io::Result<()> {
    if dir.is_dir() {
        return Ok(());
    }
    let mut b = std::fs::DirBuilder::new();
    b.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        b.mode(0o700);
    }
    b.create(dir)
}

/// Writes `data` to `path` atomically (temp file + rename), mode 0600 from the start.
pub fn write_private_atomic(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    ensure_private_dir(dir)?;
    let tmp = dir.join(format!(
        ".{}.{}.tmp",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("file"),
        std::process::id()
    ));
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    fn write_then_rename(opts: &std::fs::OpenOptions, tmp: &Path, path: &Path, data: &[u8]) -> std::io::Result<()> {
        let mut f = opts.open(tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(tmp, path)
    }
    let result = write_then_rename(&opts, &tmp, path, data);
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// `<file dir>/__runs__/<file name>` (OpenAPI `Kernel.runs_dir`).
pub fn runs_dir_for(path: &str) -> String {
    let p = Path::new(path);
    let name = p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    p.parent()
        .unwrap_or(Path::new(""))
        .join("__runs__")
        .join(name)
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_patterns() {
        assert!(is_kernel_id("k_3f9a0c1b2d4e5f607182"));
        assert!(!is_kernel_id("k_3f9a0c1b2d4e5f60718"));
        assert!(!is_kernel_id("k_3F9a0c1b2d4e5f607182"));
        assert!(is_share_id("s_0123456789abcdef"));
        assert!(!is_share_id("s_0123456789abcdeg"));
        assert!(is_run_id("20261003-142233-a1f0"));
        assert!(!is_run_id("20261003-142233-a1fg"));
        assert!(!is_run_id("latest"));
    }

    #[test]
    fn runs_dir() {
        assert_eq!(runs_dir_for("/a/b/train.py"), "/a/b/__runs__/train.py");
    }
}
