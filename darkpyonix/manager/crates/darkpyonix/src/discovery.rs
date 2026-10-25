//! Finding a manager (SPEC FR-C1) and naming a file's kernel (PROTOCOL §2.6).

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::client::{Api, CliError};

/// Overrides the executable spawned as `<exe> manager --ephemeral` (tests, wrappers).
pub const SELF_EXE_ENV: &str = "DARKPYONIX_SELF_EXE";
/// How long a freshly spawned manager has to register itself and answer `/health`.
pub const SPAWN_TIMEOUT: Duration = Duration::from_secs(5);

/// A manager given by the environment (an agent that already knows its token).
pub const URL_ENV: &str = "DARKPYONIX_MANAGER_URL";
pub const TOKEN_ENV: &str = "DARKPYONIX_TOKEN";

/// The one JSON line a manager prints on stdout once it listens (SPEC FR-M3, FR-C1).
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ManagerLine {
    pub url: String,
    pub token: String,
    #[serde(default)]
    pub pid: u32,
}

/// `DARKPYONIX_HOME`, else `~/.darkpyonix`.
pub fn home() -> PathBuf {
    if let Some(h) = std::env::var_os("DARKPYONIX_HOME").filter(|h| !h.is_empty()) {
        return PathBuf::from(h);
    }
    let base = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")).unwrap_or_default();
    PathBuf::from(base).join(".darkpyonix")
}

async fn healthy(http: &reqwest::Client, url: &str) -> bool {
    let url = format!("{}/health", url.trim_end_matches('/'));
    matches!(
        tokio::time::timeout(Duration::from_secs(2), http.get(url).send()).await,
        Ok(Ok(r)) if r.status().is_success()
    )
}

/// The manager named by `DARKPYONIX_MANAGER_URL` and `DARKPYONIX_TOKEN`, else a freshly
/// spawned ephemeral one whose address and token come from its stdout (FR-C1). No file is read.
/// With `dedicated_only`, never spawns: sharing needs a dedicated manager the caller names.
pub async fn connect(http: &reqwest::Client, dedicated_only: bool) -> Result<Api, CliError> {
    let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    match (env(URL_ENV), env(TOKEN_ENV)) {
        (Some(url), Some(token)) => return Ok(Api::new(http.clone(), &url, &token)),
        (Some(_), None) | (None, Some(_)) => {
            return Err(CliError::new(
                "bad_request",
                format!("set both {URL_ENV} and {TOKEN_ENV}, or neither"),
            ))
        }
        (None, None) => {}
    }
    if dedicated_only {
        return Err(CliError::new(
            "no_dedicated_manager",
            format!(
                "sharing needs a dedicated manager; start one with `darkpyonix manager --dedicated` \
                 and set {URL_ENV} and {TOKEN_ENV}"
            ),
        ));
    }
    let rec = spawn_ephemeral(http, &home()).await?;
    Ok(Api::new(http.clone(), &rec.url, &rec.token))
}

fn self_exe() -> Result<PathBuf, CliError> {
    match std::env::var_os(SELF_EXE_ENV).filter(|v| !v.is_empty()) {
        Some(p) => Ok(PathBuf::from(p)),
        None => std::env::current_exe()
            .map_err(|e| CliError::new("spawn_failed", format!("cannot locate the darkpyonix executable: {e}"))),
    }
}

/// Size past which `manager.log` is rotated to `manager.log.1` before the next spawn (SPEC FR-C1).
pub const MANAGER_LOG_LIMIT: u64 = 1024 * 1024;

/// Open `manager.log` for appending with mode 0600 (Unix), first rotating it to `<name>.1`
/// (replacing an older one) when it is larger than `limit` bytes.
pub fn open_manager_log(path: &Path, limit: u64) -> std::io::Result<std::fs::File> {
    if std::fs::metadata(path).map(|m| m.len() > limit).unwrap_or(false) {
        let mut rotated = path.as_os_str().to_owned();
        rotated.push(".1");
        std::fs::rename(path, PathBuf::from(rotated))?;
    }
    let mut opts = std::fs::OpenOptions::new();
    opts.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let file = opts.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // A log left by an older version may have wider permissions.
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

/// Re-execute this binary as `darkpyonix manager --ephemeral` in its own session (so the
/// terminal's Ctrl-C never reaches it), then read `{url, token, pid}` from its stdout pipe and
/// wait for `/health`. Its stderr goes to `<home>/manager.log` for the failure message.
pub async fn spawn_ephemeral(http: &reqwest::Client, home: &Path) -> Result<ManagerLine, CliError> {
    let fail = |m: String| CliError::new("spawn_failed", m);
    std::fs::create_dir_all(home).map_err(|e| fail(format!("cannot create {}: {e}", home.display())))?;
    let log_path = home.join("manager.log");
    let log = open_manager_log(&log_path, MANAGER_LOG_LIMIT)
        .map_err(|e| fail(format!("cannot open {}: {e}", log_path.display())))?;
    let exe = self_exe()?;
    let mut cmd = Command::new(&exe);
    cmd.args(["manager", "--ephemeral"])
        .env("DARKPYONIX_HOME", home)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(log));
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let mut child = cmd.spawn().map_err(|e| fail(format!("cannot start {} manager --ephemeral: {e}", exe.display())))?;
    let stdout = child.stdout.take().ok_or_else(|| fail("no stdout pipe".into()))?;
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    std::thread::spawn(move || {
        let mut line = String::new();
        let _ = std::io::BufRead::read_line(&mut std::io::BufReader::new(stdout), &mut line);
        let _ = tx.send(line);
    });
    let deadline = Instant::now() + SPAWN_TIMEOUT;
    let why_failed = |what: String| {
        let log = std::fs::read_to_string(&log_path).unwrap_or_default();
        let tail: Vec<&str> = log.lines().rev().take(5).collect::<Vec<_>>().into_iter().rev().collect();
        let mut m = format!("could not start an ephemeral manager: {what}");
        if !tail.is_empty() {
            m.push_str(&format!("\n{}", tail.join("\n")));
        }
        fail(m)
    };
    loop {
        match rx.try_recv() {
            Ok(line) if line.trim().is_empty() => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                return Err(why_failed(format!("`{} manager --ephemeral` exited before printing its address", exe.display())));
            }
            Ok(line) => {
                let Ok(rec) = serde_json::from_str::<ManagerLine>(line.trim()) else {
                    return Err(why_failed(format!("unexpected first line on stdout: {}", line.trim())));
                };
                if healthy(http, &rec.url).await {
                    return Ok(rec); // the child is left running on purpose: it is the manager
                }
                return Err(why_failed(format!("{} did not answer /health", rec.url)));
            }
            Err(_) => {}
        }
        if let Ok(Some(status)) = child.try_wait() {
            // The line may have been sent just before it exited; give the reader a moment.
            if let Ok(line) = rx.recv_timeout(Duration::from_millis(100)) {
                if let Ok(rec) = serde_json::from_str::<ManagerLine>(line.trim()) {
                    if healthy(http, &rec.url).await {
                        return Ok(rec);
                    }
                }
            }
            return Err(why_failed(format!("`{} manager --ephemeral` exited with {status}", exe.display())));
        }
        if Instant::now() >= deadline {
            return Err(why_failed(format!(
                "no address on stdout within {} s",
                SPAWN_TIMEOUT.as_secs()
            )));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The canonical path of a notebook file and its kernel id (PROTOCOL §2.6).
pub fn resolve_file(file: &Path) -> Result<(PathBuf, String), CliError> {
    let canonical = std::fs::canonicalize(file)
        .map_err(|e| CliError::new("bad_request", format!("{}: {e}", file.display())))?;
    let text = canonical.to_string_lossy();
    Ok((canonical.clone(), kernel_id_for(&text)))
}

pub fn kernel_id_for(canonical: &str) -> String {
    let digest = Sha256::digest(canonical.as_bytes());
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    format!("k_{}", &hex[..20])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../../.scratch/rcli-unit").join(name);
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    #[cfg(unix)]
    #[test]
    fn test_fr_c1_manager_log_is_created_with_mode_0600() {
        use std::os::unix::fs::PermissionsExt;
        let home = scratch("log_mode");
        let p = home.join("manager.log");
        std::fs::write(&p, "old\n").unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        drop(open_manager_log(&p, MANAGER_LOG_LIMIT).unwrap());
        assert_eq!(std::fs::metadata(&p).unwrap().permissions().mode() & 0o777, 0o600);
        let q = home.join("fresh.log");
        drop(open_manager_log(&q, MANAGER_LOG_LIMIT).unwrap());
        assert_eq!(std::fs::metadata(&q).unwrap().permissions().mode() & 0o777, 0o600);
    }

    #[test]
    fn test_fr_c1_manager_log_appends_and_rotates_past_the_limit() {
        use std::io::Write;
        let home = scratch("log_rotate");
        let p = home.join("manager.log");
        let one = home.join("manager.log.1");
        std::fs::write(&one, "ancient").unwrap();
        writeln!(open_manager_log(&p, 100).unwrap(), "first").unwrap();
        writeln!(open_manager_log(&p, 100).unwrap(), "second").unwrap();
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "first\nsecond\n");
        assert_eq!(std::fs::read_to_string(&one).unwrap(), "ancient");
        std::fs::write(&p, "x".repeat(101)).unwrap();
        writeln!(open_manager_log(&p, 100).unwrap(), "third").unwrap();
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "third\n");
        assert_eq!(std::fs::read_to_string(&one).unwrap(), "x".repeat(101));
    }

    #[test]
    fn test_fr_c1_manager_line_parses_url_token_pid() {
        let l: ManagerLine = serde_json::from_str(r#"{"url":"http://127.0.0.1:1","token":"t","pid":7}"#).unwrap();
        assert_eq!((l.url.as_str(), l.token.as_str(), l.pid), ("http://127.0.0.1:1", "t", 7));
    }

    #[test]
    fn test_pr_kernel_id_is_sha256_prefix_of_canonical_path() {
        // echo -n /home/u/exp/train.py | shasum -a 256
        assert_eq!(kernel_id_for("/home/u/exp/train.py"), "k_7570bb3a04bd69b917b4");
        let home = scratch("kid");
        let f = home.join("a.py");
        std::fs::write(&f, "").unwrap();
        std::fs::create_dir_all(home.join("sub")).unwrap();
        let (canon, id) = resolve_file(&home.join("sub/../a.py")).unwrap();
        assert_eq!(canon, std::fs::canonicalize(&f).unwrap());
        assert_eq!(id, kernel_id_for(&canon.to_string_lossy()));
        assert!(resolve_file(&home.join("missing.py")).is_err());
    }
}
