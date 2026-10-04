//! Finding a manager (SPEC FR-C1) and naming a file's kernel (PROTOCOL §2.6).

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::client::{Api, CliError};

/// Overrides the executable spawned as `<exe> manager --ephemeral` (tests, wrappers).
pub const SELF_EXE_ENV: &str = "DARKPYONIX_SELF_EXE";
/// How long a freshly spawned manager has to register itself and answer `/health`.
pub const SPAWN_TIMEOUT: Duration = Duration::from_secs(5);

/// `<DARKPYONIX_HOME>/managers/<pid>.json` (PROTOCOL §1), written 0600 by every manager.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ManagerRecord {
    pub url: String,
    pub token: String,
    #[serde(default = "default_mode")]
    pub mode: String,
    pub pid: u32,
    #[serde(default)]
    pub started_at: String,
}

fn default_mode() -> String {
    "ephemeral".into()
}

/// `DARKPYONIX_HOME`, else `~/.darkpyonix`.
pub fn home() -> PathBuf {
    if let Some(h) = std::env::var_os("DARKPYONIX_HOME").filter(|h| !h.is_empty()) {
        return PathBuf::from(h);
    }
    let base = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")).unwrap_or_default();
    PathBuf::from(base).join(".darkpyonix")
}

/// Registered managers whose process is alive, best first: dedicated before ephemeral,
/// then the most recently started. Unreadable or stale files are skipped, never deleted
/// (they belong to their manager).
pub fn live_records(home: &Path) -> Vec<ManagerRecord> {
    let Ok(dir) = std::fs::read_dir(home.join("managers")) else { return Vec::new() };
    let mut recs: Vec<ManagerRecord> = dir
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .filter_map(|p| serde_json::from_slice::<ManagerRecord>(&std::fs::read(p).ok()?).ok())
        .filter(|r| pid_alive(r.pid))
        .collect();
    recs.sort_by(|a, b| {
        (b.mode == "dedicated").cmp(&(a.mode == "dedicated")).then_with(|| b.started_at.cmp(&a.started_at))
    });
    recs
}

#[cfg(unix)]
pub fn pid_alive(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else { return false };
    if pid <= 0 {
        return false;
    }
    // SAFETY: signal 0 only checks for existence and permission.
    let r = unsafe { libc::kill(pid, 0) };
    r == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
pub fn pid_alive(_pid: u32) -> bool {
    true // the /health check decides
}

async fn healthy(http: &reqwest::Client, url: &str) -> bool {
    let url = format!("{}/health", url.trim_end_matches('/'));
    matches!(
        tokio::time::timeout(Duration::from_secs(2), http.get(url).send()).await,
        Ok(Ok(r)) if r.status().is_success()
    )
}

/// A live manager from the registry, else a freshly spawned ephemeral one (FR-C1).
/// With `dedicated_only`, never spawns and fails when no dedicated manager is registered.
pub async fn connect(http: &reqwest::Client, dedicated_only: bool) -> Result<Api, CliError> {
    let home = home();
    for rec in live_records(&home) {
        if dedicated_only && rec.mode != "dedicated" {
            continue;
        }
        if healthy(http, &rec.url).await {
            return Ok(Api::new(http.clone(), &rec.url, &rec.token));
        }
    }
    if dedicated_only {
        return Err(CliError::new(
            "no_dedicated_manager",
            "sharing needs a dedicated manager, and none is running for this user; \
             start one with `darkpyonix manager --dedicated`",
        ));
    }
    let rec = spawn_ephemeral(http, &home).await?;
    Ok(Api::new(http.clone(), &rec.url, &rec.token))
}

fn self_exe() -> Result<PathBuf, CliError> {
    match std::env::var_os(SELF_EXE_ENV).filter(|v| !v.is_empty()) {
        Some(p) => Ok(PathBuf::from(p)),
        None => std::env::current_exe()
            .map_err(|e| CliError::new("spawn_failed", format!("cannot locate the darkpyonix executable: {e}"))),
    }
}

/// Re-execute this binary as `darkpyonix manager --ephemeral` in its own session (so the
/// terminal's Ctrl-C never reaches it), then wait for `managers/<pid>.json` and `/health`.
pub async fn spawn_ephemeral(http: &reqwest::Client, home: &Path) -> Result<ManagerRecord, CliError> {
    let fail = |m: String| CliError::new("spawn_failed", m);
    let managers = home.join("managers");
    std::fs::create_dir_all(&managers).map_err(|e| fail(format!("cannot create {}: {e}", managers.display())))?;
    let log_path = managers.join("spawn.log");
    let log = std::fs::File::create(&log_path).map_err(|e| fail(format!("cannot create {}: {e}", log_path.display())))?;
    let exe = self_exe()?;
    let mut cmd = Command::new(&exe);
    cmd.args(["manager", "--ephemeral"])
        .env("DARKPYONIX_HOME", home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let mut child = cmd.spawn().map_err(|e| fail(format!("cannot start {} manager --ephemeral: {e}", exe.display())))?;
    let reg = managers.join(format!("{}.json", child.id()));
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
        if let Ok(Some(status)) = child.try_wait() {
            return Err(why_failed(format!("`{} manager --ephemeral` exited with {status}", exe.display())));
        }
        if let Some(rec) = std::fs::read(&reg).ok().and_then(|b| serde_json::from_slice::<ManagerRecord>(&b).ok()) {
            if healthy(http, &rec.url).await {
                return Ok(rec); // the child is left running on purpose: it is the manager
            }
        }
        if Instant::now() >= deadline {
            return Err(why_failed(format!(
                "no healthy registration at {} within {} s",
                reg.display(),
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
        std::fs::create_dir_all(root.join("managers")).unwrap();
        root
    }

    fn write(home: &Path, rec: &ManagerRecord) {
        let p = home.join("managers").join(format!("{}.json", rec.pid));
        std::fs::write(p, serde_json::to_vec(rec).unwrap()).unwrap();
    }

    fn rec(pid: u32, mode: &str, started: &str) -> ManagerRecord {
        ManagerRecord {
            url: format!("http://127.0.0.1:{}", 40000 + pid % 1000),
            token: "t".into(),
            mode: mode.into(),
            pid,
            started_at: started.into(),
        }
    }

    fn dead_pid() -> u32 {
        let mut c = Command::new("true").spawn().unwrap();
        let pid = c.id();
        c.wait().unwrap();
        pid
    }

    #[test]
    fn test_fr_c1_registry_keeps_live_managers_best_first() {
        let home = scratch("registry");
        let me = std::process::id();
        let parent = std::os::unix::process::parent_id();
        write(&home, &rec(dead_pid(), "dedicated", "2026-10-03T09:00:00Z"));
        write(&home, &rec(me, "ephemeral", "2026-10-03T08:00:00Z"));
        write(&home, &rec(parent, "ephemeral", "2026-10-03T08:30:00Z"));
        std::fs::write(home.join("managers/garbage.json"), b"{not json").unwrap();
        std::fs::write(home.join("managers/spawn.log"), b"x").unwrap();
        let found = live_records(&home);
        assert_eq!(found.iter().map(|r| r.pid).collect::<Vec<_>>(), vec![parent, me]);

        write(&home, &rec(1, "dedicated", "2026-10-01T00:00:00Z")); // launchd/init: alive
        assert_eq!(live_records(&home)[0].mode, "dedicated");
    }

    #[test]
    fn test_fr_c1_missing_registry_dir_means_no_managers() {
        let home = scratch("empty");
        std::fs::remove_dir_all(home.join("managers")).unwrap();
        assert!(live_records(&home).is_empty());
    }

    #[test]
    fn test_pr_kernel_id_is_sha256_prefix_of_canonical_path() {
        // echo -n /home/u/exp/train.py | shasum -a 256
        assert_eq!(kernel_id_for("/home/u/exp/train.py"), "k_7570bb3a04bd69b917b4");
        let home = scratch("kid");
        let f = home.join("a.py");
        std::fs::write(&f, "").unwrap();
        let (canon, id) = resolve_file(&home.join("managers/../a.py")).unwrap();
        assert_eq!(canon, std::fs::canonicalize(&f).unwrap());
        assert_eq!(id, kernel_id_for(&canon.to_string_lossy()));
        assert!(resolve_file(&home.join("missing.py")).is_err());
    }
}
