//! The embedded stdlib-Python kernel (INTENT D3, D10): extraction to the runtime home,
//! interpreter selection, detached launch (FR-K1, FR-K4, FR-M2) and the FR-R4 document
//! builder, which stays in Python (`darkpyonix._document` is the authority).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use dpx_core::{DpxError, Result};
use serde_json::Value;

mod embedded {
    include!(concat!(env!("OUT_DIR"), "/embedded.rs"));
}

pub use embedded::{KERNEL_FILES, KERNEL_HASH};

pub const DOCUMENT_TIMEOUT: Duration = Duration::from_secs(30);

fn internal(msg: impl Into<String>) -> DpxError {
    DpxError::new("internal", msg)
}

/// `<crate version>-<content hash>`: the name of the extraction directory.
pub fn runtime_tag() -> String {
    format!("{}-{}", env!("CARGO_PKG_VERSION"), KERNEL_HASH)
}

/// Extract the embedded `darkpyonix` package once to `<home>/runtime/<tag>/` and return that
/// directory (the root to put on `sys.path`). Extraction goes to a temp dir that is renamed
/// into place, so concurrent managers never see a half-written tree.
pub fn extract_runtime(home: &Path) -> std::io::Result<PathBuf> {
    let runtime = crate::home::subdir(home, "runtime")?;
    let root = runtime.join(runtime_tag());
    let marker = root.join("darkpyonix").join("__main__.py");
    if marker.is_file() {
        return Ok(root);
    }
    let tmp = runtime.join(format!(".tmp-{}", crate::home::random_hex(6)));
    let result = (|| {
        for (rel, data) in KERNEL_FILES {
            let path = rel.split('/').fold(tmp.clone(), |p, c| p.join(c));
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&path, data)?;
        }
        std::fs::rename(&tmp, &root)
    })();
    if let Err(e) = result {
        let _ = std::fs::remove_dir_all(&tmp);
        // Another process may have won the race; its tree is just as good.
        if !marker.is_file() {
            return Err(e);
        }
    }
    Ok(root)
}

/// A Python string literal for `s` (JSON string syntax is valid Python).
fn py_str(s: &str) -> String {
    serde_json::to_string(s).expect("string serializes")
}

/// `import sys; sys.path.insert(0, ROOT); from darkpyonix.__main__ import main; ...`
/// (launcher.py `BOOTSTRAP` with the extracted root).
pub fn bootstrap(root: &Path) -> String {
    format!(
        "import sys; sys.path.insert(0, {}); from darkpyonix.__main__ import main; sys.exit(main(sys.argv[1:]))",
        py_str(&root.to_string_lossy())
    )
}

fn find_on_path(names: &[&str]) -> Option<String> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        for name in names {
            let candidates = if cfg!(windows) {
                vec![format!("{name}.exe"), name.to_string()]
            } else {
                vec![name.to_string()]
            };
            for c in candidates {
                let p = dir.join(&c);
                if p.is_file() {
                    return Some(p.to_string_lossy().into_owned());
                }
            }
        }
    }
    None
}

/// `requested`, else `DARKPYONIX_PYTHON` (via `configured`), else `python3`/`python` on PATH.
pub fn resolve_python(requested: Option<&str>, configured: Option<&str>) -> Result<String> {
    if let Some(p) = requested.or(configured).filter(|p| !p.is_empty()) {
        return Ok(p.to_string());
    }
    find_on_path(&["python3", "python"]).ok_or_else(|| {
        DpxError::new(
            "bad_request",
            "no Python interpreter: set DARKPYONIX_PYTHON or pass python",
        )
    })
}

pub struct Launch<'a> {
    pub home: &'a Path,
    pub root: &'a Path,
    pub python: &'a str,
    pub canonical: &'a str,
    pub kernel_id: &'a str,
    pub cwd: Option<&'a str>,
    pub env: &'a std::collections::BTreeMap<String, String>,
}

/// A launched kernel process: its pid and a way to stop it while it is still our child.
pub struct Spawned {
    pub pid: u32,
    abort: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Spawned {
    /// SIGKILL the child unless it has already exited and been reaped (no pid-reuse race:
    /// the reaper task owns the handle).
    pub fn abort(&mut self) {
        if let Some(tx) = self.abort.take() {
            let _ = tx.send(());
        }
    }
}

/// Spawn `[python, "-c", BOOTSTRAP, "--file", canonical]` detached from the manager, with
/// output appended to `kernels/<kernel_id>.log`. The child is reaped by a background task.
pub fn spawn_kernel(l: &Launch<'_>) -> Result<Spawned> {
    let kernels = crate::home::subdir(l.home, "kernels")
        .map_err(|e| internal(format!("kernels dir: {e}")))?;
    let log = open_log(&kernels.join(format!("{}.log", l.kernel_id)))
        .map_err(|e| internal(format!("kernel log: {e}")))?;
    let log2 = log.try_clone().map_err(|e| internal(e.to_string()))?;
    let cwd = match l.cwd {
        Some(c) => PathBuf::from(c),
        None => Path::new(l.canonical)
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from(".")),
    };
    let mut cmd = tokio::process::Command::new(l.python);
    cmd.arg("-c")
        .arg(bootstrap(l.root))
        .arg("--file")
        .arg(l.canonical)
        .current_dir(&cwd)
        .env("DARKPYONIX_HOME", l.home)
        .envs(l.env)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log2))
        .kill_on_drop(false);
    detach(&mut cmd);
    let mut child = cmd
        .spawn()
        .map_err(|e| DpxError::new("bad_request", format!("cannot start {}: {e}", l.python)))?;
    let pid = child.id().unwrap_or(0);
    let (abort, mut abort_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        // Reap: an exited kernel must not linger as a zombie.
        tokio::select! {
            _ = child.wait() => {}
            Ok(()) = &mut abort_rx => {
                let _ = child.start_kill();
                let _ = child.wait().await;
            }
        }
    });
    Ok(Spawned {
        pid,
        abort: Some(abort),
    })
}

#[cfg(unix)]
fn detach(cmd: &mut tokio::process::Command) {
    // SAFETY: setsid is async-signal-safe and touches no Rust state in the child.
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(windows)]
fn detach(cmd: &mut tokio::process::Command) {
    const DETACHED_PROCESS: u32 = 0x0000_0008;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
}

fn open_log(path: &Path) -> std::io::Result<std::fs::File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.append(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)
}

const DOCUMENT_SCRIPT: &str = r#"
import sys, json
sys.path.insert(0, ROOT)
try:
    from darkpyonix._document import build_document
    out = {"ok": True, "result": build_document(sys.argv[1], viewer_outputs=sys.argv[2] == "1")}
except FileNotFoundError as e:
    out = {"ok": False, "code": "not_found", "message": str(e)}
except Exception as e:
    code = getattr(e, "code", None)
    out = {"ok": False, "code": code if isinstance(code, str) else "internal",
           "message": "%s: %s" % (type(e).__name__, e)}
sys.stdout.write(json.dumps(out))
"#;

/// FR-R4: run `darkpyonix._document.build_document` in `python`.
pub async fn build_document(
    python: &str,
    home: &Path,
    root: &Path,
    canonical: &str,
    viewer_outputs: bool,
) -> Result<Value> {
    let script = DOCUMENT_SCRIPT.replace("ROOT", &py_str(&root.to_string_lossy()));
    let mut cmd = tokio::process::Command::new(python);
    cmd.arg("-c")
        .arg(script)
        .arg(canonical)
        .arg(if viewer_outputs { "1" } else { "0" })
        .env("DARKPYONIX_HOME", home)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let child = cmd
        .spawn()
        .map_err(|e| internal(format!("cannot start {python}: {e}")))?;
    let out = tokio::time::timeout(DOCUMENT_TIMEOUT, child.wait_with_output())
        .await
        .map_err(|_| {
            internal(format!(
                "document build timed out after {} s",
                DOCUMENT_TIMEOUT.as_secs()
            ))
        })?
        .map_err(|e| internal(format!("document build failed: {e}")))?;
    let parsed: Value = serde_json::from_slice(&out.stdout).map_err(|_| {
        internal(format!(
            "document build failed (exit {:?}): {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    })?;
    if parsed.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(parsed.get("result").cloned().unwrap_or(Value::Null));
    }
    let code = parsed
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or("internal");
    let message = parsed
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or(code);
    Err(DpxError::new(code, message))
}
