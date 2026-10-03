//! Test helpers: scratch homes under `<repo>/.scratch/`, interpreters, process guards and
//! the Python fake DKP/1 kernel (`tests/helpers/fake_kernel.py`).
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use dpx_kernel::{Config, RealBackend};
use serde_json::Value;

pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .unwrap()
}

/// A fresh directory under `<repo>/.scratch/rust-kernel/` (never /tmp, never the real home).
pub fn scratch(name: &str) -> PathBuf {
    let dir = repo_root()
        .join(".scratch")
        .join("rust-kernel")
        .join(format!(
            "{name}-{}-{}",
            std::process::id(),
            dpx_kernel::home::random_hex(3)
        ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The development venv: in this checkout or, for a worktree, in an enclosing checkout.
pub fn venv_python() -> PathBuf {
    if let Some(p) = std::env::var_os("DPX_TEST_PYTHON") {
        return PathBuf::from(p);
    }
    for dir in repo_root().ancestors() {
        let p = dir.join(".venv/bin/python");
        if p.is_file() {
            return p;
        }
    }
    panic!("no .venv/bin/python found above {}", repo_root().display());
}

/// Every interpreter on this machine the kernel must run on (NFR-K1).
pub fn interpreters() -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = ["/usr/bin/python3", "/opt/homebrew/bin/python3.11"]
        .iter()
        .map(PathBuf::from)
        .filter(|p| p.is_file())
        .collect();
    out.push(venv_python());
    out
}

pub fn backend(home: &Path, multicast: bool) -> RealBackend {
    let mut cfg = Config::with_home(home);
    cfg.multicast = multicast;
    cfg.python = Some(venv_python().to_string_lossy().into_owned());
    RealBackend::new(cfg).unwrap()
}

pub fn backend_with(cfg: Config) -> RealBackend {
    RealBackend::new(cfg).unwrap()
}

pub fn write_notebook(dir: &Path, name: &str) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, "# %%\nx = 1\nprint(x)\n").unwrap();
    p
}

/// Kills every registered pid with SIGKILL when dropped (also on test failure).
#[derive(Default)]
pub struct Reaper {
    pids: Mutex<Vec<u32>>,
}

impl Reaper {
    pub fn add(&self, pid: u32) {
        self.pids.lock().unwrap().push(pid);
    }
}

impl Drop for Reaper {
    fn drop(&mut self) {
        for pid in self.pids.lock().unwrap().drain(..) {
            let _ = dpx_kernel::process::kill_pid(pid);
        }
    }
}

/// `python tests/helpers/fake_kernel.py FILE ANNOUNCE_JSON` with `DARKPYONIX_HOME=home`.
pub struct FakeKernel {
    pub child: Child,
    pub announce: Value,
    pub kernel_id: String,
    pub port: u16,
    pub pid: u32,
}

impl FakeKernel {
    pub fn start(home: &Path, file: &Path) -> FakeKernel {
        let out = file.with_extension("announce.json");
        let _ = std::fs::remove_file(&out);
        let child = Command::new(venv_python())
            .arg(repo_root().join("tests/helpers/fake_kernel.py"))
            .arg(file)
            .arg(&out)
            .env("DARKPYONIX_HOME", home)
            .stdin(Stdio::null())
            .spawn()
            .expect("start fake kernel");
        let mut fake = FakeKernel {
            child,
            announce: Value::Null,
            kernel_id: String::new(),
            port: 0,
            pid: 0,
        };
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(data) = std::fs::read(&out) {
                if let Ok(v) = serde_json::from_slice::<Value>(&data) {
                    fake.kernel_id = v["kernel_id"].as_str().unwrap().to_string();
                    fake.port = v["port"].as_u64().unwrap() as u16;
                    fake.pid = v["pid"].as_u64().unwrap() as u32;
                    fake.announce = v;
                    return fake;
                }
            }
            assert!(Instant::now() < deadline, "fake kernel did not start");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for FakeKernel {
    fn drop(&mut self) {
        self.kill();
    }
}

pub fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i]
}
