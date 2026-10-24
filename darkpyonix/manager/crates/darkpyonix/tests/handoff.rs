//! SPEC FR-M6 end to end: a real `darkpyonix manager` process, a real Python kernel, and a real
//! `SIGKILL` of the manager. The handed-off event stream keeps flowing afterwards.
//!
//! `cargo test -p darkpyonix --test handoff`
#![cfg(unix)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

const TOKEN: &str = "handoff-test-token";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..").canonicalize().unwrap()
}

fn scratch(name: &str) -> PathBuf {
    let dir = repo_root()
        .join(".scratch")
        .join("rust-handoff")
        .join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn venv_python() -> PathBuf {
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

struct Manager {
    child: Child,
    url: String,
}

impl Manager {
    fn start(home: &Path) -> Manager {
        let child = Command::new(env!("CARGO_BIN_EXE_darkpyonix"))
            .args(["manager", "--ephemeral", "--idle-timeout", "600"])
            .env("DARKPYONIX_HOME", home)
            .env("DARKPYONIX_PYTHON", venv_python())
            .env("DARKPYONIX_MANAGER_TOKEN", TOKEN)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn darkpyonix manager");
        let file = home.join("managers").join(format!("{}.json", child.id()));
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(text) = std::fs::read_to_string(&file) {
                if let Ok(v) = serde_json::from_str::<Value>(&text) {
                    let url = v["url"].as_str().unwrap().to_string();
                    return Manager { child, url };
                }
            }
            assert!(Instant::now() < deadline, "manager did not register {}", file.display());
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn addr(&self) -> String {
        self.url.trim_start_matches("http://").to_string()
    }

    /// One request with `Connection: close`; returns the status and the parsed JSON body.
    fn call(&self, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
        let mut s = TcpStream::connect(self.addr()).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
        let payload = body.map(|b| b.to_string()).unwrap_or_default();
        let req = format!(
            "{method} {path} HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {TOKEN}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
            payload.len()
        );
        s.write_all(req.as_bytes()).unwrap();
        let mut raw = Vec::new();
        let _ = s.read_to_end(&mut raw);
        let text = String::from_utf8_lossy(&raw).into_owned();
        let status = text.split_whitespace().nth(1).and_then(|c| c.parse().ok()).unwrap_or(0);
        let body = text.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
        (status, serde_json::from_str(body).unwrap_or(Value::Null))
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn read_until(s: &mut TcpStream, needle: &str, seen: &mut String, within: Duration) {
    let deadline = Instant::now() + within;
    s.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
    let mut chunk = [0u8; 8192];
    while !seen.contains(needle) {
        assert!(Instant::now() < deadline, "did not see {needle:?}; got {seen:?}");
        match s.read(&mut chunk) {
            Ok(0) => panic!("stream closed before {needle:?}; got {seen:?}"),
            Ok(n) => seen.push_str(&String::from_utf8_lossy(&chunk[..n])),
            Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {}
            Err(e) => panic!("read: {e}; got {seen:?}"),
        }
    }
}

#[test]
fn test_fr_m6_plain_stream_survives_manager_kill() {
    let dir = scratch("kill");
    let home = dir.join("home");
    std::fs::create_dir_all(home.join("managers")).unwrap();
    let nb = dir.join("nb.py");
    std::fs::write(&nb, "# %%\nprint('hello from the kernel')\n").unwrap();

    let mut first = Manager::start(&home);
    let (status, kernel) = first.call("POST", "/api/kernels", Some(json!({"path": nb.to_string_lossy()})));
    assert!(status == 200 || status == 201, "start kernel: {status} {kernel}");
    let kid = kernel["kernel_id"].as_str().unwrap().to_string();
    let kernel_pid = kernel["pid"].as_u64().unwrap() as u32;

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // An event stream on plain TCP: answered by the kernel after the handoff.
        let mut stream = TcpStream::connect(first.addr()).unwrap();
        stream
            .write_all(
                format!("GET /api/kernels/{kid}/events HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {TOKEN}\r\n\r\n")
                    .as_bytes(),
            )
            .unwrap();
        let mut seen = String::new();
        read_until(&mut stream, ": darkpyonix events", &mut seen, Duration::from_secs(20));
        assert!(seen.starts_with("HTTP/1.1 200") && seen.contains("text/event-stream"), "{seen}");

        // SIGKILL: the manager gets no chance to clean up or close anything.
        first.child.kill().unwrap();
        first.child.wait().unwrap();

        // A second manager starts a run; the first manager's stream carries its events.
        let second = Manager::start(&home);
        let (status, run) = second.call("POST", &format!("/api/kernels/{kid}/runs"), Some(json!({"mode": "all"})));
        assert!((200..300).contains(&status), "start run: {status} {run}");
        read_until(&mut stream, "event: run.finished", &mut seen, Duration::from_secs(30));
        assert!(seen.contains("hello from the kernel"), "{seen}");
    }));
    let _ = dpx_kernel::process::kill_pid(kernel_pid);
    let _ = std::fs::remove_dir_all(&dir);
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}
