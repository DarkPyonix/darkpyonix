//! FR-A3 on the real stack: share tokens with different capability sets against a real Python
//! kernel (`dpx_kernel::RealBackend`) behind a dedicated `dpx_server::serve`, over HTTP and over
//! the 2025 WebSocket (the kernel answers the upgrade itself after the stream handoff).
//!
//! `cargo test -j2 -p darkpyonix --test share_views`
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use dpx_kernel::{Config, RealBackend};
use dpx_server::{serve, ServerConfig, ServerHandle};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../..")
        .canonicalize()
        .unwrap()
}

/// A fresh directory under `<repo>/.scratch/` (never /tmp, never the real home).
fn scratch(name: &str) -> PathBuf {
    let dir = repo_root()
        .join(".scratch")
        .join("rust-share-views")
        .join(format!(
            "{name}-{}-{}",
            std::process::id(),
            dpx_kernel::home::random_hex(3)
        ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The development venv: in this checkout or, for a worktree, in an enclosing checkout.
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

/// Kills every registered kernel pid when dropped (also on test failure).
#[derive(Default)]
struct Reaper {
    pids: Mutex<Vec<u32>>,
}

impl Reaper {
    fn add(&self, pid: u32) {
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

/// A dedicated manager (real backend) on `home`; `token` is its master token.
struct Manager {
    handle: Option<ServerHandle>,
    url: String,
    token: String,
    http: reqwest::Client,
}

impl Manager {
    async fn start(home: &Path) -> Manager {
        let mut cfg = Config::with_home(home);
        cfg.python = Some(venv_python().to_string_lossy().into_owned());
        let backend = RealBackend::new(cfg).unwrap();
        let handle = serve(
            ServerConfig::dedicated(home, "127.0.0.1", 0),
            std::sync::Arc::new(backend),
        )
        .await
        .expect("serve");
        Manager {
            url: handle.url.clone(),
            token: handle.token.clone(),
            handle: Some(handle),
            http: reqwest::Client::new(),
        }
    }

    /// One request with `token`; the client id header is always sent so that capability
    /// checks, not header validation, decide the answer.
    async fn call(
        &self,
        token: &str,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        let mut rb = self
            .http
            .request(method, format!("{}{path}", self.url))
            .bearer_auth(token)
            .header("X-DarkPyonix-Client", "share-views-01");
        if let Some(b) = body {
            rb = rb
                .header("content-type", "application/json")
                .body(serde_json::to_vec(&b).unwrap());
        }
        let resp = rb.send().await.expect("request");
        let status = resp.status().as_u16();
        let bytes = resp.bytes().await.expect("body");
        (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    async fn stop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.shutdown();
            h.wait().await;
        }
    }
}

// ---------------------------------------------------------------- minimal WebSocket client

/// A raw RFC 6455 client over a plain TCP stream: HTTP/1.1 upgrade, masked text frames out,
/// unmasked frames in. Enough to talk to the kernel after the manager handed the socket over.
struct Ws {
    stream: TcpStream,
}

impl Ws {
    async fn connect(url: &str, kernel_id: &str, token: &str, client_id: &str) -> Ws {
        let addr = url.trim_start_matches("http://");
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let req = format!(
            "GET /api/ws/kernels/{kernel_id}?token={token}&client_id={client_id} HTTP/1.1\r\n\
             Host: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
        );
        stream.write_all(req.as_bytes()).await.unwrap();
        // Read the response head byte by byte so no frame byte is swallowed.
        let mut head = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(15);
        while !head.ends_with(b"\r\n\r\n") {
            let left = deadline.saturating_duration_since(Instant::now());
            let mut b = [0u8; 1];
            tokio::time::timeout(left, stream.read_exact(&mut b))
                .await
                .expect("upgrade response timed out")
                .expect("upgrade response");
            head.push(b[0]);
        }
        let head = String::from_utf8_lossy(&head).into_owned();
        assert!(head.starts_with("HTTP/1.1 101"), "expected 101, got: {head}");
        Ws { stream }
    }

    /// A masked text frame (payloads here are well under 64 KiB).
    async fn send_json(&mut self, v: &Value) {
        let payload = serde_json::to_vec(v).unwrap();
        let mask = [0x12u8, 0x34, 0x56, 0x78];
        let mut frame = vec![0x81u8];
        if payload.len() < 126 {
            frame.push(0x80 | payload.len() as u8);
        } else {
            frame.push(0x80 | 126);
            frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
        frame.extend_from_slice(&mask);
        frame.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
        self.stream.write_all(&frame).await.unwrap();
    }

    /// The next text message as JSON; control frames are skipped, `None` once closed.
    async fn next_json(&mut self, within: Duration) -> Option<Value> {
        let deadline = Instant::now() + within;
        let mut text: Vec<u8> = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let (fin, opcode, payload) = tokio::time::timeout(left, self.read_frame())
                .await
                .expect("WebSocket read timed out")?;
            match opcode {
                0x8 => return None,
                0x1 | 0x0 => {
                    text.extend_from_slice(&payload);
                    if fin {
                        return Some(serde_json::from_slice(&text).expect("JSON message"));
                    }
                }
                _ => {} // ping, pong
            }
        }
    }

    async fn read_frame(&mut self) -> Option<(bool, u8, Vec<u8>)> {
        let mut h = [0u8; 2];
        self.stream.read_exact(&mut h).await.ok()?;
        let fin = h[0] & 0x80 != 0;
        let opcode = h[0] & 0x0f;
        let mut len = (h[1] & 0x7f) as usize;
        if len == 126 {
            let mut b = [0u8; 2];
            self.stream.read_exact(&mut b).await.ok()?;
            len = u16::from_be_bytes(b) as usize;
        } else if len == 127 {
            let mut b = [0u8; 8];
            self.stream.read_exact(&mut b).await.ok()?;
            len = u64::from_be_bytes(b) as usize;
        }
        let mut mask = [0u8; 4];
        let masked = h[1] & 0x80 != 0;
        if masked {
            self.stream.read_exact(&mut mask).await.ok()?;
        }
        let mut payload = vec![0u8; len];
        self.stream.read_exact(&mut payload).await.ok()?;
        if masked {
            for (i, b) in payload.iter_mut().enumerate() {
                *b ^= mask[i % 4];
            }
        }
        Some((fin, opcode, payload))
    }

    /// Send `request` and return the first reply of type `reply` (other messages, such as the
    /// presence snapshot after connecting, are skipped), or any `error` message.
    async fn ask(&mut self, request: Value, reply: &str) -> Value {
        self.send_json(&request).await;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let msg = self
                .next_json(left)
                .await
                .unwrap_or_else(|| panic!("socket closed while waiting for {reply}"));
            let kind = msg["type"].as_str().unwrap_or("");
            if kind == reply || kind == "error" {
                return msg;
            }
        }
    }
}

// ---------------------------------------------------------------- the test

/// Poll `GET /runs` (master token) until the newest run has left queued/running.
async fn wait_run_done(m: &Manager, kid: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let (status, body) = m
            .call(&m.token, reqwest::Method::GET, &format!("/api/kernels/{kid}/runs"), None)
            .await;
        assert_eq!(status, 200, "{body}");
        if let Some(run) = body["runs"].as_array().and_then(|r| r.first()) {
            let st = run["status"].as_str().unwrap_or("");
            if st != "queued" && st != "running" {
                return run.clone();
            }
        }
        assert!(Instant::now() < deadline, "run did not finish: {body}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// FR-A3: a share token's capability set decides what a real kernel hands it, over REST and
/// over the WebSocket. `read` shows code, `history` adds outputs and run logs, `execute` runs,
/// and none of these three may edit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_fr_a3_share_views_on_a_real_kernel() {
    use reqwest::Method;
    let dir = scratch("a3");
    let home = dir.join("home");
    let reaper = Reaper::default();
    let mut m = Manager::start(&home).await;

    let nb = dir.join("shared.py");
    std::fs::write(&nb, "# %%\nprint('visible-' + 'output')\n").unwrap();
    let (status, k) = m
        .call(
            &m.token.clone(),
            Method::POST,
            "/api/kernels",
            Some(json!({"path": nb.to_string_lossy()})),
        )
        .await;
    assert!(status == 200 || status == 201, "start kernel: {status} {k}");
    reaper.add(k["pid"].as_u64().unwrap() as u32);
    let kid = k["kernel_id"].as_str().unwrap().to_string();
    let b = format!("/api/kernels/{kid}");
    let master = m.token.clone();

    // One finished run, so there is history to show or hide.
    let (status, body) = m
        .call(&master, Method::POST, &format!("{b}/runs"), Some(json!({"mode": "all"})))
        .await;
    assert_eq!(status, 202, "{body}");
    let done = wait_run_done(&m, &kid).await;
    assert_eq!(done["status"], "ok", "{done}");

    // Three shares: read; read+history; read+history+execute.
    let sets: [(&str, Vec<&str>); 3] = [
        ("read", vec!["read"]),
        ("history", vec!["read", "history"]),
        ("execute", vec!["read", "history", "execute"]),
    ];
    let mut tokens: Vec<(&str, Vec<&str>, String)> = Vec::new();
    for (label, caps) in sets {
        let (status, body) = m
            .call(
                &master,
                Method::POST,
                &format!("{b}/shares"),
                Some(json!({"capabilities": caps, "label": label})),
            )
            .await;
        assert_eq!(status, 201, "{body}");
        assert_eq!(body["capabilities"], json!(caps));
        tokens.push((label, caps.clone(), body["token"].as_str().unwrap().to_string()));
    }

    // Document, runs, cells and locks over REST.
    let mut cell_id = String::from("c_unknown");
    for (label, caps, token) in &tokens {
        let has_history = caps.contains(&"history");
        let (status, doc) = m.call(token, Method::GET, &format!("{b}/document"), None).await;
        assert_eq!(status, 200, "{label}: {doc}");
        let cells = doc["cells"].as_array().unwrap_or_else(|| panic!("{label}: no cells: {doc}"));
        assert!(!cells.is_empty(), "{label}: {doc}");
        for c in cells {
            let has_outputs = c.get("outputs").is_some();
            assert_eq!(has_outputs, has_history, "{label}: outputs vs history in {c}");
        }
        if has_history {
            assert!(
                doc.to_string().contains("visible-output"),
                "{label}: the run's output is in the document: {doc}"
            );
        } else {
            assert!(!doc.to_string().contains("visible-output"), "{label}: output leaked: {doc}");
        }
        if let Some(id) = cells.iter().find_map(|c| c.get("cell_id").and_then(Value::as_str)) {
            cell_id = id.to_string();
        }

        let (status, body) = m.call(token, Method::GET, &format!("{b}/runs"), None).await;
        if has_history {
            assert_eq!(status, 200, "{label}: {body}");
            assert!(!body["runs"].as_array().unwrap().is_empty(), "{label}: {body}");
        } else {
            assert_eq!(status, 403, "{label}: {body}");
        }

        let (status, body) = m
            .call(token, Method::POST, &format!("{b}/cells"), Some(json!({"source": "z = 0\n"})))
            .await;
        assert_eq!(status, 403, "{label}: POST cells: {body}");
        let (status, body) = m
            .call(token, Method::PUT, &format!("{b}/cells/{cell_id}/lock"), None)
            .await;
        assert_eq!(status, 403, "{label}: PUT lock: {body}");
    }

    // The WebSocket: the kernel answers each request by the label it was handed.
    for (label, caps, token) in &tokens {
        let has_history = caps.contains(&"history");
        let mut ws = Ws::connect(&m.url, &kid, token, "share-views-ws").await;

        let hist = ws.ask(json!({"type": "request_history", "kernel_id": kid}), "history_data").await;
        assert_eq!(hist["type"], "history_data", "{label}: {hist}");
        let entries = hist["history"].as_array().unwrap_or_else(|| panic!("{label}: {hist}"));
        if has_history {
            assert!(!entries.is_empty(), "{label}: history entries expected: {hist}");
        } else {
            assert!(entries.is_empty(), "{label}: history must be empty: {hist}");
        }

        let typing = ws
            .ask(
                json!({"type": "start_typing", "cell_id": cell_id, "user_id": "share-views-ws"}),
                "cell_locked",
            )
            .await;
        assert_eq!(typing["type"], "error", "{label}: {typing}");
        assert_eq!(typing["error"], "INSUFFICIENT_PERMISSION", "{label}: {typing}");
    }

    // Execute: only the token that holds it can start a run.
    for (label, caps, token) in &tokens {
        let (status, body) = m
            .call(token, Method::POST, &format!("{b}/runs"), Some(json!({"mode": "all"})))
            .await;
        if caps.contains(&"execute") {
            assert_eq!(status, 202, "{label}: {body}");
            wait_run_done(&m, &kid).await;
        } else {
            assert_eq!(status, 403, "{label}: POST runs: {body}");
        }
    }

    m.stop().await;
}
