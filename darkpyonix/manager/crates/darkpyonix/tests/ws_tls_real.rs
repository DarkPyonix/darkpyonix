//! FR-S9 over TLS on the real stack: a real Python kernel behind a dedicated manager that serves
//! HTTPS with a self-signed certificate made inside the test. The WebSocket upgrade travels
//! through the manager's socket-pair pump (TLS has no descriptor to hand over), the kernel answers
//! the `101` itself, and its permission label, history filter and share revocation work through
//! the pump. HTTP/2 WebSocket is refused (no RFC 8441), see `dpx-server/tests/ws_tls.rs`.
//!
//! `cargo test -j2 -p darkpyonix --test ws_tls_real`
#![cfg(any(unix, windows))]

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dpx_kernel::{Config, RealBackend};
use dpx_server::{serve, ServerConfig, ServerHandle, TlsConfig};
use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::rustls::{self, pki_types::ServerName, RootCertStore};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..").canonicalize().unwrap()
}

/// A fresh directory under `<repo>/.scratch/` (git-ignored; the certificate and key live only here).
fn scratch(name: &str) -> PathBuf {
    let dir = repo_root()
        .join(".scratch")
        .join("rust-ws-tls")
        .join(format!("{name}-{}-{}", std::process::id(), dpx_kernel::home::random_hex(3)));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn venv_python() -> PathBuf {
    if let Some(p) = std::env::var_os("DPX_TEST_PYTHON") {
        return PathBuf::from(p);
    }
    for dir in repo_root().ancestors() {
        for rel in [".venv/bin/python", ".venv/Scripts/python.exe"] {
            let p = dir.join(rel);
            if p.is_file() {
                return p;
            }
        }
    }
    panic!("no .venv python found above {}; set DPX_TEST_PYTHON", repo_root().display());
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

/// A dedicated HTTPS manager (real backend); `token` is its master token.
struct Manager {
    handle: Option<ServerHandle>,
    port: u16,
    token: String,
    der: Vec<u8>,
    http: reqwest::Client,
}

impl Manager {
    async fn start(dir: &Path) -> Manager {
        let home = dir.join("home");
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        std::fs::write(dir.join("cert.pem"), cert.cert.pem()).unwrap();
        std::fs::write(dir.join("key.pem"), cert.signing_key.serialize_pem()).unwrap();
        let mut cfg = ServerConfig::dedicated(&home, "127.0.0.1", 0);
        cfg.tls = Some(TlsConfig::PemFiles { cert: dir.join("cert.pem"), key: dir.join("key.pem") });
        let mut kcfg = Config::with_home(&home);
        kcfg.python = Some(venv_python().to_string_lossy().into_owned());
        let backend = RealBackend::new(kcfg).unwrap();
        let handle = serve(cfg, Arc::new(backend)).await.expect("serve");
        let port = handle.local_addr.port();
        let http = reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(cert.cert.pem().as_bytes()).unwrap())
            .resolve("localhost", handle.local_addr)
            .build()
            .unwrap();
        Manager { token: handle.token.clone(), handle: Some(handle), port, der: cert.cert.der().to_vec(), http }
    }

    async fn call(&self, token: &str, method: reqwest::Method, path: &str, body: Option<Value>) -> (u16, Value) {
        let mut rb = self
            .http
            .request(method, format!("https://localhost:{}{path}", self.port))
            .bearer_auth(token)
            .header("X-DarkPyonix-Client", "ws-tls-01");
        if let Some(b) = body {
            rb = rb.header("content-type", "application/json").body(serde_json::to_vec(&b).unwrap());
        }
        let resp = rb.send().await.expect("request");
        let status = resp.status().as_u16();
        let bytes = resp.bytes().await.expect("body");
        (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    /// A TLS connection that offers only HTTP/1.1, the only protocol a WebSocket can use here.
    async fn tls(&self) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
        let mut roots = RootCertStore::empty();
        roots.add(self.der.clone().into()).unwrap();
        let mut cfg = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", self.port)).await.unwrap();
        tokio_rustls::TlsConnector::from(Arc::new(cfg))
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .expect("TLS handshake with the self-signed certificate")
    }

    async fn stop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.shutdown();
            h.wait().await;
        }
    }
}

// ---------------------------------------------------------------- minimal WebSocket client

/// A raw RFC 6455 client over any byte stream (here TLS): HTTP/1.1 upgrade, masked text frames out,
/// unmasked frames in.
struct Ws<S> {
    stream: S,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Ws<S> {
    async fn upgrade(mut stream: S, port: u16, kernel_id: &str, token: &str, client_id: &str) -> Ws<S> {
        let req = format!(
            "GET /api/ws/kernels/{kernel_id}?token={token}&client_id={client_id} HTTP/1.1\r\n\
             Host: localhost:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
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
        // The accept key for this nonce is fixed by RFC 6455: the kernel computed it.
        assert!(head.contains("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="), "wrong Sec-WebSocket-Accept: {head}");
        Ws { stream }
    }

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
        self.stream.flush().await.unwrap();
    }

    /// The next text message as JSON; control frames are skipped, `None` once the socket is closed.
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
                _ => {}
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
        let mut payload = vec![0u8; len];
        self.stream.read_exact(&mut payload).await.ok()?;
        Some((fin, opcode, payload))
    }

    /// Send `request` and return the first reply of type `reply`, or any `error` message.
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

/// FR-S9 over TLS against a real kernel: history filtered by the label, `start_typing` needing
/// `edit`, and a revoked share closing its own socket while another stays open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_fr_s9_ws_over_tls_real_kernel_history_typing_and_revocation() {
    use reqwest::Method;
    let dir = scratch("tls");
    let reaper = Reaper::default();
    let mut m = Manager::start(&dir).await;
    let nb = dir.join("shared.py");
    std::fs::write(&nb, "# %%\nprint('visible-' + 'output')\n").unwrap();
    let (status, k) = m.call(&m.token, Method::POST, "/api/kernels", Some(json!({"path": nb.to_string_lossy()}))).await;
    assert!(status == 200 || status == 201, "start kernel: {status} {k}");
    reaper.add(k["pid"].as_u64().unwrap() as u32);
    let kid = k["kernel_id"].as_str().unwrap().to_string();
    let b = format!("/api/kernels/{kid}");
    let master = m.token.clone();

    let (status, body) = m.call(&master, Method::POST, &format!("{b}/runs"), Some(json!({"mode": "all"}))).await;
    assert_eq!(status, 202, "{body}");
    assert_eq!(wait_run_done(&m, &kid).await["status"], "ok");
    let (_, doc) = m.call(&master, Method::GET, &format!("{b}/document"), None).await;
    let cell_id = doc["cells"]
        .as_array()
        .and_then(|c| c.iter().find_map(|c| c["cell_id"].as_str().map(String::from)))
        .expect("a cell id");

    // read; read+history; read+history+edit
    let mut shares: Vec<(&str, Vec<&str>, String, String)> = Vec::new();
    for (label, caps) in [
        ("read", vec!["read"]),
        ("history", vec!["read", "history"]),
        ("edit", vec!["read", "history", "edit"]),
    ] {
        let (status, body) = m
            .call(&master, Method::POST, &format!("{b}/shares"), Some(json!({"capabilities": caps, "label": label})))
            .await;
        assert_eq!(status, 201, "{body}");
        shares.push((label, caps, body["token"].as_str().unwrap().to_string(), body["share_id"].as_str().unwrap().to_string()));
    }

    // History and typing, each by its own label, over the TLS pump.
    for (label, caps, token, _) in &shares {
        let mut ws = Ws::upgrade(m.tls().await, m.port, &kid, token, "ws-tls-01").await;
        let hist = ws.ask(json!({"type": "request_history", "kernel_id": kid}), "history_data").await;
        assert_eq!(hist["type"], "history_data", "{label}: {hist}");
        let entries = hist["history"].as_array().unwrap_or_else(|| panic!("{label}: {hist}"));
        if caps.contains(&"history") {
            assert!(!entries.is_empty(), "{label}: history expected: {hist}");
        } else {
            assert!(entries.is_empty(), "{label}: history must be empty: {hist}");
        }
        let typing = ws
            .ask(json!({"type": "start_typing", "cell_id": cell_id, "user_id": "ws-tls"}), "cell_locked")
            .await;
        if caps.contains(&"edit") {
            assert_eq!(typing["type"], "cell_locked", "{label}: {typing}");
        } else {
            assert_eq!(typing["error"], "INSUFFICIENT_PERMISSION", "{label}: {typing}");
        }
    }

    // Revocation: the edit share's socket ends, the master's stays.
    let (_, _, edit_token, edit_id) = shares.last().unwrap().clone();
    let mut ws_edit = Ws::upgrade(m.tls().await, m.port, &kid, &edit_token, "ws-tls-02").await;
    let mut ws_master = Ws::upgrade(m.tls().await, m.port, &kid, &master, "ws-tls-03").await;
    assert_eq!(ws_edit.ask(json!({"type": "request_code"}), "code_data").await["type"], "code_data");
    let (status, body) = m.call(&master, Method::DELETE, &format!("{b}/shares/{edit_id}"), None).await;
    assert_eq!(status, 204, "{body}");
    let mut closed = false;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), ws_edit.next_json(Duration::from_secs(2))).await {
            Ok(None) => {
                closed = true;
                break;
            }
            Ok(Some(_)) => {}   // events still queued before the close
            Err(_) => {}
        }
    }
    assert!(closed, "the revoked share's WebSocket must end");
    let hist = ws_master.ask(json!({"type": "request_history", "kernel_id": kid}), "history_data").await;
    assert_eq!(hist["type"], "history_data", "{hist}");

    drop(reaper);
    m.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}
