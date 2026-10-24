//! Stream handoff on the manager side (SPEC FR-M6, FR-M3, FR-K9; PROTOCOL §3.7).
//!
//! A backend with a fake 'kernel' that receives the handed-off descriptor: it writes an SSE
//! answer on it and keeps it, like the Python kernel does. The real Python kernel and a real
//! `SIGKILL` of the manager are in `crates/darkpyonix/tests/handoff.rs`.
#![cfg(unix)]

mod common;

use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use common::{kernel_id_for, scratch, FakeBackend};
use dpx_core::{AdoptRequest, Ensured, EventStream, KernelBackend, KernelInfo, Result, StartKernel};
use dpx_server::{serve, ExitReason, ServerConfig, ServerHandle};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Adopted {
    kind: String,
    transport: String,
    request: String,
    label: Value,
}

enum Kept {
    Tcp(std::net::TcpStream),
    Unix(std::os::unix::net::UnixStream),
}

impl Kept {
    fn write_all(&mut self, b: &[u8]) {
        let _ = match self {
            Kept::Tcp(s) => s.write_all(b),
            Kept::Unix(s) => s.write_all(b),
        };
    }
}

/// The fake backend plus a fake kernel end for `adopt`.
struct HandoffBackend {
    inner: Arc<FakeBackend>,
    adopted: Mutex<Vec<Adopted>>,
    streams: Mutex<Vec<(Option<String>, Kept)>>,
    closes: Mutex<Vec<Value>>,
}

impl HandoffBackend {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: FakeBackend::new(),
            adopted: Mutex::new(Vec::new()),
            streams: Mutex::new(Vec::new()),
            closes: Mutex::new(Vec::new()),
        })
    }
    fn adopted_count(&self) -> usize {
        self.adopted.lock().unwrap().len()
    }
    fn adopted(&self, i: usize) -> (String, String, String, Value) {
        let a = &self.adopted.lock().unwrap()[i];
        (a.kind.clone(), a.transport.clone(), a.request.clone(), a.label.clone())
    }
    /// What the kernel writes later on every stream it holds.
    fn write_all_streams(&self, b: &[u8]) {
        for (_, s) in self.streams.lock().unwrap().iter_mut() {
            s.write_all(b);
        }
    }
}

#[async_trait]
impl KernelBackend for HandoffBackend {
    async fn list(&self, refresh: bool) -> Result<Vec<KernelInfo>> {
        self.inner.list(refresh).await
    }
    async fn get(&self, kernel_id: &str) -> Result<KernelInfo> {
        self.inner.get(kernel_id).await
    }
    async fn ensure(&self, req: StartKernel) -> Result<Ensured> {
        self.inner.ensure(req).await
    }
    async fn request(&self, kernel_id: &str, method: &str, params: Value) -> Result<Value> {
        if method == "streams.close" {
            let share = params["share_id"].as_str().map(String::from);
            self.closes.lock().unwrap().push(params);
            let mut streams = self.streams.lock().unwrap();
            let before = streams.len();
            streams.retain(|(s, _)| *s != share); // dropping a stream closes its socket
            return Ok(json!({ "closed": before - streams.len() }));
        }
        self.inner.request(kernel_id, method, params).await
    }
    async fn subscribe(&self, kernel_id: &str, since: Option<u64>) -> Result<EventStream> {
        self.inner.subscribe(kernel_id, since).await
    }
    async fn document(&self, path: &str, viewer_outputs: bool) -> Result<Value> {
        self.inner.document(path, viewer_outputs).await
    }
    async fn kill(&self, kernel_id: &str) -> Result<()> {
        self.inner.kill(kernel_id).await
    }
    fn supports_handoff(&self) -> bool {
        true
    }
    async fn adopt(&self, _kernel_id: &str, req: AdoptRequest) -> Result<String> {
        let mut kept = if req.transport == "pump" {
            Kept::Unix(std::os::unix::net::UnixStream::from(req.fd))
        } else {
            Kept::Tcp(std::net::TcpStream::from(req.fd))
        };
        let share = req.label["share_id"].as_str().map(String::from);
        self.adopted.lock().unwrap().push(Adopted {
            kind: req.kind.clone(),
            transport: req.transport.clone(),
            request: String::from_utf8_lossy(&req.request).into_owned(),
            label: req.label,
        });
        if req.kind == "wait" {
            let body = json!({"status": "ok", "next": null}).to_string();
            kept.write_all(
                format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len())
                    .as_bytes(),
            );
            return Ok("st_wait".into()); // dropped: closed after the answer
        }
        kept.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n: hello\n\n");
        self.streams.lock().unwrap().push((share, kept));
        Ok("st_1".into())
    }
}

struct Rig {
    handle: Option<ServerHandle>,
    backend: Arc<HandoffBackend>,
    kid: String,
    scratch: std::path::PathBuf,
}

async fn start(dedicated: bool, tweak: impl FnOnce(&mut ServerConfig)) -> Rig {
    let scratch = scratch("handoff");
    let home = scratch.join("home");
    let mut cfg = if dedicated {
        ServerConfig::dedicated(&home, "127.0.0.1", 0)
    } else {
        ServerConfig::ephemeral(&home)
    };
    cfg.idle_timeout = None;
    tweak(&mut cfg);
    let backend = HandoffBackend::new();
    let nb = scratch.join("train.py");
    std::fs::write(&nb, "print('hello')\n").unwrap();
    let path = nb.canonicalize().unwrap().to_string_lossy().into_owned();
    let kid = backend.inner.add(&path);
    assert_eq!(kid, kernel_id_for(&path));
    let handle = serve(cfg, backend.clone()).await.expect("serve");
    Rig { handle: Some(handle), backend, kid, scratch }
}

impl Rig {
    fn h(&self) -> &ServerHandle {
        self.handle.as_ref().unwrap()
    }
    async fn raw(&self, request: &str) -> tokio::net::TcpStream {
        let mut s = tokio::net::TcpStream::connect(self.h().local_addr).await.unwrap();
        s.write_all(request.as_bytes()).await.unwrap();
        s
    }
    fn events_request(&self, token: &str, query: &str) -> String {
        format!(
            "GET /api/kernels/{}/events{query} HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {token}\r\n\r\n",
            self.kid
        )
    }
    async fn stop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.shutdown();
            h.wait().await;
        }
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        if let Some(h) = &self.handle {
            h.shutdown();
        }
        let _ = std::fs::remove_dir_all(&self.scratch);
    }
}

async fn read_until(s: &mut tokio::net::TcpStream, needle: &str) -> String {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !String::from_utf8_lossy(&buf).contains(needle) {
        let n = tokio::time::timeout_at(deadline, s.read(&mut chunk))
            .await
            .unwrap_or_else(|_| panic!("timeout waiting for {needle:?}; got {:?}", String::from_utf8_lossy(&buf)))
            .unwrap();
        assert!(n > 0, "closed before {needle:?}; got {:?}", String::from_utf8_lossy(&buf));
        buf.extend_from_slice(&chunk[..n]);
    }
    String::from_utf8_lossy(&buf).into_owned()
}

async fn is_eof(s: &mut tokio::net::TcpStream, within: Duration) -> bool {
    let mut chunk = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + within;
    loop {
        match tokio::time::timeout_at(deadline, s.read(&mut chunk)).await {
            Err(_) => return false,
            Ok(Ok(0)) | Ok(Err(_)) => return true,
            Ok(Ok(_)) => {}
        }
    }
}

// ------------------------------------------------------------------ FR-M6

#[tokio::test]
async fn test_fr_m6_plain_stream_is_handed_off_and_survives_the_manager() {
    let mut rig = start(false, |_| {}).await;
    let token = rig.h().token.clone();
    let mut c = rig.raw(&rig.events_request(&token, "?client_id=dev_8f2c1a&token=in-query")).await;
    let seen = read_until(&mut c, ": hello").await;
    assert!(seen.starts_with("HTTP/1.1 200 OK") && seen.contains("text/event-stream"), "{seen}");

    assert_eq!(rig.backend.adopted_count(), 1);
    let (kind, transport, request, label) = rig.backend.adopted(0);
    assert_eq!((kind.as_str(), transport.as_str()), ("events", "fd"));
    assert!(request.starts_with(&format!("GET /api/kernels/{}/events?client_id=dev_8f2c1a HTTP/1.1", rig.kid)), "{request}");
    assert!(!request.contains(&token) && !request.contains("in-query"), "token leaked to the kernel: {request}");
    assert_eq!(label["capabilities"], json!(["read", "history", "execute", "edit", "manage"]));
    assert_eq!(label["client_id"], "dev_8f2c1a");
    assert!(label["share_id"].is_null());

    // The manager goes away; the kernel still writes on the same connection.
    rig.stop().await;
    rig.backend.write_all_streams(b"data: after the manager ended\n\n");
    let more = read_until(&mut c, "after the manager ended").await;
    assert!(more.contains("data: after the manager ended"));
}

#[tokio::test]
async fn test_fr_m6_unauthenticated_stream_never_reaches_kernel() {
    let rig = start(false, |_| {}).await;
    for req in [
        rig.events_request("wrong-token", ""),
        format!("GET /api/kernels/{}/events HTTP/1.1\r\nHost: x\r\n\r\n", rig.kid),
        format!("GET /api/kernels/{}/events?token=nope HTTP/1.1\r\nHost: x\r\n\r\n", rig.kid),
    ] {
        let mut c = rig.raw(&req).await;
        let seen = read_until(&mut c, "\r\n\r\n").await;
        assert!(seen.starts_with("HTTP/1.1 401"), "{seen}");
    }
    assert_eq!(rig.backend.adopted_count(), 0);
}

#[tokio::test]
async fn test_fr_m6_unknown_kernel_and_bad_parameters_are_answered_by_the_manager() {
    let rig = start(false, |_| {}).await;
    let token = rig.h().token.clone();
    let mut c = rig.raw(&format!(
        "GET /api/kernels/k_00000000000000000000/events HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {token}\r\n\r\n"
    ))
    .await;
    assert!(read_until(&mut c, "\r\n\r\n").await.starts_with("HTTP/1.1 404"));
    let mut c = rig.raw(&rig.events_request(&token, "?since=abc")).await;
    assert!(read_until(&mut c, "\r\n\r\n").await.starts_with("HTTP/1.1 400"));
    assert_eq!(rig.backend.adopted_count(), 0);
}

#[tokio::test]
async fn test_fr_m6_wait_is_handed_off_with_its_run_ref() {
    let rig = start(false, |_| {}).await;
    let token = rig.h().token.clone();
    let mut c = rig
        .raw(&format!(
            "GET /api/kernels/{}/runs/latest/wait?timeout=5 HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {token}\r\n\r\n",
            rig.kid
        ))
        .await;
    let seen = read_until(&mut c, "\"next\":null").await;
    assert!(seen.starts_with("HTTP/1.1 200 OK"), "{seen}");
    let (kind, _, request, _) = rig.backend.adopted(0);
    assert_eq!(kind, "wait");
    assert!(request.starts_with(&format!("GET /api/kernels/{}/runs/latest/wait?timeout=5 HTTP/1.1", rig.kid)));
}

#[tokio::test]
async fn test_fr_m6_tls_http2_stream_is_pumped_and_ends_with_manager() {
    // HTTP/2 without TLS (h2c) takes the same path as TLS and tunnels: no descriptor of the
    // client's own, so the kernel gets one end of a socket pair and the manager pumps bytes.
    let mut rig = start(false, |_| {}).await;
    let client = reqwest::Client::builder().http2_prior_knowledge().build().unwrap();
    let mut resp = client
        .get(format!("{}/api/kernels/{}/events?client_id=dev_8f2c1a", rig.h().url, rig.kid))
        .bearer_auth(&rig.h().token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.version(), reqwest::Version::HTTP_2);
    assert_eq!(resp.headers()["content-type"], "text/event-stream");
    let first = resp.chunk().await.unwrap().unwrap();
    assert_eq!(&first[..], b": hello\n\n");
    let (_, transport, request, _) = rig.backend.adopted(0);
    assert_eq!(transport, "pump");
    assert!(!request.contains(&rig.h().token), "token leaked to the kernel: {request}");

    rig.backend.write_all_streams(b"data: through the pump\n\n");
    let next = resp.chunk().await.unwrap().unwrap();
    assert_eq!(&next[..], b"data: through the pump\n\n");

    rig.stop().await; // pumped streams end with the manager
    let end = tokio::time::timeout(Duration::from_secs(5), resp.chunk()).await.expect("stream did not end");
    assert!(matches!(end, Ok(None) | Err(_)));
}

#[tokio::test]
async fn test_fr_m6_revoking_a_share_closes_its_streams() {
    let rig = start(true, |_| {}).await;
    let http = reqwest::Client::new();
    let admin = rig.h().token.clone();
    let share: Value = http
        .post(format!("{}/api/kernels/{}/shares", rig.h().url, rig.kid))
        .bearer_auth(&admin)
        .json(&json!({"permission": "viewer2", "label": "guest"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let (share_id, share_token) = (share["share_id"].as_str().unwrap().to_string(), share["token"].as_str().unwrap().to_string());

    let mut shared = rig.raw(&rig.events_request(&share_token, "")).await;
    let mut master = rig.raw(&rig.events_request(&admin, "")).await;
    read_until(&mut shared, ": hello").await;
    read_until(&mut master, ": hello").await;
    let (_, _, _, label) = rig.backend.adopted(0);
    assert_eq!(label["share_id"], share_id);
    assert_eq!(label["capabilities"], json!(["read", "history"]));

    let r = http
        .delete(format!("{}/api/kernels/{}/shares/{share_id}", rig.h().url, rig.kid))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    assert_eq!(rig.backend.closes.lock().unwrap().as_slice(), [json!({"share_id": share_id})]);
    assert!(is_eof(&mut shared, Duration::from_secs(1)).await, "share stream stayed open");
    rig.backend.write_all_streams(b"data: still here\n\n");
    read_until(&mut master, "still here").await;
}

#[tokio::test]
async fn test_fr_m6_a_viewer_without_history_cannot_wait() {
    let rig = start(true, |_| {}).await;
    let http = reqwest::Client::new();
    let share: Value = http
        .post(format!("{}/api/kernels/{}/shares", rig.h().url, rig.kid))
        .bearer_auth(&rig.h().token)
        .json(&json!({"permission": "viewer1"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = share["token"].as_str().unwrap();
    let mut c = rig
        .raw(&format!(
            "GET /api/kernels/{}/runs/latest/wait HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {token}\r\n\r\n",
            rig.kid
        ))
        .await;
    assert!(read_until(&mut c, "\r\n\r\n").await.starts_with("HTTP/1.1 403"));
    assert_eq!(rig.backend.adopted_count(), 0);
}

// ------------------------------------------------------------------ FR-M3

#[tokio::test]
async fn test_fr_m3_idle_counts_http_requests_only_not_handed_off_streams() {
    let mut rig = start(false, |c| c.idle_timeout = Some(Duration::from_millis(800))).await;
    let token = rig.h().token.clone();
    let mut c = rig.raw(&rig.events_request(&token, "")).await;
    read_until(&mut c, ": hello").await;
    let h = rig.handle.take().unwrap();
    let reason = tokio::time::timeout(Duration::from_secs(10), h.wait())
        .await
        .expect("an open handed-off stream kept the ephemeral manager alive");
    assert_eq!(reason, ExitReason::Idle);
    // The stream outlived the manager.
    rig.backend.write_all_streams(b"data: after idle exit\n\n");
    read_until(&mut c, "after idle exit").await;
}
