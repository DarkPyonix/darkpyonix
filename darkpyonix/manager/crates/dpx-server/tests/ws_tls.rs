//! WebSocket sync over TLS (SPEC FR-S9, FR-M6): HTTP/1.1 upgrade through the socket-pair pump,
//! and HTTP/2 (no RFC 8441 extended CONNECT) refused with a clear error.
//!
//! The fake kernel answers `101` on the pumped socket and speaks WebSocket frames with
//! tungstenite. The real Python kernel behind TLS is not covered here.
#![cfg(unix)]

mod common;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use common::{kernel_id_for, scratch, FakeBackend};
use dpx_core::{AdoptRequest, Ensured, EventStream, KernelBackend, KernelInfo, Result, StartKernel};
use dpx_server::{serve, ServerConfig, TlsConfig};
use futures::{SinkExt, StreamExt};
use http_body_util::Empty;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde_json::Value;
use tokio_rustls::rustls::{self, pki_types::ServerName, RootCertStore};
use tokio_tungstenite::tungstenite::{self, handshake::derive_accept_key, protocol::Role, Message};

struct WsBackend {
    inner: Arc<FakeBackend>,
}

#[async_trait]
impl KernelBackend for WsBackend {
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
        assert_eq!(req.transport, "pump", "TLS has no descriptor of its own to hand over");
        let request = String::from_utf8_lossy(&req.request).into_owned();
        let key = request
            .lines()
            .find_map(|l| l.to_ascii_lowercase().starts_with("sec-websocket-key:").then(|| l.split_once(':').unwrap().1.trim().to_string()))
            .expect("sec-websocket-key reached the kernel");
        let sock = std::os::unix::net::UnixStream::from(req.fd);
        sock.set_nonblocking(false).unwrap();
        std::thread::spawn(move || {
            use std::io::Write;
            let mut sock = sock;
            let head = format!(
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
                derive_accept_key(key.as_bytes())
            );
            sock.write_all(head.as_bytes()).unwrap();
            let mut ws = tungstenite::WebSocket::from_raw_socket(sock, Role::Server, None);
            // One request frame is answered; then the client closes and the kernel finishes the close.
            while let Ok(msg) = ws.read() {
                match msg {
                    Message::Text(t) => {
                        ws.send(Message::text(format!("echo:{t}"))).unwrap();
                    }
                    Message::Close(_) => {
                        let _ = ws.flush();
                        break;
                    }
                    _ => {}
                }
            }
        });
        Ok("st_ws".into())
    }
}

async fn tls_connect(
    addr: std::net::SocketAddr,
    cert_der: &[u8],
    alpn: &[u8],
) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
    let mut roots = RootCertStore::empty();
    roots.add(cert_der.to_vec().into()).unwrap();
    let mut cfg = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    cfg.alpn_protocols = vec![alpn.to_vec()];
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let tls = tokio_rustls::TlsConnector::from(Arc::new(cfg))
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .unwrap();
    assert_eq!(tls.get_ref().1.alpn_protocol(), Some(alpn));
    tls
}

struct Rig {
    handle: dpx_server::ServerHandle,
    kid: String,
    der: Vec<u8>,
}

async fn start() -> Rig {
    let dir = scratch("ws_tls");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    std::fs::write(dir.join("cert.pem"), cert.cert.pem()).unwrap();
    std::fs::write(dir.join("key.pem"), cert.signing_key.serialize_pem()).unwrap();
    let mut cfg = ServerConfig::dedicated(dir.join("home"), "127.0.0.1", 0);
    cfg.idle_timeout = None;
    cfg.tls = Some(TlsConfig::PemFiles { cert: dir.join("cert.pem"), key: dir.join("key.pem") });
    let inner = FakeBackend::new();
    let nb = dir.join("train.py");
    std::fs::write(&nb, "print('hello')\n").unwrap();
    let path = nb.canonicalize().unwrap().to_string_lossy().into_owned();
    let kid = inner.add(&path);
    assert_eq!(kid, kernel_id_for(&path));
    let handle = serve(cfg, Arc::new(WsBackend { inner })).await.expect("serve");
    Rig { handle, kid, der: cert.cert.der().to_vec() }
}

#[tokio::test]
async fn test_fr_s9_ws_over_tls_http1_upgrade_exchanges_frames_and_closes() {
    let rig = start().await;
    let addr = rig.handle.local_addr;
    let tls = tls_connect(addr, &rig.der, b"http/1.1").await;
    let url = format!("wss://localhost:{}/api/ws/kernels/{}?token={}", addr.port(), rig.kid, rig.handle.token);
    let (mut ws, resp) = tokio_tungstenite::client_async(url, tls).await.expect("101 and a valid accept key");
    assert_eq!(resp.status(), 101);
    ws.send(Message::text("request_code")).await.unwrap();
    let reply = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next()).await.expect("reply").unwrap().unwrap();
    assert_eq!(reply, Message::text("echo:request_code"));
    ws.close(None).await.unwrap();
    // The kernel completes the close handshake; the stream then ends.
    let rest = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(m) = ws.next().await {
            if m.is_err() {
                break;
            }
        }
    })
    .await;
    assert!(rest.is_ok(), "the close handshake did not finish");
}

#[tokio::test]
async fn test_fr_s9_ws_over_http2_is_refused_without_reaching_the_kernel() {
    let rig = start().await;
    let addr = rig.handle.local_addr;
    let io = TokioIo::new(tls_connect(addr, &rig.der, b"h2").await);
    let (mut h2, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io).await.unwrap();
    tokio::spawn(conn);
    let req = hyper::Request::get(format!("https://localhost/api/ws/kernels/{}", rig.kid))
        .header("authorization", format!("Bearer {}", rig.handle.token))
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("upgrade", "websocket")
        .body(Empty::<Bytes>::new())
        .unwrap();
    let r = h2.send_request(req).await.unwrap();
    assert_eq!(r.status(), 400, "HTTP/2 WebSocket (RFC 8441) is not supported");
}
