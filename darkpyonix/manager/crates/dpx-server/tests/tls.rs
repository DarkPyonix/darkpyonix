//! TLS smoke test with a self-signed certificate: HTTP/1.1 and HTTP/2 via ALPN (INTENT D10).

mod common;

use std::sync::Arc;

use bytes::Bytes;
use common::*;
use dpx_server::{Mode, TlsConfig};
use http_body_util::{BodyExt, Empty};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio_rustls::rustls::{self, pki_types::ServerName, RootCertStore};

async fn connect(addr: std::net::SocketAddr, cert_der: &[u8], alpn: &[u8]) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
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

fn get(path: &str, token: &str) -> hyper::Request<Empty<Bytes>> {
    hyper::Request::get(format!("https://localhost{path}"))
        .header("authorization", format!("Bearer {token}"))
        .body(Empty::new())
        .unwrap()
}

#[tokio::test]
async fn test_d10_tls_serves_http1_and_http2_via_alpn() {
    let dir = scratch("tls");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    std::fs::write(dir.join("cert.pem"), cert.cert.pem()).unwrap();
    std::fs::write(dir.join("key.pem"), cert.signing_key.serialize_pem()).unwrap();
    let tls = TlsConfig::PemFiles { cert: dir.join("cert.pem"), key: dir.join("key.pem") };
    let s = TestServer::start_with(Mode::Dedicated, |c| c.tls = Some(tls)).await;
    assert!(s.url.starts_with("https://127.0.0.1:"));
    let addr = s.handle.as_ref().unwrap().local_addr;
    let der = cert.cert.der().to_vec();

    // HTTP/2
    let io = TokioIo::new(connect(addr, &der, b"h2").await);
    let (mut h2, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io).await.unwrap();
    tokio::spawn(conn);
    let r = h2.send_request(get("/api/manager", &s.token)).await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.version(), hyper::Version::HTTP_2);
    let body: serde_json::Value = serde_json::from_slice(&r.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["mode"], "dedicated");
    let r = h2.send_request(get("/api/kernels", "wrong")).await.unwrap();
    assert_eq!(r.status(), 401);

    // HTTP/1.1
    let io = TokioIo::new(connect(addr, &der, b"http/1.1").await);
    let (mut h1, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);
    let r = h1.send_request(get("/health", "")).await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.version(), hyper::Version::HTTP_11);

    // ACME is designed for but not implemented: refused at startup.
    let acme = TlsConfig::Acme {
        domains: vec!["x.example".into()],
        contact: None,
        cache_dir: dir.join("acme"),
        directory_url: "https://acme.invalid/directory".into(),
    };
    let mut cfg = dpx_server::ServerConfig::dedicated(dir.join("home2"), "127.0.0.1", 0);
    cfg.tls = Some(acme);
    assert!(dpx_server::serve(cfg, FakeBackend::new()).await.is_err());
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn test_d10_plain_http2_prior_knowledge() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let tcp = tokio::net::TcpStream::connect(s.handle.as_ref().unwrap().local_addr).await.unwrap();
    let (mut h2, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(tcp)).await.unwrap();
    tokio::spawn(conn);
    let req = hyper::Request::get(format!("{}/api/kernels", s.url))
        .header("authorization", format!("Bearer {}", s.token))
        .body(Empty::<Bytes>::new())
        .unwrap();
    let r = h2.send_request(req).await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.version(), hyper::Version::HTTP_2);
}
