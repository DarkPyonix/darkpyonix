//! Reverse proxy routes (INTENT D10: no nginx in front of VS Code serve-web / ember).

mod common;

use std::time::Duration;

use common::*;
use dpx_server::{Mode, ProxyRoute};
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;

/// A tiny HTTP/1.1 upstream: echoes method, path, Host and X-Forwarded-* as plain text, and
/// accepts WebSocket upgrades that echo every message.
async fn upstream() -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = l.accept().await else { return };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let mut n = 0;
                while !buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                    let m = s.read(&mut buf[n..]).await.unwrap();
                    if m == 0 {
                        return;
                    }
                    n += m;
                }
                let head = String::from_utf8_lossy(&buf[..n]).to_string();
                if head.to_ascii_lowercase().contains("upgrade: websocket") {
                    // Hand the raw stream (request already read) to tungstenite.
                    let key = head
                        .lines()
                        .find_map(|l| l.to_ascii_lowercase().starts_with("sec-websocket-key:").then(|| l[18..].trim().to_string()))
                        .unwrap();
                    let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
                    let resp = format!(
                        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
                    );
                    s.write_all(resp.as_bytes()).await.unwrap();
                    let ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
                        s,
                        tokio_tungstenite::tungstenite::protocol::Role::Server,
                        None,
                    )
                    .await;
                    let (mut tx, mut rx) = ws.split();
                    while let Some(Ok(m)) = rx.next().await {
                        if m.is_text() || m.is_binary() {
                            tx.send(m).await.unwrap();
                        }
                    }
                    return;
                }
                let first = head.lines().next().unwrap_or_default().to_string();
                let pick = |name: &str| {
                    head.lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.eq_ignore_ascii_case(name).then(|| v.trim().to_string())
                        })
                        .unwrap_or_default()
                };
                let body = format!(
                    "{first}\nhost={}\nxff={}\nproto={}\nconnection={}\n",
                    pick("host"),
                    pick("x-forwarded-for"),
                    pick("x-forwarded-proto"),
                    pick("connection")
                );
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nX-Upstream: yes\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(resp.as_bytes()).await;
            });
        }
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn test_d10_proxy_forwards_http_and_preserves_host() {
    let up = upstream().await;
    let mut stripped = ProxyRoute::new("/ember", up.clone());
    stripped.strip_prefix = true;
    let s = TestServer::start_with(Mode::Dedicated, |c| c.proxies = vec![ProxyRoute::new("/vscode", up.clone()), stripped]).await;

    // No manager token needed: the upstream authenticates itself.
    let r = reqwest::Client::new()
        .get(format!("{}/vscode/static/app.js?x=1", s.url))
        .header("Host", "ide.example.test")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["x-upstream"], "yes");
    let body = r.text().await.unwrap();
    assert!(body.starts_with("GET /vscode/static/app.js?x=1 HTTP/1.1"), "{body}");
    assert!(body.contains("host=ide.example.test\n"), "{body}");
    assert!(body.contains("xff=127.0.0.1\n"), "{body}");
    assert!(body.contains("proto=http\n"), "{body}");

    let body = reqwest::get(format!("{}/ember/index.html", s.url)).await.unwrap().text().await.unwrap();
    assert!(body.starts_with("GET /index.html HTTP/1.1"), "{body}");
    let body = reqwest::get(format!("{}/ember", s.url)).await.unwrap().text().await.unwrap();
    assert!(body.starts_with("GET / HTTP/1.1"), "{body}");

    // Prefix match is by path segment, and the API is still guarded.
    s.anon().get("/vscodex").await.error(401, "unauthorized");
    s.anon().get("/api/kernels").await.error(401, "unauthorized");
}

#[tokio::test]
async fn test_d10_proxy_passes_websocket_upgrades_through() {
    let up = upstream().await;
    let s = TestServer::start_with(Mode::Ephemeral, |c| {
        c.proxies = vec![ProxyRoute::new("/vscode", up.clone())];
        c.idle_timeout = Some(Duration::from_millis(500));
    })
    .await;
    let ws_url = format!("{}/vscode/socket", s.url.replace("http://", "ws://"));
    let (mut ws, resp) = tokio_tungstenite::connect_async(ws_url).await.expect("ws connect");
    assert_eq!(resp.status(), 101);
    for i in 0..3 {
        ws.send(Message::text(format!("ping {i}"))).await.unwrap();
        let echo = tokio::time::timeout(Duration::from_secs(5), ws.next()).await.unwrap().unwrap().unwrap();
        assert_eq!(echo, Message::text(format!("ping {i}")));
    }
    // An open tunnel counts as activity: the idle watchdog must not fire.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    ws.send(Message::binary(vec![1u8, 2, 3])).await.unwrap();
    let echo = tokio::time::timeout(Duration::from_secs(5), ws.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(echo, Message::binary(vec![1u8, 2, 3]));
    assert!(s.handle.as_ref().unwrap().registry_file.as_ref().unwrap().exists());
}

#[tokio::test]
async fn test_d10_proxy_upstream_down_is_502() {
    let s = TestServer::start_with(Mode::Dedicated, |c| c.proxies = vec![ProxyRoute::new("/dead", "http://127.0.0.1:9")]).await;
    let r = reqwest::get(format!("{}/dead/x", s.url)).await.unwrap();
    assert_eq!(r.status(), 502);
    let body: serde_json::Value = r.json().await.unwrap();
    assert!(body["error"]["message"].is_string());
}
