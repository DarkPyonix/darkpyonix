//! NFR-H1: the hub only ever carries ciphertext between devices.

use std::sync::{Arc, Mutex};

use iroh::SecretKey;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::common::*;

const MARKER: &[u8] = b"DARKPYONIX-NFR-H1-PLAINTEXT-MARKER";

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Copies one direction, recording every byte.
async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    log: Arc<Mutex<Vec<u8>>>,
) {
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        log.lock().unwrap().extend_from_slice(&buf[..n]);
        if to.write_all(&buf[..n]).await.is_err() {
            break;
        }
    }
    let _ = to.shutdown().await;
}

#[tokio::test]
async fn test_nfr_h1_relay_sees_only_ciphertext() {
    // A recording TCP proxy in front of a plain-HTTP hub: even with no TLS to the relay,
    // what crosses the hub must not contain the application's plaintext.
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy.local_addr().unwrap();
    let hub = start_hub_with(
        "nfr_h1",
        HubOptions {
            public_url: Some(format!("http://{proxy_addr}").parse().unwrap()),
            ..Default::default()
        },
    )
    .await;
    let target = hub.hub.addr();
    let log = Arc::new(Mutex::new(Vec::new()));
    let proxy_log = log.clone();
    let _proxy = tokio::spawn(async move {
        while let Ok((inbound, _)) = proxy.accept().await {
            let log = proxy_log.clone();
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect(target).await else {
                    return;
                };
                let (in_read, in_write) = inbound.into_split();
                let (out_read, out_write) = outbound.into_split();
                tokio::join!(
                    pump(in_read, out_write, log.clone()),
                    pump(out_read, in_write, log),
                );
            });
        }
    });

    let account = hub.create_account().await;
    let key_a = SecretKey::generate();
    let key_b = SecretKey::generate();
    hub.register(&account, &key_a, "main_server").await;
    hub.register(&account, &key_b, "computer").await;
    let relay_only = || EndpointOptions {
        relay: Some(hub.relay_config()),
        loopback_ip: false,
        directory_token: None,
    };
    let a = endpoint(&hub, key_a, relay_only()).await;
    let _server = spawn_server(a.clone());
    let b = endpoint(&hub, key_b, relay_only()).await;

    let conn = b
        .connect(via_relay(a.id(), &hub.relay_url), ALPN)
        .await
        .unwrap();
    assert!(
        wait_selected_path(&conn, true).await,
        "selected path is not the relay"
    );
    let payload = MARKER.repeat(64);
    assert_eq!(
        echo(&conn, &payload).await,
        payload,
        "the peer receives the plaintext"
    );
    conn.close(0u32.into(), b"done");
    a.close().await;
    b.close().await;

    let seen = log.lock().unwrap().clone();
    assert!(
        contains(&seen, b"GET /relay"),
        "the proxy did not see the relay traffic"
    );
    assert!(!contains(&seen, MARKER), "plaintext crossed the hub");
}
