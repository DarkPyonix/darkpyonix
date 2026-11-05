//! NFR-H1: the relay host only ever carries ciphertext.
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use iroh::{endpoint::presets, Endpoint, RelayMode, SecretKey};
use iroh_base::EndpointAddr;
use iroh_relay::RelayMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::common::*;

const ALPN: &[u8] = b"darkpyonix/nfr-h1";

/// Bytes seen on the wire between the relay clients and the relay, one buffer per direction.
#[derive(Default)]
struct Tap {
    to_relay: Vec<u8>,
    from_relay: Vec<u8>,
}

/// A transparent TCP proxy in front of the relay that records every byte in both directions.
async fn start_tap(upstream: SocketAddr) -> (SocketAddr, Arc<Mutex<Tap>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tap = Arc::new(Mutex::new(Tap::default()));
    let shared = tap.clone();
    tokio::spawn(async move {
        loop {
            let (client, _) = listener.accept().await.unwrap();
            let server = TcpStream::connect(upstream).await.unwrap();
            let (mut cr, mut cw) = client.into_split();
            let (mut sr, mut sw) = server.into_split();
            let up = shared.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                loop {
                    match cr.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            up.lock().unwrap().to_relay.extend_from_slice(&buf[..n]);
                            if sw.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });
            let down = shared.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                loop {
                    match sr.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            down.lock().unwrap().from_relay.extend_from_slice(&buf[..n]);
                            if cw.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });
        }
    });
    (addr, tap)
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

async fn relay_only_endpoint(map: RelayMap, key: SecretKey) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .secret_key(key)
        .alpns(vec![ALPN.to_vec()])
        .relay_mode(RelayMode::Custom(map))
        .clear_ip_transports()
        .bind()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_nfr_h1_relay_sees_only_ciphertext() {
    // The relay runs without TLS (plain HTTP), so the tap sees exactly what the relay sees.
    let hub = start_hub("nfr-h1").await;
    let (tap_addr, tap) = start_tap(hub.hub.addr()).await;
    let relay_url: iroh::RelayUrl = format!("http://{tap_addr}").parse().unwrap();
    let map = RelayMap::from(relay_url.clone());

    // 64 distinctive bytes that cannot occur by chance and are not repetitive enough to be compressed away.
    let marker: Vec<u8> = (0..64u8).map(|i| i.wrapping_mul(37).wrapping_add(11) ^ 0xA5).collect();
    let marker_text = b"DARKPYONIX-NFR-H1-PLAINTEXT-MARKER-".to_vec();
    let payload: Vec<u8> = [marker_text.clone(), marker.clone()].concat();

    let b_key = SecretKey::generate();
    let b_id = b_key.public();
    let b = relay_only_endpoint(map.clone(), b_key).await;
    // B is reachable through the relay once the hub saw it come online.
    wait_for(|| !hub.calls.lock().unwrap().presence.is_empty()).await;
    let a = relay_only_endpoint(map, SecretKey::generate()).await;

    let expected = payload.clone();
    let accept = tokio::spawn(async move {
        let conn = b.accept().await.unwrap().await.unwrap();
        let mut recv = conn.accept_uni().await.unwrap();
        let got = recv.read_to_end(4096).await.unwrap();
        conn.close(0u32.into(), b"done");
        b.close().await;
        got
    });

    let dst = EndpointAddr::new(b_id).with_relay_url(relay_url);
    let send = async {
        let conn = a.connect(dst, ALPN).await.unwrap();
        let mut stream = conn.open_uni().await.unwrap();
        stream.write_all(&payload).await.unwrap();
        stream.finish().unwrap();
        conn.closed().await;
        a.close().await;
    };
    let got = tokio::time::timeout(Duration::from_secs(20), async {
        let (_, got) = tokio::join!(send, accept);
        got.unwrap()
    })
    .await
    .expect("transfer through the relay timed out");

    // (a) the far end received the marker intact.
    assert_eq!(got, expected);

    // (b) the marker never crossed the relay in the clear, in either direction.
    let tap = tap.lock().unwrap();
    // Control: the tap does see plaintext at the HTTP layer, so an empty result is not a blind tap.
    assert!(contains(&tap.to_relay, b"HTTP/1.1"), "tap saw no relay handshake");
    assert!(!tap.from_relay.is_empty());
    for part in [&marker_text, &marker, &payload] {
        assert!(!contains(&tap.to_relay, part), "plaintext reached the relay");
        assert!(!contains(&tap.from_relay, part), "plaintext left the relay");
    }
}
