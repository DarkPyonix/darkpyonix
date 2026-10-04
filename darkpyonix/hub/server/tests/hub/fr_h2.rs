//! FR-H2 address directory through iroh's own pkarr publisher and resolver.

use std::time::Duration;

use iroh::SecretKey;
use iroh_dns::endpoint_info::EndpointInfo;
use serde_json::Value;

use crate::common::*;

#[tokio::test]
async fn test_fr_h2_publish_and_resolve_with_stock_iroh_lookup() {
    let hub = start_hub("fr_h2_lookup").await;
    let account = hub.create_account().await;
    let key_a = SecretKey::generate();
    let key_b = SecretKey::generate();
    let token_a = hub.register(&account, &key_a, "main_server").await;
    let token_b = hub.register(&account, &key_b, "computer").await;

    let opts = |token: String| EndpointOptions {
        relay: Some(hub.relay_config()),
        loopback_ip: true,
        directory_token: Some(token),
    };
    let a = endpoint(&hub, key_a.clone(), opts(token_a.clone())).await;
    let _server = spawn_server(a.clone());
    let b = endpoint(&hub, key_b.clone(), opts(token_b.clone())).await;

    // A's signed record reaches the hub with its relay URL and its loopback address.
    let relay = hub.relay_url.to_string();
    let record = hub
        .wait_record(&token_b, key_a.public(), |r: &Value| {
            r["relay_urls"]
                .as_array()
                .is_some_and(|u| u.iter().any(|u| u == relay.as_str()))
                && r["direct_addresses"].as_array().is_some_and(|d| {
                    d.iter()
                        .any(|a| a.as_str().is_some_and(|a| a.starts_with("127.0.0.1:")))
                })
        })
        .await;
    assert_eq!(record["endpoint_id"], key_a.public().to_string());

    // B dials A by endpoint id alone; iroh's stock PkarrResolver asks the hub.
    let conn = tokio::time::timeout(Duration::from_secs(15), b.connect(key_a.public(), ALPN))
        .await
        .expect("connect timed out")
        .expect("connect by id");
    assert_eq!(
        echo(&conn, b"resolved through the hub").await,
        b"resolved through the hub"
    );
    conn.close(0u32.into(), b"done");

    // The raw pkarr GET is the same packet, verifiable end to end with A's key.
    let url = hub.url(&format!(
        "/pkarr/{}?token={token_b}",
        key_a.public().to_z32()
    ));
    let res = hub.client.get(url).send().await.unwrap();
    assert_eq!(res.status(), 200);
    let payload = res.bytes().await.unwrap();
    let packet =
        iroh_dns::pkarr::SignedPacket::from_relay_payload(&key_a.public(), &payload).unwrap();
    let info = EndpointInfo::from_pkarr_signed_packet(&packet).unwrap();
    assert!(info.relay_urls().any(|u| u.to_string() == relay));

    a.close().await;
    b.close().await;
}

#[tokio::test]
async fn test_fr_h2_directory_rejects_unregistered_and_foreign() {
    let hub = start_hub("fr_h2_reject").await;
    let mine = hub.create_account().await;
    let theirs = hub.create_account().await;
    let key = SecretKey::generate();
    let token = hub.register(&mine, &key, "computer").await;
    let their_key = SecretKey::generate();
    let their_token = hub.register(&theirs, &their_key, "computer").await;

    let packet = |k: &SecretKey| {
        EndpointInfo::new(k.public())
            .with_relay_url(hub.relay_url.clone())
            .to_pkarr_signed_packet(k, 30)
            .unwrap()
    };
    let put = |k: &SecretKey, body: Vec<u8>| {
        hub.client
            .put(hub.url(&format!("/pkarr/{}", k.public().to_z32())))
            .body(body)
            .send()
    };

    // An unregistered key cannot publish.
    let stranger = SecretKey::generate();
    let res = put(&stranger, packet(&stranger).to_relay_payload())
        .await
        .unwrap();
    assert_eq!(res.status(), 403);

    // A tampered payload is refused.
    let mut tampered = packet(&key).to_relay_payload();
    let last = tampered.len() - 1;
    tampered[last] ^= 0x01;
    assert_eq!(put(&key, tampered).await.unwrap().status(), 400);

    // A registered device publishes; replaying the same packet is not newer.
    let first = packet(&key).to_relay_payload();
    assert_eq!(put(&key, first.clone()).await.unwrap().status(), 204);
    assert_eq!(put(&key, first).await.unwrap().status(), 409);

    let get = |token: Option<&str>| {
        let mut url = hub.url(&format!("/pkarr/{}", key.public().to_z32()));
        if let Some(t) = token {
            url.push_str(&format!("?token={t}"));
        }
        hub.client.get(url).send()
    };
    assert_eq!(get(None).await.unwrap().status(), 401);
    assert_eq!(get(Some(&their_token)).await.unwrap().status(), 404);
    assert_eq!(get(Some(&token)).await.unwrap().status(), 200);
    assert_eq!(get(Some(&mine.1)).await.unwrap().status(), 200);

    let path = format!("/devices/{}/addresses", key.public());
    assert_eq!(hub.get_json(&path, &their_token).await.0, 404);
    let (status, body) = hub.get_json(&path, &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["relay_urls"][0], hub.relay_url.to_string());
}
