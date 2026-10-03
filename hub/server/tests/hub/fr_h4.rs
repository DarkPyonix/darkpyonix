//! FR-H4 share links: resolve to the hosting device, guest relay pass, viewer pages.

use std::time::Duration;

use iroh::{RelayUrl, SecretKey};
use serde_json::{json, Value};

use crate::common::*;

const SHARE: &str = "s_0f1e2d3c4b5a6978";

#[tokio::test]
async fn test_fr_h4_share_resolves_to_hosting_device() {
    let hub = start_hub("fr_h4_resolve").await;
    let account = hub.create_account().await;
    let host = SecretKey::generate();
    let host_token = hub.register(&account, &host, "main_server").await;
    let other = SecretKey::generate();
    let other_token = hub.register(&account, &other, "computer").await;

    // Only a device publishes, with a well-formed id.
    let publish = |token: &str, body: Value| {
        hub.client
            .post(hub.url("/v1/shares"))
            .bearer_auth(token)
            .json(&body)
            .send()
    };
    assert_eq!(
        publish(&account.1, json!({ "share_id": SHARE }))
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        publish(&host_token, json!({ "share_id": "nope" }))
            .await
            .unwrap()
            .status(),
        400
    );
    let res = publish(&host_token, json!({ "share_id": SHARE }))
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["url"], format!("{}/s/{SHARE}", hub.base));
    assert_eq!(
        publish(&other_token, json!({ "share_id": SHARE }))
            .await
            .unwrap()
            .status(),
        409
    );

    // Anyone resolves it, without a token.
    let res = hub
        .client
        .get(hub.url(&format!("/v1/shares/{SHARE}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["endpoint_id"], host.public().to_string());
    assert_eq!(body["relay_url"], hub.relay_url.to_string());
    assert!(body["relay_token"]
        .as_str()
        .is_some_and(|t| t.starts_with("dpg_")));

    // Another device of the account may not unpublish it; the host may.
    let path = hub.url(&format!("/v1/shares/{SHARE}"));
    let res = hub
        .client
        .delete(&path)
        .bearer_auth(&other_token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 404);
    let res = hub
        .client
        .delete(&path)
        .bearer_auth(&host_token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    assert_eq!(hub.client.get(&path).send().await.unwrap().status(), 404);
}

#[tokio::test]
async fn test_fr_h4_guest_reaches_share_host_through_relay_with_pass() {
    let hub = start_hub("fr_h4_guest").await;
    let account = hub.create_account().await;
    let host_key = SecretKey::generate();
    let host_token = hub.register(&account, &host_key, "main_server").await;
    let host = endpoint(
        &hub,
        host_key,
        EndpointOptions {
            relay: Some(hub.relay_config()),
            loopback_ip: false,
            directory_token: None,
        },
    )
    .await;
    let _server = spawn_server(host.clone());
    let res = hub
        .client
        .post(hub.url("/v1/shares"))
        .bearer_auth(&host_token)
        .json(&json!({ "share_id": SHARE }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);

    // What ash does: resolve the share, then dial the host through the relay with the pass.
    let resolved: Value = hub
        .client
        .get(hub.url(&format!("/v1/shares/{SHARE}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let relay: RelayUrl = resolved["relay_url"].as_str().unwrap().parse().unwrap();
    let host_id: iroh::EndpointId = resolved["endpoint_id"].as_str().unwrap().parse().unwrap();
    let pass = resolved["relay_token"].as_str().unwrap().to_string();

    let guest = endpoint(
        &hub,
        SecretKey::generate(),
        EndpointOptions {
            relay: Some(hub.relay_config().with_auth_token(pass)),
            loopback_ip: false,
            directory_token: None,
        },
    )
    .await;
    let conn = tokio::time::timeout(
        Duration::from_secs(15),
        guest.connect(via_relay(host_id, &relay), ALPN),
    )
    .await
    .expect("guest connect timed out")
    .expect("guest connect");
    assert_eq!(echo(&conn, b"viewer hello").await, b"viewer hello");
    conn.close(0u32.into(), b"done");

    // Without a pass the relay refuses the guest.
    let no_pass = endpoint(
        &hub,
        SecretKey::generate(),
        EndpointOptions {
            relay: Some(hub.relay_config()),
            loopback_ip: false,
            directory_token: None,
        },
    )
    .await;
    let attempt = tokio::time::timeout(
        Duration::from_secs(8),
        no_pass.connect(via_relay(host_id, &relay), ALPN),
    )
    .await;
    assert!(
        !matches!(attempt, Ok(Ok(_))),
        "guest without a pass connected"
    );

    // A made-up pass is refused too.
    let forged = endpoint(
        &hub,
        SecretKey::generate(),
        EndpointOptions {
            relay: Some(hub.relay_config().with_auth_token("dpg_forged")),
            loopback_ip: false,
            directory_token: None,
        },
    )
    .await;
    assert!(
        tokio::time::timeout(Duration::from_secs(5), forged.online())
            .await
            .is_err(),
        "the relay admitted a forged pass"
    );

    host.close().await;
    guest.close().await;
    no_pass.close().await;
    forged.close().await;
}

#[tokio::test]
async fn test_fr_h4_viewer_pages_are_served() {
    let hub = start_hub("fr_h4_pages").await;
    let account = hub.create_account().await;
    let key = SecretKey::generate();
    let token = hub.register(&account, &key, "main_server").await;
    let res = hub
        .client
        .post(hub.url("/v1/shares"))
        .bearer_auth(&token)
        .json(&json!({ "share_id": SHARE }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);

    for path in ["/ash/".to_string(), format!("/s/{SHARE}")] {
        let res = hub.client.get(hub.url(&path)).send().await.unwrap();
        assert_eq!(res.status(), 200, "{path}");
        let content_type = res.headers()["content-type"].to_str().unwrap().to_string();
        assert!(
            content_type.starts_with("text/html"),
            "{path}: {content_type}"
        );
    }
    let res = hub
        .client
        .get(hub.url("/s/s_ffffffffffffffff"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 404);
}
