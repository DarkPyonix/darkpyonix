//! FR-H1 device registry.

use std::time::Duration;

use iroh::{endpoint::presets, Endpoint, SecretKey};
use serde_json::Value;

use crate::common::*;

#[tokio::test]
async fn test_fr_h1_register_two_iroh_endpoints() {
    let hub = start_hub("fr_h1_register").await;
    let account = hub.create_account().await;

    // Real iroh endpoints; their ids are what the hub registers.
    let a = Endpoint::builder(presets::Minimal)
        .relay_mode(iroh::RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    let b = Endpoint::builder(presets::Minimal)
        .relay_mode(iroh::RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    hub.register(&account, a.secret_key(), "main_server").await;
    hub.register(&account, b.secret_key(), "computer").await;

    let (status, body) = hub.get_json("/devices", &account.1).await;
    assert_eq!(status, 200);
    let ids: Vec<&str> = body["devices"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["endpoint_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![a.id().to_string(), b.id().to_string()]);
    assert_eq!(body["devices"][0]["role"], "main_server");
    assert_eq!(body["devices"][1]["online"], false);

    a.close().await;
    b.close().await;
}

#[tokio::test]
async fn test_fr_h1_registration_requires_key_possession() {
    let hub = start_hub("fr_h1_possession").await;
    let account = hub.create_account().await;
    let other_account = hub.create_account().await;
    let key = SecretKey::generate();
    let id = key.public().to_string();

    // Signed by a different key.
    let challenge = hub.challenge(&account.1).await;
    let wrong = sign_registration(&SecretKey::generate(), &account.0, &challenge);
    let res = hub
        .register_raw(&account.1, &id, &challenge, &wrong, "computer")
        .await;
    assert_eq!(res.status(), 400);

    // The challenge was consumed by the failed attempt: reusing it fails even when signed right.
    let right = sign_registration(&key, &account.0, &challenge);
    let res = hub
        .register_raw(&account.1, &id, &challenge, &right, "computer")
        .await;
    assert_eq!(res.status(), 400);

    // A challenge of another account cannot be used.
    let foreign = hub.challenge(&other_account.1).await;
    let sig = sign_registration(&key, &account.0, &foreign);
    let res = hub
        .register_raw(&account.1, &id, &foreign, &sig, "computer")
        .await;
    assert_eq!(res.status(), 400);

    // A signature over another account id does not verify.
    let challenge = hub.challenge(&account.1).await;
    let sig = sign_registration(&key, &other_account.0, &challenge);
    let res = hub
        .register_raw(&account.1, &id, &challenge, &sig, "computer")
        .await;
    assert_eq!(res.status(), 400);

    // Done properly it works, once.
    hub.register(&account, &key, "computer").await;
    let challenge = hub.challenge(&account.1).await;
    let sig = sign_registration(&key, &account.0, &challenge);
    let res = hub
        .register_raw(&account.1, &id, &challenge, &sig, "computer")
        .await;
    assert_eq!(res.status(), 409);

    // Without the account token.
    let res = hub
        .client
        .post(hub.url("/challenges"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);
}

#[tokio::test]
async fn test_fr_h1_devices_are_scoped_to_their_account() {
    let hub = start_hub("fr_h1_scope").await;
    let mine = hub.create_account().await;
    let theirs = hub.create_account().await;
    let key = SecretKey::generate();
    let device_token = hub.register(&mine, &key, "computer").await;
    let path = format!("/devices/{}", key.public());

    assert_eq!(hub.get_json(&path, &mine.1).await.0, 200);
    // The device's own token sees its account too.
    assert_eq!(hub.get_json(&path, &device_token).await.0, 200);
    assert_eq!(hub.get_json(&path, &theirs.1).await.0, 404);
    let (status, body) = hub.get_json("/devices", &theirs.1).await;
    assert_eq!(status, 200);
    assert_eq!(body["devices"], Value::Array(vec![]));
    assert_eq!(hub.get_json("/devices", "dpa_not-a-token").await.0, 401);
}

#[tokio::test]
async fn test_fr_h1_removed_device_is_revoked() {
    let hub = start_hub("fr_h1_revoke").await;
    let account = hub.create_account().await;
    let key_a = SecretKey::generate();
    let key_b = SecretKey::generate();
    let token_a = hub.register(&account, &key_a, "computer").await;
    let token_b = hub.register(&account, &key_b, "computer").await;

    // A is online on the relay; B can reach it.
    let a = endpoint(
        &hub,
        key_a.clone(),
        EndpointOptions {
            relay: Some(hub.relay_config()),
            loopback_ip: false,
            directory_token: None,
        },
    )
    .await;
    let _server = spawn_server(a.clone());
    let b = endpoint(
        &hub,
        key_b.clone(),
        EndpointOptions {
            relay: Some(hub.relay_config()),
            loopback_ip: false,
            directory_token: None,
        },
    )
    .await;
    assert!(hub.wait_online(&account.1, key_a.public(), true).await);
    let conn = b
        .connect(via_relay(a.id(), &hub.relay_url), ALPN)
        .await
        .unwrap();
    assert_eq!(echo(&conn, b"before").await, b"before");
    conn.close(0u32.into(), b"done");

    // A device token cannot remove devices; the account token can.
    let path = format!("/devices/{}", key_a.public());
    let res = hub
        .client
        .delete(hub.url(&path))
        .bearer_auth(&token_b)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);
    let res = hub
        .client
        .delete(hub.url(&path))
        .bearer_auth(&account.1)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);

    // Its token is dead, it is not listed, and its key cannot come back.
    assert_eq!(hub.get_json("/devices", &token_a).await.0, 401);
    assert_eq!(hub.get_json(&path, &account.1).await.0, 404);
    let challenge = hub.challenge(&account.1).await;
    let sig = sign_registration(&key_a, &account.0, &challenge);
    let res = hub
        .register_raw(
            &account.1,
            &key_a.public().to_string(),
            &challenge,
            &sig,
            "computer",
        )
        .await;
    assert_eq!(res.status(), 409);

    // The relay dropped it and no longer admits it: B cannot reach A any more.
    let attempt = tokio::time::timeout(
        Duration::from_secs(8),
        b.connect(via_relay(a.id(), &hub.relay_url), ALPN),
    )
    .await;
    assert!(
        !matches!(attempt, Ok(Ok(_))),
        "removed device still reachable through the relay"
    );

    a.close().await;
    b.close().await;
}
