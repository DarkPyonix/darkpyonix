use std::time::Duration;
use iroh::SecretKey;
use serde_json::json;
use crate::common::*;

#[tokio::test]
async fn test_fr_h3_relay_admits_via_the_worker() {
    let hub = start_hub("admission").await;
    let key = SecretKey::generate();
    let id = key.public().to_string();
    let client = connect(&hub, key, None).await.unwrap();
    wait_for(|| hub.calls.lock().unwrap().presence.len() == 1).await;
    assert_eq!(hub.calls.lock().unwrap().admissions[0], json!({"endpoint_id": id, "token": null}));
    assert_eq!(hub.calls.lock().unwrap().presence[0], json!({"endpoint_id": id, "online": true}));
    drop(client);
    wait_for(|| hub.calls.lock().unwrap().presence.len() == 2).await;
    assert_eq!(hub.calls.lock().unwrap().presence[1]["online"], false);
}
#[tokio::test]
async fn test_fr_h3_relay_caches_admission_for_cache_secs() {
    let hub = start_hub("cache").await;
    let key = SecretKey::generate();
    let a = connect(&hub, key.clone(), None).await.unwrap();
    let b = connect(&hub, key.clone(), None).await.unwrap();
    assert_eq!(hub.calls.lock().unwrap().admissions.len(), 1);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let c = connect(&hub, key, None).await.unwrap();
    assert_eq!(hub.calls.lock().unwrap().admissions.len(), 2);
    drop((a, b, c));
}
#[tokio::test]
async fn test_fr_h3_relay_refuses_an_endpoint_the_worker_does_not_admit() {
    let hub = start_hub("refusal").await;
    let key = SecretKey::generate();
    let id = key.public().to_string();
    // The stub admission API allows no token (a registered device) and "guest-pass" only.
    let refused = connect(&hub, key.clone(), Some("forged")).await;
    assert!(refused.is_err(), "an endpoint the Worker refuses must not get a relay connection");
    assert_eq!(hub.calls.lock().unwrap().admissions[0], json!({"endpoint_id": id, "token": "forged"}));
    // A refusal is not cached: the next attempt is asked about again, and refused again.
    assert!(connect(&hub, key, Some("forged")).await.is_err());
    assert_eq!(hub.calls.lock().unwrap().admissions.len(), 2);
    // The endpoint never came online.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(hub.calls.lock().unwrap().presence.is_empty());
}
#[tokio::test]
async fn test_fr_h4_guest_pass_admits_through_the_worker() {
    let hub = start_hub("guest").await;
    let key = SecretKey::generate();
    let a = connect(&hub, key.clone(), Some("guest-pass")).await.unwrap();
    let b = connect(&hub, key, Some("guest-pass")).await.unwrap();
    let calls = hub.calls.lock().unwrap();
    assert_eq!(calls.admissions.len(), 2);
    assert_eq!(calls.admissions[0]["token"], "guest-pass");
    drop(calls);
    drop((a, b));
}
#[tokio::test]
async fn test_fr_h3_admin_disconnect_drops_the_endpoint_and_needs_the_secret() {
    let hub = start_hub("disconnect").await;
    let key = SecretKey::generate();
    let id = key.public().to_string();
    let client = connect(&hub, key.clone(), None).await.unwrap();
    wait_for(|| hub.calls.lock().unwrap().presence.len() == 1).await;
    let url = hub.url("/admin/disconnect");
    assert_eq!(hub.client.post(&url).json(&json!({"endpoint_id": id})).send().await.unwrap().status(), 401);
    assert_eq!(hub.client.post(&url).bearer_auth(SECRET).json(&json!({"endpoint_id": id})).send().await.unwrap().status(), 204);
    wait_for(|| hub.calls.lock().unwrap().presence.len() == 2).await;
    assert_eq!(hub.calls.lock().unwrap().presence[1]["online"], false);
    let next = connect(&hub, key, None).await.unwrap();
    assert_eq!(hub.calls.lock().unwrap().admissions.len(), 2);
    drop((client, next));
}
