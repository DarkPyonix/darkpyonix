//! FR-H5 (Draft) names and ACME DNS-01 challenge records, with the in-memory provider.

use iroh::SecretKey;
use serde_json::{json, Value};

use crate::common::*;

const DIGEST_A: &str = "Xq3UCnYkcz7k1JmTnU3hj3qTTGuCQq0fcR6dIKr_SDc";
const DIGEST_B: &str = "5bGx9Kd8WjzT0ZsI0PFaXz3L6kT1iuq7S0VW7rP3o2M";

#[tokio::test]
async fn test_fr_h5_name_reservation_and_acme_txt() {
    let hub = start_hub("fr_h5_names").await;
    let account = hub.create_account().await;
    let main = SecretKey::generate();
    let main_token = hub.register(&account, &main, "main_server").await;
    let laptop = SecretKey::generate();
    let laptop_token = hub.register(&account, &laptop, "computer").await;
    let other_account = hub.create_account().await;
    let rival = SecretKey::generate();
    let rival_token = hub.register(&other_account, &rival, "main_server").await;

    let reserve = |token: &str, name: &str| {
        hub.client
            .put(hub.url(&format!("/names/{name}")))
            .bearer_auth(token)
            .send()
    };
    assert_eq!(reserve(&main_token, "ab").await.unwrap().status(), 400);
    assert_eq!(reserve(&main_token, "relay").await.unwrap().status(), 400);
    assert_eq!(
        reserve(&laptop_token, "studio").await.unwrap().status(),
        403
    );
    assert_eq!(reserve(&account.1, "studio").await.unwrap().status(), 401);
    let res = reserve(&main_token, "studio").await.unwrap();
    assert_eq!(res.status(), 201);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["fqdn"], "studio.darkpyonix.test");
    assert_eq!(body["endpoint_id"], main.public().to_string());
    assert_eq!(reserve(&main_token, "studio").await.unwrap().status(), 200);
    assert_eq!(reserve(&rival_token, "studio").await.unwrap().status(), 409);

    let (status, body) = hub.get_json("/names", &account.1).await;
    assert_eq!(status, 200);
    assert_eq!(body["names"][0]["name"], "studio");

    // The main server's ACME client asks the hub to publish its DNS-01 TXT values.
    let fqdn = "_acme-challenge.studio.darkpyonix.test";
    let challenge = |token: &str, body: Value| {
        hub.client
            .put(hub.url("/names/studio/acme-challenge"))
            .bearer_auth(token)
            .json(&body)
            .send()
    };
    assert_eq!(
        challenge(&main_token, json!({ "values": ["short"] }))
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(
        challenge(&main_token, json!({ "values": [] }))
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(
        challenge(&laptop_token, json!({ "values": [DIGEST_A] }))
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        challenge(&rival_token, json!({ "values": [DIGEST_A] }))
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(hub.dns.txt(fqdn), None);
    let res = challenge(&main_token, json!({ "values": [DIGEST_A, DIGEST_B] }))
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    assert_eq!(
        hub.dns.txt(fqdn),
        Some(vec![DIGEST_A.to_string(), DIGEST_B.to_string()])
    );

    let res = hub
        .client
        .delete(hub.url("/names/studio/acme-challenge"))
        .bearer_auth(&main_token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    assert_eq!(hub.dns.txt(fqdn), None);

    // Releasing the name frees it.
    let res = hub
        .client
        .delete(hub.url("/names/studio"))
        .bearer_auth(&account.1)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    assert_eq!(reserve(&rival_token, "studio").await.unwrap().status(), 201);
}
