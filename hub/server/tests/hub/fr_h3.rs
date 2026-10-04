//! FR-H3 relay: iroh-relay in the hub with the registered-device policy.

use std::time::Duration;

use iroh::{EndpointAddr, SecretKey};

use crate::common::*;

/// Two registered, relay-only endpoints on a TLS hub (with QUIC address discovery).
async fn relay_pair(name: &str) -> (TestHub, iroh::Endpoint, iroh::Endpoint, (String, String)) {
    let hub = start_hub_with(
        name,
        HubOptions {
            tls: true,
            ..Default::default()
        },
    )
    .await;
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
    let b = endpoint(&hub, key_b, relay_only()).await;
    tokio::time::timeout(Duration::from_secs(10), a.online())
        .await
        .expect("a reaches the relay");
    tokio::time::timeout(Duration::from_secs(10), b.online())
        .await
        .expect("b reaches the relay");
    (hub, a, b, account)
}

#[tokio::test]
async fn test_fr_h3_relay_only_connection_through_hub() {
    let (hub, a, b, account) = relay_pair("fr_h3_relay").await;
    let _server = spawn_server(a.clone());

    assert!(hub.wait_online(&account.1, a.id(), true).await);
    let conn = b
        .connect(via_relay(a.id(), &hub.relay_url), ALPN)
        .await
        .unwrap();
    assert!(
        wait_selected_path(&conn, true).await,
        "selected path is not the relay"
    );
    assert_eq!(
        echo(&conn, b"over the hub relay").await,
        b"over the hub relay"
    );
    conn.close(0u32.into(), b"done");

    a.close().await;
    b.close().await;
}

#[tokio::test]
async fn test_fr_h3_direct_connection_on_loopback() {
    let hub = start_hub("fr_h3_direct").await;
    let account = hub.create_account().await;
    let key_a = SecretKey::generate();
    let key_b = SecretKey::generate();
    hub.register(&account, &key_a, "main_server").await;
    hub.register(&account, &key_b, "computer").await;
    let direct = || EndpointOptions {
        relay: None,
        loopback_ip: true,
        directory_token: None,
    };
    let a = endpoint(&hub, key_a, direct()).await;
    let b = endpoint(&hub, key_b, direct()).await;
    let _server = spawn_server(a.clone());

    let addr = a
        .bound_sockets()
        .into_iter()
        .find(|s| s.ip().is_loopback())
        .expect("loopback socket");
    let conn = b
        .connect(EndpointAddr::new(a.id()).with_ip_addr(addr), ALPN)
        .await
        .unwrap();
    assert!(
        wait_selected_path(&conn, false).await,
        "selected path is not direct"
    );
    assert_eq!(echo(&conn, b"direct").await, b"direct");

    let rtts = measure_rtts(&conn, 200).await;
    let mib_s = measure_throughput(&conn, 32 * 1024 * 1024).await;
    eprintln!(
        "FR-H3 direct loopback: rtt p50 {} us, p99 {} us; throughput {:.1} MiB/s",
        percentile(&rtts, 0.5),
        percentile(&rtts, 0.99),
        mib_s
    );
    conn.close(0u32.into(), b"done");
    a.close().await;
    b.close().await;
}

#[tokio::test]
async fn test_fr_h3_relay_rejects_unregistered_endpoint() {
    let hub = start_hub_with(
        "fr_h3_reject",
        HubOptions {
            tls: true,
            ..Default::default()
        },
    )
    .await;
    let account = hub.create_account().await;
    let key = SecretKey::generate();
    hub.register(&account, &key, "main_server").await;
    let relay_only = || EndpointOptions {
        relay: Some(hub.relay_config()),
        loopback_ip: false,
        directory_token: None,
    };
    let registered = endpoint(&hub, key, relay_only()).await;
    let _server = spawn_server(registered.clone());
    tokio::time::timeout(Duration::from_secs(10), registered.online())
        .await
        .expect("registered endpoint reaches the relay");

    let stranger = endpoint(&hub, SecretKey::generate(), relay_only()).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(5), stranger.online())
            .await
            .is_err(),
        "the relay admitted an unregistered endpoint"
    );
    let attempt = tokio::time::timeout(
        Duration::from_secs(8),
        stranger.connect(via_relay(registered.id(), &hub.relay_url), ALPN),
    )
    .await;
    assert!(
        !matches!(attempt, Ok(Ok(_))),
        "unregistered endpoint connected through the relay"
    );

    registered.close().await;
    stranger.close().await;
}

#[tokio::test]
async fn test_fr_h3_relay_throughput_and_latency() {
    let (hub, a, b, _account) = relay_pair("fr_h3_measure").await;
    let _server = spawn_server(a.clone());
    let conn = b
        .connect(via_relay(a.id(), &hub.relay_url), ALPN)
        .await
        .unwrap();
    assert!(
        wait_selected_path(&conn, true).await,
        "selected path is not the relay"
    );

    let rtts = measure_rtts(&conn, 200).await;
    let mib_s = measure_throughput(&conn, 32 * 1024 * 1024).await;
    eprintln!(
        "FR-H3 relay (TLS, loopback): rtt p50 {} us, p99 {} us; throughput {:.1} MiB/s",
        percentile(&rtts, 0.5),
        percentile(&rtts, 0.99),
        mib_s
    );
    assert!(mib_s > 1.0, "relay throughput below 1 MiB/s: {mib_s:.2}");
    conn.close(0u32.into(), b"done");
    a.close().await;
    b.close().await;
}
