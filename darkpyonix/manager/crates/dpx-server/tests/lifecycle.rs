//! Ephemeral manager lifecycle (SPEC FR-M3) and dedicated mode (FR-M4).

mod common;

use std::time::{Duration, Instant};

use common::*;
use dpx_server::{ExitReason, Mode};

#[tokio::test]
async fn test_fr_m3_writes_no_registry_file() {
    let mut s = TestServer::start(Mode::Ephemeral).await;
    assert!(!s.home.join("managers").exists(), "no managers directory");
    let h = s.handle.take().unwrap();
    assert!(h.url.starts_with("http://127.0.0.1:"));
    h.shutdown();
    let reason = tokio::time::timeout(Duration::from_secs(5), h.wait()).await.expect("did not stop");
    assert_eq!(reason, ExitReason::Shutdown);
    assert!(!s.home.join("managers").exists(), "none appears on exit either");
    s.stop().await;
}

#[tokio::test]
async fn test_fr_m3_ephemeral_manager_exits_when_idle_and_kernels_remain() {
    let mut s = TestServer::start_with(Mode::Ephemeral, |c| c.idle_timeout = Some(Duration::from_millis(800))).await;
    let (kid, _) = s.kernel("train.py");
    assert_eq!(s.admin().get("/api/manager").await.body["idle_timeout"], 1);

    // An open event stream keeps the manager alive past its idle timeout.
    let r = s.admin().req("GET", &format!("/api/kernels/{kid}/events")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    tokio::time::sleep(Duration::from_millis(2000)).await;
    assert!(file.exists(), "exited with an open stream");
    // Requests keep it alive too.
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(s.admin().get("/health").await.status, 200);
    }
    drop(r);
    let closed = Instant::now();
    let h = s.handle.take().unwrap();
    let reason = tokio::time::timeout(Duration::from_secs(10), h.wait()).await.expect("did not exit");
    assert_eq!(reason, ExitReason::Idle);
    assert!(closed.elapsed() >= Duration::from_millis(700), "exited too early: {:?}", closed.elapsed());
    assert!(!s.home.join("managers").exists(), "registry file left behind");
    // Kernels are never touched (FR-M3, INTENT D2).
    assert!(s.backend.method_calls(&kid).is_empty());
    assert!(s.backend.kills.lock().unwrap().is_empty());
    assert!(reqwest::get(format!("{}/health", s.url)).await.is_err());
}

#[tokio::test]
async fn test_fr_m3_shutdown_with_an_open_stream_stops_the_process() {
    let mut s = TestServer::start(Mode::Ephemeral).await;
    // A stream open at shutdown does not hold the process.
    let (kid, _) = s.kernel("train.py");
    let r = s.admin().req("GET", &format!("/api/kernels/{kid}/events")).send().await.unwrap();
    let h = s.handle.take().unwrap();
    h.shutdown();
    let reason = tokio::time::timeout(Duration::from_secs(5), h.wait()).await.expect("did not stop");
    assert_eq!(reason, ExitReason::Shutdown);
    drop(r);
    s.stop().await;
}

#[tokio::test]
async fn test_fr_m4_dedicated_manager_never_idles_and_requires_token() {
    let s = TestServer::start_with(Mode::Dedicated, |c| c.idle_timeout = Some(Duration::from_millis(100))).await;
    assert!(!s.home.join("managers").exists());
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(s.anon().get("/health").await.status, 200);
    s.anon().get("/api/kernels").await.error(401, "unauthorized");
    let info = s.admin().get("/api/manager").await;
    assert_eq!(info.body["mode"], "dedicated");
    assert_eq!(info.body["idle_timeout"], serde_json::Value::Null);
}
