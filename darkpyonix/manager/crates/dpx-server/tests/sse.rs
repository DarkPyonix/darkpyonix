//! Events stream (SPEC FR-M1, PROTOCOL §3.4 / PR-3 as seen through HTTP).

mod common;

use std::time::Duration;

use common::*;
use dpx_core::DpxError;
use dpx_server::Mode;
use serde_json::json;

async fn open(s: &TestServer, path: &str, last_event_id: Option<&str>) -> reqwest::Response {
    let mut rb = s.admin().req("GET", path);
    if let Some(id) = last_event_id {
        rb = rb.header("Last-Event-ID", id);
    }
    let r = rb.send().await.unwrap();
    contract().check("GET", path, r.status().as_u16());
    r
}

#[tokio::test]
async fn test_fr_m1_events_stream_resumes_with_last_event_id() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, _) = s.kernel("train.py");
    let url = format!("/api/kernels/{kid}/events");
    let r = open(&s, &url, None).await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["content-type"], "text/event-stream");
    assert_eq!(r.headers()["cache-control"], "no-cache");
    let mut sse = Sse::new(r);
    assert!(sse.next_block().await.unwrap().starts_with(": darkpyonix events"));
    assert_eq!(s.admin().post(&format!("/api/kernels/{kid}/runs"), json!({"mode": "all"})).await.status, 202);
    let live = sse.take(4).await;
    let kinds: Vec<_> = live.iter().map(|m| m.event.clone().unwrap()).collect();
    assert_eq!(kinds, ["kernel.status", "run.started", "cell.started", "output"]);
    let ids: Vec<u64> = live.iter().map(|m| m.id.as_ref().unwrap().parse().unwrap()).collect();
    assert_eq!(ids, [1, 2, 3, 4]);
    assert_eq!(live[3].data["output"], json!({"output_type": "stream", "name": "stdout", "text": "hello\n"}));
    drop(sse);

    // Missed while disconnected; Last-Event-ID wins over ?since=.
    s.backend.finish_run(&kid);
    let mut sse = Sse::new(open(&s, &format!("{url}?since=0"), Some("2")).await);
    let resumed = sse.take(5).await;
    let ids: Vec<u64> = resumed.iter().map(|m| m.id.as_ref().unwrap().parse().unwrap()).collect();
    assert_eq!(ids, [3, 4, 5, 6, 7]);
    let kinds: Vec<_> = resumed.iter().map(|m| m.event.clone().unwrap()).collect();
    assert_eq!(kinds, ["cell.started", "output", "cell.finished", "run.finished", "kernel.status"]);
    drop(sse);

    let mut sse = Sse::new(open(&s, &format!("{url}?since=4"), None).await);
    assert_eq!(sse.next_msg().await.unwrap().event.as_deref(), Some("cell.finished"));
    // An unparsable Last-Event-ID falls back to ?since=.
    let mut sse = Sse::new(open(&s, &format!("{url}?since=6"), Some("garbage")).await);
    assert_eq!(sse.next_msg().await.unwrap().id.as_deref(), Some("7"));
}

#[tokio::test]
async fn test_pr3_resume_older_than_the_ring_reports_replay_truncated() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let path = s.notebook("train.py");
    let kid = s.backend.add_with_ring(&path, 3);
    for i in 0..6 {
        s.backend.emit(&kid, "output", json!({"run_id": "x", "index": 1, "output": {"output_type": "stream", "name": "stdout", "text": format!("{i}\n")}}));
    }
    let r = open(&s, &format!("/api/kernels/{kid}/events?since=1"), None).await;
    let mut sse = Sse::new(r);
    let got = sse.take(4).await;
    assert_eq!(got[0], SseMsg { id: None, event: Some("replay_truncated".into()), data: json!({"oldest_seq": 4}) });
    let ids: Vec<_> = got[1..].iter().map(|m| m.id.clone().unwrap()).collect();
    assert_eq!(ids, ["4", "5", "6"]);
}

#[tokio::test]
async fn test_fr_a3_viewer1_events_omit_outputs() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (kid, _) = s.kernel("train.py");
    let share = s.admin().post(&format!("/api/kernels/{kid}/shares"), json!({"permission": "viewer1"})).await;
    let token = share.body["token"].as_str().unwrap();
    // ?token= works for share tokens too (EventSource).
    let r = reqwest::get(format!("{}/api/kernels/{kid}/events?token={token}", s.url)).await.unwrap();
    assert_eq!(r.status(), 200);
    let mut sse = Sse::new(r);
    s.admin().post(&format!("/api/kernels/{kid}/runs"), json!({"mode": "all"})).await;
    s.backend.emit(&kid, "output.clear", json!({"run_id": "x", "index": 0}));
    s.backend.finish_run(&kid);
    let kinds: Vec<_> = sse.take(6).await.into_iter().map(|m| m.event.unwrap()).collect();
    assert_eq!(kinds, ["kernel.status", "run.started", "cell.started", "cell.finished", "run.finished", "kernel.status"]);
}

#[tokio::test]
async fn test_fr_m1_events_errors_and_keepalive() {
    let s = TestServer::start_with(Mode::Ephemeral, |c| c.sse_keepalive = Duration::from_millis(200)).await;
    let (kid, _) = s.kernel("train.py");
    s.admin().get("/api/kernels/k_00000000000000000000/events").await.error(404, "not_found");
    s.backend.fail_on("subscribe", DpxError::new("kernel_unreachable", "lost"));
    s.admin().get(&format!("/api/kernels/{kid}/events")).await.error(502, "kernel_unreachable");
    s.backend.clear_failures();

    let mut sse = Sse::new(open(&s, &format!("/api/kernels/{kid}/events"), None).await);
    assert!(sse.next_block().await.unwrap().starts_with(": darkpyonix"));
    assert_eq!(sse.next_block().await.unwrap(), ": keepalive");
    assert_eq!(sse.next_block().await.unwrap(), ": keepalive");
}

#[tokio::test]
async fn test_fr_m1_dropping_the_client_drops_only_its_subscription() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, _) = s.kernel("train.py");
    let url = format!("/api/kernels/{kid}/events");
    let keep = Sse::new(open(&s, &url, None).await);
    let gone = open(&s, &url, None).await;
    assert!(wait_for(|| s.backend.subscribers.load(std::sync::atomic::Ordering::SeqCst) == 2, Duration::from_secs(5)).await);
    drop(gone);
    assert!(wait_for(|| s.backend.subscribers.load(std::sync::atomic::Ordering::SeqCst) == 1, Duration::from_secs(5)).await);
    // The kernel and the remaining subscriber are unaffected.
    let mut keep = keep;
    s.backend.emit(&kid, "kernel.status", json!({"status": "idle"}));
    assert_eq!(keep.next_msg().await.unwrap().event.as_deref(), Some("kernel.status"));
    assert!(s.backend.method_calls(&kid).is_empty());
    assert_eq!(s.admin().get(&format!("/api/kernels/{kid}")).await.status, 200);
}
