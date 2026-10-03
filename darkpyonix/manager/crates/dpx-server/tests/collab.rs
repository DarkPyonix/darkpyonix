//! Collaborative document through the manager (SPEC §10a FR-S1..S8, PROTOCOL §4) against the
//! fake backend: cell edits, locks, presence, heartbeats, run attribution and the alarm
//! long-poll. Every response is also checked against the statuses the YAML documents (NFR-M3).

mod common;

use std::time::{Duration, Instant};

use common::*;
use dpx_core::DpxError;
use dpx_server::Mode;
use serde_json::{json, Value};

const A: &str = "laptop-of-kim";
const B: &str = "phone-of-lee";

/// A request as client `client` (headers `X-DarkPyonix-Client` / `-Nickname`).
async fn send_as(api: &Api, client: Option<&str>, method: &str, path: &str, body: Option<Value>) -> Resp {
    let mut rb = api.req(method, path);
    if let Some(c) = client {
        rb = rb.header("X-DarkPyonix-Client", c).header("X-DarkPyonix-Nickname", format!("{c}-device"));
    }
    if let Some(b) = body {
        rb = rb.json(&b);
    }
    api.send(method, path, rb).await
}

fn os_user() -> String {
    dpx_server::util::os_user()
}

#[tokio::test]
async fn test_fr_s1_document_combines_snapshot_and_outputs() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, path) = s.kernel("train.py");
    let a = s.admin();
    let url = format!("/api/v1/kernels/{kid}/document");

    let d = a.get(&url).await;
    assert_eq!(d.status, 200, "{}", d.text);
    assert_eq!(d.body["path"], path);
    assert_eq!(d.body["kernel_id"], kid);
    assert_eq!(d.body["doc_version"], 0);
    assert_eq!(d.body["seq"], 0);
    assert_eq!(d.body["presence"], json!([]));
    let cells = d.body["cells"].as_array().unwrap();
    assert_eq!(cells.len(), 2);
    assert_eq!(cells[0]["cell_id"], "c_preamble");
    assert_eq!(cells[0]["version"], 1);
    assert_eq!(cells[0]["lock"], json!(null));
    assert_eq!(cells[0]["conflict"], json!(null));
    assert_eq!(cells[0]["outputs"][0]["text"], "hello\n");
    assert_eq!(cells[0]["run_id"], FAKE_DOC_RUN);
    assert_eq!(cells[0]["stale"], false);
    // A cell the run log does not know has no outputs.
    assert_eq!(cells[1]["cell_id"], "c_second");
    assert_eq!(cells[1]["outputs"], json!([]));
    assert_eq!(cells[1]["stale"], false);
    assert_eq!(cells[1]["run_id"], json!(null));

    // Edit the preamble: the live source wins and the old outputs become stale.
    let e = send_as(
        &a,
        Some(A),
        "PATCH",
        &format!("/api/v1/kernels/{kid}/cells/c_preamble"),
        Some(json!({"source": "print('bye')\n", "base_version": 1})),
    )
    .await;
    assert_eq!(e.status, 200, "{}", e.text);
    let d = a.get(&url).await;
    assert_eq!(d.body["doc_version"], 1);
    assert_eq!(d.body["seq"], 1);
    assert_eq!(d.body["cells"][0]["source"], "print('bye')\n");
    assert_eq!(d.body["cells"][0]["version"], 2);
    assert_eq!(d.body["cells"][0]["outputs"][0]["text"], "hello\n");
    assert_eq!(d.body["cells"][0]["stale"], true);

    // A kernel older than PROTOCOL §4 still gets a Document (ids from position).
    s.backend.fail_on("doc.snapshot", DpxError::new("unknown_method", "doc.snapshot"));
    let d = a.get(&url).await;
    assert_eq!(d.status, 200, "{}", d.text);
    assert_eq!(d.body["cells"][0]["cell_id"], "i_0");
    assert_eq!(d.body["cells"][0]["version"], 1);
    assert_eq!(d.body["doc_version"], 0);
    assert_eq!(d.body["presence"], json!([]));
    s.backend.fail_on("doc.snapshot", DpxError::new("kernel_unreachable", "lost"));
    a.get(&url).await.error(502, "kernel_unreachable");
}

#[test]
fn test_fr_s1_snapshot_cells_find_their_outputs_after_a_move() {
    let out = |t: &str| json!([{"output_type": "stream", "name": "stdout", "text": t}]);
    let built = |i: u64, src: &str, t: &str| {
        json!({"index": i, "type": "code", "source": src, "source_sha256": sha_hex(src), "metadata": {},
               "outputs": out(t), "execution_count": i, "status": "ok", "stale": false, "run_id": FAKE_DOC_RUN})
    };
    let live = |i: u64, id: &str, src: &str| {
        json!({"cell_id": id, "index": i, "type": "code", "title": null, "metadata": {}, "source": src,
               "source_sha256": sha_hex(src), "version": 1})
    };
    let doc = json!({"path": "/x/a.py", "cells": [built(0, "", ""), built(1, "a = 1\n", "a"), built(2, "b = 2\n", "b")]});
    let snap = json!({
        "doc_version": 7, "seq": 42, "presence": [],
        "cells": [live(0, "c_p", ""), live(1, "c_b", "b = 2\n"), live(2, "c_a", "a = 1\n"), live(3, "c_new", "c = 3\n")],
    });
    let merged = dpx_server::api::merge_snapshot(doc, Some(snap));
    assert_eq!(merged["doc_version"], 7);
    assert_eq!(merged["seq"], 42);
    let cells = merged["cells"].as_array().unwrap();
    let ids: Vec<&str> = cells.iter().map(|c| c["cell_id"].as_str().unwrap()).collect();
    assert_eq!(ids, ["c_p", "c_b", "c_a", "c_new"]);
    assert_eq!(cells[1]["outputs"], out("b"));
    assert_eq!(cells[2]["outputs"], out("a"));
    assert_eq!(cells[3]["outputs"], json!([]));
    assert!(cells.iter().all(|c| c["stale"] == false));
}

#[tokio::test]
async fn test_fr_s2_cell_edits_versions_and_events() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, _) = s.kernel("train.py");
    let a = s.admin();
    let base = format!("/api/v1/kernels/{kid}");
    let cells = format!("{base}/cells");

    let r = s.admin().req("GET", &format!("{base}/events")).send().await.unwrap();
    let mut sse = Sse::new(r);
    assert!(sse.next_block().await.unwrap().starts_with(": darkpyonix events"));

    // A client id is required for edits.
    send_as(&a, None, "POST", &cells, Some(json!({"source": "y = 2\n"}))).await.error(400, "bad_request");
    send_as(&a, Some("short"), "POST", &cells, Some(json!({"source": "y = 2\n"}))).await.error(400, "bad_request");
    let mut rb = a.req("POST", &cells).header("X-DarkPyonix-Client", A).header("X-DarkPyonix-Nickname", "n".repeat(65));
    rb = rb.json(&json!({}));
    a.send("POST", &cells, rb).await.error(400, "bad_request");

    // createCell
    let c = send_as(&a, Some(A), "POST", &cells, Some(json!({"type": "code", "source": "y = 2\n", "after": "c_preamble"})))
        .await;
    assert_eq!(c.status, 201, "{}", c.text);
    let new_id = c.body["cell_id"].as_str().unwrap().to_string();
    assert_eq!(c.body["index"], 1);
    assert_eq!(c.body["version"], 1);
    let (m, p) = s.backend.last_call(&kid).unwrap();
    assert_eq!(m, "doc.cell.create");
    assert_eq!(
        p,
        json!({"client": {"client_id": A, "nickname": format!("{A}-device"), "user": os_user(), "permission": "admin"},
               "type": "code", "source": "y = 2\n", "after": "c_preamble"})
    );
    let ev = sse.next_msg().await.unwrap();
    assert_eq!(ev.event.as_deref(), Some("doc.cell.created"));
    assert_eq!(ev.data["by"]["client_id"], A);
    assert_eq!(ev.data["cell"]["cell_id"], new_id.as_str());

    send_as(&a, Some(A), "POST", &cells, Some(json!({"after": "c_preamble", "before": "c_second"})))
        .await
        .error(400, "bad_request");
    send_as(&a, Some(A), "POST", &cells, Some(json!({"source": 3}))).await.error(400, "bad_request");
    send_as(&a, Some(A), "POST", &cells, None).await.error(400, "bad_request");
    send_as(&a, Some(A), "POST", &cells, Some(json!({"after": "c_missing"}))).await.error(404, "not_found");

    // updateCell: base_version is required and checked.
    let second = format!("{cells}/c_second");
    let u = send_as(&a, Some(A), "PATCH", &second, Some(json!({"source": "x = 2\n", "base_version": 1}))).await;
    assert_eq!(u.status, 200, "{}", u.text);
    assert_eq!(u.body["version"], 2);
    assert_eq!(u.body["source"], "x = 2\n");
    let e = send_as(&a, Some(B), "PATCH", &second, Some(json!({"source": "x = 9\n", "base_version": 1})))
        .await
        .error(409, "conflict");
    assert_eq!(e["data"]["cell"]["version"], 2);
    assert_eq!(e["data"]["cell"]["source"], "x = 2\n");
    send_as(&a, Some(A), "PATCH", &second, Some(json!({"source": "x = 3\n"}))).await.error(400, "bad_request");
    send_as(&a, Some(A), "PATCH", &format!("{cells}/c_missing"), Some(json!({"source": "", "base_version": 1})))
        .await
        .error(404, "not_found");
    send_as(&a, Some(A), "PATCH", &format!("{cells}/bad%20id"), Some(json!({"source": "", "base_version": 1})))
        .await
        .error(404, "not_found");

    // moveCell
    let mv = send_as(&a, Some(A), "POST", &format!("{cells}/{new_id}/move"), Some(json!({"to_index": 2}))).await;
    assert_eq!(mv.status, 200, "{}", mv.text);
    assert_eq!(mv.body["index"], 2);
    send_as(&a, Some(A), "POST", &format!("{cells}/{new_id}/move"), Some(json!({"to_index": 0})))
        .await
        .error(400, "bad_request");
    send_as(&a, Some(A), "POST", &format!("{cells}/{new_id}/move"), Some(json!({}))).await.error(400, "bad_request");

    // deleteCell: base_version in the query.
    send_as(&a, Some(A), "DELETE", &second, None).await.error(400, "bad_request");
    send_as(&a, Some(A), "DELETE", &format!("{second}?base_version=abc"), None).await.error(400, "bad_request");
    send_as(&a, Some(A), "DELETE", &format!("{second}?base_version=1"), None).await.error(409, "conflict");
    let d = send_as(&a, Some(A), "DELETE", &format!("{second}?base_version=2"), None).await;
    assert_eq!(d.status, 204, "{}", d.text);
    assert_eq!(s.backend.last_call(&kid).unwrap().1["base_version"], 2);
    let ids: Vec<String> = s.backend.doc_cells(&kid).into_iter().map(|c| c.cell_id).collect();
    assert_eq!(ids, ["c_preamble".to_string(), new_id.clone()]);

    // Unknown kernels and kernel failures.
    send_as(&a, Some(A), "POST", "/api/v1/kernels/k_00000000000000000000/cells", Some(json!({})))
        .await
        .error(404, "not_found");
    s.backend.fail_on("doc.cell.update", DpxError::new("kernel_unreachable", "lost"));
    send_as(&a, Some(A), "PATCH", &format!("{cells}/{new_id}"), Some(json!({"source": "", "base_version": 1})))
        .await
        .error(502, "kernel_unreachable");
}

#[tokio::test]
async fn test_fr_s3_locks_are_exclusive_and_unlock_saves() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, _) = s.kernel("train.py");
    let a = s.admin();
    let cell = format!("/api/v1/kernels/{kid}/cells/c_second");
    let lock = format!("{cell}/lock");

    send_as(&a, None, "PUT", &lock, None).await.error(400, "bad_request");
    let l = send_as(&a, Some(A), "PUT", &lock, None).await;
    assert_eq!(l.status, 200, "{}", l.text);
    assert_eq!(l.body["cell_id"], "c_second");
    assert_eq!(l.body["locked_by"], A);
    assert_eq!(l.body["user"], os_user());
    // Renewing one's own lock is fine.
    assert_eq!(send_as(&a, Some(A), "PUT", &lock, None).await.status, 200);

    let e = send_as(&a, Some(B), "PUT", &lock, None).await.error(409, "locked");
    assert_eq!(e["data"]["locked_by"], A);
    let e = send_as(&a, Some(B), "PATCH", &cell, Some(json!({"source": "z\n", "base_version": 1})))
        .await
        .error(409, "locked");
    assert_eq!(e["data"]["locked_by"], A);
    send_as(&a, Some(B), "DELETE", &format!("{cell}?base_version=1"), None).await.error(409, "locked");
    send_as(&a, Some(B), "DELETE", &lock, None).await.error(409, "locked");

    // The document shows the lock.
    let d = a.get(&format!("/api/v1/kernels/{kid}/document")).await;
    assert_eq!(d.body["cells"][1]["lock"]["locked_by"], A);

    // Unlock with the final source (2025 cell_unlocked_with_code); a stale base is refused.
    send_as(&a, Some(A), "DELETE", &lock, Some(json!({"source": "x = 5\n", "base_version": 7})))
        .await
        .error(409, "conflict");
    let u = send_as(&a, Some(A), "DELETE", &lock, Some(json!({"source": "x = 5\n", "base_version": 1}))).await;
    assert_eq!(u.status, 200, "{}", u.text);
    assert_eq!(u.body["source"], "x = 5\n");
    assert_eq!(u.body["version"], 2);
    assert!(u.body.get("lock").is_none());
    // The body is optional.
    assert_eq!(send_as(&a, Some(A), "DELETE", &lock, None).await.status, 200);
    send_as(&a, Some(A), "DELETE", &lock, Some(json!({"source": 1}))).await.error(400, "bad_request");
    // Now B may lock.
    assert_eq!(send_as(&a, Some(B), "PUT", &lock, None).await.status, 200);
    send_as(&a, Some(A), "PUT", &format!("/api/v1/kernels/{kid}/cells/c_missing/lock"), None)
        .await
        .error(404, "not_found");
}

#[tokio::test]
async fn test_fr_s4_presence_update_and_leave() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, _) = s.kernel("train.py");
    let a = s.admin();
    let url = format!("/api/v1/kernels/{kid}/presence");

    send_as(&a, None, "PUT", &url, Some(json!({}))).await.error(400, "bad_request");
    send_as(&a, Some(A), "PUT", &url, None).await.error(400, "bad_request");
    for bad in [
        json!({"focused_cell_id": 5}),
        json!({"cursor": {"cell_id": 1, "line": 0, "column": 0}}),
        json!({"cursor": {"cell_id": "c_second", "line": -1, "column": 0}}),
        json!({"cursor": {"cell_id": "c_second", "line": 0, "column": 0, "selection": [[0, 1]]}}),
        json!({"cursor": "here"}),
        json!({"avatar": 3}),
    ] {
        send_as(&a, Some(A), "PUT", &url, Some(bad.clone())).await.error(400, "bad_request");
    }

    let cursor = json!({"cell_id": "c_second", "line": 0, "column": 3, "selection": [[0, 1], [0, 3]]});
    let r = send_as(
        &a,
        Some(A),
        "PUT",
        &url,
        Some(json!({"focused_cell_id": "c_second", "cursor": cursor, "avatar": "https://example.test/a.png"})),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text);
    let presence = r.body["presence"].as_array().unwrap();
    assert_eq!(presence.len(), 1);
    assert_eq!(presence[0]["client_id"], A);
    assert_eq!(presence[0]["nickname"], format!("{A}-device"));
    assert_eq!(presence[0]["user"], os_user());
    assert_eq!(presence[0]["permission"], "admin");
    assert_eq!(presence[0]["avatar"], "https://example.test/a.png");
    assert_eq!(presence[0]["focused_cell_id"], "c_second");
    assert_eq!(presence[0]["cursor"], cursor);
    let sent = s.backend.calls_of(&kid, "presence.update").pop().unwrap();
    assert_eq!(sent["client"]["avatar"], "https://example.test/a.png");
    assert_eq!(sent["cursor"], cursor);

    // Keys left out are not sent (the kernel keeps them); the avatar is remembered.
    let r = send_as(&a, Some(A), "PUT", &url, Some(json!({"focused_cell_id": null}))).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body["presence"][0]["focused_cell_id"], json!(null));
    assert_eq!(r.body["presence"][0]["cursor"], cursor);
    let sent = s.backend.calls_of(&kid, "presence.update").pop().unwrap();
    assert!(sent.get("cursor").is_none());
    assert_eq!(sent["client"]["avatar"], "https://example.test/a.png");

    let r = send_as(&a, Some(B), "PUT", &url, Some(json!({}))).await;
    assert_eq!(r.body["presence"].as_array().unwrap().len(), 2);

    send_as(&a, None, "DELETE", &url, None).await.error(400, "bad_request");
    let d = send_as(&a, Some(A), "DELETE", &url, None).await;
    assert_eq!(d.status, 204, "{}", d.text);
    assert_eq!(s.backend.calls_of(&kid, "presence.leave")[0]["client"]["client_id"], A);
    let doc = a.get(&format!("/api/v1/kernels/{kid}/document")).await;
    let left: Vec<&str> = doc.body["presence"].as_array().unwrap().iter().map(|p| p["client_id"].as_str().unwrap()).collect();
    assert_eq!(left, [B]);
    send_as(&a, Some(A), "PUT", "/api/v1/kernels/k_00000000000000000000/presence", Some(json!({})))
        .await
        .error(404, "not_found");
}

/// Beats sent for `kid` by `client_id`.
fn beats(s: &TestServer, kid: &str, client_id: &str) -> usize {
    s.backend.calls_of(kid, "presence.update").iter().filter(|p| p["client"]["client_id"] == client_id).count()
}

#[tokio::test]
async fn test_fr_s4_event_stream_with_client_id_heartbeats_presence() {
    let s = TestServer::start_with(Mode::Ephemeral, |c| c.presence_heartbeat = Duration::from_millis(100)).await;
    let (kid, _) = s.kernel("train.py");
    let a = s.admin();
    let events = format!("/api/v1/kernels/{kid}/events");
    a.get(&format!("{events}?client_id=bad")).await.error(400, "bad_request");
    a.get(&format!("{events}?client_id=tablet-0001&nickname={}", "n".repeat(65))).await.error(400, "bad_request");

    let r = a.req("GET", &format!("{events}?client_id=tablet-0001&nickname=Tab")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let started = Instant::now();
    assert!(wait_for(|| beats(&s, &kid, "tablet-0001") >= 3, Duration::from_secs(5)).await);
    // First beat at once, then one per interval (not faster).
    assert!(started.elapsed() >= Duration::from_millis(150), "{:?}", started.elapsed());
    let first = s.backend.calls_of(&kid, "presence.update")[0].clone();
    assert_eq!(
        first,
        json!({"client": {"client_id": "tablet-0001", "nickname": "Tab", "user": os_user(), "permission": "admin"}})
    );
    let doc = a.get(&format!("/api/v1/kernels/{kid}/document")).await;
    assert_eq!(doc.body["presence"][0]["client_id"], "tablet-0001");

    // Closing the stream stops the heartbeats; the kernel expires the client by itself.
    drop(r);
    let mut stopped = false;
    for _ in 0..20 {
        let before = beats(&s, &kid, "tablet-0001");
        tokio::time::sleep(Duration::from_millis(350)).await;
        if beats(&s, &kid, "tablet-0001") == before {
            stopped = true;
            break;
        }
    }
    assert!(stopped, "heartbeats continue after the stream closed");
    assert!(s.backend.calls_of(&kid, "presence.leave").is_empty());

    // The header works too (clients that can set headers).
    let r = a.req("GET", &events).header("X-DarkPyonix-Client", "desktop-0002").send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert!(wait_for(|| beats(&s, &kid, "desktop-0002") >= 1, Duration::from_secs(5)).await);
    drop(r);
}

#[tokio::test]
async fn test_fr_s6_runs_and_interrupts_carry_the_client() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, _) = s.kernel("train.py");
    let a = s.admin();
    let runs = format!("/api/v1/kernels/{kid}/runs");
    let r = send_as(&a, Some(A), "POST", &runs, Some(json!({"mode": "cells", "cell_ids": ["c_second"]}))).await;
    assert_eq!(r.status, 202, "{}", r.text);
    let (m, p) = s.backend.last_call(&kid).unwrap();
    assert_eq!(m, "run");
    assert_eq!(p["cell_ids"], json!(["c_second"]));
    assert!(p.get("cells").is_none());
    assert_eq!(
        p["client"],
        json!({"client_id": A, "nickname": format!("{A}-device"), "user": os_user(), "permission": "admin"})
    );
    send_as(&a, Some(A), "POST", &runs, Some(json!({"mode": "cells", "cells": [1], "cell_ids": ["c_second"]})))
        .await
        .error(400, "bad_request");
    send_as(&a, Some(A), "POST", &runs, Some(json!({"mode": "cells", "cell_ids": []}))).await.error(400, "bad_request");

    // Without (or with a malformed) client id the run is still attributed to the token's user.
    let q = send_as(&a, Some("bad"), "POST", &runs, Some(json!({"mode": "all", "on_busy": "queue"}))).await;
    assert_eq!(q.status, 202, "{}", q.text);
    let client = s.backend.last_call(&kid).unwrap().1["client"].clone();
    assert!(client.get("client_id").is_none());
    assert_eq!(client["user"], os_user());

    let i = send_as(&a, Some(B), "POST", &format!("/api/v1/kernels/{kid}/interrupt"), None).await;
    assert_eq!(i.status, 200, "{}", i.text);
    assert_eq!(s.backend.calls_of(&kid, "interrupt")[0]["client"]["client_id"], B);
}

#[tokio::test]
async fn test_fr_s7_wait_returns_on_finish_or_timeout() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, _) = s.kernel("train.py");
    let a = s.admin();
    let base = format!("/api/v1/kernels/{kid}");
    let run_id = a.post(&format!("{base}/runs"), json!({"mode": "all"})).await.body["run_id"].as_str().unwrap().to_string();

    // Still running after the timeout: status running and where to poll next.
    let t = Instant::now();
    let w = a.get(&format!("{base}/runs/{run_id}/wait?timeout=1")).await;
    assert_eq!(w.status, 200, "{}", w.text);
    assert!(t.elapsed() >= Duration::from_millis(900), "{:?}", t.elapsed());
    assert_eq!(w.body["status"], "running");
    assert_eq!(w.body["run_id"], run_id.as_str());
    assert_eq!(w.body["run"]["run_id"], run_id.as_str());
    assert_eq!(w.body["next"], format!("{base}/runs/{run_id}/wait?timeout=1"));
    // `current` resolves to the run id.
    let w = a.get(&format!("{base}/runs/current/wait?timeout=1")).await;
    assert_eq!(w.body["run_id"], run_id.as_str());
    assert_eq!(w.body["next"], format!("{base}/runs/{run_id}/wait?timeout=1"));

    // Returns as soon as the run finishes.
    let backend = s.backend.clone();
    let k2 = kid.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        backend.finish_run(&k2);
    });
    let t = Instant::now();
    let w = a.get(&format!("{base}/runs/{run_id}/wait?timeout=15")).await;
    assert_eq!(w.status, 200, "{}", w.text);
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    assert_eq!(w.body["status"], "ok");
    assert_eq!(w.body["next"], json!(null));
    assert_eq!(w.body["run"]["status"], "ok");

    // timeout is clamped to 1..=300 and defaults to 60.
    let w = a.get(&format!("{base}/runs/latest/wait?timeout=1000")).await;
    assert_eq!(w.status, 200);
    assert_eq!(s.backend.calls_of(&kid, "runs.wait").pop().unwrap()["timeout"], 300);
    a.get(&format!("{base}/runs/latest/wait?timeout=0")).await;
    assert_eq!(s.backend.calls_of(&kid, "runs.wait").pop().unwrap()["timeout"], 1);
    a.get(&format!("{base}/runs/latest/wait")).await;
    assert_eq!(s.backend.calls_of(&kid, "runs.wait").pop().unwrap(), json!({"run_id": "latest", "timeout": 60}));

    a.get(&format!("{base}/runs/latest/wait?timeout=soon")).await.error(400, "bad_request");
    a.get(&format!("{base}/runs/bogus/wait")).await.error(404, "not_found");
    a.get(&format!("{base}/runs/current/wait?timeout=1")).await.error(404, "not_found");
    a.get(&format!("{base}/runs/20990101-000000-0000/wait?timeout=1")).await.error(404, "not_found");
    a.get("/api/v1/kernels/k_00000000000000000000/runs/latest/wait").await.error(404, "not_found");
    s.backend.fail_on("runs.wait", DpxError::new("kernel_unreachable", "lost"));
    a.get(&format!("{base}/runs/latest/wait")).await.error(502, "kernel_unreachable");
}

/// FR-A3 + FR-S8: edits and locks need `editor`, presence `viewer1`, waiting `viewer2`.
#[tokio::test]
async fn test_fr_s8_permission_matrix_for_collaboration() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (kid, _) = s.kernel("train.py");
    let a = s.admin();
    // A finished run so that waitRun on `latest` answers at once.
    a.post(&format!("/api/v1/kernels/{kid}/runs"), json!({"mode": "all"})).await;
    s.backend.finish_run(&kid);

    let mut tokens = vec![];
    for perm in ["viewer1", "viewer2", "viewer3", "editor"] {
        let c = a.post(&format!("/api/v1/kernels/{kid}/shares"), json!({"permission": perm, "label": perm})).await;
        assert_eq!(c.status, 201, "{}", c.text);
        assert_eq!(c.body["permission"], perm);
        tokens.push((perm, c.body["token"].as_str().unwrap().to_string()));
    }
    let listed = a.get(&format!("/api/v1/kernels/{kid}/shares")).await;
    assert!(listed.body["shares"].as_array().unwrap().iter().any(|s| s["permission"] == "editor"));
    let rank = |p: &str| match p {
        "viewer1" => 1,
        "viewer2" => 2,
        "viewer3" => 3,
        "editor" => 4,
        _ => 5,
    };
    let b = format!("/api/v1/kernels/{kid}");
    // (method, path, body, minimum permission)
    let ops: Vec<(&str, String, Option<Value>, &str)> = vec![
        ("GET", format!("{b}/document"), None, "viewer1"),
        ("PUT", format!("{b}/presence"), Some(json!({"focused_cell_id": "c_second"})), "viewer1"),
        ("GET", format!("{b}/runs/latest/wait?timeout=1"), None, "viewer2"),
        ("POST", format!("{b}/runs"), Some(json!({"mode": "all", "on_busy": "queue"})), "viewer3"),
        ("POST", format!("{b}/cells"), Some(json!({"source": "z = 0\n"})), "editor"),
        ("PATCH", format!("{b}/cells/c_second"), Some(json!({"source": "x = 1\n", "base_version": 99})), "editor"),
        ("POST", format!("{b}/cells/c_second/move"), Some(json!({"to_index": 1})), "editor"),
        ("PUT", format!("{b}/cells/c_second/lock"), None, "editor"),
        ("DELETE", format!("{b}/cells/c_second/lock"), None, "editor"),
        ("DELETE", format!("{b}/cells/c_second?base_version=99"), None, "editor"),
        ("DELETE", format!("{b}/presence"), None, "viewer1"),
        ("POST", format!("{b}/restart"), Some(json!({})), "admin"),
    ];
    for (perm, token) in &tokens {
        let v = s.with_token(token);
        assert_eq!(v.get("/api/v1/manager").await.body["permission"], *perm);
        let me = format!("client-{perm}");
        for (m, p, body, min) in &ops {
            let r = send_as(&v, Some(me.as_str()), m, p, body.clone()).await;
            if rank(perm) >= rank(min) {
                // 409: the stale base_version on purpose.
                assert!(r.status < 400 || r.status == 409, "{perm} {m} {p}: {}", r.text);
            } else {
                r.error(403, "forbidden");
            }
        }
        // Share clients are named after the share label.
        let sent = s.backend.calls_of(&kid, "presence.update").pop().unwrap();
        assert_eq!(sent["client"]["user"], *perm);
        assert_eq!(sent["client"]["permission"], *perm);
        assert_eq!(sent["client"]["client_id"], me.as_str());
    }
    // Without a label the user is "guest"; the master token is the OS user.
    let c = a.post(&format!("/api/v1/kernels/{kid}/shares"), json!({"permission": "viewer1"})).await;
    let guest = s.with_token(c.body["token"].as_str().unwrap());
    assert_eq!(send_as(&guest, Some("guest-device"), "PUT", &format!("{b}/presence"), Some(json!({}))).await.status, 200);
    assert_eq!(s.backend.calls_of(&kid, "presence.update").pop().unwrap()["client"]["user"], "guest");
    assert_eq!(send_as(&a, Some(A), "PUT", &format!("{b}/presence"), Some(json!({}))).await.status, 200);
    assert_eq!(s.backend.calls_of(&kid, "presence.update").pop().unwrap()["client"]["user"], os_user());
    // Share tokens see other kernels as missing, never forbidden.
    let (other, _) = s.kernel("other.py");
    let editor = s.with_token(&tokens[3].1);
    send_as(&editor, Some("client-editor"), "POST", &format!("/api/v1/kernels/{other}/cells"), Some(json!({})))
        .await
        .error(404, "not_found");
}
