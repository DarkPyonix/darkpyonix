//! DKP/1 client against the Python fake kernel (`tests/helpers/fake_kernel.py`), which speaks
//! the real frames and handshake and writes its FR-D2 registry entry.

mod common;

use std::time::{Duration, Instant};

use common::*;
use dpx_core::{KernelBackend, KernelEvent};
use dpx_kernel::dkp::{Connection, FanoutConfig};
use dpx_kernel::Config;
use futures::StreamExt;
use serde_json::json;

async fn next(stream: &mut dpx_core::EventStream) -> KernelEvent {
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("event within 5 s")
        .expect("stream open")
}

async fn quiet(stream: &mut dpx_core::EventStream) -> bool {
    tokio::time::timeout(Duration::from_millis(200), stream.next())
        .await
        .is_err()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pr_2_handshake_and_requests_against_fake_kernel() {
    let dir = scratch("pr2");
    let home = dir.join("home");
    let be = backend(&home, false);
    let fake = FakeKernel::start(&home, &write_notebook(&dir, "nb.py"));

    // Registry discovery (FR-D2) finds the fake.
    let list = be.list(true).await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].kernel_id, fake.kernel_id);
    assert_eq!(list[0].port, fake.port);

    // get() enriches the announce with `status` over DKP.
    let info = be.get(&fake.kernel_id).await.unwrap();
    assert_eq!(info.status, "idle");
    assert_eq!(info.kernel_version, "0.1.0");
    assert!(
        info.runs_dir.ends_with("__runs__/nb.py"),
        "{}",
        info.runs_dir
    );

    let run = be
        .request(&fake.kernel_id, "run", json!({"mode": "all"}))
        .await
        .unwrap();
    assert_eq!(run["state"], "running");
    // Kernel errors pass through with the kernel's code and data.
    let busy = be
        .request(&fake.kernel_id, "run", json!({}))
        .await
        .unwrap_err();
    assert_eq!(busy.code, "busy");
    assert_eq!(
        busy.data.as_ref().unwrap()["current"]["run_id"],
        run["run_id"]
    );
    let nf = be
        .request(&fake.kernel_id, "runs.get", json!({"run_id": "nope"}))
        .await
        .unwrap_err();
    assert_eq!(nf.code, "not_found");
    let unk = be
        .request(&fake.kernel_id, "frobnicate", json!({}))
        .await
        .unwrap_err();
    assert_eq!(unk.code, "unknown_method");
    // Unknown kernel.
    assert_eq!(
        be.request("k_00000000000000000000", "status", json!({}))
            .await
            .unwrap_err()
            .code,
        "not_found"
    );

    // Round-trip latency over the shared connection.
    let mut samples = Vec::new();
    for _ in 0..1000 {
        let t = Instant::now();
        be.request(&fake.kernel_id, "status", json!({}))
            .await
            .unwrap();
        samples.push(t.elapsed());
    }
    samples.sort();
    eprintln!(
        "MEASURE request round-trip (status, 1000x): p50 {:?} p99 {:?}",
        percentile(&samples, 0.5),
        percentile(&samples, 0.99)
    );

    // Concurrent requests multiplex on one connection.
    let futs: Vec<_> = (0..50)
        .map(|_| be.request(&fake.kernel_id, "namespace", json!({"limit": 1})))
        .collect();
    for r in futures::future::join_all(futs).await {
        assert_eq!(r.unwrap()["variables"][0]["name"], "x");
    }
}

#[tokio::test]
async fn pr_2_wrong_key_is_refused() {
    let dir = scratch("pr2-key");
    let home = dir.join("home");
    let _be = backend(&home, false);
    let fake = FakeKernel::start(&home, &write_notebook(&dir, "nb.py"));
    let err = Connection::connect(
        fake.port,
        &fake.kernel_id,
        &[7u8; 32],
        FanoutConfig::default(),
    )
    .await
    .err()
    .expect("wrong key must fail");
    assert_eq!(err.code, "auth_failed");
    // A port that belongs to another kernel id is refused before auth.
    let err = Connection::connect(
        fake.port,
        "k_ffffffffffffffffffff",
        &[7u8; 32],
        FanoutConfig::default(),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(err.code, "kernel_unreachable");
}

/// Count ESTABLISHED TCP connections from this process to `port` (lsof).
fn connections_to(port: u16) -> usize {
    let out = std::process::Command::new("lsof")
        .args([
            "-nP",
            "-a",
            "-p",
            &std::process::id().to_string(),
            &format!("-iTCP:{port}"),
            "-sTCP:ESTABLISHED",
        ])
        .output()
        .expect("lsof");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| l.contains(&format!("->127.0.0.1:{port}")))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fr_m5_two_managers_share_one_kernel() {
    let dir = scratch("m5");
    let home = dir.join("home");
    let a = backend(&home, false);
    let b = backend(&home, false);
    let fake = FakeKernel::start(&home, &write_notebook(&dir, "nb.py"));
    let kid = fake.kernel_id.clone();

    let mut subs = Vec::new();
    for _ in 0..3 {
        subs.push(a.subscribe(&kid, None).await.unwrap());
    }
    subs.push(b.subscribe(&kid, None).await.unwrap());
    a.request(&kid, "status", json!({})).await.unwrap();
    b.request(&kid, "status", json!({})).await.unwrap();
    // One connection per manager, shared by requests and every subscriber.
    assert_eq!(
        connections_to(fake.port),
        2,
        "expected one connection per manager"
    );

    let mut fanout = Vec::new();
    for round in 0..20 {
        let t = Instant::now();
        a.request(&kid, "run", json!({})).await.unwrap();
        for s in subs.iter_mut() {
            let kinds: Vec<String> = [next(s).await, next(s).await, next(s).await, next(s).await]
                .iter()
                .map(|e| e.kind.clone())
                .collect();
            fanout.push(t.elapsed());
            assert_eq!(
                kinds,
                ["kernel.status", "run.started", "cell.started", "output"],
                "round {round}"
            );
        }
        b.request(&kid, "interrupt", json!({})).await.unwrap();
        for s in subs.iter_mut() {
            for want in ["cell.finished", "run.finished", "kernel.status"] {
                assert_eq!(next(s).await.kind, want);
            }
        }
    }
    fanout.sort();
    eprintln!(
        "MEASURE event fan-out (run request -> 4th event at each of 4 subscribers, 2 managers): p50 {:?} p99 {:?}",
        percentile(&fanout, 0.5),
        percentile(&fanout, 0.99)
    );
    for s in subs.iter_mut() {
        assert!(quiet(s).await);
    }
    assert_eq!(connections_to(fake.port), 2);
}

#[tokio::test]
async fn pr_3_subscribe_since_replays_then_streams_live() {
    let dir = scratch("pr3");
    let home = dir.join("home");
    let be = backend(&home, false);
    let fake = FakeKernel::start(&home, &write_notebook(&dir, "nb.py"));
    let kid = fake.kernel_id.clone();

    be.request(&kid, "run", json!({})).await.unwrap(); // seq 1..4
    be.request(&kid, "interrupt", json!({})).await.unwrap(); // seq 5..7

    let mut all = be.subscribe(&kid, Some(0)).await.unwrap();
    let mut got = Vec::new();
    for _ in 0..7 {
        got.push(next(&mut all).await.seq.unwrap());
    }
    assert_eq!(got, (1..=7).collect::<Vec<_>>());

    let mut from5 = be.subscribe(&kid, Some(5)).await.unwrap();
    assert_eq!(next(&mut from5).await.seq, Some(6));
    assert_eq!(next(&mut from5).await.seq, Some(7));
    let mut live = be.subscribe(&kid, None).await.unwrap();
    assert!(quiet(&mut live).await, "since=None is live only");

    be.request(&kid, "run", json!({})).await.unwrap(); // seq 8..11
    for s in [&mut all, &mut from5, &mut live] {
        assert_eq!(next(s).await.seq, Some(8));
    }
}

#[tokio::test]
async fn pr_3_replay_truncated_maps_to_seq_none() {
    let dir = scratch("pr3-trunc");
    let home = dir.join("home");
    let mut cfg = Config::with_home(&home);
    cfg.multicast = false;
    cfg.fanout.ring_max_events = 5;
    let be = backend_with(cfg);
    let fake = FakeKernel::start(&home, &write_notebook(&dir, "nb.py"));
    let kid = fake.kernel_id.clone();
    for _ in 0..2 {
        be.request(&kid, "run", json!({})).await.unwrap();
        be.request(&kid, "interrupt", json!({})).await.unwrap();
    } // seq 1..14
    let mut s = be.subscribe(&kid, Some(1)).await.unwrap();
    // First subscription attaches with since=1 after the mirror filled; ring keeps 10..14.
    let first = next(&mut s).await;
    assert_eq!(first.kind, "replay_truncated");
    assert_eq!(first.seq, None);
    assert_eq!(first.data["oldest_seq"], 10);
    let rest: Vec<_> = [next(&mut s).await, next(&mut s).await]
        .iter()
        .map(|e| e.seq.unwrap())
        .collect();
    assert_eq!(rest, [10, 11]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pr_3_lagging_subscriber_does_not_block_others() {
    let dir = scratch("pr3-lag");
    let home = dir.join("home");
    let mut cfg = Config::with_home(&home);
    cfg.multicast = false;
    cfg.fanout.channel_capacity = 4;
    let be = backend_with(cfg);
    let fake = FakeKernel::start(&home, &write_notebook(&dir, "nb.py"));
    let kid = fake.kernel_id.clone();

    let mut slow = be.subscribe(&kid, None).await.unwrap();
    let mut fast = be.subscribe(&kid, None).await.unwrap();
    let rounds = 30u64;
    for _ in 0..rounds {
        be.request(&kid, "run", json!({})).await.unwrap();
        be.request(&kid, "interrupt", json!({})).await.unwrap();
        for _ in 0..7 {
            next(&mut fast).await; // the fast reader keeps up although `slow` never reads
        }
    }
    // The slow reader lagged far beyond the channel; it resumes from the mirror, gap-free.
    let mut seqs = Vec::new();
    for _ in 0..rounds * 7 {
        seqs.push(next(&mut slow).await.seq.unwrap());
    }
    assert_eq!(seqs, (1..=rounds * 7).collect::<Vec<_>>());
    assert!(quiet(&mut slow).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fr_m5_reconnects_on_demand_after_kernel_restart() {
    let dir = scratch("m5-reconnect");
    let home = dir.join("home");
    let be = backend(&home, false);
    let nb = write_notebook(&dir, "nb.py");
    let mut fake = FakeKernel::start(&home, &nb);
    let kid = fake.kernel_id.clone();
    let mut events = be.subscribe(&kid, None).await.unwrap();
    be.request(&kid, "status", json!({})).await.unwrap();

    fake.kill();
    // The shared connection's subscribers end when the kernel goes away.
    let end = tokio::time::timeout(Duration::from_secs(5), events.next())
        .await
        .unwrap();
    assert!(end.is_none());
    let err = be.request(&kid, "status", json!({})).await.unwrap_err();
    assert!(
        ["not_found", "kernel_unreachable"].contains(&err.code.as_str()),
        "{err}"
    );

    let fake2 = FakeKernel::start(&home, &nb);
    assert_eq!(fake2.kernel_id, kid);
    let st = be.request(&kid, "status", json!({})).await.unwrap();
    assert_eq!(st["port"], fake2.port);
}

#[tokio::test]
async fn fr_m2_force_kill_removes_kernel() {
    let dir = scratch("kill");
    let home = dir.join("home");
    let be = backend(&home, false);
    let mut fake = FakeKernel::start(&home, &write_notebook(&dir, "nb.py"));
    let kid = fake.kernel_id.clone();
    be.kill(&kid).await.unwrap();
    let status = fake.child.wait().unwrap();
    assert!(!status.success());
    assert!(be.list(true).await.unwrap().is_empty());
    assert_eq!(be.kill(&kid).await.unwrap_err().code, "not_found");
}
