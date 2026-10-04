//! Request latency of GET /api/kernels with the fake backend under 100 concurrent clients.
//! Measurement only: `cargo test --release -p dpx-server --test latency -- --ignored --nocapture`.

mod common;

use std::time::{Duration, Instant};

use common::*;
use dpx_server::Mode;

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn measure_list_kernels_latency_100_clients() {
    let s = TestServer::start(Mode::Ephemeral).await;
    for i in 0..20 {
        s.kernel(&format!("nb{i}.py"));
    }
    let clients = 100;
    let per_client = 200;
    let url = format!("{}/api/kernels", s.url);
    let start = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..clients {
        let url = url.clone();
        let token = s.token.clone();
        tasks.push(tokio::spawn(async move {
            let c = reqwest::Client::builder().pool_max_idle_per_host(1).build().unwrap();
            let mut lat = Vec::with_capacity(per_client);
            for _ in 0..per_client {
                let t = Instant::now();
                let r = c.get(&url).bearer_auth(&token).send().await.unwrap();
                assert_eq!(r.status(), 200);
                let _ = r.bytes().await.unwrap();
                lat.push(t.elapsed());
            }
            lat
        }));
    }
    let mut all: Vec<Duration> = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    let total = start.elapsed();
    all.sort();
    let pct = |p: f64| all[((all.len() as f64 * p) as usize).min(all.len() - 1)];
    println!(
        "GET /api/kernels (20 kernels), {clients} clients x {per_client}: n={} p50={:?} p99={:?} max={:?} throughput={:.0} req/s",
        all.len(),
        pct(0.50),
        pct(0.99),
        all[all.len() - 1],
        all.len() as f64 / total.as_secs_f64()
    );
}
