//! NFR-M1 and NFR-M2 measured on the real manager stack: `dpx_kernel::RealBackend` (real
//! Python kernels started from the embedded sources) behind `dpx_server::serve`, driven over
//! HTTP and SSE exactly as a client would.
//!
//! `cargo test -p darkpyonix --test manager_latency -- --nocapture --test-threads=1`

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dpx_kernel::{Config, RealBackend};
use dpx_server::{serve, ServerConfig, ServerHandle};
use serde_json::{json, Value};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../..")
        .canonicalize()
        .unwrap()
}

/// A fresh directory under `<repo>/.scratch/` (never /tmp, never the real home).
fn scratch(name: &str) -> PathBuf {
    let dir = repo_root()
        .join(".scratch")
        .join("rust-manager-latency")
        .join(format!(
            "{name}-{}-{}",
            std::process::id(),
            dpx_kernel::home::random_hex(3)
        ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The development venv: in this checkout or, for a worktree, in an enclosing checkout.
fn venv_python() -> PathBuf {
    if let Some(p) = std::env::var_os("DPX_TEST_PYTHON") {
        return PathBuf::from(p);
    }
    for dir in repo_root().ancestors() {
        let p = dir.join(".venv/bin/python");
        if p.is_file() {
            return p;
        }
    }
    panic!("no .venv/bin/python found above {}", repo_root().display());
}

/// Kills every registered kernel pid when dropped (also on test failure).
#[derive(Default)]
struct Reaper {
    pids: Mutex<Vec<u32>>,
}

impl Reaper {
    fn add(&self, pid: u32) {
        self.pids.lock().unwrap().push(pid);
    }
}

impl Drop for Reaper {
    fn drop(&mut self) {
        for pid in self.pids.lock().unwrap().drain(..) {
            let _ = dpx_kernel::process::kill_pid(pid);
        }
    }
}

/// A manager (real backend, ephemeral HTTP server) on `home`.
struct Manager {
    handle: Option<ServerHandle>,
    url: String,
    token: String,
    http: reqwest::Client,
}

impl Manager {
    async fn start(home: &Path) -> Manager {
        let mut cfg = Config::with_home(home);
        cfg.multicast = true;
        cfg.python = Some(venv_python().to_string_lossy().into_owned());
        let backend = RealBackend::new(cfg).unwrap();
        let handle = serve(ServerConfig::ephemeral(home), std::sync::Arc::new(backend))
            .await
            .expect("serve");
        Manager {
            url: handle.url.clone(),
            token: handle.token.clone(),
            handle: Some(handle),
            http: reqwest::Client::new(),
        }
    }

    async fn call(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> (u16, Value) {
        let mut rb = self
            .http
            .request(method, format!("{}{path}", self.url))
            .bearer_auth(&self.token);
        if let Some(b) = body {
            rb = rb
                .header("content-type", "application/json")
                .body(serde_json::to_vec(&b).unwrap());
        }
        let resp = rb.send().await.expect("request");
        let status = resp.status().as_u16();
        let bytes = resp.bytes().await.expect("body");
        (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    /// POST /api/kernels: launch (or attach to) the kernel of `path`.
    async fn start_kernel(&self, path: &Path) -> Value {
        let (status, body) = self
            .call(
                reqwest::Method::POST,
                "/api/kernels",
                Some(json!({"path": path.to_string_lossy()})),
            )
            .await;
        assert!(status == 200 || status == 201, "start kernel: {status} {body}");
        body
    }

    async fn stop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.shutdown();
            h.wait().await;
        }
    }
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i]
}

/// `GET /api/kernels{query}` `n` times; sorted latencies.
async fn time_list(m: &Manager, query: &str, n: usize, expect: usize) -> Vec<Duration> {
    let mut lat = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        let (status, body) = m
            .call(reqwest::Method::GET, &format!("/api/kernels{query}"), None)
            .await;
        lat.push(t.elapsed());
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["kernels"].as_array().map(Vec::len), Some(expect), "{body}");
    }
    lat.sort();
    lat
}

const NFR_M1_BOUND: Duration = Duration::from_millis(300);

/// NFR-M1: listing kernels on the manager takes at most 300 ms with 20 kernels, for the
/// cached view, a fresh discovery query (`refresh=true`), and a manager that just started.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_nfr_m1_list_kernels_with_20_real_kernels_within_300_ms() {
    let dir = scratch("m1");
    let home = dir.join("home");
    let reaper = Reaper::default();
    let mut m = Manager::start(&home).await;
    const KERNELS: usize = 20;
    for i in 0..KERNELS {
        let nb = dir.join(format!("nb{i:02}.py"));
        std::fs::write(&nb, "# %%\nx = 1\n").unwrap();
        let k = m.start_kernel(&nb).await;
        reaper.add(k["pid"].as_u64().unwrap() as u32);
    }

    let cached = time_list(&m, "", 50, KERNELS).await;
    let refresh = time_list(&m, "?refresh=true", 50, KERNELS).await;
    // A manager that has never queried: its first list sends the discovery query.
    let mut first = Vec::new();
    for _ in 0..5 {
        let mut fresh = Manager::start(&home).await;
        first.extend(time_list(&fresh, "", 1, KERNELS).await);
        fresh.stop().await;
    }
    first.sort();
    m.stop().await;

    for (what, lat) in [
        ("cached", &cached),
        ("refresh=true", &refresh),
        ("first list of a fresh manager", &first),
    ] {
        eprintln!(
            "MEASURE NFR-M1 GET /api/kernels ({what}), {KERNELS} kernels: n={} p50 {:?} p99 {:?} max {:?}",
            lat.len(),
            percentile(lat, 0.5),
            percentile(lat, 0.99),
            lat[lat.len() - 1]
        );
    }
    for (what, lat) in [("cached", &cached), ("refresh=true", &refresh), ("fresh", &first)] {
        assert!(
            percentile(lat, 0.99) <= NFR_M1_BOUND,
            "NFR-M1 ({what}): p99 {:?} > {NFR_M1_BOUND:?}",
            percentile(lat, 0.99)
        );
    }
}

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

/// The text of an nbformat `stream` output (`text` is a string or a list of strings).
fn stream_text(output: &Value) -> Option<String> {
    if output.get("output_type").and_then(Value::as_str) != Some("stream") {
        return None;
    }
    match output.get("text")? {
        Value::String(s) => Some(s.clone()),
        Value::Array(parts) => Some(parts.iter().filter_map(Value::as_str).collect()),
        _ => None,
    }
}

const NFR_M2_LINES: usize = 200;
const NFR_M2_BOUND: Duration = Duration::from_millis(100);

/// NFR-M2: a kernel's output reaches a manager SSE subscriber within p99 100 ms, the kernel's
/// 50 ms stream merge included. The cell prints `T=<time.time()>` lines; the subscriber notes
/// the wall clock when each line's newline arrives on `GET /api/kernels/{id}/events`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_nfr_m2_output_reaches_an_sse_subscriber_within_100_ms_p99() {
    let dir = scratch("m2");
    let home = dir.join("home");
    let reaper = Reaper::default();
    let mut m = Manager::start(&home).await;
    let nb = dir.join("emit.py");
    std::fs::write(
        &nb,
        format!(
            "# %%\nimport time\nfor i in range({NFR_M2_LINES}):\n    print('T=%.6f' % time.time())\n    time.sleep(0.01)\n"
        ),
    )
    .unwrap();
    let k = m.start_kernel(&nb).await;
    reaper.add(k["pid"].as_u64().unwrap() as u32);
    let kid = k["kernel_id"].as_str().unwrap().to_string();

    let mut sse = m
        .http
        .get(format!("{}/api/kernels/{kid}/events", m.url))
        .bearer_auth(&m.token)
        .send()
        .await
        .expect("events");
    assert_eq!(sse.status().as_u16(), 200);
    // Headers are sent once the subscription exists, so nothing of the run can be missed.
    let (status, body) = m
        .call(
            reqwest::Method::POST,
            &format!("/api/kernels/{kid}/runs"),
            Some(json!({"mode": "all"})),
        )
        .await;
    assert_eq!(status, 202, "{body}");

    let mut buf = String::new();
    let mut partial = String::new(); // stream text after the last newline seen
    let mut lat: Vec<Duration> = Vec::new();
    let mut finished = false;
    let deadline = Instant::now() + Duration::from_secs(60);
    while !finished && lat.len() < NFR_M2_LINES {
        let left = deadline.saturating_duration_since(Instant::now());
        let chunk = tokio::time::timeout(left, sse.chunk())
            .await
            .expect("SSE stalled")
            .expect("SSE read")
            .expect("SSE closed");
        let received = now_secs();
        buf.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(end) = buf.find("\n\n") {
            let block: String = buf.drain(..end + 2).collect();
            let mut kind = "";
            let mut data = "";
            for line in block.lines() {
                if let Some(v) = line.strip_prefix("event: ") {
                    kind = v;
                } else if let Some(v) = line.strip_prefix("data: ") {
                    data = v;
                }
            }
            match kind {
                "run.finished" => finished = true,
                "output" => {
                    let ev: Value = serde_json::from_str(data).unwrap();
                    let Some(text) = stream_text(&ev["output"]) else { continue };
                    partial.push_str(&text);
                    while let Some(nl) = partial.find('\n') {
                        let line: String = partial.drain(..nl + 1).collect();
                        if let Some(ts) = line.trim().strip_prefix("T=") {
                            let printed: f64 = ts.parse().unwrap();
                            lat.push(Duration::from_secs_f64((received - printed).max(0.0)));
                        }
                    }
                }
                _ => {}
            }
        }
    }
    drop(sse);
    m.stop().await;

    assert_eq!(lat.len(), NFR_M2_LINES, "every printed line arrives");
    lat.sort();
    eprintln!(
        "MEASURE NFR-M2 print -> SSE subscriber, {} lines: p50 {:?} p99 {:?} max {:?}",
        lat.len(),
        percentile(&lat, 0.5),
        percentile(&lat, 0.99),
        lat[lat.len() - 1]
    );
    assert!(
        percentile(&lat, 0.99) <= NFR_M2_BOUND,
        "NFR-M2: p99 {:?} > {NFR_M2_BOUND:?}",
        percentile(&lat, 0.99)
    );
}
