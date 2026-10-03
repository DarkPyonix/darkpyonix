//! A fake manager serving the shapes of `docs/api/manager.openapi.yaml`, registered in a
//! scratch `DARKPYONIX_HOME` the way a real manager registers itself.

#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{Path as UrlPath, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::{broadcast, Notify};
use tokio_stream::wrappers::BroadcastStream;

pub const RUN: &str = "20261003-142233-a1f0";
pub const OTHER_RUN: &str = "20261003-000000-0000";
pub const TOKEN: &str = "fake-master-token";

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Scenario {
    /// stdout, stderr and an execute_result, then `ok`.
    Ok,
    /// a stdout line and a traceback, then `error`.
    Error,
    /// prints "started" and waits for an interrupt; then `interrupted` if `finish`.
    WaitInterrupt { finish: bool },
    /// every run is refused with 409 busy.
    Busy,
}

pub struct FakeState {
    pub scenario: Scenario,
    pub calls: Mutex<Vec<(String, Value)>>,
    kernel: Mutex<Option<Value>>,
    tx: broadcast::Sender<(Option<u64>, String, Value)>,
    history: Mutex<Vec<(Option<u64>, String, Value)>>,
    seq: AtomicU64,
    interrupted: Notify,
}

impl FakeState {
    /// Emit an event to live subscribers and keep it for replay (`?since=`).
    pub fn emit(&self, kind: &str, data: Value) {
        let mut h = self.history.lock().unwrap();
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let ev = (Some(seq), kind.to_string(), data);
        h.push(ev.clone());
        let _ = self.tx.send(ev);
    }

    pub fn out(&self, run: &str, output: Value) {
        self.emit("output", json!({"run_id": run, "index": 1, "output": output}));
    }

    fn record(&self, what: String, body: Value) {
        self.calls.lock().unwrap().push((what, body));
    }

    pub fn calls(&self) -> Vec<(String, Value)> {
        self.calls.lock().unwrap().clone()
    }

    pub fn count(&self, prefix: &str) -> usize {
        self.calls().iter().filter(|(w, _)| w.starts_with(prefix)).count()
    }

    pub fn body_of(&self, prefix: &str) -> Option<Value> {
        self.calls().into_iter().find(|(w, _)| w.starts_with(prefix)).map(|(_, b)| b)
    }
}

pub struct Fake {
    pub addr: SocketAddr,
    pub state: Arc<FakeState>,
    pub home: PathBuf,
}

pub fn kernel_id(canonical: &str) -> String {
    let d = Sha256::digest(canonical.as_bytes());
    let hex: String = d.iter().map(|b| format!("{b:02x}")).collect();
    format!("k_{}", &hex[..20])
}

/// A clean `DARKPYONIX_HOME` under the repository's `.scratch/`.
pub fn scratch_home(name: &str) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../../.scratch/rcli-tests").join(name);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("managers")).unwrap();
    std::fs::canonicalize(root).unwrap()
}

/// A notebook file in the scratch home.
pub fn notebook(home: &Path) -> PathBuf {
    let f = home.join("train.py");
    std::fs::write(&f, "# %%\nprint('hello')\n").unwrap();
    f
}

impl Fake {
    /// Start the fake manager and register it as `managers/<this test process pid>.json`.
    pub fn start(name: &str, scenario: Scenario) -> Fake {
        let home = scratch_home(name);
        let fake = Self::start_unregistered(home, scenario);
        fake.register();
        fake
    }

    pub fn start_unregistered(home: PathBuf, scenario: Scenario) -> Fake {
        let (tx, _) = broadcast::channel(1024);
        let state = Arc::new(FakeState {
            scenario,
            calls: Mutex::new(Vec::new()),
            kernel: Mutex::new(None),
            tx,
            history: Mutex::new(Vec::new()),
            seq: AtomicU64::new(100),
            interrupted: Notify::new(),
        });
        let (addr_tx, addr_rx) = std::sync::mpsc::channel();
        let st = state.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
            rt.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                addr_tx.send(listener.local_addr().unwrap()).unwrap();
                axum::serve(listener, router(st)).await.unwrap();
            });
        });
        let addr = addr_rx.recv().unwrap();
        Fake { addr, state, home }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn register(&self) {
        let pid = std::process::id();
        let rec = json!({"url": self.url(), "token": TOKEN, "mode": "ephemeral", "pid": pid,
                         "started_at": "2026-10-03T00:00:00Z"});
        std::fs::write(self.home.join(format!("managers/{pid}.json")), rec.to_string()).unwrap();
    }

    pub fn file(&self) -> PathBuf {
        notebook(&self.home)
    }
}

/// The CLI with `DARKPYONIX_HOME` set and spawning disabled unless a test enables it.
pub fn cli(home: &Path) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_darkpyonix"));
    c.env("DARKPYONIX_HOME", home)
        .env("DARKPYONIX_SELF_EXE", home.join("no-such-manager-exe"))
        .current_dir(home);
    c
}

pub fn run(home: &Path, args: &[&str]) -> Output {
    cli(home).args(args).output().unwrap()
}

pub fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

pub fn wait_until(what: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

type St = State<Arc<FakeState>>;

fn router(state: Arc<FakeState>) -> Router {
    let api = Router::new()
        .route("/api/v1/manager", get(manager_info))
        .route("/api/v1/kernels", get(list_kernels).post(start_kernel))
        .route("/api/v1/kernels/{kid}", get(get_kernel).delete(delete_kernel))
        .route("/api/v1/kernels/{kid}/interrupt", post(interrupt))
        .route("/api/v1/kernels/{kid}/restart", post(restart))
        .route("/api/v1/kernels/{kid}/namespace", get(namespace))
        .route("/api/v1/kernels/{kid}/runs", post(start_run))
        .route("/api/v1/kernels/{kid}/runs/{run_ref}", get(get_run))
        .route("/api/v1/kernels/{kid}/events", get(events))
        .route("/api/v1/kernels/{kid}/shares", post(share))
        .route("/api/v1/documents", get(document))
        .layer(middleware::from_fn(auth));
    Router::new().route("/health", get(health)).merge(api).with_state(state)
}

fn err(status: StatusCode, code: &str, message: &str) -> Response {
    (status, Json(json!({"error": {"code": code, "message": message}}))).into_response()
}

async fn auth(req: Request, next: Next) -> Response {
    let expected = format!("Bearer {TOKEN}");
    let bearer = req.headers().get("authorization").and_then(|v| v.to_str().ok()).is_some_and(|v| v == expected);
    let query = req.uri().query().is_some_and(|q| q.contains(&format!("token={TOKEN}")));
    if bearer || query {
        next.run(req).await
    } else {
        err(StatusCode::UNAUTHORIZED, "unauthorized", "missing or invalid token")
    }
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok", "version": "0.0.0-fake"}))
}

async fn manager_info() -> Json<Value> {
    Json(json!({"version": "0.0.0-fake", "mode": "ephemeral", "pid": std::process::id(),
                "started_at": "2026-10-03T00:00:00Z", "idle_timeout": 120, "permission": "admin"}))
}

fn kernel_json(path: &str, status: &str, run_id: Option<&str>) -> Value {
    json!({"kernel_id": kernel_id(path), "path": path, "pid": 4242, "status": status, "run_id": run_id,
           "queue": [], "execution_count": 0,
           "python": {"version": "3.11.9", "implementation": "CPython", "executable": "/usr/bin/python3"},
           "started_at": "2026-10-03T00:00:00Z", "host": "fake", "kernel_version": "0.1.0",
           "runs_dir": format!("{path}/__runs__")})
}

async fn list_kernels(State(s): St) -> Json<Value> {
    s.record("GET /kernels".into(), Value::Null);
    let k: Vec<Value> = s.kernel.lock().unwrap().iter().cloned().collect();
    Json(json!({ "kernels": k }))
}

async fn start_kernel(State(s): St, Json(body): Json<Value>) -> Response {
    s.record("POST /kernels".into(), body.clone());
    let path = body["path"].as_str().unwrap_or_default().to_string();
    let mut k = s.kernel.lock().unwrap();
    let launched = k.is_none();
    let kj = kernel_json(&path, "idle", None);
    *k = Some(kj.clone());
    (if launched { StatusCode::CREATED } else { StatusCode::OK }, Json(kj)).into_response()
}

fn known(s: &FakeState, kid: &str) -> Option<Value> {
    s.kernel.lock().unwrap().clone().filter(|k| k["kernel_id"] == kid)
}

async fn get_kernel(State(s): St, UrlPath(kid): UrlPath<String>) -> Response {
    s.record(format!("GET /kernels/{kid}"), Value::Null);
    match known(&s, &kid) {
        Some(mut k) => {
            if matches!(s.scenario, Scenario::Busy) {
                k["status"] = json!("busy");
                k["run_id"] = json!(RUN);
            }
            Json(k).into_response()
        }
        None => err(StatusCode::NOT_FOUND, "not_found", "no such kernel"),
    }
}

async fn delete_kernel(State(s): St, UrlPath(kid): UrlPath<String>, Query(q): Query<HashMap<String, String>>) -> Response {
    s.record(format!("DELETE /kernels/{kid}"), json!(q));
    if known(&s, &kid).is_none() {
        return err(StatusCode::NOT_FOUND, "not_found", "no such kernel");
    }
    (StatusCode::ACCEPTED, Json(json!({"shutting_down": true}))).into_response()
}

async fn interrupt(State(s): St, UrlPath(kid): UrlPath<String>) -> Response {
    s.record(format!("POST /kernels/{kid}/interrupt"), Value::Null);
    if known(&s, &kid).is_none() {
        return err(StatusCode::NOT_FOUND, "not_found", "no such kernel");
    }
    s.interrupted.notify_one();
    Json(json!({"interrupted": true, "run_id": RUN})).into_response()
}

async fn restart(State(s): St, UrlPath(kid): UrlPath<String>, Json(body): Json<Value>) -> Response {
    s.record(format!("POST /kernels/{kid}/restart"), body);
    match known(&s, &kid) {
        Some(k) => Json(k).into_response(),
        None => err(StatusCode::NOT_FOUND, "not_found", "no such kernel"),
    }
}

async fn namespace(State(s): St, UrlPath(kid): UrlPath<String>) -> Response {
    if known(&s, &kid).is_none() {
        return err(StatusCode::NOT_FOUND, "not_found", "no such kernel");
    }
    Json(json!({"variables": [
        {"name": "lr", "type": "float", "repr": "0.1", "shape": null, "dtype": null, "len": null},
        {"name": "x", "type": "numpy.ndarray", "repr": "array([...])", "shape": [2, 3], "dtype": "float32", "len": 2}
    ]}))
    .into_response()
}

fn notebook_json() -> Value {
    json!({"nbformat": 4, "nbformat_minor": 5, "metadata": {"darkpyonix": {"run_id": RUN}}, "cells": [
        {"cell_type": "code", "source": "print('hello')", "metadata": {}, "execution_count": 1,
         "outputs": [{"output_type": "stream", "name": "stdout", "text": ["logged ", "line\n"]}]},
        {"cell_type": "code", "source": "1/0", "metadata": {}, "execution_count": 2,
         "outputs": [{"output_type": "error", "ename": "ZeroDivisionError", "evalue": "division by zero",
                      "traceback": ["Traceback (most recent call last):\n", "ZeroDivisionError: division by zero\n"]}]}
    ]})
}

async fn get_run(State(s): St, UrlPath((kid, run_ref)): UrlPath<(String, String)>) -> Response {
    s.record(format!("GET /kernels/{kid}/runs/{run_ref}"), Value::Null);
    if known(&s, &kid).is_none() || run_ref == "current" {
        return err(StatusCode::NOT_FOUND, "not_found", "no such run");
    }
    Json(notebook_json()).into_response()
}

async fn document(State(s): St, Query(q): Query<HashMap<String, String>>) -> Response {
    s.record("GET /documents".into(), json!(q));
    let mut nb = notebook_json();
    nb["path"] = json!(q.get("path"));
    nb["latest_run"] = json!({"run_id": RUN, "status": "error", "mode": "all", "started_at": "t"});
    Json(nb).into_response()
}

async fn share(State(s): St, UrlPath(kid): UrlPath<String>, Json(body): Json<Value>) -> Response {
    s.record(format!("POST /kernels/{kid}/shares"), body);
    err(StatusCode::FORBIDDEN, "forbidden", "an ephemeral manager cannot share")
}

async fn start_run(State(s): St, UrlPath(kid): UrlPath<String>, Json(body): Json<Value>) -> Response {
    s.record(format!("POST /kernels/{kid}/runs"), body.clone());
    if known(&s, &kid).is_none() {
        return err(StatusCode::NOT_FOUND, "not_found", "no such kernel");
    }
    if s.scenario == Scenario::Busy {
        let current = json!({"run_id": RUN, "status": "running", "mode": "all", "started_at": "2026-10-03T14:22:33Z"});
        return (
            StatusCode::CONFLICT,
            Json(json!({"error": {"code": "busy", "message": "a run is executing",
                                  "data": {"current": current, "queue_length": 0}}})),
        )
            .into_response();
    }
    let st = s.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        // Another client's run on the same kernel: must not show up.
        st.out(OTHER_RUN, json!({"output_type": "stream", "name": "stdout", "text": "NOT MINE\n"}));
        st.emit("run.started", json!({"run_id": RUN, "mode": "all", "cells": [], "params": {}}));
        st.emit("cell.started", json!({"run_id": RUN, "index": 1, "execution_count": 1}));
        let finish = |status: &str| st.emit("run.finished", json!({"run_id": RUN, "status": status, "duration": 0.1}));
        match st.scenario {
            Scenario::Ok => {
                st.out(RUN, json!({"output_type": "stream", "name": "stdout", "text": "hello\n"}));
                st.out(RUN, json!({"output_type": "stream", "name": "stderr", "text": "warn\n"}));
                st.out(RUN, json!({"output_type": "execute_result", "execution_count": 1,
                                   "data": {"text/plain": "42", "text/html": "<b>42</b>"}, "metadata": {}}));
                finish("ok");
            }
            Scenario::Error => {
                st.out(RUN, json!({"output_type": "stream", "name": "stdout", "text": "before\n"}));
                st.out(RUN, json!({"output_type": "error", "ename": "ValueError", "evalue": "bad",
                                   "traceback": ["Traceback (most recent call last):\n",
                                                 "  File \"train.py\", line 2, in <module>\n",
                                                 "\u{1b}[0;31mValueError\u{1b}[0m: bad\n"]}));
                finish("error");
            }
            Scenario::WaitInterrupt { finish: fin } => {
                st.out(RUN, json!({"output_type": "stream", "name": "stdout", "text": "started\n"}));
                st.interrupted.notified().await;
                if fin {
                    st.out(RUN, json!({"output_type": "error", "ename": "KeyboardInterrupt", "evalue": "",
                                       "traceback": ["KeyboardInterrupt\n"]}));
                    finish("interrupted");
                }
            }
            Scenario::Busy => unreachable!(),
        }
    });
    (StatusCode::ACCEPTED, Json(json!({"run_id": RUN, "state": "running"}))).into_response()
}

async fn events(
    State(s): St,
    UrlPath(kid): UrlPath<String>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let last_id = headers.get("last-event-id").and_then(|v| v.to_str().ok()).map(str::to_string);
    s.record(format!("GET /kernels/{kid}/events"), json!({"query": q, "last_event_id": last_id}));
    if known(&s, &kid).is_none() {
        return err(StatusCode::NOT_FOUND, "not_found", "no such kernel");
    }
    let since: Option<u64> = q.get("since").and_then(|v| v.parse().ok());
    let (rx, replay) = {
        let h = s.history.lock().unwrap();
        let replay: Vec<_> = match since {
            Some(n) => h.iter().filter(|e| e.0.is_some_and(|q| q > n)).cloned().map(Ok::<_, ()>).collect(),
            None => Vec::new(),
        };
        (s.tx.subscribe(), replay)
    };
    let live = BroadcastStream::new(rx).map(|m| m.map_err(|_| ()));
    let stream = futures::stream::iter(replay).chain(live).filter_map(|m| async move {
        let (seq, kind, data) = m.ok()?;
        let mut ev = Event::default().event(kind).data(data.to_string());
        if let Some(seq) = seq {
            ev = ev.id(seq.to_string());
        }
        Some(Ok::<_, std::convert::Infallible>(ev))
    });
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_millis(200))).into_response()
}
