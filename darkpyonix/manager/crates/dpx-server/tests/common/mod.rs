//! Test support: an in-memory fake KernelBackend, a contract checker that fails any response
//! whose status the OpenAPI file does not document (NFR-M3), and HTTP helpers.
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use dpx_core::{DpxError, Ensured, EventStream, KernelBackend, KernelEvent, KernelInfo, Result, StartKernel};
use dpx_server::{serve, Mode, ServerConfig, ServerHandle};
use futures::StreamExt;
use serde_json::{json, Value};
use sha2::Digest;
use tokio::sync::broadcast;

pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..").canonicalize().unwrap()
}

/// A fresh directory under `<repo>/.scratch/rust-tests/` (CLAUDE.md "Where files go").
pub fn scratch(name: &str) -> PathBuf {
    let dir = repo_root().join(".scratch").join("rust-tests").join(format!("{name}-{}", dpx_server::auth::random_hex(6)));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

pub fn kernel_id_for(path: &str) -> String {
    let h = hex::encode(sha2::Sha256::digest(path.as_bytes()));
    format!("k_{}", &h[..20])
}

fn now() -> String {
    "2026-10-03T12:00:00Z".to_string()
}

// ------------------------------------------------------------------ fake backend

pub struct FakeKernel {
    pub info: KernelInfo,
    running: Option<Value>,
    queue: Vec<String>,
    finished: Vec<Value>,
    ring: Vec<KernelEvent>,
    ring_max: usize,
    seq: u64,
    tx: broadcast::Sender<KernelEvent>,
    run_counter: u32,
    pub linger_on_shutdown: bool,
    pub doc: FakeDoc,
}

/// The kernel's shared document (PROTOCOL §4), reduced to what the manager forwards.
#[derive(Clone, Debug)]
pub struct FakeCell {
    pub cell_id: String,
    pub kind: String,
    pub source: String,
    pub version: u64,
}

#[derive(Default)]
pub struct FakeDoc {
    pub cells: Vec<FakeCell>,
    /// cell_id → Lock
    pub locks: HashMap<String, Value>,
    /// Presence entries in join order.
    pub presence: Vec<Value>,
    pub doc_version: u64,
    next_id: u64,
}

/// The run the fake document builder maps its outputs from.
pub const FAKE_DOC_RUN: &str = "20261003-120000-0001";

pub fn sha_hex(s: &str) -> String {
    hex::encode(sha2::Sha256::digest(s.as_bytes()))
}

fn cell_json(doc: &FakeDoc, i: usize) -> Value {
    let c = &doc.cells[i];
    let mut v = json!({
        "cell_id": c.cell_id, "index": i, "type": c.kind, "title": null, "metadata": {},
        "source": c.source, "source_sha256": sha_hex(&c.source), "version": c.version,
    });
    if let Some(l) = doc.locks.get(&c.cell_id) {
        v["lock"] = l.clone();
    }
    v
}

fn doc_client(params: &Value) -> Result<String> {
    match params["client"]["client_id"].as_str() {
        Some(id) if !id.is_empty() => Ok(id.to_string()),
        _ => Err(DpxError::new("bad_request", "client with a client_id is required")),
    }
}

fn by_of(params: &Value) -> Value {
    let c = &params["client"];
    json!({"client_id": c["client_id"], "user": c["user"], "nickname": c["nickname"]})
}

fn find_cell(doc: &FakeDoc, cell_id: &str) -> Result<usize> {
    doc.cells
        .iter()
        .position(|c| c.cell_id == cell_id)
        .ok_or_else(|| DpxError::new("not_found", format!("no cell {cell_id}")).with_data(json!({"cell_id": cell_id})))
}

fn check_lock(doc: &FakeDoc, cell_id: &str, client_id: &str) -> Result<()> {
    match doc.locks.get(cell_id) {
        Some(l) if l["locked_by"] != client_id => Err(DpxError::new("locked", format!("cell {cell_id} is locked"))
            .with_data(json!({"locked_by": l["locked_by"], "lock": l}))),
        _ => Ok(()),
    }
}

fn check_base(doc: &FakeDoc, i: usize, base: Option<u64>) -> Result<()> {
    if base == Some(doc.cells[i].version) {
        Ok(())
    } else {
        Err(DpxError::new("conflict", "stale base_version").with_data(json!({"cell": cell_json(doc, i)})))
    }
}

fn wait_answer(s: &Value) -> Value {
    json!({"status": s["status"], "run_id": s["run_id"], "run": s})
}

#[derive(Default)]
pub struct FakeBackend {
    kernels: Mutex<HashMap<String, FakeKernel>>,
    /// method name (or "get", "list", "ensure", "subscribe", "document", "kill") → error.
    pub fail: Mutex<HashMap<String, DpxError>>,
    pub calls: Mutex<Vec<(String, String, Value)>>,
    pub launches: Mutex<Vec<StartKernel>>,
    pub kills: Mutex<Vec<String>>,
    pub subscribers: Arc<AtomicUsize>,
}

struct SubGuard(Arc<AtomicUsize>);
impl Drop for SubGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn summary(run_id: &str, status: &str, mode: &str) -> Value {
    json!({"run_id": run_id, "status": status, "mode": mode, "started_at": now()})
}

fn notebook(summary: &Value) -> Value {
    let mut meta = summary.clone();
    if summary["status"] != "running" {
        meta["ended_at"] = json!("2026-10-03T12:00:02Z");
    }
    json!({"nbformat": 4, "nbformat_minor": 5, "metadata": {"darkpyonix": meta}, "cells": []})
}

impl FakeBackend {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Adds a running kernel for `path` and returns its id.
    pub fn add(&self, path: &str) -> String {
        self.add_with_ring(path, 1000)
    }

    pub fn add_with_ring(&self, path: &str, ring_max: usize) -> String {
        let kid = kernel_id_for(path);
        let (tx, _) = broadcast::channel(1024);
        let preamble = std::fs::read_to_string(path).unwrap_or_default();
        let doc = FakeDoc {
            cells: vec![
                FakeCell { cell_id: "c_preamble".into(), kind: "preamble".into(), source: preamble, version: 1 },
                FakeCell { cell_id: "c_second".into(), kind: "code".into(), source: "x = 1\n".into(), version: 1 },
            ],
            ..FakeDoc::default()
        };
        let info = KernelInfo {
            kernel_id: kid.clone(),
            path: path.to_string(),
            pid: 4242,
            status: "idle".into(),
            run_id: None,
            queue: vec![],
            execution_count: 0,
            python: json!({"version": "3.12.1", "implementation": "CPython", "executable": "/usr/bin/python3"}),
            started_at: now(),
            host: "test".into(),
            kernel_version: "0.1.0".into(),
            runs_dir: String::new(),
            port: 5555,
        };
        let k = FakeKernel {
            info,
            running: None,
            queue: vec![],
            finished: vec![],
            ring: vec![],
            ring_max,
            seq: 0,
            tx,
            run_counter: 0,
            linger_on_shutdown: false,
            doc,
        };
        self.kernels.lock().unwrap().insert(kid.clone(), k);
        kid
    }

    pub fn set_linger(&self, kid: &str) {
        self.kernels.lock().unwrap().get_mut(kid).unwrap().linger_on_shutdown = true;
    }

    pub fn fail_on(&self, what: &str, err: DpxError) {
        self.fail.lock().unwrap().insert(what.to_string(), err);
    }

    pub fn clear_failures(&self) {
        self.fail.lock().unwrap().clear();
    }

    fn check_fail(&self, what: &str) -> Result<()> {
        match self.fail.lock().unwrap().get(what) {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    pub fn method_calls(&self, kid: &str) -> Vec<String> {
        self.calls.lock().unwrap().iter().filter(|c| c.0 == kid).map(|c| c.1.clone()).collect()
    }

    /// Params of every `method` request to `kid`, in order.
    pub fn calls_of(&self, kid: &str, method: &str) -> Vec<Value> {
        self.calls.lock().unwrap().iter().filter(|c| c.0 == kid && c.1 == method).map(|c| c.2.clone()).collect()
    }

    pub fn doc_cells(&self, kid: &str) -> Vec<FakeCell> {
        self.kernels.lock().unwrap().get(kid).unwrap().doc.cells.clone()
    }

    /// The run `run_ref` names now (`runs.wait` polling).
    fn wait_probe(&self, kernel_id: &str, run_ref: &str) -> Result<Option<Value>> {
        let ks = self.kernels.lock().unwrap();
        let k = ks
            .get(kernel_id)
            .ok_or_else(|| DpxError::new("kernel_unreachable", format!("no connection to {kernel_id}")))?;
        Ok(match run_ref {
            "latest" => k.finished.first().cloned(),
            "current" => k.running.clone(),
            id => k
                .finished
                .iter()
                .chain(k.running.iter())
                .find(|s| s["run_id"] == id)
                .cloned()
                .or_else(|| k.queue.iter().any(|q| q == id).then(|| json!({"run_id": id, "status": "queued"}))),
        })
    }

    pub fn last_call(&self, kid: &str) -> Option<(String, Value)> {
        self.calls.lock().unwrap().iter().rev().find(|c| c.0 == kid).map(|c| (c.1.clone(), c.2.clone()))
    }

    fn emit_locked(k: &mut FakeKernel, kind: &str, data: Value) {
        k.seq += 1;
        let e = KernelEvent { seq: Some(k.seq), kind: kind.into(), time: now(), data };
        k.ring.push(e.clone());
        if k.ring.len() > k.ring_max {
            k.ring.remove(0);
        }
        let _ = k.tx.send(e);
    }

    pub fn emit(&self, kid: &str, kind: &str, data: Value) {
        let mut ks = self.kernels.lock().unwrap();
        Self::emit_locked(ks.get_mut(kid).unwrap(), kind, data);
    }

    fn start_run_locked(k: &mut FakeKernel, run_id: String, mode: &str) {
        k.running = Some(summary(&run_id, "running", mode));
        k.info.status = "busy".into();
        k.info.run_id = Some(run_id.clone());
        Self::emit_locked(k, "kernel.status", json!({"status": "busy"}));
        Self::emit_locked(k, "run.started", json!({"run_id": run_id}));
        Self::emit_locked(k, "cell.started", json!({"run_id": run_id, "index": 0}));
        Self::emit_locked(
            k,
            "output",
            json!({"run_id": run_id, "index": 0, "output": {"output_type": "stream", "name": "stdout", "text": "hello\n"}}),
        );
    }

    fn end_run_locked(k: &mut FakeKernel, status: &str) {
        let Some(mut s) = k.running.take() else { return };
        let run_id = s["run_id"].as_str().unwrap().to_string();
        s["status"] = json!(status);
        k.finished.insert(0, s);
        Self::emit_locked(k, "cell.finished", json!({"run_id": run_id, "index": 0}));
        Self::emit_locked(k, "run.finished", json!({"run_id": run_id, "status": status}));
        Self::emit_locked(k, "kernel.status", json!({"status": "idle"}));
        k.info.status = "idle".into();
        k.info.run_id = None;
        if !k.queue.is_empty() {
            let next = k.queue.remove(0);
            k.info.queue = k.queue.clone();
            Self::start_run_locked(k, next, "all");
        }
    }

    /// Finishes the executing run with status `ok` (and starts the next queued one).
    pub fn finish_run(&self, kid: &str) {
        let mut ks = self.kernels.lock().unwrap();
        Self::end_run_locked(ks.get_mut(kid).unwrap(), "ok");
    }
}

#[async_trait]
impl KernelBackend for FakeBackend {
    async fn list(&self, _refresh: bool) -> Result<Vec<KernelInfo>> {
        self.check_fail("list")?;
        let mut v: Vec<_> = self.kernels.lock().unwrap().values().map(|k| k.info.clone()).collect();
        v.sort_by(|a, b| a.kernel_id.cmp(&b.kernel_id));
        Ok(v)
    }

    async fn get(&self, kernel_id: &str) -> Result<KernelInfo> {
        self.check_fail("get")?;
        self.kernels
            .lock()
            .unwrap()
            .get(kernel_id)
            .map(|k| k.info.clone())
            .ok_or_else(|| DpxError::new("not_found", format!("no running kernel {kernel_id}")))
    }

    async fn ensure(&self, req: StartKernel) -> Result<Ensured> {
        self.check_fail("ensure")?;
        if !Path::new(&req.path).is_file() {
            return Err(DpxError::new("not_found", format!("no such file: {}", req.path)));
        }
        let path = Path::new(&req.path).canonicalize().unwrap().to_string_lossy().into_owned();
        let kid = kernel_id_for(&path);
        if let Ok(k) = self.get(&kid).await {
            return Ok(Ensured { kernel: k, launched: false });
        }
        self.launches.lock().unwrap().push(req);
        self.add(&path);
        Ok(Ensured { kernel: self.get(&kid).await?, launched: true })
    }

    async fn request(&self, kernel_id: &str, method: &str, params: Value) -> Result<Value> {
        self.check_fail(method)?;
        if method == "runs.wait" {
            // Long-poll like the kernel: until the run finishes or `timeout` seconds pass.
            self.calls.lock().unwrap().push((kernel_id.into(), method.into(), params.clone()));
            let timeout = params["timeout"].as_f64().unwrap_or(60.0);
            let deadline = tokio::time::Instant::now() + Duration::from_secs_f64(timeout);
            let mut run_ref = params["run_id"].as_str().unwrap_or_default().to_string();
            if run_ref == "current" {
                let cur = self.wait_probe(kernel_id, "current")?;
                run_ref = match cur.as_ref().and_then(|s| s["run_id"].as_str()) {
                    Some(id) => id.to_string(),
                    None => return Err(DpxError::new("not_found", "no run is executing")),
                };
            }
            loop {
                let found = self.wait_probe(kernel_id, &run_ref)?;
                if let Some(s) = &found {
                    if s["status"] != "running" && s["status"] != "queued" {
                        return Ok(wait_answer(s));
                    }
                }
                if tokio::time::Instant::now() >= deadline {
                    return match &found {
                        Some(s) => Ok(wait_answer(s)),
                        None => Err(DpxError::new("not_found", format!("unknown run {run_ref}"))),
                    };
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        let mut ks = self.kernels.lock().unwrap();
        let k = ks
            .get_mut(kernel_id)
            .ok_or_else(|| DpxError::new("kernel_unreachable", format!("no connection to {kernel_id}")))?;
        self.calls.lock().unwrap().push((kernel_id.into(), method.into(), params.clone()));
        match method {
            "run" => {
                k.run_counter += 1;
                let run_id = format!("20261003-120000-{:04x}", k.run_counter);
                let mode = params["mode"].as_str().unwrap_or("all").to_string();
                if let Some(cur) = &k.running {
                    if params["on_busy"] == "queue" {
                        k.queue.push(run_id.clone());
                        k.info.queue = k.queue.clone();
                        Self::emit_locked(k, "run.queued", json!({"run_id": run_id}));
                        return Ok(json!({"run_id": run_id, "state": "queued", "position": k.queue.len()}));
                    }
                    return Err(DpxError::new("busy", "another run is executing")
                        .with_data(json!({"current": cur, "queue_length": k.queue.len()})));
                }
                Self::start_run_locked(k, run_id.clone(), &mode);
                Ok(json!({"run_id": run_id, "state": "running"}))
            }
            "interrupt" => match &k.running {
                Some(cur) => {
                    let run_id = cur["run_id"].clone();
                    Self::end_run_locked(k, "interrupted");
                    Ok(json!({"interrupted": true, "run_id": run_id}))
                }
                None => Ok(json!({"interrupted": false})),
            },
            "cancel" => {
                let id = params["run_id"].as_str().unwrap_or_default();
                let before = k.queue.len();
                k.queue.retain(|r| r != id);
                k.info.queue = k.queue.clone();
                Ok(json!({"cancelled": k.queue.len() != before}))
            }
            "runs.list" => {
                let limit = params["limit"].as_u64().unwrap_or(20) as usize;
                Ok(json!({"runs": k.finished.iter().take(limit).cloned().collect::<Vec<_>>()}))
            }
            "runs.get" => {
                let id = params["run_id"].as_str().unwrap_or_default();
                let found = match id {
                    "latest" => k.finished.first().cloned(),
                    "current" => k.running.clone(),
                    _ => k.finished.iter().chain(k.running.iter()).find(|s| s["run_id"] == id).cloned(),
                };
                found.map(|s| notebook(&s)).ok_or_else(|| DpxError::new("not_found", format!("no such run: {id}")))
            }
            "namespace" => Ok(json!({"variables": [{"name": "x", "type": "int", "repr": "1"}]})),
            "restart" => Ok(json!({})),
            "shutdown" => {
                if !k.linger_on_shutdown {
                    ks.remove(kernel_id);
                }
                Ok(json!({}))
            }
            "status" => Ok(serde_json::to_value(&k.info).unwrap()),
            "doc.snapshot" => {
                let cells: Vec<Value> = (0..k.doc.cells.len()).map(|i| cell_json(&k.doc, i)).collect();
                Ok(json!({"doc_version": k.doc.doc_version, "seq": k.seq, "cells": cells, "presence": k.doc.presence}))
            }
            "doc.cell.create" => {
                doc_client(&params)?;
                let pos = if let Some(a) = params["after"].as_str() {
                    find_cell(&k.doc, a)? + 1
                } else if let Some(b) = params["before"].as_str() {
                    find_cell(&k.doc, b)?
                } else {
                    k.doc.cells.len()
                };
                k.doc.next_id += 1;
                let cell = FakeCell {
                    cell_id: format!("c_{:012x}", k.doc.next_id),
                    kind: params["type"].as_str().unwrap_or("code").to_string(),
                    source: params["source"].as_str().unwrap_or_default().to_string(),
                    version: 1,
                };
                k.doc.cells.insert(pos, cell);
                k.doc.doc_version += 1;
                let cell = cell_json(&k.doc, pos);
                let data = json!({"doc_version": k.doc.doc_version, "cell": cell, "by": by_of(&params)});
                Self::emit_locked(k, "doc.cell.created", data);
                Ok(json!({"cell": cell}))
            }
            "doc.cell.update" => {
                let cid = doc_client(&params)?;
                let i = find_cell(&k.doc, params["cell_id"].as_str().unwrap_or_default())?;
                check_lock(&k.doc, &k.doc.cells[i].cell_id, &cid)?;
                check_base(&k.doc, i, params["base_version"].as_u64())?;
                if let Some(src) = params["source"].as_str() {
                    k.doc.cells[i].source = src.to_string();
                }
                if let Some(t) = params["type"].as_str() {
                    k.doc.cells[i].kind = t.to_string();
                }
                k.doc.cells[i].version += 1;
                k.doc.doc_version += 1;
                let cell = cell_json(&k.doc, i);
                let data = json!({"doc_version": k.doc.doc_version, "cell": cell, "by": by_of(&params)});
                Self::emit_locked(k, "doc.cell.updated", data);
                Ok(json!({"cell": cell}))
            }
            "doc.cell.delete" => {
                let cid = doc_client(&params)?;
                let i = find_cell(&k.doc, params["cell_id"].as_str().unwrap_or_default())?;
                if i == 0 {
                    return Err(DpxError::new("bad_request", "the preamble cannot be deleted"));
                }
                check_lock(&k.doc, &k.doc.cells[i].cell_id, &cid)?;
                check_base(&k.doc, i, params["base_version"].as_u64())?;
                let gone = k.doc.cells.remove(i);
                k.doc.locks.remove(&gone.cell_id);
                k.doc.doc_version += 1;
                let data = json!({"doc_version": k.doc.doc_version, "cell_id": gone.cell_id, "by": by_of(&params)});
                Self::emit_locked(k, "doc.cell.deleted", data);
                Ok(json!({"deleted": true}))
            }
            "doc.cell.move" => {
                doc_client(&params)?;
                let i = find_cell(&k.doc, params["cell_id"].as_str().unwrap_or_default())?;
                let to = params["to_index"].as_u64().unwrap_or(0) as usize;
                if i == 0 || to < 1 || to >= k.doc.cells.len() {
                    return Err(DpxError::new("bad_request", "to_index out of range"));
                }
                let c = k.doc.cells.remove(i);
                k.doc.cells.insert(to, c);
                k.doc.doc_version += 1;
                let cell = cell_json(&k.doc, to);
                let data = json!({"doc_version": k.doc.doc_version, "cell": cell, "by": by_of(&params)});
                Self::emit_locked(k, "doc.cell.moved", data);
                Ok(json!({"cell": cell}))
            }
            "doc.lock" => {
                let cid = doc_client(&params)?;
                let i = find_cell(&k.doc, params["cell_id"].as_str().unwrap_or_default())?;
                let cell_id = k.doc.cells[i].cell_id.clone();
                check_lock(&k.doc, &cell_id, &cid)?;
                if let Some(l) = k.doc.locks.get(&cell_id) {
                    return Ok(json!({"lock": l}));
                }
                let c = &params["client"];
                let lock = json!({
                    "cell_id": cell_id, "locked_by": cid, "user": c["user"], "nickname": c["nickname"],
                    "locked_at": now(), "last_activity": now(), "expires_at": "2026-10-03T12:03:00Z",
                });
                k.doc.locks.insert(cell_id.clone(), lock.clone());
                let data = json!({"doc_version": k.doc.doc_version, "cell_id": cell_id, "lock": lock, "by": by_of(&params)});
                Self::emit_locked(k, "doc.lock", data);
                Ok(json!({"lock": lock}))
            }
            "doc.unlock" => {
                let cid = doc_client(&params)?;
                let i = find_cell(&k.doc, params["cell_id"].as_str().unwrap_or_default())?;
                let cell_id = k.doc.cells[i].cell_id.clone();
                check_lock(&k.doc, &cell_id, &cid)?;
                if let Some(src) = params["source"].as_str() {
                    if let Some(base) = params["base_version"].as_u64() {
                        check_base(&k.doc, i, Some(base))?;
                    }
                    if k.doc.cells[i].source != src {
                        k.doc.cells[i].source = src.to_string();
                        k.doc.cells[i].version += 1;
                        k.doc.doc_version += 1;
                        let data = json!({"doc_version": k.doc.doc_version, "cell": cell_json(&k.doc, i), "by": by_of(&params)});
                        Self::emit_locked(k, "doc.cell.updated", data);
                    }
                }
                if k.doc.locks.remove(&cell_id).is_some() {
                    let data = json!({"doc_version": k.doc.doc_version, "cell_id": cell_id, "by": by_of(&params), "reason": "released"});
                    Self::emit_locked(k, "doc.unlock", data);
                }
                Ok(json!({"cell": cell_json(&k.doc, i)}))
            }
            "presence.update" => {
                let cid = doc_client(&params)?;
                let c = &params["client"];
                let old = k.doc.presence.iter().position(|p| p["client_id"] == cid.as_str());
                let mut entry = match old {
                    Some(j) => k.doc.presence[j].clone(),
                    None => json!({"focused_cell_id": null, "focused_at": null, "cursor": null}),
                };
                entry["client_id"] = json!(cid);
                entry["nickname"] = c["nickname"].clone();
                entry["user"] = c["user"].clone();
                entry["permission"] = c["permission"].clone();
                entry["last_seen"] = json!(now());
                match c.get("avatar") {
                    Some(a) => entry["avatar"] = a.clone(),
                    None => {
                        if let Some(m) = entry.as_object_mut() {
                            m.remove("avatar");
                        }
                    }
                }
                if let Some(f) = params.get("focused_cell_id") {
                    entry["focused_cell_id"] = f.clone();
                    entry["focused_at"] = if f.is_null() { Value::Null } else { json!(now()) };
                }
                if let Some(cur) = params.get("cursor") {
                    entry["cursor"] = cur.clone();
                }
                // Like the kernel: a bare heartbeat of a present client emits nothing.
                let changed = old.is_none() || params.get("focused_cell_id").is_some() || params.get("cursor").is_some();
                match old {
                    Some(j) => k.doc.presence[j] = entry.clone(),
                    None => k.doc.presence.push(entry.clone()),
                }
                if changed {
                    Self::emit_locked(k, "presence.update", entry);
                }
                Ok(json!({}))
            }
            "presence.leave" => {
                let cid = doc_client(&params)?;
                if let Some(j) = k.doc.presence.iter().position(|p| p["client_id"] == cid.as_str()) {
                    let entry = k.doc.presence.remove(j);
                    Self::emit_locked(k, "presence.leave", entry);
                }
                Ok(json!({}))
            }
            other => Err(DpxError::new("bad_request", format!("unknown method {other}"))),
        }
    }

    async fn subscribe(&self, kernel_id: &str, since: Option<u64>) -> Result<EventStream> {
        self.check_fail("subscribe")?;
        let ks = self.kernels.lock().unwrap();
        let k = ks.get(kernel_id).ok_or_else(|| DpxError::new("not_found", format!("no running kernel {kernel_id}")))?;
        let rx = k.tx.subscribe();
        let mut backlog = Vec::new();
        if let Some(since) = since {
            let oldest = k.ring.first().and_then(|e| e.seq).unwrap_or(k.seq + 1);
            if since + 1 < oldest {
                backlog.push(KernelEvent {
                    seq: None,
                    kind: "replay_truncated".into(),
                    time: now(),
                    data: json!({"oldest_seq": oldest}),
                });
            }
            backlog.extend(k.ring.iter().filter(|e| e.seq.unwrap_or(0) > since).cloned());
        }
        self.subscribers.fetch_add(1, Ordering::SeqCst);
        let guard = SubGuard(self.subscribers.clone());
        let live = futures::stream::unfold((rx, guard), |(mut rx, g)| async move {
            loop {
                match rx.recv().await {
                    Ok(e) => return Some((e, (rx, g))),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        });
        Ok(Box::pin(futures::stream::iter(backlog).chain(live)))
    }

    async fn document(&self, path: &str, viewer_outputs: bool) -> Result<Value> {
        self.check_fail("document")?;
        let source = std::fs::read_to_string(path).map_err(|_| DpxError::new("not_found", format!("no such file: {path}")))?;
        let mut cell = json!({"index": 0, "type": "preamble", "title": null, "source": source,
            "source_sha256": hex::encode(sha2::Sha256::digest(source.as_bytes())), "metadata": {},
            "execution_count": 1, "status": "ok", "stale": false, "run_id": FAKE_DOC_RUN});
        if viewer_outputs {
            cell["outputs"] = json!([{"output_type": "stream", "name": "stdout", "text": "hello\n"}]);
        }
        Ok(json!({"path": path, "latest_run": null, "cells": [cell]}))
    }

    async fn kill(&self, kernel_id: &str) -> Result<()> {
        self.check_fail("kill")?;
        self.kills.lock().unwrap().push(kernel_id.to_string());
        self.kernels.lock().unwrap().remove(kernel_id);
        Ok(())
    }
}

// ------------------------------------------------------------------ contract checker (NFR-M3)

pub struct Contract {
    /// (path segments, method, documented statuses)
    ops: Vec<(Vec<String>, String, Vec<u16>)>,
    pub raw: serde_yaml::Value,
}

pub fn contract() -> &'static Contract {
    static C: OnceLock<Contract> = OnceLock::new();
    C.get_or_init(|| {
        let text = std::fs::read_to_string(repo_root().join("docs/api/manager.openapi.yaml")).unwrap();
        let raw: serde_yaml::Value = serde_yaml::from_str(&text).unwrap();
        let mut ops = Vec::new();
        for (path, item) in raw["paths"].as_mapping().unwrap() {
            let path = path.as_str().unwrap();
            for (method, op) in item.as_mapping().unwrap() {
                let method = method.as_str().unwrap();
                if !["get", "put", "post", "delete", "patch", "head", "options"].contains(&method) {
                    continue;
                }
                let statuses = op["responses"]
                    .as_mapping()
                    .unwrap()
                    .keys()
                    .map(|k| match k {
                        serde_yaml::Value::String(s) => s.parse().unwrap(),
                        serde_yaml::Value::Number(n) => n.as_u64().unwrap() as u16,
                        _ => panic!("status key"),
                    })
                    .collect();
                let segs = path.split('/').map(String::from).collect();
                ops.push((segs, method.to_uppercase(), statuses));
            }
        }
        Contract { ops, raw }
    })
}

impl Contract {
    pub fn operations(&self) -> impl Iterator<Item = (String, String, &Vec<u16>)> {
        self.ops.iter().map(|(s, m, st)| (s.join("/"), m.clone(), st))
    }

    /// Documented statuses of the operation `method path` resolves to, if any. Literal
    /// segments win over `{param}` ones (e.g. `/api/v1/documents`).
    pub fn documented(&self, method: &str, path: &str) -> Option<&Vec<u16>> {
        let segs: Vec<&str> = path.split('?').next().unwrap().split('/').collect();
        let mut best: Option<(usize, &Vec<u16>)> = None;
        for (tpl, m, st) in &self.ops {
            if m != method || tpl.len() != segs.len() {
                continue;
            }
            let mut literal = 0;
            let ok = tpl.iter().zip(&segs).all(|(t, s)| {
                if t.starts_with('{') {
                    true
                } else {
                    literal += 1;
                    t == s
                }
            });
            if ok && best.is_none_or(|(l, _)| literal > l) {
                best = Some((literal, st));
            }
        }
        best.map(|(_, st)| st)
    }

    pub fn check(&self, method: &str, path: &str, status: u16) {
        if let Some(st) = self.documented(method, path) {
            assert!(st.contains(&status), "{method} {path} answered {status}, documented {st:?}");
        }
    }
}

// ------------------------------------------------------------------ server + client

pub struct TestServer {
    pub handle: Option<ServerHandle>,
    pub url: String,
    pub token: String,
    pub backend: Arc<FakeBackend>,
    pub home: PathBuf,
    pub scratch: PathBuf,
}

impl TestServer {
    pub async fn start(mode: Mode) -> Self {
        Self::start_with(mode, |_| {}).await
    }

    pub async fn start_with(mode: Mode, tweak: impl FnOnce(&mut ServerConfig)) -> Self {
        let scratch = scratch("srv");
        let home = scratch.join("home");
        let mut cfg = match mode {
            Mode::Ephemeral => ServerConfig::ephemeral(&home),
            Mode::Dedicated => ServerConfig::dedicated(&home, "127.0.0.1", 0),
        };
        tweak(&mut cfg);
        Self::start_cfg(cfg, FakeBackend::new(), scratch).await
    }

    pub async fn start_cfg(cfg: ServerConfig, backend: Arc<FakeBackend>, scratch: PathBuf) -> Self {
        let home = cfg.home.clone();
        let handle = serve(cfg, backend.clone()).await.expect("serve");
        Self { url: handle.url.clone(), token: handle.token.clone(), handle: Some(handle), backend, home, scratch }
    }

    /// A notebook file in this server's scratch dir, with a running fake kernel.
    pub fn kernel(&self, name: &str) -> (String, String) {
        let path = self.notebook(name);
        let kid = self.backend.add(&path);
        (kid, path)
    }

    pub fn notebook(&self, name: &str) -> String {
        let path = self.scratch.join(name);
        std::fs::write(&path, "print('hello')\n").unwrap();
        path.canonicalize().unwrap().to_string_lossy().into_owned()
    }

    pub fn admin(&self) -> Api {
        Api::new(&self.url, Some(&self.token))
    }

    pub fn anon(&self) -> Api {
        Api::new(&self.url, None)
    }

    pub fn with_token(&self, token: &str) -> Api {
        Api::new(&self.url, Some(token))
    }

    pub async fn stop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.shutdown();
            h.wait().await;
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(h) = &self.handle {
            h.shutdown();
        }
        let _ = std::fs::remove_dir_all(&self.scratch);
    }
}

pub struct Api {
    pub client: reqwest::Client,
    pub base: String,
    pub token: Option<String>,
}

pub struct Resp {
    pub status: u16,
    pub headers: reqwest::header::HeaderMap,
    pub body: Value,
    pub text: String,
}

impl Resp {
    /// Asserts the Error shape and returns `error`.
    pub fn error(&self, status: u16, code: &str) -> Value {
        assert_eq!(self.status, status, "{}", self.text);
        let obj = self.body.as_object().expect("error body is an object");
        assert_eq!(obj.keys().collect::<Vec<_>>(), ["error"], "{}", self.text);
        assert_eq!(self.body["error"]["code"], code, "{}", self.text);
        assert!(self.body["error"]["message"].is_string());
        self.body["error"].clone()
    }
}

impl Api {
    pub fn new(base: &str, token: Option<&str>) -> Self {
        let client = reqwest::Client::builder().timeout(Duration::from_secs(20)).build().unwrap();
        Self { client, base: base.to_string(), token: token.map(String::from) }
    }

    pub fn req(&self, method: &str, path: &str) -> reqwest::RequestBuilder {
        let rb = self.client.request(method.parse().unwrap(), format!("{}{}", self.base, path));
        match &self.token {
            Some(t) => rb.bearer_auth(t),
            None => rb,
        }
    }

    pub async fn send(&self, method: &str, path: &str, rb: reqwest::RequestBuilder) -> Resp {
        let r = rb.send().await.expect("request");
        let status = r.status().as_u16();
        let headers = r.headers().clone();
        let text = r.text().await.unwrap_or_default();
        let body = serde_json::from_str(&text).unwrap_or(Value::Null);
        contract().check(method, path, status);
        Resp { status, headers, body, text }
    }

    pub async fn call(&self, method: &str, path: &str, body: Option<Value>) -> Resp {
        let mut rb = self.req(method, path);
        if let Some(b) = body {
            rb = rb.json(&b);
        }
        self.send(method, path, rb).await
    }

    pub async fn get(&self, path: &str) -> Resp {
        self.call("GET", path, None).await
    }
    pub async fn post(&self, path: &str, body: Value) -> Resp {
        self.call("POST", path, Some(body)).await
    }
    pub async fn delete(&self, path: &str) -> Resp {
        self.call("DELETE", path, None).await
    }
    pub async fn raw(&self, method: &str, path: &str, body: &'static [u8]) -> Resp {
        let rb = self.req(method, path).header("content-type", "application/json").body(body);
        self.send(method, path, rb).await
    }
}

// ------------------------------------------------------------------ SSE reader

pub struct Sse {
    stream: std::pin::Pin<Box<dyn futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Send>>,
    buf: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SseMsg {
    pub id: Option<String>,
    pub event: Option<String>,
    pub data: Value,
}

impl Sse {
    pub fn new(resp: reqwest::Response) -> Self {
        Self { stream: Box::pin(resp.bytes_stream()), buf: String::new() }
    }

    /// Next raw block (up to a blank line), comments included.
    pub async fn next_block(&mut self) -> Option<String> {
        loop {
            if let Some(i) = self.buf.find("\n\n") {
                let block = self.buf[..i].to_string();
                self.buf.drain(..i + 2);
                return Some(block);
            }
            let chunk = tokio::time::timeout(Duration::from_secs(10), self.stream.next()).await.ok()??.ok()?;
            self.buf.push_str(&String::from_utf8_lossy(&chunk));
        }
    }

    /// Next message, comments skipped.
    pub async fn next_msg(&mut self) -> Option<SseMsg> {
        loop {
            let block = self.next_block().await?;
            let mut m = SseMsg { id: None, event: None, data: Value::Null };
            let mut any = false;
            for line in block.lines() {
                if line.starts_with(':') {
                    continue;
                }
                let (f, v) = line.split_once(':').unwrap_or((line, ""));
                let v = v.strip_prefix(' ').unwrap_or(v);
                any = true;
                match f {
                    "id" => m.id = Some(v.to_string()),
                    "event" => m.event = Some(v.to_string()),
                    "data" => m.data = serde_json::from_str(v).unwrap(),
                    _ => {}
                }
            }
            if any {
                return Some(m);
            }
        }
    }

    pub async fn take(&mut self, n: usize) -> Vec<SseMsg> {
        let mut out = Vec::new();
        for _ in 0..n {
            out.push(self.next_msg().await.expect("sse message"));
        }
        out
    }
}

pub async fn wait_for(mut pred: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if pred() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    pred()
}
