//! `RealBackend`: the [`KernelBackend`] of the DarkPyonix manager against real kernels.
//!
//! * discovery: loopback UDP multicast merged with the registry (FR-D1, FR-D2);
//! * one shared DKP/1 connection per kernel for requests and event fan-out (FR-M5, PR-2, PR-3);
//! * idempotent start of the embedded stdlib-Python kernel (FR-M2, INTENT D3, D10);
//! * the FR-R4 document via `darkpyonix.kernel.document` in Python.
//!
//! See dpx-core for the contract.

pub mod discovery;
pub mod dkp;
pub mod home;
pub mod ident;
pub mod launch;
pub mod process;
pub mod registry;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use dpx_core::{DpxError, Ensured, EventStream, KernelBackend, KernelInfo, Result, StartKernel};
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use crate::discovery::Discovery;
use crate::dkp::{Connection, FanoutConfig};
use crate::registry::{entry_pid, Entry};

pub use crate::ident::{canonical_path, kernel_id_for};

/// FR-M2: how long `ensure` waits for a launched kernel to announce itself.
pub const START_TIMEOUT: Duration = Duration::from_secs(10);
/// Extra time a `runs.wait` request gets beyond its own `timeout` before the manager gives up.
const RUNS_WAIT_MARGIN: Duration = Duration::from_secs(15);
const READY_STATUSES: [&str; 2] = ["idle", "busy"];

/// Backend settings. [`Config::from_env`] reads `DARKPYONIX_HOME`, `DARKPYONIX_DISCOVERY` and
/// `DARKPYONIX_PYTHON`; tests build one explicitly so that parallel tests never share state.
#[derive(Debug, Clone)]
pub struct Config {
    /// The runtime home (PROTOCOL §1).
    pub home: PathBuf,
    /// `false` = `DARKPYONIX_DISCOVERY=registry` (no multicast).
    pub multicast: bool,
    /// Default interpreter for kernels and the document builder (`DARKPYONIX_PYTHON`).
    pub python: Option<String>,
    pub fanout: FanoutConfig,
    pub start_timeout: Duration,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            home: home::default_home(),
            multicast: std::env::var("DARKPYONIX_DISCOVERY")
                .map(|v| v.trim().to_lowercase() != "registry")
                .unwrap_or(true),
            python: std::env::var("DARKPYONIX_PYTHON")
                .ok()
                .filter(|p| !p.is_empty()),
            fanout: FanoutConfig::default(),
            start_timeout: START_TIMEOUT,
        }
    }

    /// Defaults with an explicit home.
    pub fn with_home(home: impl Into<PathBuf>) -> Self {
        Self {
            home: ident::abspath(&home.into()),
            ..Self::from_env()
        }
    }
}

type Slot = Arc<AsyncMutex<Option<Arc<Connection>>>>;

pub struct RealBackend {
    cfg: Config,
    key: Vec<u8>,
    user_tag: String,
    discovery: Arc<Discovery>,
    conns: std::sync::Mutex<HashMap<String, Slot>>,
    starting: std::sync::Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
    runtime_root: OnceLock<PathBuf>,
}

fn internal(msg: impl Into<String>) -> DpxError {
    DpxError::new("internal", msg)
}

fn not_found(kernel_id: &str) -> DpxError {
    DpxError::new("not_found", format!("no running kernel {kernel_id}"))
}

/// Build the OpenAPI `Kernel` from an announce / registry entry / `status` result.
pub fn kernel_info(entry: &Entry) -> KernelInfo {
    let s = |k: &str| {
        entry
            .get(k)
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    };
    KernelInfo {
        kernel_id: s("kernel_id"),
        path: s("path"),
        pid: entry_pid(entry),
        status: entry
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("starting")
            .to_string(),
        run_id: entry
            .get("run_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        queue: entry
            .get("queue")
            .and_then(Value::as_array)
            .map(|q| {
                q.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
        execution_count: entry
            .get("execution_count")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        python: entry.get("python").cloned().unwrap_or_else(|| json!({})),
        started_at: s("started_at"),
        host: s("host"),
        kernel_version: entry
            .get("kernel_version")
            .or_else(|| entry.get("dkp_kernel_version"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        runs_dir: s("runs_dir"),
        port: entry
            .get("port")
            .and_then(Value::as_u64)
            .filter(|p| *p <= u16::MAX as u64)
            .unwrap_or(0) as u16,
    }
}

impl RealBackend {
    /// Open (and create if needed) the runtime home and its `user.key`.
    pub fn new(cfg: Config) -> Result<Self> {
        home::create_dir_private(&cfg.home)
            .map_err(|e| internal(format!("runtime home {}: {e}", cfg.home.display())))?;
        let key = home::user_key(&cfg.home).map_err(|e| internal(format!("user.key: {e}")))?;
        let user_tag = home::user_tag(&key);
        let kernels_dir = home::subdir(&cfg.home, "kernels")
            .map_err(|e| internal(format!("kernels dir: {e}")))?;
        let discovery = Discovery::new(user_tag.clone(), kernels_dir, cfg.multicast);
        Ok(Self {
            cfg,
            key,
            user_tag,
            discovery,
            conns: Default::default(),
            starting: Default::default(),
            runtime_root: OnceLock::new(),
        })
    }

    pub fn from_env() -> Result<Self> {
        Self::new(Config::from_env())
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    pub fn user_tag(&self) -> &str {
        &self.user_tag
    }

    pub fn home(&self) -> &Path {
        &self.cfg.home
    }

    /// The extracted kernel root (`<home>/runtime/<version>-<hash>`), extracting on first use.
    pub fn runtime_root(&self) -> Result<PathBuf> {
        if let Some(r) = self.runtime_root.get() {
            return Ok(r.clone());
        }
        let root = launch::extract_runtime(&self.cfg.home)
            .map_err(|e| internal(format!("extracting the kernel runtime: {e}")))?;
        Ok(self.runtime_root.get_or_init(|| root).clone())
    }

    fn slot(&self, kernel_id: &str) -> Slot {
        self.conns
            .lock()
            .unwrap()
            .entry(kernel_id.to_string())
            .or_default()
            .clone()
    }

    /// The shared connection to `kernel_id`, (re)connecting on demand.
    async fn connection(&self, kernel_id: &str) -> Result<Arc<Connection>> {
        let slot = self.slot(kernel_id);
        let mut guard = slot.lock().await;
        if let Some(c) = guard.as_ref() {
            if !c.is_closed() {
                return Ok(c.clone());
            }
        }
        *guard = None;
        let mut last_err = None;
        for attempt in 0..2 {
            let entry = self
                .discovery
                .find(kernel_id)
                .await
                .ok_or_else(|| not_found(kernel_id))?;
            let port = kernel_info(&entry).port;
            if port == 0 {
                return Err(DpxError::new(
                    "kernel_unreachable",
                    format!("kernel {kernel_id} has no control channel"),
                ));
            }
            match Connection::connect(port, kernel_id, &self.key, self.cfg.fanout).await {
                Ok(c) => {
                    *guard = Some(c.clone());
                    return Ok(c);
                }
                Err(e) if e.code == "kernel_unreachable" && attempt == 0 => {
                    // A cached announce may name an old port (e.g. after a hard restart).
                    self.discovery.forget_cached(kernel_id);
                    last_err = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err.unwrap_or_else(|| DpxError::new("kernel_unreachable", "cannot connect")))
    }

    fn drop_connection(&self, kernel_id: &str) {
        if let Some(slot) = self.conns.lock().unwrap().remove(kernel_id) {
            if let Ok(guard) = slot.try_lock() {
                if let Some(c) = guard.as_ref() {
                    c.close();
                }
            }
        }
    }

    async fn info_with_status(&self, entry: Entry) -> KernelInfo {
        let mut info = kernel_info(&entry);
        if info.port == 0 {
            return info;
        }
        let status = match self.connection(&info.kernel_id).await {
            Ok(c) => {
                c.request_timeout("status", json!({}), Duration::from_secs(5))
                    .await
            }
            Err(e) => Err(e),
        };
        if let Ok(Value::Object(mut st)) = status {
            for (k, v) in entry {
                st.entry(k).or_insert(v);
            }
            info = kernel_info(&st);
        }
        info
    }

    /// Wait for `kernel_id` to announce `idle`/`busy`; `child` is the process we launched.
    async fn wait_ready(&self, kernel_id: &str, child: &mut launch::Spawned) -> Result<Ensured> {
        let pid = child.pid;
        let deadline = tokio::time::Instant::now() + self.cfg.start_timeout;
        let mut child_gone_since: Option<tokio::time::Instant> = None;
        loop {
            let notified = self.discovery.changed.notified();
            if let Some(entry) = self.discovery.snapshot(Some(kernel_id)).pop() {
                let status = entry.get("status").and_then(Value::as_str).unwrap_or("");
                if READY_STATUSES.contains(&status) {
                    let launched = entry_pid(&entry) == pid;
                    if !launched {
                        // Another manager's kernel won the start race and holds the file
                        // lock. Ours has not announced, so it is not a kernel yet; stop it
                        // so that it cannot take over later when the winner exits.
                        child.abort();
                    }
                    return Ok(Ensured {
                        kernel: self.info_with_status(entry).await,
                        launched,
                    });
                }
            }
            let now = tokio::time::Instant::now();
            if !process::pid_alive(pid) {
                // Lost a start race (exit 3) or crashed: give a winner's announce a moment.
                if child_gone_since.is_none() {
                    // The winner may be a live kernel whose registry entry is missing (it is
                    // rewritten only every 5 s); ask it directly once.
                    self.discovery
                        .query(Some(kernel_id), discovery::QUERY_TIMEOUT, &Default::default())
                        .await;
                    child_gone_since = Some(now);
                    continue;
                }
                let since = child_gone_since.unwrap_or(now);
                if now - since > Duration::from_millis(500) {
                    let log = self
                        .cfg
                        .home
                        .join("kernels")
                        .join(format!("{kernel_id}.log"));
                    let tail = std::fs::read_to_string(&log).unwrap_or_default();
                    let tail: String = tail
                        .chars()
                        .rev()
                        .take(2000)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect();
                    return Err(DpxError::new("start_timeout", "kernel process exited before announcing itself")
                        .with_data(json!({"kernel_id": kernel_id, "log": log.to_string_lossy(), "log_tail": tail})));
                }
            }
            if now >= deadline {
                return Err(DpxError::new(
                    "start_timeout",
                    format!(
                        "kernel {kernel_id} did not announce itself within {} s",
                        self.cfg.start_timeout.as_secs()
                    ),
                )
                .with_data(json!({"kernel_id": kernel_id, "pid": pid})));
            }
            let _ = tokio::time::timeout(Duration::from_millis(25), notified).await;
        }
    }
}

#[async_trait]
impl KernelBackend for RealBackend {
    async fn list(&self, refresh: bool) -> Result<Vec<KernelInfo>> {
        Ok(self
            .discovery
            .discover(refresh)
            .await
            .iter()
            .map(kernel_info)
            .collect())
    }

    async fn get(&self, kernel_id: &str) -> Result<KernelInfo> {
        let entry = self
            .discovery
            .find(kernel_id)
            .await
            .ok_or_else(|| not_found(kernel_id))?;
        Ok(self.info_with_status(entry).await)
    }

    async fn ensure(&self, req: StartKernel) -> Result<Ensured> {
        if req.path.trim().is_empty() {
            return Err(DpxError::new("bad_request", "path is required"));
        }
        let canonical = canonical_path(Path::new(&req.path));
        let kernel_id = ident::kernel_id_for_canonical(&canonical);
        let lock = self
            .starting
            .lock()
            .unwrap()
            .entry(kernel_id.clone())
            .or_default()
            .clone();
        let _guard = lock.lock().await;

        // Listen before launching so that the child's first announce is never missed.
        self.discovery.ensure_listening();
        // Cache + registry only, no multicast query: every announce is mirrored to the
        // registry (PROTOCOL §2.4), so a live kernel of this home is already visible here.
        // A targeted query would cost its full QUERY_TIMEOUT (200 ms) on every real launch,
        // because a kernel that does not exist never answers. The rare live kernel without a
        // registry entry is still found: our child then loses the file lock (FR-K3) and
        // `wait_ready` queries for the winner.
        if let Some(entry) = self.discovery.snapshot(Some(&kernel_id)).pop() {
            return Ok(Ensured {
                kernel: self.info_with_status(entry).await,
                launched: false,
            });
        }
        if !Path::new(&canonical).is_file() {
            return Err(DpxError::new(
                "bad_request",
                format!("no such file: {canonical}"),
            ));
        }
        if let Some(cwd) = &req.cwd {
            if !Path::new(cwd).is_dir() {
                return Err(DpxError::new(
                    "bad_request",
                    format!("cwd is not a directory: {cwd}"),
                ));
            }
        }
        let python = launch::resolve_python(req.python.as_deref(), self.cfg.python.as_deref())?;
        let root = self.runtime_root()?;
        let mut child = launch::spawn_kernel(&launch::Launch {
            home: &self.cfg.home,
            root: &root,
            python: &python,
            canonical: &canonical,
            kernel_id: &kernel_id,
            cwd: req.cwd.as_deref(),
            env: &req.env,
        })?;
        self.wait_ready(&kernel_id, &mut child).await
    }

    async fn request(&self, kernel_id: &str, method: &str, params: Value) -> Result<Value> {
        if matches!(method, "subscribe" | "unsubscribe") {
            return Err(DpxError::new(
                "bad_request",
                "use subscribe() for events; the connection is shared",
            ));
        }
        let conn = self.connection(kernel_id).await?;
        if method == "runs.wait" {
            // FR-S7 long-poll: the kernel answers after up to `timeout` (<= 300) seconds, so the
            // usual request timeout would cut it short.
            let wait = params.get("timeout").and_then(Value::as_f64).unwrap_or(60.0).clamp(1.0, 300.0);
            return conn
                .request_timeout(method, params, Duration::from_secs_f64(wait) + RUNS_WAIT_MARGIN)
                .await;
        }
        conn.request(method, params).await
    }

    async fn subscribe(&self, kernel_id: &str, since: Option<u64>) -> Result<EventStream> {
        let conn = self.connection(kernel_id).await?;
        conn.subscribe(since).await
    }

    async fn document(&self, path: &str, viewer_outputs: bool) -> Result<Value> {
        let canonical = canonical_path(Path::new(path));
        let python = launch::resolve_python(None, self.cfg.python.as_deref())?;
        let root = self.runtime_root()?;
        launch::build_document(&python, &self.cfg.home, &root, &canonical, viewer_outputs).await
    }

    async fn kill(&self, kernel_id: &str) -> Result<()> {
        let entry = self
            .discovery
            .find(kernel_id)
            .await
            .ok_or_else(|| not_found(kernel_id))?;
        let pid = entry_pid(&entry);
        self.drop_connection(kernel_id);
        match process::kill_pid(pid) {
            Ok(_) => {
                self.discovery.forget(kernel_id);
                Ok(())
            }
            Err(e) => Err(internal(format!("cannot kill pid {pid}: {e}"))),
        }
    }
}
