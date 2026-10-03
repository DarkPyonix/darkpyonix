//! Shared contract between the manager crates.
//!
//! * `dpx-kernel` implements [`KernelBackend`] against real kernels (DKP/1, discovery,
//!   launching the embedded stdlib-Python kernel).
//! * `dpx-server` serves `docs/api/manager.openapi.yaml` over HTTP on top of any
//!   [`KernelBackend`], so it can be tested with a fake.
//! * `darkpyonix` is the single binary: CLI plus `darkpyonix manager`.
//!
//! JSON shapes are those of PROTOCOL.md / manager.openapi.yaml; they are carried as
//! `serde_json::Value` where the kernel is the authority (run notebooks, events, outputs).

use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// An error that maps onto the OpenAPI `Error` schema and a DKP/1 error code.
#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize)]
#[error("{code}: {message}")]
pub struct DpxError {
    /// One of: bad_request, unauthorized, forbidden, not_found, busy, start_timeout,
    /// kernel_unreachable, shutting_down, internal (and DKP codes passed through).
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl DpxError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self { code: code.into(), message: message.into(), data: None }
    }
    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }
}

pub type Result<T> = std::result::Result<T, DpxError>;

/// The OpenAPI `Kernel` schema. Built from a discovery announce plus `status` when connected.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KernelInfo {
    pub kernel_id: String,
    pub path: String,
    pub pid: u32,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default)]
    pub queue: Vec<String>,
    #[serde(default)]
    pub execution_count: u64,
    pub python: Value,
    pub started_at: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub kernel_version: String,
    #[serde(default)]
    pub runs_dir: String,
    /// Control-channel port on 127.0.0.1 (not exposed over HTTP).
    #[serde(skip_serializing, default)]
    pub port: u16,
}

/// `POST /api/v1/kernels` body.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StartKernel {
    pub path: String,
    #[serde(default)]
    pub python: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
}

/// Result of [`KernelBackend::ensure`]: whether the kernel was already running (200) or launched (201).
#[derive(Debug, Clone)]
pub struct Ensured {
    pub kernel: KernelInfo,
    pub launched: bool,
}

/// One PROTOCOL §3.4 event: `{seq, type, time, data}`; `seq` is `None` for `replay_truncated`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KernelEvent {
    pub seq: Option<u64>,
    #[serde(rename = "type")]
    pub kind: String,
    pub time: String,
    pub data: Value,
}

pub type EventStream = Pin<Box<dyn Stream<Item = KernelEvent> + Send>>;

/// Everything the HTTP layer needs from kernels.
#[async_trait]
pub trait KernelBackend: Send + Sync + 'static {
    /// Running kernels of this OS user (FR-D1/D2). `refresh` forces a new discovery query.
    async fn list(&self, refresh: bool) -> Result<Vec<KernelInfo>>;
    /// One kernel, `not_found` if absent.
    async fn get(&self, kernel_id: &str) -> Result<KernelInfo>;
    /// Idempotent start per file (FR-M2). `start_timeout` after 10 s without an announce.
    async fn ensure(&self, req: StartKernel) -> Result<Ensured>;
    /// A DKP/1 request (`run`, `interrupt`, `restart`, `shutdown`, `namespace`, `cancel`,
    /// `runs.list`, `runs.get`, `status`). Kernel errors come back as `DpxError` with the
    /// kernel's code (`busy`, `not_found`, ...); transport failures as `kernel_unreachable`.
    async fn request(&self, kernel_id: &str, method: &str, params: Value) -> Result<Value>;
    /// Events after `since` (replayed) followed by live events. One kernel connection is
    /// shared by all subscribers of this manager (FR-M5).
    async fn subscribe(&self, kernel_id: &str, since: Option<u64>) -> Result<EventStream>;
    /// FR-R4 document for a file, with or without a running kernel (reads `__runs__/`).
    async fn document(&self, path: &str, viewer_outputs: bool) -> Result<Value>;
    /// Force-kill a kernel process that did not exit after `shutdown` (DELETE ?force=true).
    async fn kill(&self, kernel_id: &str) -> Result<()>;
}
