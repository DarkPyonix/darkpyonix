//! Every operation of `docs/api/manager.openapi.yaml` (SPEC FR-M1) with the per-operation
//! minimum permission of SPEC FR-A3.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{FromRequest, Path, Request, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use dpx_core::{KernelBackend, KernelInfo, StartKernel};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use tokio::sync::watch;

use crate::auth::{Auth, Permission, Principal};
use crate::error::{ApiError, ApiResult};
use crate::{sse, util, Mode, VERSION};

/// Everything the handlers share.
pub struct AppState {
    pub backend: Arc<dyn KernelBackend>,
    pub auth: Auth,
    pub mode: Mode,
    pub pid: u32,
    pub started_at: String,
    pub host: String,
    pub idle_timeout: Option<Duration>,
    pub share_base: String,
    pub sse_keepalive: Duration,
    pub shutdown: watch::Receiver<bool>,
}

type St = State<Arc<AppState>>;

// Documented status codes per operation (NFR-M3). Anything else becomes 500 internal.
const OP_GET_MANAGER: &[u16] = &[200, 401, 500];
const OP_LIST_KERNELS: &[u16] = &[200, 400, 401, 500];
const OP_START_KERNEL: &[u16] = &[200, 201, 400, 401, 403, 500, 504];
const OP_GET_KERNEL: &[u16] = &[200, 401, 404, 500, 502];
const OP_SHUTDOWN: &[u16] = &[202, 400, 401, 403, 404, 500, 502];
const OP_INTERRUPT: &[u16] = &[200, 401, 403, 404, 500, 502];
const OP_RESTART: &[u16] = &[200, 400, 401, 403, 404, 500, 502];
const OP_NAMESPACE: &[u16] = &[200, 400, 401, 403, 404, 500, 502];
const OP_KERNEL_DOCUMENT: &[u16] = &[200, 401, 404, 500, 502];
const OP_DOCUMENT: &[u16] = &[200, 400, 401, 403, 404, 500];
const OP_LIST_RUNS: &[u16] = &[200, 400, 401, 403, 404, 500, 502];
const OP_START_RUN: &[u16] = &[202, 400, 401, 403, 404, 409, 500, 502];
const OP_GET_RUN: &[u16] = &[200, 400, 401, 403, 404, 500, 502];
const OP_CANCEL_RUN: &[u16] = &[200, 400, 401, 403, 404, 500, 502];
const OP_EVENTS: &[u16] = &[200, 400, 401, 404, 500, 502];
const OP_LIST_SHARES: &[u16] = &[200, 401, 403, 404, 500, 502];
const OP_CREATE_SHARE: &[u16] = &[201, 400, 401, 403, 404, 500, 502];
const OP_REVOKE_SHARE: &[u16] = &[204, 401, 403, 404, 500, 502];

fn finish(r: ApiResult<Response>, documented: &[u16]) -> Response {
    match r {
        Ok(resp) => resp,
        Err(e) => e.documented(documented).into_response(),
    }
}

// ------------------------------------------------------------------ request parsing

/// The raw request body; size-limit rejections keep the Error shape.
pub struct RawBody(pub Bytes);

impl<S: Send + Sync> FromRequest<S> for RawBody {
    type Rejection = ApiError;
    async fn from_request(req: Request, state: &S) -> Result<Self, ApiError> {
        Bytes::from_request(req, state)
            .await
            .map(RawBody)
            .map_err(|r| ApiError::new(StatusCode::BAD_REQUEST, "bad_request", r.body_text()))
    }
}

/// JSON body validation: any failure is `400 bad_request` with `data.errors` (never 422).
fn parse_json<T: DeserializeOwned>(body: &[u8]) -> ApiResult<T> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Err(ApiError::invalid("a JSON request body is required"));
    }
    serde_json::from_slice(body).map_err(|e| ApiError::invalid(e.to_string()))
}

fn query(uri: &Uri) -> ApiResult<BTreeMap<String, String>> {
    let q = uri.query().unwrap_or("");
    let pairs: Vec<(String, String)> =
        serde_urlencoded::from_str(q).map_err(|e| ApiError::invalid(format!("query string: {e}")))?;
    Ok(pairs.into_iter().collect())
}

fn flag(q: &BTreeMap<String, String>, name: &str) -> ApiResult<bool> {
    match q.get(name).map(|v| v.to_ascii_lowercase()) {
        None => Ok(false),
        Some(v) => match v.as_str() {
            "true" | "1" | "yes" | "on" => Ok(true),
            "false" | "0" | "no" | "off" => Ok(false),
            _ => Err(ApiError::invalid(format!("{name}: expected a boolean"))),
        },
    }
}

/// `limit`-style integers are lenient (prototype and pytest oracle): an unparsable value
/// falls back to the default and out-of-range values are clamped.
fn clamp(q: &BTreeMap<String, String>, name: &str, default: i64, lo: i64, hi: i64) -> i64 {
    q.get(name).and_then(|v| v.trim().parse::<i64>().ok()).unwrap_or(default).clamp(lo, hi)
}

// ------------------------------------------------------------------ access checks

fn check_id(p: &Principal, kernel_id: &str) -> ApiResult<()> {
    if util::is_kernel_id(kernel_id) && p.can_see(kernel_id) {
        Ok(())
    } else {
        Err(ApiError::not_found(format!("no such kernel: {kernel_id}")))
    }
}

fn require(p: &Principal, perm: Permission) -> ApiResult<()> {
    if p.at_least(perm) {
        Ok(())
    } else {
        Err(ApiError::forbidden(format!("permission {} required", perm.as_str())))
    }
}

async fn visible(st: &AppState, p: &Principal, kernel_id: &str) -> ApiResult<KernelInfo> {
    check_id(p, kernel_id)?;
    Ok(st.backend.get(kernel_id).await?)
}

async fn call(st: &AppState, kernel_id: &str, method: &str, params: Value) -> ApiResult<Value> {
    Ok(st.backend.request(kernel_id, method, params).await?)
}

/// The OpenAPI `Kernel` object.
pub fn kernel_json(k: &KernelInfo) -> Value {
    let mut v = serde_json::to_value(k).unwrap_or_else(|_| json!({}));
    if let Value::Object(m) = &mut v {
        m.entry("run_id").or_insert(Value::Null);
        let empty = m.get("runs_dir").and_then(Value::as_str).is_none_or(str::is_empty);
        if empty {
            m.insert("runs_dir".into(), Value::String(util::runs_dir_for(&k.path)));
        }
    }
    v
}

fn ok_json(status: StatusCode, v: Value) -> ApiResult<Response> {
    Ok((status, Json(v)).into_response())
}

// ------------------------------------------------------------------ System

pub async fn get_health() -> Response {
    Json(json!({"status": "ok", "version": VERSION})).into_response()
}

pub async fn get_manager(State(st): St, Extension(p): Extension<Principal>) -> Response {
    let idle = match st.mode {
        Mode::Ephemeral => st.idle_timeout.map(|d| json!(d.as_secs_f64().round() as u64)),
        Mode::Dedicated => None,
    };
    finish(
        ok_json(
            StatusCode::OK,
            json!({
                "version": VERSION,
                "mode": st.mode.as_str(),
                "pid": st.pid,
                "started_at": st.started_at,
                "idle_timeout": idle.unwrap_or(Value::Null),
                "permission": p.permission.as_str(),
                "host": st.host,
            }),
        ),
        OP_GET_MANAGER,
    )
}

// ------------------------------------------------------------------ Kernels

pub async fn list_kernels(State(st): St, Extension(p): Extension<Principal>, uri: Uri) -> Response {
    let r = async {
        let refresh = flag(&query(&uri)?, "refresh")?;
        let kernels = st.backend.list(refresh).await?;
        let out: Vec<Value> = kernels.iter().filter(|k| p.can_see(&k.kernel_id)).map(kernel_json).collect();
        ok_json(StatusCode::OK, json!({ "kernels": out }))
    };
    finish(r.await, OP_LIST_KERNELS)
}

#[derive(Deserialize)]
struct StartKernelBody {
    path: String,
    #[serde(default)]
    python: Option<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    env: Option<BTreeMap<String, String>>,
}

pub async fn start_kernel(State(st): St, Extension(p): Extension<Principal>, RawBody(body): RawBody) -> Response {
    let r = async {
        require(&p, Permission::Admin)?;
        let b: StartKernelBody = parse_json(&body)?;
        if b.path.is_empty() {
            return Err(ApiError::invalid("path: must not be empty"));
        }
        let req = StartKernel { path: b.path, python: b.python, cwd: b.cwd, env: b.env.unwrap_or_default() };
        let ensured = st.backend.ensure(req).await.map_err(|e| {
            // A missing or unusable file is a malformed request for this operation.
            if e.code == "not_found" {
                ApiError::bad_request(e.message)
            } else {
                ApiError::from(e)
            }
        })?;
        let status = if ensured.launched { StatusCode::CREATED } else { StatusCode::OK };
        ok_json(status, kernel_json(&ensured.kernel))
    };
    finish(r.await, OP_START_KERNEL)
}

pub async fn get_kernel(State(st): St, Extension(p): Extension<Principal>, Path(kid): Path<String>) -> Response {
    let r = async {
        let k = visible(&st, &p, &kid).await?;
        ok_json(StatusCode::OK, kernel_json(&k))
    };
    finish(r.await, OP_GET_KERNEL)
}

/// `force=true`: grace period before the process is killed (contract: 5 seconds).
const FORCE_KILL_GRACE: Duration = Duration::from_secs(5);

pub async fn shutdown_kernel(
    State(st): St,
    Extension(p): Extension<Principal>,
    Path(kid): Path<String>,
    uri: Uri,
) -> Response {
    let r = async {
        check_id(&p, &kid)?;
        require(&p, Permission::Admin)?;
        let force = flag(&query(&uri)?, "force")?;
        visible(&st, &p, &kid).await?;
        match call(&st, &kid, "shutdown", json!({})).await {
            Ok(_) => {}
            Err(e) if force && e.status == StatusCode::BAD_GATEWAY => {}
            Err(e) => return Err(e),
        }
        if force {
            let backend = st.backend.clone();
            let kid = kid.clone();
            tokio::spawn(async move {
                // The only place the manager kills a kernel (INTENT D9): after the grace
                // period, if the kernel is still there.
                let deadline = tokio::time::Instant::now() + FORCE_KILL_GRACE;
                while tokio::time::Instant::now() < deadline {
                    if matches!(backend.get(&kid).await, Err(e) if e.code == "not_found") {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                if let Err(e) = backend.kill(&kid).await {
                    tracing::warn!("force kill of {kid} failed: {e}");
                }
            });
        }
        ok_json(StatusCode::ACCEPTED, json!({"shutting_down": true}))
    };
    finish(r.await, OP_SHUTDOWN)
}

pub async fn interrupt_kernel(State(st): St, Extension(p): Extension<Principal>, Path(kid): Path<String>) -> Response {
    let r = async {
        check_id(&p, &kid)?;
        require(&p, Permission::Viewer3)?;
        visible(&st, &p, &kid).await?;
        let res = call(&st, &kid, "interrupt", json!({})).await?;
        let mut out = Map::new();
        out.insert("interrupted".into(), json!(res.get("interrupted").and_then(Value::as_bool).unwrap_or(false)));
        if let Some(run_id) = res.get("run_id").filter(|v| v.is_string()) {
            out.insert("run_id".into(), run_id.clone());
        }
        ok_json(StatusCode::OK, Value::Object(out))
    };
    finish(r.await, OP_INTERRUPT)
}

/// How long a hard restart waits for the re-executed process to be reachable again.
const RESTART_WAIT: Duration = Duration::from_secs(10);

pub async fn restart_kernel(
    State(st): St,
    Extension(p): Extension<Principal>,
    Path(kid): Path<String>,
    RawBody(body): RawBody,
) -> Response {
    let r = async {
        check_id(&p, &kid)?;
        require(&p, Permission::Admin)?;
        let hard = if body.iter().all(u8::is_ascii_whitespace) {
            false
        } else {
            #[derive(Deserialize)]
            struct RestartBody {
                #[serde(default)]
                hard: bool,
            }
            parse_json::<RestartBody>(&body)?.hard
        };
        visible(&st, &p, &kid).await?;
        call(&st, &kid, "restart", json!({ "hard": hard })).await?;
        let deadline = tokio::time::Instant::now() + RESTART_WAIT;
        loop {
            match st.backend.get(&kid).await {
                Ok(k) => return ok_json(StatusCode::OK, kernel_json(&k)),
                Err(e)
                    if hard
                        && matches!(e.code.as_str(), "not_found" | "kernel_unreachable" | "shutting_down")
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
    };
    finish(r.await, OP_RESTART)
}

pub async fn get_namespace(
    State(st): St,
    Extension(p): Extension<Principal>,
    Path(kid): Path<String>,
    uri: Uri,
) -> Response {
    let r = async {
        check_id(&p, &kid)?;
        require(&p, Permission::Viewer2)?;
        let limit = clamp(&query(&uri)?, "limit", 200, 1, 1000);
        visible(&st, &p, &kid).await?;
        let res = call(&st, &kid, "namespace", json!({ "limit": limit })).await?;
        let vars = res.get("variables").cloned().unwrap_or_else(|| json!([]));
        ok_json(StatusCode::OK, json!({ "variables": vars }))
    };
    finish(r.await, OP_NAMESPACE)
}

// ------------------------------------------------------------------ Documents

fn shape_document(mut doc: Value, kernel_id: Option<&str>, outputs: bool) -> Value {
    if let Value::Object(m) = &mut doc {
        if let Some(kid) = kernel_id {
            m.entry("kernel_id").or_insert_with(|| Value::String(kid.to_string()));
        }
        if !outputs {
            if let Some(Value::Array(cells)) = m.get_mut("cells") {
                for c in cells.iter_mut().filter_map(Value::as_object_mut) {
                    c.remove("outputs");
                }
            }
        }
    }
    doc
}

pub async fn get_kernel_document(
    State(st): St,
    Extension(p): Extension<Principal>,
    Path(kid): Path<String>,
) -> Response {
    let r = async {
        let k = visible(&st, &p, &kid).await?;
        let outputs = p.at_least(Permission::Viewer2);
        let doc = st.backend.document(&k.path, outputs).await?;
        ok_json(StatusCode::OK, shape_document(doc, Some(&kid), outputs))
    };
    finish(r.await, OP_KERNEL_DOCUMENT)
}

pub async fn get_document(State(st): St, Extension(p): Extension<Principal>, uri: Uri) -> Response {
    let r = async {
        require(&p, Permission::Admin)?;
        let q = query(&uri)?;
        let path = q.get("path").filter(|s| !s.is_empty()).ok_or_else(|| ApiError::invalid("path: required"))?;
        if !(path.ends_with(".py") || path.ends_with(".pynb")) {
            return Err(ApiError::bad_request(format!("not a .py or .pynb file: {path}")));
        }
        if !std::path::Path::new(path).is_file() {
            return Err(ApiError::not_found(format!("no such file: {path}")));
        }
        let doc = st.backend.document(path, true).await?;
        ok_json(StatusCode::OK, shape_document(doc, None, true))
    };
    finish(r.await, OP_DOCUMENT)
}

// ------------------------------------------------------------------ Runs

pub async fn list_runs(State(st): St, Extension(p): Extension<Principal>, Path(kid): Path<String>, uri: Uri) -> Response {
    let r = async {
        check_id(&p, &kid)?;
        require(&p, Permission::Viewer2)?;
        let limit = clamp(&query(&uri)?, "limit", 20, 1, 500);
        visible(&st, &p, &kid).await?;
        let res = call(&st, &kid, "runs.list", json!({ "limit": limit })).await?;
        let runs = res.get("runs").cloned().unwrap_or_else(|| json!([]));
        ok_json(StatusCode::OK, json!({ "runs": runs }))
    };
    finish(r.await, OP_LIST_RUNS)
}

#[derive(Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum RunMode {
    All,
    Cells,
}

#[derive(Deserialize, serde::Serialize, Default)]
#[serde(rename_all = "lowercase")]
enum OnBusy {
    #[default]
    Reject,
    Queue,
}

#[derive(Deserialize)]
struct RunBody {
    mode: RunMode,
    #[serde(default)]
    cells: Option<Vec<u64>>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    params: Option<Map<String, Value>>,
    #[serde(default)]
    on_busy: Option<OnBusy>,
}

pub async fn start_run(
    State(st): St,
    Extension(p): Extension<Principal>,
    Path(kid): Path<String>,
    RawBody(body): RawBody,
) -> Response {
    let r = async {
        check_id(&p, &kid)?;
        require(&p, Permission::Viewer3)?;
        let b: RunBody = parse_json(&body)?;
        if matches!(b.mode, RunMode::Cells) && b.cells.as_ref().is_none_or(Vec::is_empty) {
            return Err(ApiError::invalid("cells: mode 'cells' needs a non-empty 'cells' list"));
        }
        visible(&st, &p, &kid).await?;
        let mut params = Map::new();
        params.insert("mode".into(), serde_json::to_value(&b.mode).unwrap_or_default());
        if let Some(c) = b.cells {
            params.insert("cells".into(), json!(c));
        }
        if let Some(s) = b.source {
            params.insert("source".into(), Value::String(s));
        }
        if let Some(pm) = b.params {
            params.insert("params".into(), Value::Object(pm));
        }
        params.insert("on_busy".into(), serde_json::to_value(b.on_busy.unwrap_or_default()).unwrap_or_default());
        let res = call(&st, &kid, "run", Value::Object(params)).await?;
        ok_json(StatusCode::ACCEPTED, res)
    };
    finish(r.await, OP_START_RUN)
}

fn check_run_ref(run_ref: &str) -> ApiResult<()> {
    if util::is_run_id(run_ref) || run_ref == "latest" || run_ref == "current" {
        Ok(())
    } else {
        Err(ApiError::not_found(format!("no such run: {run_ref}")))
    }
}

/// `RunSummary` from a run log's `metadata.darkpyonix` (FR-R1).
pub fn run_summary(nb: &Value, runs_dir: &str) -> Value {
    let meta = nb.pointer("/metadata/darkpyonix").cloned().unwrap_or_else(|| json!({}));
    let mut out = Map::new();
    for k in ["run_id", "status", "mode", "started_at"] {
        out.insert(k.into(), meta.get(k).cloned().unwrap_or(Value::Null));
    }
    for k in ["cells", "params", "ended_at", "duration"] {
        if let Some(v) = meta.get(k) {
            out.insert(k.into(), v.clone());
        }
    }
    if out.get("duration").is_none_or(Value::is_null) {
        let t = |k: &str| meta.get(k).and_then(Value::as_str).and_then(util::parse_datetime);
        if let (Some(t0), Some(t1)) = (t("started_at"), t("ended_at")) {
            out.insert("duration".into(), json!((t1 - t0).as_seconds_f64()));
        }
    }
    if let Some(run_id) = out.get("run_id").and_then(Value::as_str) {
        let path = std::path::Path::new(runs_dir).join(format!("{run_id}.ipynb"));
        out.insert("path".into(), Value::String(path.to_string_lossy().into_owned()));
    }
    Value::Object(out)
}

pub async fn get_run(
    State(st): St,
    Extension(p): Extension<Principal>,
    Path((kid, run_ref)): Path<(String, String)>,
    uri: Uri,
) -> Response {
    let r = async {
        check_id(&p, &kid)?;
        require(&p, Permission::Viewer2)?;
        check_run_ref(&run_ref)?;
        let summary = match query(&uri)?.get("format").map(String::as_str) {
            None | Some("ipynb") => false,
            Some("summary") => true,
            Some(other) => return Err(ApiError::invalid(format!("format: expected ipynb or summary, got {other}"))),
        };
        let k = visible(&st, &p, &kid).await?;
        let nb = call(&st, &kid, "runs.get", json!({ "run_id": run_ref })).await?;
        if !nb.is_object() {
            return Err(ApiError::not_found(format!("no such run: {run_ref}")));
        }
        if summary {
            let runs_dir = if k.runs_dir.is_empty() { util::runs_dir_for(&k.path) } else { k.runs_dir.clone() };
            return ok_json(StatusCode::OK, run_summary(&nb, &runs_dir));
        }
        ok_json(StatusCode::OK, nb)
    };
    finish(r.await, OP_GET_RUN)
}

pub async fn cancel_run(
    State(st): St,
    Extension(p): Extension<Principal>,
    Path((kid, run_ref)): Path<(String, String)>,
) -> Response {
    let r = async {
        check_id(&p, &kid)?;
        require(&p, Permission::Viewer3)?;
        check_run_ref(&run_ref)?;
        visible(&st, &p, &kid).await?;
        if !util::is_run_id(&run_ref) {
            // `latest` has finished and `current` is executing; neither is queued.
            return ok_json(StatusCode::OK, json!({"cancelled": false}));
        }
        let res = call(&st, &kid, "cancel", json!({ "run_id": run_ref })).await?;
        let cancelled = res.get("cancelled").and_then(Value::as_bool).unwrap_or(false);
        ok_json(StatusCode::OK, json!({ "cancelled": cancelled }))
    };
    finish(r.await, OP_CANCEL_RUN)
}

// ------------------------------------------------------------------ Events

pub async fn stream_events(
    State(st): St,
    Extension(p): Extension<Principal>,
    Path(kid): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let r = async {
        check_id(&p, &kid)?;
        let q = query(&uri)?;
        // Last-Event-ID takes precedence over ?since=.
        let from_header = headers
            .get("last-event-id")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok());
        let since = match from_header {
            Some(s) => Some(s),
            None => match q.get("since") {
                None => None,
                Some(v) => Some(
                    v.trim().parse::<u64>().map_err(|_| ApiError::invalid("since: expected an integer >= 0"))?,
                ),
            },
        };
        let events = st.backend.subscribe(&kid, since).await?;
        let hide_outputs = !p.at_least(Permission::Viewer2);
        Ok(sse::response(events, &kid, hide_outputs, st.sse_keepalive, st.shutdown.clone()))
    };
    finish(r.await, OP_EVENTS)
}

// ------------------------------------------------------------------ Sharing

pub async fn list_shares(State(st): St, Extension(p): Extension<Principal>, Path(kid): Path<String>) -> Response {
    let r = async {
        check_id(&p, &kid)?;
        require(&p, Permission::Admin)?;
        let shares = match &st.auth.shares {
            None => Vec::new(),
            Some(store) => store.list(&kid).map_err(|e| ApiError::internal(e.to_string()))?,
        };
        ok_json(StatusCode::OK, json!({ "shares": shares }))
    };
    finish(r.await, OP_LIST_SHARES)
}

#[derive(Deserialize)]
struct ShareBody {
    permission: String,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    expires_at: Option<String>,
}

pub async fn create_share(
    State(st): St,
    Extension(p): Extension<Principal>,
    Path(kid): Path<String>,
    RawBody(body): RawBody,
) -> Response {
    let r = async {
        check_id(&p, &kid)?;
        require(&p, Permission::Admin)?;
        let Some(store) = &st.auth.shares else {
            return Err(ApiError::forbidden("share tokens need a dedicated manager (FR-A3)"));
        };
        let b: ShareBody = parse_json(&body)?;
        let permission = Permission::parse_share(&b.permission)
            .ok_or_else(|| ApiError::invalid("permission: expected viewer1, viewer2 or viewer3"))?;
        if b.label.as_ref().is_some_and(|l| l.chars().count() > 120) {
            return Err(ApiError::invalid("label: at most 120 characters"));
        }
        let expires = match &b.expires_at {
            None => None,
            Some(s) => Some(util::parse_datetime(s).ok_or_else(|| ApiError::invalid("expires_at: expected RFC 3339 date-time"))?),
        };
        visible(&st, &p, &kid).await?;
        let (share, token) =
            store.create(&kid, permission, b.label, expires).map_err(|e| ApiError::internal(e.to_string()))?;
        let mut v = serde_json::to_value(&share).unwrap_or_default();
        if let Value::Object(m) = &mut v {
            let url = format!("{}/{}#{}", st.share_base.trim_end_matches('/'), share.share_id, token);
            m.insert("token".into(), Value::String(token));
            m.insert("url".into(), Value::String(url));
        }
        ok_json(StatusCode::CREATED, v)
    };
    finish(r.await, OP_CREATE_SHARE)
}

pub async fn revoke_share(
    State(st): St,
    Extension(p): Extension<Principal>,
    Path((kid, share_id)): Path<(String, String)>,
) -> Response {
    let r = async {
        check_id(&p, &kid)?;
        require(&p, Permission::Admin)?;
        let gone = ApiError::not_found(format!("no such share: {share_id}"));
        let Some(store) = &st.auth.shares else { return Err(gone) };
        if !util::is_share_id(&share_id) {
            return Err(gone);
        }
        if !store.revoke(&kid, &share_id).map_err(|e| ApiError::internal(e.to_string()))? {
            return Err(gone);
        }
        Ok(StatusCode::NO_CONTENT.into_response())
    };
    finish(r.await, OP_REVOKE_SHARE)
}

// ------------------------------------------------------------------ fallbacks

pub async fn not_found(uri: Uri) -> Response {
    ApiError::not_found(format!("no such path: {}", uri.path())).into_response()
}

pub async fn method_not_allowed() -> Response {
    ApiError::new(StatusCode::METHOD_NOT_ALLOWED, "bad_request", "method not allowed").into_response()
}
