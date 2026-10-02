//! HTTP layer of the DarkPyonix manager (SPEC FR-M1..M4, FR-A2/A3, INTENT D10).
//!
//! [`serve`] answers every operation of `docs/api/manager.openapi.yaml` on top of any
//! [`KernelBackend`], plus three things outside the contract: the API reference page at
//! `/docs/`, configured reverse-proxy routes ([`ProxyRoute`]) and nothing else. It speaks
//! HTTP/1.1 and HTTP/2 (h2c, or ALPN over TLS), SSE and WebSocket (proxied), without nginx.
//!
//! Ephemeral managers (FR-M3) bind `127.0.0.1`, publish `<home>/managers/<pid>.json` (0600)
//! and stop by themselves after `idle_timeout` without requests or open streams. Dedicated
//! managers (FR-M4) keep share tokens hashed in `<home>/manager.db`. Neither ever touches
//! kernels when it stops.

pub mod api;
pub mod auth;
pub mod docs;
pub mod error;
pub mod proxy;
pub mod sse;
pub mod tls;
pub mod util;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::Uri;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use dpx_core::KernelBackend;
use http_body::{Body as HttpBody, Frame, SizeHint};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use tokio::sync::watch;
use tower::ServiceExt;

pub use proxy::ProxyRoute;
pub use tls::TlsConfig;

use crate::api::AppState;
use crate::auth::{Auth, ShareStore};
use crate::error::ApiError;
use crate::proxy::{ConnInfo, Proxy};

/// Manager version reported by `/health`, `getManager` and the registry file.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
pub const DEFAULT_DEDICATED_PORT: u16 = 46881;
pub const DEFAULT_SHARE_BASE: &str = "https://darkpyonix.dev/s";
const SSE_KEEPALIVE: Duration = Duration::from_secs(15);
const MAX_BODY: usize = 64 * 1024 * 1024;
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const GRACEFUL_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Ephemeral,
    Dedicated,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Ephemeral => "ephemeral",
            Mode::Dedicated => "dedicated",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub mode: Mode,
    /// Ignored for ephemeral managers, which always bind `127.0.0.1` (FR-M3).
    pub host: String,
    /// `0` picks a free port.
    pub port: u16,
    /// Ephemeral only; dedicated managers never idle out.
    pub idle_timeout: Option<Duration>,
    /// `DARKPYONIX_HOME` (`managers/<pid>.json`, `manager.db`).
    pub home: PathBuf,
    pub tls: Option<TlsConfig>,
    /// Share links are `<share_base>/<share_id>#<token>`.
    pub share_base: String,
    pub proxies: Vec<ProxyRoute>,
    /// Master token; random per process when `None`.
    pub master_token: Option<String>,
    /// Interval of SSE keep-alive comments (15 s).
    pub sse_keepalive: Duration,
}

impl ServerConfig {
    pub fn ephemeral(home: impl Into<PathBuf>) -> Self {
        Self {
            mode: Mode::Ephemeral,
            host: "127.0.0.1".into(),
            port: 0,
            idle_timeout: Some(DEFAULT_IDLE_TIMEOUT),
            home: home.into(),
            tls: None,
            share_base: DEFAULT_SHARE_BASE.into(),
            proxies: Vec::new(),
            master_token: None,
            sse_keepalive: SSE_KEEPALIVE,
        }
    }

    pub fn dedicated(home: impl Into<PathBuf>, host: impl Into<String>, port: u16) -> Self {
        Self { mode: Mode::Dedicated, host: host.into(), port, idle_timeout: None, ..Self::ephemeral(home) }
    }
}

/// `$DARKPYONIX_HOME`, else `~/.darkpyonix`.
pub fn default_home() -> PathBuf {
    if let Some(h) = std::env::var_os("DARKPYONIX_HOME").filter(|h| !h.is_empty()) {
        return PathBuf::from(h);
    }
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")).unwrap_or_default();
    PathBuf::from(home).join(".darkpyonix")
}

// ------------------------------------------------------------------ activity (FR-M3)

/// Open requests and streams, and when the last one ended (idle watchdog).
pub struct Activity {
    active: AtomicUsize,
    last: Mutex<Instant>,
}

impl Activity {
    fn new() -> Arc<Self> {
        Arc::new(Self { active: AtomicUsize::new(0), last: Mutex::new(Instant::now()) })
    }
    fn touch(&self) {
        *self.last.lock().unwrap_or_else(|p| p.into_inner()) = Instant::now();
    }
    pub fn enter(self: &Arc<Self>) -> ActivityGuard {
        self.active.fetch_add(1, Ordering::SeqCst);
        self.touch();
        ActivityGuard(self.clone())
    }
    /// Zero while anything is open, else the time since the last thing ended.
    pub fn idle_for(&self) -> Duration {
        if self.active.load(Ordering::SeqCst) > 0 {
            return Duration::ZERO;
        }
        self.last.lock().unwrap_or_else(|p| p.into_inner()).elapsed()
    }
}

pub struct ActivityGuard(Arc<Activity>);

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        self.0.touch();
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A response body that keeps its request counted as active until it is fully sent or dropped.
struct GuardedBody {
    inner: Body,
    _guard: ActivityGuard,
}

impl HttpBody for GuardedBody {
    type Data = axum::body::Bytes;
    type Error = axum::Error;
    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

async fn track(State(activity): State<Arc<Activity>>, mut req: Request, next: Next) -> Response {
    let guard = activity.enter();
    req.extensions_mut().insert(activity.clone());
    let resp = next.run(req).await;
    resp.map(|inner| Body::new(GuardedBody { inner, _guard: guard }))
}

// ------------------------------------------------------------------ auth gate (FR-A2)

fn is_events_path(path: &str) -> bool {
    let mut parts = path.split('/');
    parts.next() == Some("")
        && parts.next() == Some("api")
        && parts.next() == Some("v1")
        && parts.next() == Some("kernels")
        && parts.next().is_some_and(|k| !k.is_empty())
        && parts.next() == Some("events")
        && parts.next().is_none()
}

fn query_token(uri: &Uri) -> Option<String> {
    let pairs: Vec<(String, String)> = serde_urlencoded::from_str(uri.query()?).ok()?;
    pairs.into_iter().find(|(k, _)| k == "token").map(|(_, v)| v)
}

/// Authenticates before routing, so an anonymous caller always gets 401 (never 400/404).
async fn auth_gate(State(st): State<Arc<AppState>>, mut req: Request, next: Next) -> Response {
    let path = req.uri().path();
    if path == "/health" || path == "/docs" || path.starts_with("/docs/") {
        return next.run(req).await;
    }
    let bearer = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            let (scheme, rest) = v.split_once(' ')?;
            scheme.eq_ignore_ascii_case("bearer").then(|| rest.trim().to_string())
        });
    // `?token=` only on the events stream (EventSource cannot set headers).
    let token = bearer.or_else(|| if is_events_path(path) { query_token(req.uri()) } else { None });
    match st.auth.authenticate(token.as_deref()) {
        Some(p) => {
            req.extensions_mut().insert(p);
            next.run(req).await
        }
        None => ApiError::unauthorized().into_response(),
    }
}

async fn proxy_gate(State(proxy): State<Arc<Proxy>>, req: Request, next: Next) -> Response {
    match proxy.route_for(req.uri().path()) {
        Some(i) => proxy.forward(i, req).await,
        None => next.run(req).await,
    }
}

pub fn router(state: Arc<AppState>, proxy: Arc<Proxy>, activity: Arc<Activity>) -> Router {
    use axum::routing::{delete, post};
    let api = Router::new()
        .route("/health", get(api::get_health))
        .route("/api/v1/manager", get(api::get_manager))
        .route("/api/v1/kernels", get(api::list_kernels).post(api::start_kernel))
        .route("/api/v1/kernels/{kernel_id}", get(api::get_kernel).delete(api::shutdown_kernel))
        .route("/api/v1/kernels/{kernel_id}/interrupt", post(api::interrupt_kernel))
        .route("/api/v1/kernels/{kernel_id}/restart", post(api::restart_kernel))
        .route("/api/v1/kernels/{kernel_id}/namespace", get(api::get_namespace))
        .route("/api/v1/kernels/{kernel_id}/document", get(api::get_kernel_document))
        .route("/api/v1/documents", get(api::get_document))
        .route("/api/v1/kernels/{kernel_id}/runs", get(api::list_runs).post(api::start_run))
        .route("/api/v1/kernels/{kernel_id}/runs/{run_ref}", get(api::get_run).delete(api::cancel_run))
        .route("/api/v1/kernels/{kernel_id}/events", get(api::stream_events))
        .route("/api/v1/kernels/{kernel_id}/shares", get(api::list_shares).post(api::create_share))
        .route("/api/v1/kernels/{kernel_id}/shares/{share_id}", delete(api::revoke_share))
        .route("/docs", get(docs::redirect))
        .route("/docs/", get(docs::index))
        .route("/docs/{name}", get(docs::file))
        .fallback(api::not_found)
        .method_not_allowed_fallback(api::method_not_allowed)
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .layer(middleware::from_fn_with_state(state.clone(), auth_gate))
        .with_state(state);
    let api = if proxy.is_empty() { api } else { api.layer(middleware::from_fn_with_state(proxy, proxy_gate)) };
    api.layer(middleware::from_fn_with_state(activity, track))
}

// ------------------------------------------------------------------ serving

/// Why [`ServerHandle::wait`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    /// [`ServerHandle::shutdown`] (or a [`ShutdownTrigger`]) was called.
    Shutdown,
    /// Ephemeral manager idle for `idle_timeout` (FR-M3).
    Idle,
}

/// Stops the server from anywhere (signal handlers, CLI).
#[derive(Clone)]
pub struct ShutdownTrigger(Arc<watch::Sender<bool>>);

impl ShutdownTrigger {
    pub fn shutdown(&self) {
        self.0.send_replace(true);
    }
}

pub struct ServerHandle {
    /// `http(s)://host:port` clients use (loopback when bound to a wildcard address).
    pub url: String,
    pub token: String,
    pub local_addr: SocketAddr,
    /// `<home>/managers/<pid>.json` for an ephemeral manager.
    pub registry_file: Option<PathBuf>,
    pub cert_store: Option<Arc<tls::CertStore>>,
    trigger: ShutdownTrigger,
    done: tokio::task::JoinHandle<ExitReason>,
}

impl ServerHandle {
    pub fn shutdown(&self) {
        self.trigger.shutdown();
    }
    pub fn trigger(&self) -> ShutdownTrigger {
        self.trigger.clone()
    }
    /// Resolves once the server has stopped and removed its registry file.
    pub async fn wait(self) -> ExitReason {
        self.done.await.unwrap_or(ExitReason::Shutdown)
    }
}

fn bind(host: &str, port: u16) -> std::io::Result<tokio::net::TcpListener> {
    let ip: std::net::IpAddr = match host {
        "" | "localhost" => std::net::Ipv4Addr::LOCALHOST.into(),
        h => h.trim_start_matches('[').trim_end_matches(']').parse().map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("host {h:?}: {e}"))
        })?,
    };
    let addr = SocketAddr::new(ip, port);
    let sock = if ip.is_ipv4() { tokio::net::TcpSocket::new_v4()? } else { tokio::net::TcpSocket::new_v6()? };
    #[cfg(unix)]
    if port != 0 {
        sock.set_reuseaddr(true)?;
    }
    sock.bind(addr)?;
    sock.listen(1024)
}

/// Starts the manager's HTTP server and returns once it is accepting connections.
pub async fn serve(config: ServerConfig, backend: Arc<dyn KernelBackend>) -> std::io::Result<ServerHandle> {
    let mode = config.mode;
    let host = match mode {
        Mode::Ephemeral => "127.0.0.1".to_string(),
        Mode::Dedicated => config.host.clone(),
    };
    let idle_timeout = match mode {
        Mode::Ephemeral => config.idle_timeout,
        Mode::Dedicated => None,
    };
    let token = config.master_token.clone().unwrap_or_else(|| auth::random_hex(32));
    let shares = match mode {
        Mode::Dedicated => {
            util::ensure_private_dir(&config.home)?;
            Some(ShareStore::open(&config.home.join("manager.db"))?)
        }
        Mode::Ephemeral => None,
    };
    let (acceptor, cert_store) = match &config.tls {
        Some(t) => {
            let (a, s) = tls::acceptor(t)?;
            (Some(a), Some(s))
        }
        None => (None, None),
    };
    let proxy = Arc::new(Proxy::new(&config.proxies)?);
    let listener = bind(&host, config.port)?;
    let local_addr = listener.local_addr()?;
    let reach = if local_addr.ip().is_unspecified() {
        "127.0.0.1".to_string()
    } else if local_addr.is_ipv6() {
        format!("[{}]", local_addr.ip())
    } else {
        local_addr.ip().to_string()
    };
    let scheme = if acceptor.is_some() { "https" } else { "http" };
    let url = format!("{scheme}://{reach}:{}", local_addr.port());

    let pid = std::process::id();
    let started_at = util::now_iso();
    let (tx, rx) = watch::channel(false);
    let trigger = ShutdownTrigger(Arc::new(tx));
    let activity = Activity::new();
    let state = Arc::new(AppState {
        backend,
        auth: Auth::new(&token, shares),
        mode,
        pid,
        started_at: started_at.clone(),
        host: gethostname::gethostname().to_string_lossy().into_owned(),
        idle_timeout,
        share_base: config.share_base.clone(),
        sse_keepalive: config.sse_keepalive,
        shutdown: rx.clone(),
    });
    let app = router(state, proxy, activity.clone());

    let registry_file = match mode {
        Mode::Ephemeral => {
            let path = config.home.join("managers").join(format!("{pid}.json"));
            let record = serde_json::json!({
                "pid": pid, "url": url, "token": token, "mode": mode.as_str(),
                "started_at": started_at, "version": VERSION,
            });
            util::write_private_atomic(&path, record.to_string().as_bytes())?;
            Some(path)
        }
        Mode::Dedicated => None,
    };

    let idle = Arc::new(std::sync::atomic::AtomicBool::new(false));
    if let Some(limit) = idle_timeout {
        let activity = activity.clone();
        let trigger = trigger.clone();
        let idle = idle.clone();
        let mut rx = rx.clone();
        tokio::spawn(async move {
            let tick = (limit / 4).clamp(Duration::from_millis(50), Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(tick) => {}
                    _ = rx.changed() => return,
                }
                if activity.idle_for() >= limit {
                    idle.store(true, Ordering::SeqCst);
                    trigger.shutdown();
                    return;
                }
            }
        });
    }

    let registry = registry_file.clone();
    let tls_on = acceptor.is_some();
    let done = tokio::spawn(async move {
        let graceful = hyper_util::server::graceful::GracefulShutdown::new();
        let mut rx = rx;
        loop {
            let accepted = tokio::select! {
                a = listener.accept() => a,
                _ = rx.wait_for(|v| *v) => break,
            };
            let Ok((stream, peer)) = accepted else { continue };
            let _ = stream.set_nodelay(true);
            let info = ConnInfo { peer, tls: tls_on };
            let svc = app.clone().map_request(move |mut r: Request<hyper::body::Incoming>| {
                r.extensions_mut().insert(info);
                r.map(Body::new)
            });
            let svc = TowerToHyperService::new(svc);
            let watcher = graceful.watcher();
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let builder = auto::Builder::new(TokioExecutor::new());
                match acceptor {
                    Some(acc) => {
                        let Ok(Ok(tls)) = tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acc.accept(stream)).await else {
                            return;
                        };
                        let conn = builder.serve_connection_with_upgrades(TokioIo::new(tls), svc);
                        let _ = watcher.watch(conn.into_owned()).await;
                    }
                    None => {
                        let conn = builder.serve_connection_with_upgrades(TokioIo::new(stream), svc);
                        let _ = watcher.watch(conn.into_owned()).await;
                    }
                }
            });
        }
        drop(listener);
        let _ = tokio::time::timeout(GRACEFUL_TIMEOUT, graceful.shutdown()).await;
        if let Some(path) = registry {
            let _ = std::fs::remove_file(path);
        }
        if idle.load(Ordering::SeqCst) {
            ExitReason::Idle
        } else {
            ExitReason::Shutdown
        }
    });

    Ok(ServerHandle { url, token, local_addr, registry_file, cert_store, trigger, done })
}

/// Serves until SIGINT/SIGTERM (or idle exit) and returns why it stopped.
pub async fn run_until_signal(handle: ServerHandle) -> ExitReason {
    let trigger = handle.trigger();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut term = signal(SignalKind::terminate()).ok();
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = async { match term.as_mut() { Some(t) => { t.recv().await; } None => std::future::pending().await } } => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        trigger.shutdown();
    });
    handle.wait().await
}
