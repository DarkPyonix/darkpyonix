//! The VS Code server a dedicated manager installs, supervises and serves (SPEC FR-M8, FR-M9,
//! FR-M10, INTENT D27).
//!
//! Two runtimes: `ose` (DarkPyonix's Code-OSS server build, from Ember's `ose-v*` releases) and
//! `vsc` (the user's own Microsoft `code serve-web`). `darkpyonix editor install` records what is
//! installed in `<home>/editor.toml`; this module reads it. A dedicated manager starts every
//! installed runtime as a child process listening on a 0700 Unix socket without a connection
//! token, restarts it when it ends, and ends it with the manager. The only way in is the manager:
//! `/ide/<runtime>/` is proxied to the socket for the master token or a launch ticket.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{Path as UrlPath, Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::watch;

use crate::api::AppState;
use crate::auth::{self, Principal};
use crate::error::ApiError;

/// The runtimes, in the order `GET /api/editor` lists them. Ember's editor server types map onto
/// them: Code OSE → `ose`, VS Code → `vsc`, Manual Script → `script` (INTENT D27).
pub const RUNTIMES: [&str; 3] = ["ose", "vsc", "script"];
pub const TICKET_COOKIE: &str = "dpx-editor";
pub const TICKET_QUERY: &str = "dpx-ticket";
/// A ticket nobody used is gone after this long (FR-M9).
pub const UNUSED_TICKET_TTL: Duration = Duration::from_secs(5 * 60);
/// A used ticket is gone this long after its last use (FR-M9).
pub const USED_TICKET_TTL: Duration = Duration::from_secs(12 * 60 * 60);
/// How long `launch` waits for the socket to answer (FR-M10).
pub const START_TIMEOUT: Duration = Duration::from_secs(30);
/// Longest wait for one `--install-extension` run.
const EXTENSION_TIMEOUT: Duration = Duration::from_secs(300);
const BACKOFF_FIRST: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(60);
/// A child that ran this long is healthy again: the next crash restarts after `BACKOFF_FIRST`.
const HEALTHY_AFTER: Duration = Duration::from_secs(60);
/// `sockaddr_un.sun_path` is 104 bytes on macOS and 108 on Linux, with the trailing NUL.
const SOCKET_PATH_MAX: usize = 100;
/// The Microsoft licence the user accepts for `vsc` (FR-M8).
pub const VSC_LICENSE_URL: &str = "https://code.visualstudio.com/license/server";

pub fn is_runtime(s: &str) -> bool {
    RUNTIMES.contains(&s)
}

// ------------------------------------------------------------------ editor.toml (FR-M8)

/// An OSE release unpacked by `darkpyonix editor install --runtime ose`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OseInstall {
    /// The release tag, e.g. `ose-v1.139.1`.
    pub release: String,
    /// The unpacked server: `bin/dpx-ose-server` and `product.json` are in it.
    pub dir: PathBuf,
    pub commit: String,
    pub version: String,
}

/// The user's own `code`, recorded by `darkpyonix editor install --runtime vsc`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VscInstall {
    pub code: PathBuf,
    pub version: String,
    #[serde(default)]
    pub commit: String,
    /// The user accepted the VS Code Server licence (`--accept-server-license-terms`).
    #[serde(default)]
    pub license_accepted: bool,
}

/// The server a Manual Script installed: the command its last stdout line named.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScriptInstall {
    pub program: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub commit: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EditorConfig {
    /// The runtime a VS Code window opens with (`editor use`).
    #[serde(default = "default_runtime")]
    pub default_runtime: String,
    /// Whether a dedicated manager starts the installed runtimes (`editor autostart`).
    #[serde(default = "yes")]
    pub autostart: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ose: Option<OseInstall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vsc: Option<VscInstall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script: Option<ScriptInstall>,
}

fn default_runtime() -> String {
    "ose".into()
}

fn yes() -> bool {
    true
}

impl Default for EditorConfig {
    fn default() -> Self {
        Self {
            default_runtime: default_runtime(),
            autostart: true,
            ose: None,
            vsc: None,
            script: None,
        }
    }
}

/// How to start one installed runtime.
#[derive(Debug, Clone, PartialEq)]
pub struct Installed {
    pub program: PathBuf,
    /// Arguments before the server flags (`serve-web` for `code`).
    pub pre_args: Vec<String>,
    pub accept_license: bool,
    /// Code-OSS has no Marketplace signature verifier: extension checks go off (FR-M10).
    pub unsigned_extensions: bool,
    pub commit: String,
    pub version: String,
}

impl EditorConfig {
    pub fn path(home: &Path) -> PathBuf {
        home.join("editor.toml")
    }

    /// The file, or the defaults when there is none.
    pub fn load(home: &Path) -> std::io::Result<Self> {
        match std::fs::read_to_string(Self::path(home)) {
            Ok(s) => toml::from_str(&s).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{}: {e}", Self::path(home).display()),
                )
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e),
        }
    }

    pub fn save(&self, home: &Path) -> std::io::Result<()> {
        crate::util::ensure_private_dir(home)?;
        let text = toml::to_string(self).map_err(|e| std::io::Error::other(e.to_string()))?;
        crate::util::write_private_atomic(&Self::path(home), text.as_bytes())
    }

    pub fn installed(&self, runtime: &str) -> Option<Installed> {
        match runtime {
            "ose" => self.ose.as_ref().map(|o| Installed {
                program: ose_launcher(&o.dir),
                pre_args: Vec::new(),
                accept_license: true,
                unsigned_extensions: true,
                commit: o.commit.clone(),
                version: o.version.clone(),
            }),
            "vsc" => self.vsc.as_ref().map(|v| Installed {
                program: v.code.clone(),
                pre_args: vec!["serve-web".into()],
                accept_license: v.license_accepted,
                unsigned_extensions: false,
                commit: v.commit.clone(),
                version: v.version.clone(),
            }),
            "script" => self.script.as_ref().map(|s| Installed {
                program: s.program.clone(),
                pre_args: s.args.clone(),
                accept_license: false,
                unsigned_extensions: false,
                commit: s.commit.clone(),
                version: s.version.clone(),
            }),
            _ => None,
        }
    }
}

/// `bin/dpx-ose-server` of an unpacked OSE (`.cmd` on Windows).
pub fn ose_launcher(dir: &Path) -> PathBuf {
    if cfg!(windows) {
        dir.join("bin").join("dpx-ose-server.cmd")
    } else {
        dir.join("bin").join("dpx-ose-server")
    }
}

// ------------------------------------------------------------------ paths and arguments (FR-M9)

pub fn editor_dir(home: &Path) -> PathBuf {
    home.join("editor")
}

/// `--server-data-dir`: settings and extensions of one runtime.
pub fn data_dir(home: &Path, runtime: &str) -> PathBuf {
    editor_dir(home).join("data").join(runtime)
}

/// The path the runtime is served under (and its `--server-base-path`).
pub fn base_path(runtime: &str) -> String {
    format!("/ide/{runtime}/")
}

/// The socket a runtime listens on: `<home>/editor/sockets/<runtime>.sock`, or a private
/// directory under the temp dir when that path is too long for a Unix socket.
pub fn socket_path(home: &Path, runtime: &str) -> PathBuf {
    let preferred = editor_dir(home)
        .join("sockets")
        .join(format!("{runtime}.sock"));
    if preferred.as_os_str().len() <= SOCKET_PATH_MAX {
        return preferred;
    }
    socket_fallback(home, runtime, &std::env::temp_dir())
}

/// The private socket path under `tmp`, or under `/tmp` when `tmp` itself is too long (FR-M9).
fn socket_fallback(home: &Path, runtime: &str, tmp: &Path) -> PathBuf {
    let digest = auth::sha256_hex(&home.to_string_lossy());
    let under = |base: &Path| {
        base.join(format!("darkpyonix-{}-{}", user_id(), &digest[..12]))
            .join(format!("{runtime}.sock"))
    };
    let path = under(tmp);
    if path.as_os_str().len() <= SOCKET_PATH_MAX || cfg!(not(unix)) {
        return path;
    }
    under(Path::new("/tmp"))
}

#[cfg(unix)]
fn user_id() -> u32 {
    // SAFETY: getuid cannot fail.
    unsafe { libc::getuid() }
}

#[cfg(not(unix))]
fn user_id() -> u32 {
    0
}

/// The flags every start passes (FR-M9).
pub fn server_args(inst: &Installed, home: &Path, runtime: &str, socket: &Path) -> Vec<String> {
    let mut a = inst.pre_args.clone();
    a.extend([
        "--without-connection-token".to_string(),
        "--socket-path".into(),
        socket.to_string_lossy().into_owned(),
        "--server-base-path".into(),
        base_path(runtime),
        "--server-data-dir".into(),
        data_dir(home, runtime).to_string_lossy().into_owned(),
    ]);
    if inst.accept_license {
        a.push("--accept-server-license-terms".into());
    }
    a
}

/// Makes the socket's directory, 0700 and ours, and removes a socket left behind.
fn prepare_socket(socket: &Path) -> std::io::Result<()> {
    let dir = socket
        .parent()
        .ok_or_else(|| std::io::Error::other("socket path has no directory"))?;
    crate::util::ensure_private_dir(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let meta = std::fs::symlink_metadata(dir)?;
        if !meta.is_dir() || meta.uid() != user_id() {
            return Err(std::io::Error::other(format!(
                "{} is not a directory of this user",
                dir.display()
            )));
        }
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    match std::fs::remove_file(socket) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

// ------------------------------------------------------------------ supervisor (FR-M9)

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RunState {
    Stopped,
    Starting,
    Running,
    Failed,
}

type Status = (RunState, Option<String>);

struct Slot {
    want: watch::Sender<bool>,
    status: watch::Receiver<Status>,
}

struct Ticket {
    runtime: String,
    created: Instant,
    last_used: Option<Instant>,
}

impl Ticket {
    fn alive(&self, now: Instant) -> bool {
        match self.last_used {
            None => now.duration_since(self.created) < UNUSED_TICKET_TTL,
            Some(t) => now.duration_since(t) < USED_TICKET_TTL,
        }
    }
}

/// Why `launch` gave no ticket.
#[derive(Debug, Clone, PartialEq)]
pub enum LaunchError {
    /// Ephemeral managers run no editor server (FR-M9).
    Ephemeral,
    NotInstalled,
    /// No answer on the socket within `START_TIMEOUT`; the last reason, if any.
    Timeout(Option<String>),
    Config(String),
}

/// The editor servers of one manager.
pub struct Editors {
    home: PathBuf,
    enabled: bool,
    slots: Mutex<HashMap<String, Slot>>,
    tickets: Mutex<HashMap<String, Ticket>>,
}

impl Editors {
    /// `enabled` is false for an ephemeral manager: it lists what is installed and starts nothing.
    pub fn new(home: impl Into<PathBuf>, enabled: bool) -> Arc<Self> {
        Arc::new(Self {
            home: home.into(),
            enabled,
            slots: Mutex::new(HashMap::new()),
            tickets: Mutex::new(HashMap::new()),
        })
    }

    pub fn config(&self) -> std::io::Result<EditorConfig> {
        EditorConfig::load(&self.home)
    }

    pub fn socket(&self, runtime: &str) -> PathBuf {
        socket_path(&self.home, runtime)
    }

    /// Starts every installed runtime when autostart is on (a dedicated manager's start).
    pub fn autostart(self: &Arc<Self>) {
        if !self.enabled {
            return;
        }
        let Ok(cfg) = self.config() else { return };
        if !cfg.autostart {
            return;
        }
        for rt in RUNTIMES {
            if cfg.installed(rt).is_some() {
                self.want(rt, true);
            }
        }
    }

    pub fn status(&self, runtime: &str) -> Status {
        let slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        slots
            .get(runtime)
            .map(|s| s.status.borrow().clone())
            .unwrap_or((RunState::Stopped, None))
    }

    fn want(self: &Arc<Self>, runtime: &str, on: bool) -> watch::Receiver<Status> {
        let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = slots.get(runtime) {
            s.want.send_replace(on);
            return s.status.clone();
        }
        let (want_tx, want_rx) = watch::channel(on);
        let (status_tx, status_rx) = watch::channel((RunState::Stopped, None));
        tokio::spawn(supervise(
            self.home.clone(),
            runtime.to_string(),
            self.socket(runtime),
            want_rx,
            status_tx,
        ));
        slots.insert(
            runtime.to_string(),
            Slot {
                want: want_tx,
                status: status_rx.clone(),
            },
        );
        status_rx
    }

    /// Starts the runtime if it is not running, waits for its socket and issues a ticket.
    pub async fn launch(self: &Arc<Self>, runtime: &str) -> Result<String, LaunchError> {
        if !self.enabled {
            return Err(LaunchError::Ephemeral);
        }
        let cfg = self
            .config()
            .map_err(|e| LaunchError::Config(e.to_string()))?;
        if cfg.installed(runtime).is_none() {
            return Err(LaunchError::NotInstalled);
        }
        let mut status = self.want(runtime, true);
        let running = matches!(
            tokio::time::timeout(
                START_TIMEOUT,
                status.wait_for(|(s, _)| *s == RunState::Running)
            )
            .await,
            Ok(Ok(_))
        );
        if running {
            Ok(self.issue(runtime))
        } else {
            Err(LaunchError::Timeout(status.borrow().1.clone()))
        }
    }

    /// Ends the runtime and revokes its tickets. It stays off until the next `launch`.
    pub async fn stop(&self, runtime: &str) {
        let status = {
            let slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
            slots.get(runtime).map(|s| {
                s.want.send_replace(false);
                s.status.clone()
            })
        };
        if let Some(mut status) = status {
            let _ = tokio::time::timeout(
                Duration::from_secs(10),
                status.wait_for(|(s, _)| *s == RunState::Stopped),
            )
            .await;
        }
        self.tickets
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|_, t| t.runtime != runtime);
    }

    /// Restarts a running runtime (its presets changed); tickets stay valid.
    pub async fn restart(&self, runtime: &str) {
        let slot = {
            let slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
            slots
                .get(runtime)
                .map(|s| (s.want.clone(), s.status.clone()))
        };
        let Some((want, mut status)) = slot else {
            return;
        };
        if !*want.borrow() {
            return;
        }
        want.send_replace(false);
        let _ = tokio::time::timeout(
            Duration::from_secs(10),
            status.wait_for(|(s, _)| *s == RunState::Stopped),
        )
        .await;
        want.send_replace(true);
    }

    /// Ends every runtime (the manager is stopping).
    pub async fn shutdown(&self) {
        for rt in RUNTIMES {
            self.stop(rt).await;
        }
    }

    fn issue(&self, runtime: &str) -> String {
        let ticket = auth::random_hex(32);
        let mut tickets = self.tickets.lock().unwrap_or_else(|p| p.into_inner());
        let now = Instant::now();
        tickets.retain(|_, t| t.alive(now));
        tickets.insert(
            ticket.clone(),
            Ticket {
                runtime: runtime.to_string(),
                created: now,
                last_used: None,
            },
        );
        ticket
    }

    /// Whether `ticket` is alive for `runtime`; using it extends its life.
    pub fn redeem(&self, ticket: &str, runtime: &str) -> bool {
        let mut tickets = self.tickets.lock().unwrap_or_else(|p| p.into_inner());
        let now = Instant::now();
        tickets.retain(|_, t| t.alive(now));
        match tickets.get_mut(ticket) {
            Some(t) if t.runtime == runtime => {
                t.last_used = Some(now);
                true
            }
            _ => false,
        }
    }

    #[doc(hidden)]
    pub fn age_tickets_for_test(&self, by: Duration) {
        let mut tickets = self.tickets.lock().unwrap_or_else(|p| p.into_inner());
        for t in tickets.values_mut() {
            t.created = t.created.checked_sub(by).unwrap_or(t.created);
            t.last_used = t.last_used.map(|u| u.checked_sub(by).unwrap_or(u));
        }
    }
}

/// One runtime's supervisor: starts the child while it is wanted, restarts it with backoff,
/// and ends it when it is no longer wanted or the manager stops.
async fn supervise(
    home: PathBuf,
    runtime: String,
    socket: PathBuf,
    mut want: watch::Receiver<bool>,
    status: watch::Sender<Status>,
) {
    let mut backoff = BACKOFF_FIRST;
    loop {
        while !*want.borrow_and_update() {
            status.send_replace((RunState::Stopped, None));
            if want.changed().await.is_err() {
                return;
            }
        }
        let started = Instant::now();
        let reason = match run_once(&home, &runtime, &socket, &mut want, &status).await {
            Ended::Unwanted => continue,
            Ended::Closed => return,
            Ended::Failed(reason) => reason,
        };
        if started.elapsed() >= HEALTHY_AFTER {
            backoff = BACKOFF_FIRST;
        }
        tracing::warn!(runtime = %runtime, "editor server: {reason}; restarting in {backoff:?}");
        status.send_replace((RunState::Failed, Some(reason)));
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            r = want.changed() => if r.is_err() { return },
        }
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

enum Ended {
    /// No longer wanted; the child was ended.
    Unwanted,
    /// The manager is gone; the child was ended.
    Closed,
    Failed(String),
}

async fn run_once(
    home: &Path,
    runtime: &str,
    socket: &Path,
    want: &mut watch::Receiver<bool>,
    status: &watch::Sender<Status>,
) -> Ended {
    let inst = match EditorConfig::load(home) {
        Ok(cfg) => match cfg.installed(runtime) {
            Some(i) => i,
            None => return Ended::Failed(format!("{runtime} is not installed")),
        },
        Err(e) => return Ended::Failed(e.to_string()),
    };
    if let Err(e) = prepare_socket(socket) {
        return Ended::Failed(format!("cannot prepare {}: {e}", socket.display()));
    }
    if let Err(e) = crate::util::ensure_private_dir(&data_dir(home, runtime)) {
        return Ended::Failed(format!("cannot create the data dir: {e}"));
    }
    if inst.unsigned_extensions {
        // A user's settings file that is not plain JSON stays as it is; the server still starts.
        let _ = disable_signature_checks(&data_dir(home, runtime));
    }
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(editor_dir(home).join(format!("{runtime}.log")));
    let mut cmd = tokio::process::Command::new(&inst.program);
    cmd.args(server_args(&inst, home, runtime, socket))
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    match log {
        Ok(f) => {
            let err = f
                .try_clone()
                .map(std::process::Stdio::from)
                .unwrap_or_else(|_| std::process::Stdio::null());
            cmd.stdout(std::process::Stdio::from(f)).stderr(err);
        }
        Err(_) => {
            cmd.stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
        }
    }
    #[cfg(unix)]
    cmd.process_group(0);
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return Ended::Failed(format!("cannot start {}: {e}", inst.program.display())),
    };
    status.send_replace((RunState::Starting, None));
    let mut ready = false;
    loop {
        tokio::select! {
            exit = child.wait() => {
                let what = match exit {
                    Ok(s) => format!("exited with {s}"),
                    Err(e) => format!("wait failed: {e}"),
                };
                return Ended::Failed(what);
            }
            r = want.changed() => {
                if r.is_err() {
                    end_child(&mut child).await;
                    return Ended::Closed;
                }
                if !*want.borrow() {
                    end_child(&mut child).await;
                    return Ended::Unwanted;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)), if !ready => {
                if answers(socket).await {
                    ready = true;
                    status.send_replace((RunState::Running, None));
                }
            }
        }
    }
}

#[cfg(unix)]
async fn answers(socket: &Path) -> bool {
    tokio::net::UnixStream::connect(socket).await.is_ok()
}

#[cfg(not(unix))]
async fn answers(_socket: &Path) -> bool {
    false
}

/// SIGTERM to the child's process group, then SIGKILL after 3 seconds.
async fn end_child(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        // SAFETY: signalling the process group this child leads (`process_group(0)`).
        unsafe {
            libc::kill(-(pid as i32), libc::SIGTERM);
        }
        if tokio::time::timeout(Duration::from_secs(3), child.wait())
            .await
            .is_ok()
        {
            return;
        }
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
    }
    let _ = child.start_kill();
    let _ = child.wait().await;
}

// ------------------------------------------------------------------ /ide/<runtime>/ (FR-M9)

/// The runtime of a path under `/ide/`.
fn ide_runtime(path: &str) -> Option<&'static str> {
    let rest = path.strip_prefix("/ide/")?;
    let name = rest.split('/').next()?;
    RUNTIMES.iter().copied().find(|r| *r == name)
}

fn query_pairs(query: Option<&str>) -> Vec<(String, String)> {
    query
        .and_then(|q| serde_urlencoded::from_str(q).ok())
        .unwrap_or_default()
}

fn cookie_ticket(req: &Request) -> Option<String> {
    req.headers()
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .find_map(|c| {
            let (k, v) = c.trim().split_once('=')?;
            (k == TICKET_COOKIE).then(|| v.to_string())
        })
}

/// The request without our credentials: the editor server never sees the master token or the
/// ticket.
fn strip_credentials(req: &mut Request) {
    req.headers_mut().remove(header::AUTHORIZATION);
    let rest: Vec<String> = req
        .headers()
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty() && c.split_once('=').map(|(k, _)| k) != Some(TICKET_COOKIE))
        .collect();
    req.headers_mut().remove(header::COOKIE);
    if !rest.is_empty() {
        if let Ok(v) = HeaderValue::from_str(&rest.join("; ")) {
            req.headers_mut().insert(header::COOKIE, v);
        }
    }
}

fn bearer(req: &Request) -> Option<String> {
    let v = req.headers().get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, rest) = v.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| rest.trim().to_string())
}

/// Authenticates and proxies `/ide/<runtime>/…`; every other path goes on.
pub async fn gate(State(st): State<Arc<AppState>>, mut req: Request, next: Next) -> Response {
    let Some(runtime) = ide_runtime(req.uri().path()) else {
        return next.run(req).await;
    };
    let pairs = query_pairs(req.uri().query());
    if let Some((_, ticket)) = pairs.iter().find(|(k, _)| k == TICKET_QUERY) {
        if !st.editors.redeem(ticket, runtime) {
            return ApiError::unauthorized().into_response();
        }
        let rest: Vec<&(String, String)> =
            pairs.iter().filter(|(k, _)| k != TICKET_QUERY).collect();
        let mut location = req.uri().path().to_string();
        if !rest.is_empty() {
            location.push('?');
            location.push_str(&serde_urlencoded::to_string(&rest).unwrap_or_default());
        }
        let cookie = format!(
            "{TICKET_COOKIE}={ticket}; HttpOnly; SameSite=Strict; Path={}",
            base_path(runtime)
        );
        let mut resp = StatusCode::FOUND.into_response();
        if let (Ok(l), Ok(c)) = (
            HeaderValue::from_str(&location),
            HeaderValue::from_str(&cookie),
        ) {
            resp.headers_mut().insert(header::LOCATION, l);
            resp.headers_mut().insert(header::SET_COOKIE, c);
        }
        return resp;
    }
    let by_ticket = cookie_ticket(&req).is_some_and(|t| st.editors.redeem(&t, runtime));
    if !by_ticket {
        match st.auth.lookup(bearer(&req).as_deref()) {
            auth::Lookup::Found(p) if p.master => {}
            auth::Lookup::Found(_) => {
                return ApiError::forbidden("the editor server opens with the master token only")
                    .into_response()
            }
            _ => return ApiError::unauthorized().into_response(),
        }
    }
    if st.editors.status(runtime).0 != RunState::Running {
        return unavailable(runtime, None);
    }
    strip_credentials(&mut req);
    forward(&st.editors.socket(runtime), req).await
}

#[cfg(unix)]
async fn forward(socket: &Path, req: Request) -> Response {
    crate::proxy::forward_unix(socket, req).await
}

#[cfg(not(unix))]
async fn forward(_socket: &Path, _req: Request) -> Response {
    unavailable(
        "",
        Some("the editor server is not served on Windows yet".into()),
    )
}

fn unavailable(runtime: &str, reason: Option<String>) -> Response {
    let mut message =
        format!("the {runtime} editor server is not running; POST /api/editor/{runtime}/launch");
    if let Some(r) = reason {
        message = format!("{message} ({r})");
    }
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "editor_unavailable",
        message,
    )
    .into_response()
}

// ------------------------------------------------------------------ /api/editor (FR-M10)

const OP_GET_EDITOR: &[u16] = &[200, 401, 403, 500];
const OP_LAUNCH_EDITOR: &[u16] = &[200, 401, 403, 404, 409, 500, 503];
const OP_STOP_EDITOR: &[u16] = &[204, 401, 403, 404, 500];
const OP_INSTALL_EXTENSION: &[u16] = &[200, 400, 401, 403, 404, 409, 422, 500];
const OP_DELETE_EXTENSION: &[u16] = &[204, 400, 401, 403, 404, 409, 422, 500];

fn finish(r: Result<Response, ApiError>, documented: &[u16]) -> Response {
    match r {
        Ok(resp) => resp,
        Err(e) => e.documented(documented).into_response(),
    }
}

fn master_only(p: &Principal) -> Result<(), ApiError> {
    if p.master {
        Ok(())
    } else {
        Err(ApiError::forbidden(
            "the editor server is controlled with the master token only",
        ))
    }
}

fn runtime_of(name: &str) -> Result<&'static str, ApiError> {
    RUNTIMES
        .iter()
        .copied()
        .find(|r| *r == name)
        .ok_or_else(|| ApiError::not_found(format!("no such editor runtime: {name}")))
}

fn config_error(e: std::io::Error) -> ApiError {
    ApiError::internal(format!("cannot read editor.toml: {e}"))
}

pub async fn get_editor(
    State(st): State<Arc<AppState>>,
    Extension(p): Extension<Principal>,
) -> Response {
    let r = (|| {
        master_only(&p)?;
        let cfg = st.editors.config().map_err(config_error)?;
        let runtimes: Vec<Value> = RUNTIMES
            .iter()
            .filter_map(|rt| {
                let inst = cfg.installed(rt)?;
                let (state, reason) = st.editors.status(rt);
                let mut v = json!({
                    "runtime": rt,
                    "version": inst.version,
                    "commit": inst.commit,
                    "state": state,
                    "path": base_path(rt),
                });
                if let Some(r) = reason {
                    v["reason"] = json!(r);
                }
                if let Some(p) = load_presets(&st.editors.home, rt) {
                    v["presets"] = serde_json::to_value(p).unwrap_or_default();
                }
                Some(v)
            })
            .collect();
        Ok((
            StatusCode::OK,
            Json(json!({"runtimes": runtimes, "default_runtime": cfg.default_runtime, "autostart": cfg.autostart})),
        )
            .into_response())
    })();
    finish(r, OP_GET_EDITOR)
}

pub async fn launch_editor(
    State(st): State<Arc<AppState>>,
    Extension(p): Extension<Principal>,
    UrlPath(runtime): UrlPath<String>,
) -> Response {
    let r = async {
        master_only(&p)?;
        let rt = runtime_of(&runtime)?;
        let ticket = st.editors.launch(rt).await.map_err(|e| match e {
            LaunchError::Ephemeral => ApiError::new(
                StatusCode::CONFLICT,
                "conflict",
                "an ephemeral manager runs no editor server",
            ),
            LaunchError::NotInstalled => ApiError::new(
                StatusCode::CONFLICT,
                "conflict",
                format!("{rt} is not installed: darkpyonix editor install --runtime {rt}"),
            ),
            LaunchError::Timeout(reason) => {
                let mut m = format!(
                    "the {rt} editor server did not answer within {}s",
                    START_TIMEOUT.as_secs()
                );
                if let Some(r) = reason {
                    m = format!("{m} ({r})");
                }
                ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "editor_unavailable", m)
            }
            LaunchError::Config(m) => ApiError::internal(m),
        })?;
        let cfg = st.editors.config().map_err(config_error)?;
        let inst = cfg.installed(rt);
        Ok((
            StatusCode::OK,
            Json(json!({
                "ticket": ticket,
                "path": base_path(rt),
                "commit": inst.as_ref().map(|i| i.commit.clone()).unwrap_or_default(),
                "version": inst.map(|i| i.version).unwrap_or_default(),
            })),
        )
            .into_response())
    }
    .await;
    finish(r, OP_LAUNCH_EDITOR)
}

pub async fn stop_editor(
    State(st): State<Arc<AppState>>,
    Extension(p): Extension<Principal>,
    UrlPath(runtime): UrlPath<String>,
) -> Response {
    let r = async {
        master_only(&p)?;
        let rt = runtime_of(&runtime)?;
        st.editors.stop(rt).await;
        Ok(StatusCode::NO_CONTENT.into_response())
    }
    .await;
    finish(r, OP_STOP_EDITOR)
}

#[derive(Deserialize)]
pub struct ExtensionRequest {
    pub id: String,
    #[serde(default)]
    pub version: Option<String>,
}

/// `publisher.name`, the form `--install-extension` takes besides paths and URLs, which are not
/// accepted here.
pub fn is_extension_id(id: &str) -> bool {
    let ok = |s: &str| {
        !s.is_empty()
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    };
    matches!(id.split_once('.'), Some((p, n)) if ok(p) && ok(n))
}

fn is_extension_version(v: &str) -> bool {
    !v.is_empty()
        && v.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+'))
}

pub async fn install_extension(
    State(st): State<Arc<AppState>>,
    Extension(p): Extension<Principal>,
    UrlPath(runtime): UrlPath<String>,
    body: crate::api::RawBody,
) -> Response {
    let r = async {
        master_only(&p)?;
        let rt = runtime_of(&runtime)?;
        let req: ExtensionRequest = serde_json::from_slice(&body.0)
            .map_err(|e| ApiError::bad_request(format!("invalid body: {e}")))?;
        if !is_extension_id(&req.id) {
            return Err(ApiError::bad_request("id must be publisher.name"));
        }
        if req
            .version
            .as_deref()
            .is_some_and(|v| !is_extension_version(v))
        {
            return Err(ApiError::bad_request("invalid version"));
        }
        if rt == "vsc" {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "conflict",
                "VS Code installs its own extensions from the Microsoft marketplace",
            ));
        }
        let cfg = st.editors.config().map_err(config_error)?;
        let Some(inst) = cfg.installed(rt) else {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "conflict",
                format!("{rt} is not installed"),
            ));
        };
        let data = data_dir(&st.editors.home, rt);
        let ext = ExtensionRef {
            id: req.id,
            version: req.version,
        };
        run_server_cli(&inst, &data, &["--install-extension", &ext.spec()])
            .await
            .map_err(|out| extension_failed(&ext.id, "installed", out))?;
        Ok((StatusCode::OK, Json(json!({"installed": true}))).into_response())
    }
    .await;
    finish(r, OP_INSTALL_EXTENSION)
}

/// `DELETE /api/editor/{runtime}/extensions/{id}`: uninstalls through the runtime's CLI; an
/// extension that is not installed is already gone (SPEC FR-M10).
pub async fn delete_extension(
    State(st): State<Arc<AppState>>,
    Extension(p): Extension<Principal>,
    UrlPath((runtime, id)): UrlPath<(String, String)>,
) -> Response {
    let r = async {
        master_only(&p)?;
        let rt = runtime_of(&runtime)?;
        if !is_extension_id(&id) {
            return Err(ApiError::bad_request("id must be publisher.name"));
        }
        if rt == "vsc" {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "conflict",
                "VS Code manages its own extensions",
            ));
        }
        if id.eq_ignore_ascii_case(PRESETS_ID) {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "conflict",
                "the presets extension follows PUT /api/editor/{runtime}/presets",
            ));
        }
        let cfg = st.editors.config().map_err(config_error)?;
        let Some(inst) = cfg.installed(rt) else {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "conflict",
                format!("{rt} is not installed"),
            ));
        };
        let data = data_dir(&st.editors.home, rt);
        let listed = run_server_cli(&inst, &data, &["--list-extensions", "--show-versions"])
            .await
            .map_err(|out| extension_failed(&id, "removed", out))?;
        if parse_extension_list(&listed).contains_key(&id.to_ascii_lowercase()) {
            run_server_cli(&inst, &data, &["--uninstall-extension", &id])
                .await
                .map_err(|out| extension_failed(&id, "removed", out))?;
        }
        Ok(StatusCode::NO_CONTENT.into_response())
    }
    .await;
    finish(r, OP_DELETE_EXTENSION)
}

fn extension_failed(id: &str, what: &str, output: String) -> ApiError {
    ApiError::new(
        StatusCode::UNPROCESSABLE_ENTITY,
        "extension_failed",
        format!("{id} was not {what}"),
    )
    .with_data(json!({"id": id, "output": output}))
}

/// `"extensions.verifySignature": false` in the Machine settings (read by the running server)
/// and the User settings (read by its CLI) of a data dir, keeping every other key (FR-M10).
pub fn disable_signature_checks(data: &Path) -> std::io::Result<()> {
    for scope in ["Machine", "User"] {
        let dir = data.join("data").join(scope);
        let path = dir.join("settings.json");
        let mut settings = match std::fs::read_to_string(&path) {
            Ok(text) if text.trim().is_empty() => serde_json::Map::new(),
            Ok(text) => match serde_json::from_str::<Value>(&text) {
                Ok(Value::Object(m)) => m,
                _ => {
                    return Err(std::io::Error::other(format!(
                        "{} is not plain JSON; add \"extensions.verifySignature\": false to it",
                        path.display()
                    )))
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => serde_json::Map::new(),
            Err(e) => return Err(e),
        };
        if settings.get("extensions.verifySignature") == Some(&Value::Bool(false)) {
            continue;
        }
        settings.insert("extensions.verifySignature".into(), Value::Bool(false));
        std::fs::create_dir_all(&dir)?;
        let text = serde_json::to_string_pretty(&Value::Object(settings)).map_err(std::io::Error::other)?;
        std::fs::write(&path, text + "\n")?;
    }
    Ok(())
}

/// Runs the runtime's server CLI against its data dir; `Err` carries the output.
async fn run_server_cli(inst: &Installed, data: &Path, args: &[&str]) -> Result<String, String> {
    crate::util::ensure_private_dir(data).map_err(|e| e.to_string())?;
    if inst.unsigned_extensions {
        disable_signature_checks(data).map_err(|e| e.to_string())?;
    }
    let mut cmd = tokio::process::Command::new(&inst.program);
    cmd.args(&inst.pre_args)
        .args(args)
        .args(["--server-data-dir", &data.to_string_lossy()])
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    if inst.accept_license {
        cmd.arg("--accept-server-license-terms");
    }
    let out = match tokio::time::timeout(EXTENSION_TIMEOUT, cmd.output()).await {
        Err(_) => return Err(format!("no result within {}s", EXTENSION_TIMEOUT.as_secs())),
        Ok(Err(e)) => return Err(format!("cannot run {}: {e}", inst.program.display())),
        Ok(Ok(o)) => o,
    };
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        Err(text.trim().to_string())
    }
}

// ------------------------------------------------------------------ presets (FR-M10)

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtensionRef {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

impl ExtensionRef {
    fn spec(&self) -> String {
        match &self.version {
            Some(v) => format!("{}@{v}", self.id),
            None => self.id.clone(),
        }
    }
}

/// What Ember's settings ask a runtime to have (main server settings, overridden per node).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Presets {
    #[serde(default)]
    pub extensions: Vec<ExtensionRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub theme: Option<String>,
    #[serde(default)]
    pub settings: serde_json::Map<String, Value>,
}

impl Presets {
    /// The settings the presets extension contributes as defaults.
    pub fn defaults(&self) -> serde_json::Map<String, Value> {
        let mut d = self.settings.clone();
        if let Some(t) = &self.theme {
            d.insert("workbench.colorTheme".into(), json!(t));
        }
        d
    }
}

pub fn presets_path(home: &Path, runtime: &str) -> PathBuf {
    editor_dir(home)
        .join("presets")
        .join(format!("{runtime}.json"))
}

pub fn load_presets(home: &Path, runtime: &str) -> Option<Presets> {
    let text = std::fs::read(presets_path(home, runtime)).ok()?;
    serde_json::from_slice(&text).ok()
}

/// The folder of the extension that carries the defaults, in the runtime's extensions dir.
pub const PRESETS_EXTENSION: &str = "darkpyonix.presets-1.0.0";
/// Its extension id.
const PRESETS_ID: &str = "darkpyonix.presets";

pub fn presets_extension_dir(home: &Path, runtime: &str) -> PathBuf {
    data_dir(home, runtime)
        .join("extensions")
        .join(PRESETS_EXTENSION)
}

/// Where the manager keeps the VSIX it last installed for a runtime.
pub fn presets_vsix_path(home: &Path, runtime: &str) -> PathBuf {
    editor_dir(home)
        .join("presets")
        .join(format!("{runtime}-{PRESETS_EXTENSION}.vsix"))
}

/// The presets extension's `package.json`, or `None` when there are no defaults (FR-M10).
pub fn presets_manifest(presets: &Presets) -> Option<String> {
    let defaults = presets.defaults();
    if defaults.is_empty() {
        return None;
    }
    serde_json::to_string_pretty(&json!({
        "name": "presets",
        "publisher": "darkpyonix",
        "displayName": "DarkPyonix presets",
        "description": "Default settings from the Ember settings of this computer, written by the DarkPyonix manager.",
        "version": "1.0.0",
        "engines": {"vscode": "*"},
        "contributes": {"configurationDefaults": defaults},
    }))
    .ok()
}

/// A VSIX (an uncompressed zip) holding `manifest` as `extension/package.json`. The server
/// loads only extensions it installed itself, so the presets go through its CLI (FR-M10).
pub fn presets_vsix(manifest: &str) -> Vec<u8> {
    let content_types = r#"<?xml version="1.0" encoding="utf-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension=".json" ContentType="application/json"/><Default Extension=".vsixmanifest" ContentType="text/xml"/></Types>
"#;
    let vsixmanifest = r#"<?xml version="1.0" encoding="utf-8"?>
<PackageManifest Version="2.0.0" xmlns="http://schemas.microsoft.com/developer/vsx-schema/2011" xmlns:d="http://schemas.microsoft.com/developer/vsx-schema-design/2011">
  <Metadata>
    <Identity Language="en-US" Id="presets" Version="1.0.0" Publisher="darkpyonix" />
    <DisplayName>DarkPyonix presets</DisplayName>
    <Description xml:space="preserve">Default settings written by the DarkPyonix manager.</Description>
    <Categories>Other</Categories>
    <Properties><Property Id="Microsoft.VisualStudio.Code.Engine" Value="*" /></Properties>
  </Metadata>
  <Installation><InstallationTarget Id="Microsoft.VisualStudio.Code" /></Installation>
  <Dependencies />
  <Assets><Asset Type="Microsoft.VisualStudio.Code.Manifest" Path="extension/package.json" Addressable="true" /></Assets>
</PackageManifest>
"#;
    stored_zip(&[
        ("[Content_Types].xml", content_types.as_bytes()),
        ("extension.vsixmanifest", vsixmanifest.as_bytes()),
        ("extension/package.json", manifest.as_bytes()),
    ])
}

/// A zip with every entry stored (method 0), dated 1980-01-01.
fn stored_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    const DOS_DATE: u16 = (1 << 5) | 1;
    let mut out = Vec::new();
    let mut central = Vec::new();
    for (name, data) in entries {
        let crc = crc32fast::hash(data);
        let offset = out.len() as u32;
        let (n, len) = (name.len() as u16, data.len() as u32);
        out.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
        for v in [20u16, 0, 0, 0, DOS_DATE] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        for v in [crc, len, len] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out.extend_from_slice(&n.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(data);

        central.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
        for v in [20u16, 20, 0, 0, 0, DOS_DATE] {
            central.extend_from_slice(&v.to_le_bytes());
        }
        for v in [crc, len, len] {
            central.extend_from_slice(&v.to_le_bytes());
        }
        for v in [n, 0, 0, 0, 0] {
            central.extend_from_slice(&v.to_le_bytes());
        }
        central.extend_from_slice(&0u32.to_le_bytes());
        central.extend_from_slice(&offset.to_le_bytes());
        central.extend_from_slice(name.as_bytes());
    }
    let (cd_offset, cd_len, count) = (out.len() as u32, central.len() as u32, entries.len() as u16);
    out.extend_from_slice(&central);
    out.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
    for v in [0u16, 0, count, count] {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out.extend_from_slice(&cd_len.to_le_bytes());
    out.extend_from_slice(&cd_offset.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out
}

/// Installs (or, with no defaults, uninstalls) the presets extension through the runtime's
/// CLI when it differs from the last one installed; whether anything changed (FR-M10).
async fn apply_presets_extension(
    home: &Path,
    runtime: &str,
    inst: &Installed,
    presets: &Presets,
) -> Result<bool, ApiError> {
    let data = data_dir(home, runtime);
    let path = presets_vsix_path(home, runtime);
    let last = std::fs::read(&path).ok();
    match presets_manifest(presets) {
        None => {
            if last.is_none() {
                return Ok(false);
            }
            run_server_cli(inst, &data, &["--uninstall-extension", PRESETS_ID])
                .await
                .map_err(|out| extension_failed(PRESETS_ID, "removed", out))?;
            std::fs::remove_file(&path).map_err(|e| ApiError::internal(e.to_string()))?;
            Ok(true)
        }
        Some(manifest) => {
            let vsix = presets_vsix(&manifest);
            if last.as_deref() == Some(vsix.as_slice()) {
                return Ok(false);
            }
            if let Some(dir) = path.parent() {
                crate::util::ensure_private_dir(dir).map_err(|e| ApiError::internal(e.to_string()))?;
            }
            std::fs::write(&path, &vsix).map_err(|e| ApiError::internal(e.to_string()))?;
            let spec = path.to_string_lossy().into_owned();
            if let Err(out) = run_server_cli(inst, &data, &["--install-extension", &spec, "--force"]).await {
                // Not installed: the next PUT tries again.
                let _ = std::fs::remove_file(&path);
                return Err(extension_failed(PRESETS_ID, "installed", out));
            }
            Ok(true)
        }
    }
}

/// `publisher.name@version` lines of `--list-extensions --show-versions`, lowercased ids.
pub fn parse_extension_list(out: &str) -> HashMap<String, String> {
    out.lines()
        .filter_map(|l| {
            let (id, v) = l.trim().split_once('@')?;
            is_extension_id(id).then(|| (id.to_ascii_lowercase(), v.to_string()))
        })
        .collect()
}

#[derive(Deserialize)]
pub struct PresetsRequest {
    #[serde(default)]
    pub extensions: Vec<ExtensionRef>,
    #[serde(default)]
    pub theme: Option<String>,
    #[serde(default)]
    pub settings: serde_json::Map<String, Value>,
}

const OP_PUT_PRESETS: &[u16] = &[200, 400, 401, 403, 404, 409, 422, 500];

pub async fn put_presets(
    State(st): State<Arc<AppState>>,
    Extension(p): Extension<Principal>,
    UrlPath(runtime): UrlPath<String>,
    body: crate::api::RawBody,
) -> Response {
    let r = async {
        master_only(&p)?;
        let rt = runtime_of(&runtime)?;
        let req: PresetsRequest = serde_json::from_slice(&body.0)
            .map_err(|e| ApiError::bad_request(format!("invalid body: {e}")))?;
        for e in &req.extensions {
            if !is_extension_id(&e.id)
                || e.version
                    .as_deref()
                    .is_some_and(|v| !is_extension_version(v))
            {
                return Err(ApiError::bad_request(format!(
                    "invalid extension {:?}",
                    e.id
                )));
            }
        }
        if req.theme.as_deref().is_some_and(str::is_empty) {
            return Err(ApiError::bad_request("theme must not be empty"));
        }
        if rt == "vsc" && !req.extensions.is_empty() {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "conflict",
                "VS Code installs its own extensions from the Microsoft marketplace",
            ));
        }
        let cfg = st.editors.config().map_err(config_error)?;
        let Some(inst) = cfg.installed(rt) else {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "conflict",
                format!("{rt} is not installed"),
            ));
        };
        let home = st.editors.home.clone();
        let data = data_dir(&home, rt);
        let presets = Presets {
            extensions: req.extensions,
            theme: req.theme,
            settings: req.settings,
        };
        let save = |p: &Presets| -> Result<(), ApiError> {
            let path = presets_path(&home, rt);
            if let Some(dir) = path.parent() {
                crate::util::ensure_private_dir(dir)
                    .map_err(|e| ApiError::internal(e.to_string()))?;
            }
            let text =
                serde_json::to_vec_pretty(p).map_err(|e| ApiError::internal(e.to_string()))?;
            crate::util::write_private_atomic(&path, &text)
                .map_err(|e| ApiError::internal(e.to_string()))
        };
        save(&presets)?;
        let changed = apply_presets_extension(&home, rt, &inst, &presets).await?;
        if !presets.extensions.is_empty() {
            let listed = run_server_cli(&inst, &data, &["--list-extensions", "--show-versions"])
                .await
                .map(|out| parse_extension_list(&out))
                .unwrap_or_default();
            for e in &presets.extensions {
                let have = listed.get(&e.id.to_ascii_lowercase());
                let missing = match (&e.version, have) {
                    (_, None) => true,
                    (Some(want), Some(v)) => want != v,
                    (None, Some(_)) => false,
                };
                if missing {
                    run_server_cli(&inst, &data, &["--install-extension", &e.spec()])
                        .await
                        .map_err(|out| extension_failed(&e.id, "installed", out))?;
                }
            }
        }
        if changed {
            st.editors.restart(rt).await;
        }
        Ok((
            StatusCode::OK,
            Json(serde_json::to_value(&presets).unwrap_or_default()),
        )
            .into_response())
    }
    .await;
    finish(r, OP_PUT_PRESETS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fr_m9_ide_paths_name_a_runtime() {
        assert_eq!(ide_runtime("/ide/ose/"), Some("ose"));
        assert_eq!(ide_runtime("/ide/vsc/static/x.js"), Some("vsc"));
        assert_eq!(ide_runtime("/ide/ose"), Some("ose"));
        assert_eq!(ide_runtime("/ide/other/"), None);
        assert_eq!(ide_runtime("/api/editor"), None);
    }

    #[test]
    #[cfg(unix)]
    fn test_fr_m9_a_long_tmpdir_puts_the_socket_under_slash_tmp() {
        let home = PathBuf::from(format!("/{}", "x".repeat(120)));
        let short = socket_fallback(&home, "ose", Path::new("/var/t"));
        assert!(short.starts_with("/var/t"), "{short:?}");
        let long_tmp = PathBuf::from(format!("/{}", "t".repeat(90)));
        let s = socket_fallback(&home, "ose", &long_tmp);
        assert!(s.to_string_lossy().starts_with("/tmp/darkpyonix-"), "{s:?}");
        assert!(s.as_os_str().len() <= SOCKET_PATH_MAX, "{s:?}");
    }

    #[test]
    fn test_fr_m9_a_long_home_puts_the_socket_in_a_private_temp_dir() {
        let short = socket_path(Path::new("/h"), "ose");
        assert_eq!(short, PathBuf::from("/h/editor/sockets/ose.sock"));
        let long = PathBuf::from(format!("/{}", "x".repeat(120)));
        let s = socket_path(&long, "ose");
        assert!(s.starts_with(std::env::temp_dir()), "{s:?}");
        assert!(
            s.as_os_str().len() <= SOCKET_PATH_MAX + std::env::temp_dir().as_os_str().len(),
            "{s:?}"
        );
        assert_ne!(
            s,
            socket_path(&PathBuf::from(format!("/{}", "y".repeat(120))), "ose")
        );
        assert!(s.file_name().unwrap() == "ose.sock");
    }

    #[test]
    fn test_fr_m9_server_args_have_the_socket_and_no_token() {
        let inst = Installed {
            program: "code".into(),
            pre_args: vec!["serve-web".into()],
            accept_license: false,
            unsigned_extensions: false,
            commit: String::new(),
            version: String::new(),
        };
        let a = server_args(
            &inst,
            Path::new("/h"),
            "vsc",
            Path::new("/h/editor/sockets/vsc.sock"),
        );
        assert_eq!(a[0], "serve-web");
        assert!(a.contains(&"--without-connection-token".into()));
        assert!(a
            .windows(2)
            .any(|w| w == ["--socket-path", "/h/editor/sockets/vsc.sock"]));
        assert!(a
            .windows(2)
            .any(|w| w == ["--server-base-path", "/ide/vsc/"]));
        assert!(!a
            .iter()
            .any(|x| x == "--accept-server-license-terms" || x == "--port" || x == "--host"));
    }

    #[test]
    fn test_fr_m8_editor_toml_round_trips() {
        let cfg = EditorConfig {
            default_runtime: "vsc".into(),
            autostart: false,
            ose: Some(OseInstall {
                release: "ose-v1.139.1".into(),
                dir: "/h/editor/ose/ose-v1.139.1/dpx-ose-1.139.1-darwin-arm64".into(),
                commit: "abc".into(),
                version: "1.139.1".into(),
            }),
            vsc: Some(VscInstall {
                code: "/usr/bin/code".into(),
                version: "1.106.0".into(),
                commit: "def".into(),
                license_accepted: true,
            }),
            script: Some(ScriptInstall {
                program: "/opt/srv/bin/server".into(),
                args: vec!["--x".into()],
                version: String::new(),
                commit: String::new(),
            }),
        };
        let text = toml::to_string(&cfg).unwrap();
        assert_eq!(toml::from_str::<EditorConfig>(&text).unwrap(), cfg);
        assert_eq!(
            toml::from_str::<EditorConfig>("").unwrap(),
            EditorConfig::default()
        );
    }

    #[test]
    fn test_fr_m10_extension_ids_are_publisher_and_name_only() {
        assert!(is_extension_id("redhat.vscode-yaml"));
        assert!(!is_extension_id("../x.vsix"));
        assert!(!is_extension_id("https://example.com/x.vsix"));
        assert!(!is_extension_id("noname"));
        assert!(is_extension_version("1.2.3-rc.1"));
        assert!(!is_extension_version("1 2"));
    }

    #[test]
    fn test_fr_m9_ticket_is_scoped_to_its_runtime() {
        let e = Editors::new("/nonexistent", true);
        let t = e.issue("ose");
        assert!(!e.redeem(&t, "vsc"));
        assert!(!e.redeem(&t, "script"));
        assert!(e.redeem(&t, "ose"));
    }

    #[test]
    fn test_fr_m10_presets_extension_carries_the_defaults() {
        assert_eq!(presets_manifest(&Presets::default()), None);
        let p = Presets {
            extensions: vec![],
            theme: Some("T".into()),
            settings: serde_json::Map::new(),
        };
        let manifest = presets_manifest(&p).unwrap();
        let v: Value = serde_json::from_str(&manifest).unwrap();
        assert_eq!(v["contributes"]["configurationDefaults"]["workbench.colorTheme"], "T");
        let vsix = presets_vsix(&manifest);
        assert_eq!(&vsix[..4], b"PK\x03\x04");
        assert_eq!(&vsix[vsix.len() - 22..vsix.len() - 18], b"PK\x05\x06");
        let text = String::from_utf8_lossy(&vsix);
        assert!(text.contains("extension/package.json") && text.contains(&manifest));
        assert_eq!(vsix, presets_vsix(&manifest), "the same presets give the same bytes");
    }

    #[test]
    fn test_fr_m9_unused_ticket_expires() {
        let e = Editors::new("/nonexistent", true);
        let t = e.issue("ose");
        e.age_tickets_for_test(UNUSED_TICKET_TTL + Duration::from_secs(1));
        assert!(!e.redeem(&t, "ose"));
        let t = e.issue("ose");
        assert!(e.redeem(&t, "ose"));
        e.age_tickets_for_test(UNUSED_TICKET_TTL + Duration::from_secs(1));
        assert!(
            e.redeem(&t, "ose"),
            "a used ticket lives until 12 hours after its last use"
        );
        e.age_tickets_for_test(USED_TICKET_TTL + Duration::from_secs(1));
        assert!(!e.redeem(&t, "ose"));
    }
}
