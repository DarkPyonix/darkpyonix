//! `darkpyonix service install|uninstall|status`: the dedicated manager as a user service
//! (SPEC FR-M7, INTENT D26). No administrator rights: a launchd user agent on macOS, a
//! `systemd --user` unit on Linux, a logon task on Windows. The unit runs this binary with
//! `manager --dedicated --token-file`; the token lives only in `$DARKPYONIX_HOME/manager-token`.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::args::{ServiceArgs, ServiceCommand, ServiceInstallArgs};

/// Replaces the user's home directory as the place the unit is written (tests).
pub const ROOT_ENV: &str = "DARKPYONIX_SERVICE_ROOT";
/// Runs `<ctl> <tool> <args…>` instead of `launchctl`, `systemctl` or `schtasks` (tests).
pub const CTL_ENV: &str = "DARKPYONIX_SERVICE_CTL";

pub const LAUNCHD_LABEL: &str = "dev.darkpyonix.manager";
pub const SYSTEMD_UNIT: &str = "darkpyonix-manager.service";
pub const TASK_NAME: &str = "DarkPyonix Manager";
pub const TOKEN_FILE: &str = "manager-token";
/// What `install` recorded: the address the unit listens on.
const STATE_FILE: &str = "service.json";
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Launchd,
    Systemd,
    Task,
}

impl Kind {
    pub fn current() -> Kind {
        if cfg!(target_os = "macos") {
            Kind::Launchd
        } else if cfg!(windows) {
            Kind::Task
        } else {
            Kind::Systemd
        }
    }

    /// Where the registration is written under `root` (the user's home).
    pub fn unit_path(self, root: &Path, home: &Path) -> PathBuf {
        match self {
            Kind::Launchd => root.join("Library/LaunchAgents").join(format!("{LAUNCHD_LABEL}.plist")),
            Kind::Systemd => root.join(".config/systemd/user").join(SYSTEMD_UNIT),
            // schtasks keeps the task itself; the XML it was made from stays beside the token.
            Kind::Task => home.join("service").join("darkpyonix-manager.xml"),
        }
    }
}

struct Fail(i32, String);

fn fail(msg: impl Into<String>) -> Fail {
    Fail(crate::render::EXIT_ERROR, msg.into())
}

pub fn run(a: ServiceArgs) -> i32 {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("tokio runtime");
    let home = dpx_server::default_home();
    match rt.block_on(command(&home, a.command)) {
        Ok(v) => {
            println!("{v}");
            crate::render::EXIT_OK
        }
        Err(Fail(code, msg)) => {
            eprintln!("darkpyonix service: {msg}");
            code
        }
    }
}

fn root() -> PathBuf {
    if let Some(r) = std::env::var_os(ROOT_ENV).filter(|r| !r.is_empty()) {
        return PathBuf::from(r);
    }
    let base = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")).unwrap_or_default();
    PathBuf::from(base)
}

async fn command(home: &Path, c: ServiceCommand) -> Result<Value, Fail> {
    let kind = Kind::current();
    let unit = kind.unit_path(&root(), home);
    match c {
        ServiceCommand::Install(a) => install(home, kind, &unit, &a).await,
        ServiceCommand::Uninstall => uninstall(home, kind, &unit),
        ServiceCommand::Status => Ok(status(home, &unit).await),
    }
}

// ------------------------------------------------------------------------------- install

async fn install(home: &Path, kind: Kind, unit: &Path, a: &ServiceInstallArgs) -> Result<Value, Fail> {
    debug_assert!(a.dedicated);
    std::fs::create_dir_all(home).map_err(|e| fail(format!("cannot create {}: {e}", home.display())))?;
    let token_path = home.join(TOKEN_FILE);
    let token = ensure_token(&token_path)?;
    let url = match (unit.exists(), recorded_url(home)) {
        // Already registered: leave the registration alone and report the running manager.
        (true, Some(url)) => {
            if !healthy(&url).await {
                let _ = ctl(kind, Action::Start, unit);
            }
            url
        }
        _ => {
            let host = a.host.clone().unwrap_or_else(|| "127.0.0.1".into());
            let port = match a.port {
                Some(p) => p,
                None => free_port(&host)?,
            };
            let program =
                std::env::current_exe().map_err(|e| fail(format!("cannot find this binary: {e}")))?;
            let args = manager_args(&host, port, &token_path);
            let text = match kind {
                Kind::Launchd => launchd_plist(&program, &args, home),
                Kind::Systemd => systemd_unit(&program, &args, home),
                Kind::Task => task_xml(&program, &args, home),
            };
            if let Some(dir) = unit.parent() {
                std::fs::create_dir_all(dir).map_err(|e| fail(format!("cannot create {}: {e}", dir.display())))?;
            }
            let bytes = match kind {
                // schtasks reads the task XML as UTF-16 with a byte order mark.
                Kind::Task => {
                    let mut b = vec![0xFF, 0xFE];
                    b.extend(text.encode_utf16().flat_map(u16::to_le_bytes));
                    b
                }
                _ => text.into_bytes(),
            };
            std::fs::write(unit, bytes).map_err(|e| fail(format!("cannot write {}: {e}", unit.display())))?;
            let url = format!("http://{}:{port}", connect_host(&host));
            let state = json!({"url": url, "unit": unit});
            std::fs::write(home.join(STATE_FILE), state.to_string())
                .map_err(|e| fail(format!("cannot write {STATE_FILE}: {e}")))?;
            if let Err(e) = ctl(kind, Action::Start, unit) {
                let _ = std::fs::remove_file(unit);
                let _ = std::fs::remove_file(home.join(STATE_FILE));
                return Err(fail(e));
            }
            url
        }
    };
    let deadline = Instant::now() + HEALTH_TIMEOUT;
    while !healthy(&url).await {
        if Instant::now() > deadline {
            return Err(fail(format!(
                "the service did not answer {url}/health within {}s",
                HEALTH_TIMEOUT.as_secs()
            )));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let pid = manager_pid(&url, &token).await.ok_or_else(|| {
        fail(format!("{url} answers, but not to the token in {}", token_path.display()))
    })?;
    Ok(json!({"url": url, "token": token, "pid": pid}))
}

/// The `manager` arguments the unit runs (SPEC FR-M7).
pub fn manager_args(host: &str, port: u16, token_file: &Path) -> Vec<String> {
    vec![
        "manager".into(),
        "--dedicated".into(),
        "--host".into(),
        host.into(),
        "--port".into(),
        port.to_string(),
        "--token-file".into(),
        token_file.to_string_lossy().into_owned(),
    ]
}

/// The existing token, or a new one written owner-only (SPEC FR-M7).
fn ensure_token(path: &Path) -> Result<String, Fail> {
    if let Ok(t) = crate::manager::read_token_file(path) {
        return Ok(t);
    }
    let token = dpx_server::auth::random_hex(32);
    write_private(path, &format!("{token}\n")).map_err(|e| fail(format!("cannot write {}: {e}", path.display())))?;
    Ok(token)
}

#[cfg(unix)]
fn write_private(path: &Path, text: &str) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let mut f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(path)?;
    f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    f.write_all(text.as_bytes())
}

#[cfg(not(unix))]
fn write_private(path: &Path, text: &str) -> std::io::Result<()> {
    std::fs::write(path, text)?;
    // Owner-only ACL: drop inherited entries and grant the current user alone.
    let user = std::env::var("USERNAME").unwrap_or_default();
    let ok = Command::new("icacls")
        .arg(path)
        .args(["/inheritance:r", "/grant:r", &format!("{user}:F")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if ok {
        Ok(())
    } else {
        Err(std::io::Error::other("icacls could not restrict the token file"))
    }
}

fn free_port(host: &str) -> Result<u16, Fail> {
    std::net::TcpListener::bind((host, 0))
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .map_err(|e| fail(format!("cannot find a free port on {host}: {e}")))
}

/// A wildcard listen address is reached over loopback.
fn connect_host(host: &str) -> String {
    match host {
        "0.0.0.0" | "" => "127.0.0.1".into(),
        "::" => "[::1]".into(),
        h if h.contains(':') && !h.starts_with('[') => format!("[{h}]"),
        h => h.into(),
    }
}

fn recorded_url(home: &Path) -> Option<String> {
    let v: Value = serde_json::from_slice(&std::fs::read(home.join(STATE_FILE)).ok()?).ok()?;
    v["url"].as_str().map(str::to_string)
}

fn http() -> reqwest::Client {
    reqwest::Client::builder().timeout(Duration::from_secs(2)).build().expect("http client")
}

async fn healthy(url: &str) -> bool {
    matches!(http().get(format!("{url}/health")).send().await, Ok(r) if r.status().is_success())
}

async fn manager_pid(url: &str, token: &str) -> Option<u64> {
    let r = http().get(format!("{url}/api/manager")).bearer_auth(token).send().await.ok()?;
    if !r.status().is_success() {
        return None;
    }
    let body = r.bytes().await.ok()?;
    serde_json::from_slice::<Value>(&body).ok()?["pid"].as_u64()
}

// ----------------------------------------------------------------------- uninstall, status

fn uninstall(home: &Path, kind: Kind, unit: &Path) -> Result<Value, Fail> {
    if unit.exists() {
        ctl(kind, Action::Stop, unit).map_err(fail)?;
        std::fs::remove_file(unit).map_err(|e| fail(format!("cannot remove {}: {e}", unit.display())))?;
        if kind == Kind::Systemd {
            let _ = run_tool("systemctl", &["--user", "daemon-reload"]);
        }
    }
    let _ = std::fs::remove_file(home.join(STATE_FILE));
    Ok(json!({"installed": false}))
}

async fn status(home: &Path, unit: &Path) -> Value {
    let installed = unit.exists();
    let url = if installed { recorded_url(home) } else { None };
    let running = match &url {
        Some(u) => healthy(u).await,
        None => false,
    };
    let pid = match (&url, running, crate::manager::read_token_file(&home.join(TOKEN_FILE))) {
        (Some(u), true, Ok(t)) => manager_pid(u, &t).await,
        _ => None,
    };
    json!({"installed": installed, "running": running, "url": url, "pid": pid})
}

// ------------------------------------------------------------------------- service control

#[derive(Clone, Copy)]
enum Action {
    Start,
    Stop,
}

fn ctl(kind: Kind, action: Action, unit: &Path) -> Result<(), String> {
    let unit_s = unit.to_string_lossy().into_owned();
    match (kind, action) {
        (Kind::Launchd, Action::Start) => {
            let domain = format!("gui/{}", uid());
            // A stale registration of the same label would make bootstrap fail.
            let _ = run_tool("launchctl", &["bootout", &format!("{domain}/{LAUNCHD_LABEL}")]);
            run_tool("launchctl", &["bootstrap", &domain, &unit_s])
        }
        (Kind::Launchd, Action::Stop) => {
            run_tool("launchctl", &["bootout", &format!("gui/{}/{LAUNCHD_LABEL}", uid())]).or(Ok(()))
        }
        (Kind::Systemd, Action::Start) => {
            run_tool("systemctl", &["--user", "daemon-reload"])?;
            run_tool("systemctl", &["--user", "enable", "--now", SYSTEMD_UNIT])
        }
        (Kind::Systemd, Action::Stop) => run_tool("systemctl", &["--user", "disable", "--now", SYSTEMD_UNIT]),
        (Kind::Task, Action::Start) => {
            run_tool("schtasks", &["/Create", "/TN", TASK_NAME, "/XML", &unit_s, "/F"])?;
            run_tool("schtasks", &["/Run", "/TN", TASK_NAME])
        }
        (Kind::Task, Action::Stop) => {
            let _ = run_tool("schtasks", &["/End", "/TN", TASK_NAME]);
            run_tool("schtasks", &["/Delete", "/TN", TASK_NAME, "/F"])
        }
    }
}

fn run_tool(tool: &str, args: &[&str]) -> Result<(), String> {
    let mut cmd = match std::env::var_os(CTL_ENV).filter(|c| !c.is_empty()) {
        Some(fake) => {
            let mut c = Command::new(fake);
            c.arg(tool);
            c
        }
        None => Command::new(tool),
    };
    let out = cmd
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("cannot run {tool}: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "{tool} {} failed: {}{}",
            args.join(" "),
            String::from_utf8_lossy(&out.stdout).trim(),
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

#[cfg(unix)]
fn uid() -> u32 {
    // SAFETY: getuid cannot fail.
    unsafe { libc::getuid() }
}

#[cfg(not(unix))]
fn uid() -> u32 {
    0
}

// ------------------------------------------------------------------------------ unit files

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

/// The launchd user agent (SPEC FR-M7): kept alive, started at login, no token.
pub fn launchd_plist(program: &Path, args: &[String], home: &Path) -> String {
    let mut argv = format!("    <string>{}</string>\n", xml_escape(&program.to_string_lossy()));
    for a in args {
        argv.push_str(&format!("    <string>{}</string>\n", xml_escape(a)));
    }
    let home_s = xml_escape(&home.to_string_lossy());
    let log = xml_escape(&home.join("manager-service.log").to_string_lossy());
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{LAUNCHD_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
{argv}  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>DARKPYONIX_HOME</key>
    <string>{home_s}</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>{log}</string>
  <key>StandardErrorPath</key>
  <string>{log}</string>
</dict>
</plist>
"#
    )
}

/// A `systemd` argument in double quotes (`\`, `"` escaped; `%` doubled for specifiers).
fn systemd_quote(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"").replace('%', "%%"))
}

/// The `systemd --user` unit (SPEC FR-M7): restarted on failure, enabled for the user, no token.
pub fn systemd_unit(program: &Path, args: &[String], home: &Path) -> String {
    let mut exec = systemd_quote(&program.to_string_lossy());
    for a in args {
        exec.push(' ');
        exec.push_str(&systemd_quote(a));
    }
    let env = systemd_quote(&format!("DARKPYONIX_HOME={}", home.to_string_lossy()));
    format!(
        "[Unit]\nDescription=DarkPyonix manager (dedicated)\nAfter=network.target\n\n\
         [Service]\nExecStart={exec}\nEnvironment={env}\nRestart=on-failure\nRestartSec=2\n\n\
         [Install]\nWantedBy=default.target\n"
    )
}

/// A Windows argument in double quotes.
fn win_quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\\\""))
}

/// The logon task (SPEC FR-M7): started at the user's logon, restarted on failure, no token.
pub fn task_xml(program: &Path, args: &[String], home: &Path) -> String {
    let mut line = format!(
        "/d /c set \"DARKPYONIX_HOME={}\" &amp;&amp; {}",
        xml_escape(&home.to_string_lossy()),
        xml_escape(&win_quote(&program.to_string_lossy()))
    );
    for a in args {
        line.push(' ');
        line.push_str(&xml_escape(&win_quote(a)));
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <Triggers>
    <LogonTrigger><Enabled>true</Enabled></LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author"><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Hidden>true</Hidden>
    <RestartOnFailure><Interval>PT1M</Interval><Count>999</Count></RestartOnFailure>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>cmd.exe</Command>
      <Arguments>{line}</Arguments>
    </Exec>
  </Actions>
</Task>
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> Vec<String> {
        manager_args("127.0.0.1", 8765, Path::new("/h/manager-token"))
    }

    #[test]
    fn test_fr_m7_every_unit_runs_the_dedicated_manager_with_the_token_file() {
        let p = Path::new("/opt/dpx/darkpyonix");
        for text in [
            launchd_plist(p, &args(), Path::new("/h")),
            systemd_unit(p, &args(), Path::new("/h")),
            task_xml(p, &args(), Path::new("/h")),
        ] {
            for want in ["/opt/dpx/darkpyonix", "manager", "--dedicated", "--token-file", "/h/manager-token", "8765"] {
                assert!(text.contains(want), "{want} missing in {text}");
            }
        }
        assert!(launchd_plist(p, &args(), Path::new("/h")).contains("<key>KeepAlive</key>\n  <true/>"));
        assert!(systemd_unit(p, &args(), Path::new("/h")).contains("Restart=on-failure"));
    }

    #[test]
    fn test_fr_m7_unit_values_are_quoted() {
        let u = systemd_unit(Path::new("/a b/50%\"x"), &[], Path::new("/h"));
        assert!(u.contains(r#"ExecStart="/a b/50%%\"x""#), "{u}");
        let p = launchd_plist(Path::new("/a&b"), &[], Path::new("/h<"));
        assert!(p.contains("/a&amp;b") && p.contains("/h&lt;"), "{p}");
    }

    #[test]
    fn test_fr_m7_wildcard_hosts_are_reached_over_loopback() {
        assert_eq!(connect_host("0.0.0.0"), "127.0.0.1");
        assert_eq!(connect_host("::"), "[::1]");
        assert_eq!(connect_host("fe80::1"), "[fe80::1]");
        assert_eq!(connect_host("10.0.0.2"), "10.0.0.2");
    }
}
