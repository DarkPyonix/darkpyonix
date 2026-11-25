//! SPEC FR-M7: `darkpyonix service install|uninstall|status`. The unit goes under a scratch
//! root (`DARKPYONIX_SERVICE_ROOT`), and `tests/fake_ctl.py` stands in for `launchctl` and
//! `systemctl --user`: it runs the command the unit names, so a real dedicated manager answers.
#![cfg(unix)]

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Output;

use common::*;
use serde_json::Value;

struct Svc {
    home: PathBuf,
    root: PathBuf,
    state: PathBuf,
}

impl Svc {
    fn new(name: &str) -> Svc {
        let base = scratch_home(name);
        let s = Svc { home: base.join("h"), root: base.join("r"), state: base.join("s") };
        std::fs::create_dir_all(&s.root).unwrap();
        // The CLI runs from its home (`common::cli`).
        std::fs::create_dir_all(&s.home).unwrap();
        s
    }

    fn run(&self, args: &[&str]) -> Output {
        let ctl = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fake_ctl.py");
        cli(&self.home)
            .env("DARKPYONIX_SERVICE_ROOT", &self.root)
            .env("DARKPYONIX_SERVICE_CTL", ctl)
            .env("FAKE_CTL_STATE", &self.state)
            .env_remove("DARKPYONIX_MANAGER_TOKEN")
            .args(["service"])
            .args(args)
            .output()
            .unwrap()
    }

    fn install(&self, extra: &[&str]) -> Value {
        let mut args = vec!["install", "--dedicated"];
        args.extend_from_slice(extra);
        let out = self.run(&args);
        assert!(out.status.success(), "{}{}", text(&out.stderr), self.log());
        let stdout = text(&out.stdout);
        assert_eq!(stdout.lines().count(), 1, "{stdout}");
        serde_json::from_str(&stdout).unwrap()
    }

    fn unit(&self) -> PathBuf {
        if cfg!(target_os = "macos") {
            self.root.join("Library/LaunchAgents/dev.darkpyonix.manager.plist")
        } else {
            self.root.join(".config/systemd/user/darkpyonix-manager.service")
        }
    }

    fn calls(&self, verb: &str) -> usize {
        std::fs::read_to_string(self.state.join("calls.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter(|l| l.contains(verb))
            .count()
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.state.join("manager.log")).unwrap_or_default()
    }
}

impl Drop for Svc {
    fn drop(&mut self) {
        if let Ok(pid) = std::fs::read_to_string(self.state.join("pid")) {
            unsafe { libc::kill(pid.trim().parse().unwrap(), libc::SIGKILL) };
        }
    }
}

fn health(url: &str) -> bool {
    std::process::Command::new("curl")
        .args(["-sf", "-m", "2", &format!("{url}/health")])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn start_verb() -> &'static str {
    if cfg!(target_os = "macos") {
        "bootstrap"
    } else {
        "enable"
    }
}

#[test]
fn test_fr_m7_service_unit_runs_the_dedicated_manager_without_the_token() {
    let s = Svc::new("svc-unit");
    let line = s.install(&[]);
    let unit = std::fs::read_to_string(s.unit()).unwrap();
    let token_file = s.home.join("manager-token");
    for want in [env!("CARGO_BIN_EXE_darkpyonix"), "manager", "--dedicated", "--token-file", token_file.to_str().unwrap()] {
        assert!(unit.contains(want), "{want} missing:\n{unit}");
    }
    let policy = if cfg!(target_os = "macos") { "<key>KeepAlive</key>" } else { "Restart=on-failure" };
    assert!(unit.contains(policy), "{unit}");
    assert!(!unit.contains(line["token"].as_str().unwrap()), "the token is in the unit:\n{unit}");
}

#[test]
fn test_fr_m7_install_prints_the_manager_line_once_and_is_idempotent() {
    let s = Svc::new("svc-idem");
    let first = s.install(&[]);
    let url = first["url"].as_str().unwrap();
    assert!(health(url), "{url}");
    assert!(first["pid"].as_u64().unwrap() > 0);
    let unit = std::fs::read(s.unit()).unwrap();
    let again = s.install(&["--port", "1"]);
    assert_eq!(again, first, "a second install reports the same manager");
    assert_eq!(std::fs::read(s.unit()).unwrap(), unit, "the registration did not change");
    assert_eq!(s.calls(start_verb()), 1, "started once");
    let status: Value = serde_json::from_slice(&s.run(&["status"]).stdout).unwrap();
    assert_eq!(status["installed"], true);
    assert_eq!(status["running"], true);
    assert_eq!(status["url"], first["url"]);
    assert_eq!(status["pid"], first["pid"]);
}

#[test]
fn test_fr_m7_install_without_port_keeps_one_port() {
    let s = Svc::new("svc-port");
    let line = s.install(&[]);
    let port = line["url"].as_str().unwrap().rsplit(':').next().unwrap().to_string();
    assert_ne!(port, "0");
    let unit = std::fs::read_to_string(s.unit()).unwrap();
    assert!(unit.contains(&format!("{port}<")) || unit.contains(&format!("\"{port}\"")), "{port} not in:\n{unit}");
}

#[test]
fn test_fr_m7_token_file_is_0600() {
    let s = Svc::new("svc-token");
    let line = s.install(&[]);
    let path = s.home.join("manager-token");
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    assert_eq!(std::fs::read_to_string(&path).unwrap().trim(), line["token"].as_str().unwrap());
}

#[test]
fn test_fr_m7_uninstall_removes_the_registration_and_keeps_the_token() {
    let s = Svc::new("svc-uninstall");
    let line = s.install(&[]);
    let url = line["url"].as_str().unwrap().to_string();
    let out = s.run(&["uninstall"]);
    assert!(out.status.success(), "{}", text(&out.stderr));
    assert!(!s.unit().exists());
    assert!(s.home.join("manager-token").exists());
    wait_until("the manager to stop", std::time::Duration::from_secs(10), || !health(&url));
    let status: Value = serde_json::from_slice(&s.run(&["status"]).stdout).unwrap();
    assert_eq!(status["installed"], false);
    assert_eq!(status["running"], false);
    // Installing again keeps the token.
    let back = s.install(&[]);
    assert_eq!(back["token"], line["token"]);
    s.run(&["uninstall"]);
}

#[test]
fn test_fr_m7_token_file_sets_the_master_token() {
    let home = scratch_home("svc-token-file");
    let file = home.join("tok");
    std::fs::write(&file, "from-the-file\nignored\n").unwrap();
    let mut child = cli(&home)
        .env("DARKPYONIX_MANAGER_TOKEN", "from-the-env")
        .args(["manager", "--dedicated", "--port", "0", "--token-file", file.to_str().unwrap()])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut line = String::new();
    std::io::BufRead::read_line(&mut std::io::BufReader::new(child.stdout.take().unwrap()), &mut line).unwrap();
    let _ = child.kill();
    let _ = child.wait();
    let v: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(v["token"], "from-the-file");

    std::fs::write(&file, "\n").unwrap();
    let out = cli(&home).args(["manager", "--dedicated", "--port", "0", "--token-file", file.to_str().unwrap()]).output().unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert!(text(&out.stderr).contains("holds no token"), "{}", text(&out.stderr));
}
