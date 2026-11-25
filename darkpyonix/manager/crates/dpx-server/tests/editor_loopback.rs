//! SPEC FR-M9, Windows serving: the editor server listens on 127.0.0.1 with a connection token
//! read from a user-only file; the token is in no command line, and the manager's proxy adds it
//! as the server's `vscode-tkn` cookie. Runs on every OS with `editor_loopback` on (the default
//! on Windows); `tests/fake_editor.py` stands in for the VS Code server.

mod common;

use std::path::{Path, PathBuf};
use std::time::Duration;

use common::*;
use dpx_server::editor::{self, EditorConfig, OseInstall};
use dpx_server::ServerConfig;
use serde_json::Value;

fn python() -> &'static str {
    if cfg!(windows) {
        "python"
    } else {
        "python3"
    }
}

/// A home with the fake editor installed as OSE, started at the manager's start.
fn home_with_ose(scratch: &Path) -> PathBuf {
    let home = scratch.join("home");
    let dir = home.join("editor").join("ose").join("ose-vtest").join("dpx-ose-1.0.0-test");
    std::fs::create_dir_all(dir.join("bin")).unwrap();
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests").join("fake_editor.py");
    let launcher = editor::ose_launcher(&dir);
    if cfg!(windows) {
        std::fs::write(&launcher, format!("@{} \"{}\" %*\r\n", python(), script.display())).unwrap();
    } else {
        std::fs::write(&launcher, format!("#!/bin/sh\nexec {} '{}' \"$@\"\n", python(), script.display())).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }
    let cfg = EditorConfig {
        autostart: true,
        ose: Some(OseInstall {
            release: "ose-vtest".into(),
            dir,
            commit: "c0ffee".into(),
            version: "1.0.0".into(),
        }),
        ..EditorConfig::default()
    };
    cfg.save(&home).unwrap();
    home
}

struct Editor {
    s: TestServer,
    home: PathBuf,
}

impl Editor {
    async fn start() -> Self {
        let scratch = scratch("editor-lb");
        let home = home_with_ose(&scratch);
        let mut cfg = ServerConfig::dedicated(&home, "127.0.0.1", 0);
        cfg.editor_loopback = true;
        let s = TestServer::start_cfg(cfg, FakeBackend::new(), scratch).await;
        let e = Self { s, home };
        for _ in 0..300 {
            let r = e.s.admin().get("/api/editor").await;
            if r.body["runtimes"][0]["state"] == "running" {
                return e;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("the editor never ran: {}", e.s.admin().get("/api/editor").await.text);
    }

    fn data(&self) -> PathBuf {
        editor::data_dir(&self.home, "ose")
    }

    fn lines(&self, name: &str) -> Vec<Value> {
        std::fs::read_to_string(self.data().join(name))
            .unwrap_or_default()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    fn argv(&self) -> Vec<String> {
        serde_json::from_value(self.lines("starts.jsonl").last().unwrap().clone()).unwrap()
    }

    fn token(&self) -> String {
        std::fs::read_to_string(editor::token_path(&self.home, "ose")).unwrap().trim().to_string()
    }

    fn flag(&self, name: &str) -> Option<String> {
        let a = self.argv();
        a.iter().position(|x| x == name).map(|i| a[i + 1].clone())
    }
}

#[tokio::test]
async fn test_fr_m9_windows_editor_listens_on_loopback_with_a_token_file() {
    let e = Editor::start().await;
    let argv = e.argv();
    assert_eq!(e.flag("--host").as_deref(), Some("127.0.0.1"), "{argv:?}");
    let port: u16 = e.flag("--port").unwrap().parse().unwrap();
    assert_ne!(port, 0);
    assert_eq!(
        e.flag("--connection-token-file").map(PathBuf::from),
        Some(editor::token_path(&e.home, "ose")),
        "{argv:?}"
    );
    for absent in ["--socket-path", "--without-connection-token"] {
        assert!(!argv.iter().any(|a| a == absent), "{absent} in {argv:?}");
    }
    assert_eq!(e.token().len(), 64);
    // Straight to the port without the token: the server answers nothing useful.
    let direct = reqwest::get(format!("http://127.0.0.1:{port}/ide/ose/")).await.unwrap();
    assert_eq!(direct.status().as_u16(), 403);
}

#[tokio::test]
async fn test_fr_m9_windows_proxy_sends_the_token_cookie() {
    let e = Editor::start().await;
    let r = e
        .s
        .admin()
        .req("GET", "/ide/ose/workbench")
        .header("cookie", "vscode-tkn=from-the-browser; theme=dark")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
    assert!(
        !r.headers().get_all("set-cookie").iter().any(|v| v.to_str().unwrap_or("").contains("vscode-tkn")),
        "the token reached the browser"
    );
    let seen = e.lines("requests.jsonl");
    let cookie = seen.last().unwrap()["headers"]["cookie"].as_str().unwrap().to_string();
    assert!(cookie.contains(&format!("vscode-tkn={}", e.token())), "{cookie}");
    assert!(!cookie.contains("from-the-browser"), "{cookie}");
    assert!(cookie.contains("theme=dark"), "{cookie}");
    assert!(seen.last().unwrap()["headers"].get("authorization").is_none());
}

#[tokio::test]
async fn test_fr_m9_windows_token_is_not_in_the_command_line() {
    let e = Editor::start().await;
    let token = e.token();
    assert!(!e.argv().iter().any(|a| a.contains(&token)), "the token is an argument");
    let pid = std::fs::read_to_string(e.data().join("pid")).unwrap();
    let line = if cfg!(windows) {
        let q = format!("(Get-CimInstance Win32_Process -Filter \"ProcessId={}\").CommandLine", pid.trim());
        std::process::Command::new("powershell").args(["-NoProfile", "-Command", &q]).output().unwrap()
    } else {
        std::process::Command::new("ps").args(["-o", "args=", "-p", pid.trim()]).output().unwrap()
    };
    let line = String::from_utf8_lossy(&line.stdout).to_string();
    assert!(line.contains("--connection-token-file"), "not the server's command line: {line}");
    assert!(!line.contains(&token), "the token is in the command line: {line}");
}

#[tokio::test]
async fn test_fr_m9_windows_token_file_is_user_only() {
    let e = Editor::start().await;
    let file = editor::token_path(&e.home, "ose");
    #[cfg(windows)]
    {
        let out = std::process::Command::new("icacls").arg(&file).output().unwrap();
        let text = String::from_utf8_lossy(&out.stdout).to_string();
        let user = std::env::var("USERNAME").unwrap().to_lowercase();
        // One line per entry after the path; the summary line comes last.
        let entries: Vec<&str> = text
            .lines()
            .map(|l| l.trim())
            .filter(|l| l.contains(":(") )
            .collect();
        assert!(!entries.is_empty(), "{text}");
        for entry in &entries {
            let who = entry.rsplit_once(":(").unwrap().0;
            let who = who.rsplit(' ').next().unwrap_or(who).to_lowercase();
            assert!(who.ends_with(&format!("\\{user}")) || who == user, "a second entry, {who}: {text}");
        }
        assert!(!text.contains("(I)"), "inherited entries remain: {text}");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
