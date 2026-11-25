//! SPEC FR-M9, FR-M10: a dedicated manager starts the installed editor server on a 0700 socket
//! without a token, restarts it, ends it with itself, and serves it under `/ide/<runtime>/` for
//! the master token or a launch ticket. `tests/fake_editor.py` stands in for the VS Code server.
#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::time::Duration;

use common::*;
use dpx_server::editor::{self, EditorConfig, OseInstall};
use dpx_server::{Mode, ServerConfig};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn fake_script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fake_editor.py")
}

/// A home with a fake OSE installed (and `vsc` too when asked).
fn home_with_ose(scratch: &Path, autostart: bool) -> PathBuf {
    let home = scratch.join("home");
    let dir = home
        .join("editor")
        .join("ose")
        .join("ose-vtest")
        .join("dpx-ose-1.0.0-test");
    std::fs::create_dir_all(dir.join("bin")).unwrap();
    let launcher = editor::ose_launcher(&dir);
    std::fs::write(
        &launcher,
        format!(
            "#!/bin/sh\nexec python3 '{}' \"$@\"\n",
            fake_script().display()
        ),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o755)).unwrap();
    let cfg = EditorConfig {
        autostart,
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
    async fn start(mode: Mode, autostart: bool, before: impl FnOnce(&Path)) -> Self {
        let scratch = scratch("editor");
        let home = home_with_ose(&scratch, autostart);
        before(&home);
        let cfg = match mode {
            Mode::Ephemeral => ServerConfig::ephemeral(&home),
            Mode::Dedicated => ServerConfig::dedicated(&home, "127.0.0.1", 0),
        };
        let s = TestServer::start_cfg(cfg, FakeBackend::new(), scratch).await;
        Self { s, home }
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

    async fn state(&self) -> String {
        let r = self.s.admin().get("/api/editor").await;
        r.body["runtimes"][0]["state"]
            .as_str()
            .unwrap_or("")
            .to_string()
    }

    async fn wait_state(&self, want: &str) {
        for _ in 0..200 {
            if self.state().await == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "editor never became {want}: {}",
            self.s.admin().get("/api/editor").await.text
        );
    }

    fn pid(&self) -> i32 {
        std::fs::read_to_string(self.data().join("pid"))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }

    fn client(&self) -> reqwest::Client {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap()
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.s.url)
    }
}

impl Drop for Editor {
    fn drop(&mut self) {
        let sock = editor::socket_path(&self.home, "ose");
        if !sock.starts_with(&self.home) {
            if let Some(dir) = sock.parent() {
                let _ = std::fs::remove_dir_all(dir);
            }
        }
    }
}

fn alive(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

#[tokio::test]
async fn test_fr_m9_dedicated_manager_starts_the_editor_with_its_socket_and_no_token() {
    let e = Editor::start(Mode::Dedicated, true, |_| {}).await;
    e.wait_state("running").await;
    let starts = e.lines("starts.jsonl");
    assert_eq!(starts.len(), 1, "{starts:?}");
    let argv: Vec<String> = serde_json::from_value(starts[0].clone()).unwrap();
    let after = |flag: &str| {
        argv.iter()
            .position(|a| a == flag)
            .map(|i| argv[i + 1].clone())
    };
    assert!(
        argv.contains(&"--without-connection-token".into()),
        "{argv:?}"
    );
    assert!(
        argv.contains(&"--accept-server-license-terms".into()),
        "{argv:?}"
    );
    assert_eq!(after("--server-base-path").as_deref(), Some("/ide/ose/"));
    assert_eq!(
        after("--server-data-dir").map(PathBuf::from),
        Some(e.data())
    );
    assert_eq!(
        after("--socket-path").map(PathBuf::from),
        Some(editor::socket_path(&e.home, "ose"))
    );
    assert!(
        !argv
            .iter()
            .any(|a| a == "--port" || a == "--host" || a.starts_with("--connection-token")),
        "{argv:?}"
    );
}

#[tokio::test]
async fn test_fr_m9_socket_directory_is_0700() {
    use std::os::unix::fs::PermissionsExt;
    let e = Editor::start(Mode::Dedicated, true, |_| {}).await;
    e.wait_state("running").await;
    let dir = editor::socket_path(&e.home, "ose")
        .parent()
        .unwrap()
        .to_path_buf();
    assert_eq!(
        std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
        0o700
    );
}

#[tokio::test]
async fn test_fr_m9_ephemeral_manager_starts_no_editor() {
    let e = Editor::start(Mode::Ephemeral, true, |_| {}).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(e.lines("starts.jsonl").is_empty());
    assert_eq!(e.state().await, "stopped");
    e.s.admin()
        .post("/api/editor/ose/launch", json!({}))
        .await
        .error(409, "conflict");
}

#[tokio::test]
async fn test_fr_m9_a_crashed_editor_is_restarted() {
    let e = Editor::start(Mode::Dedicated, true, |home| {
        let data = editor::data_dir(home, "ose");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("crash-once"), "").unwrap();
    })
    .await;
    e.wait_state("running").await;
    assert_eq!(e.lines("starts.jsonl").len(), 2, "one crash, one restart");
}

#[tokio::test]
async fn test_fr_m9_the_editor_ends_with_the_manager() {
    let mut e = Editor::start(Mode::Dedicated, true, |_| {}).await;
    e.wait_state("running").await;
    let pid = e.pid();
    assert!(alive(pid));
    let handle = e.s.handle.take().unwrap();
    handle.shutdown();
    handle.wait().await;
    for _ in 0..100 {
        if !alive(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("editor server {pid} outlived the manager");
}

#[tokio::test]
async fn test_fr_m9_unauthenticated_request_never_reaches_the_editor() {
    let e = Editor::start(Mode::Dedicated, true, |_| {}).await;
    e.wait_state("running").await;
    let c = e.client();
    for path in ["/ide/ose/", "/ide/ose/static/x.js", "/ide/ose"] {
        assert_eq!(
            c.get(e.url(path)).send().await.unwrap().status(),
            401,
            "{path}"
        );
    }
    let r = c
        .get(e.url("/ide/ose/"))
        .bearer_auth("wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    let r = c
        .get(e.url("/ide/ose/?dpx-ticket=forged"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    let r = c
        .get(e.url("/ide/ose/"))
        .header("cookie", "dpx-editor=forged")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    assert!(e.lines("requests.jsonl").is_empty());
}

#[tokio::test]
async fn test_fr_m9_a_share_token_does_not_open_the_editor() {
    let e = Editor::start(Mode::Dedicated, true, |_| {}).await;
    e.wait_state("running").await;
    let (kid, _) = e.s.kernel("a.py");
    let share =
        e.s.admin()
            .post(
                &format!("/api/kernels/{kid}/shares"),
                json!({"capabilities": ["read", "execute", "edit"]}),
            )
            .await;
    let token = share.body["token"].as_str().unwrap().to_string();
    let r = e
        .client()
        .get(e.url("/ide/ose/"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    Api::new(&e.s.url, Some(&token))
        .get("/api/editor")
        .await
        .error(403, "forbidden");
    assert!(e.lines("requests.jsonl").is_empty());
}

#[tokio::test]
async fn test_fr_m9_bearer_request_and_websocket_are_proxied() {
    let e = Editor::start(Mode::Dedicated, true, |_| {}).await;
    e.wait_state("running").await;
    let r = e
        .client()
        .get(e.url("/ide/ose/static/x.js?v=1"))
        .bearer_auth(&e.s.token)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: Value = r.json().await.unwrap();
    assert_eq!(
        v["path"], "/ide/ose/static/x.js?v=1",
        "the base path is kept"
    );
    assert!(
        v["headers"].get("authorization").is_none(),
        "the master token never reaches the editor: {v}"
    );

    let addr = e.s.url.trim_start_matches("http://").to_string();
    let mut tcp = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let req = format!(
        "GET /ide/ose/socket HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {}\r\nConnection: Upgrade\r\n\
         Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
        e.s.token
    );
    tcp.write_all(req.as_bytes()).await.unwrap();
    let mut head = Vec::new();
    let mut b = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        tcp.read_exact(&mut b).await.unwrap();
        head.push(b[0]);
    }
    assert!(
        String::from_utf8_lossy(&head).starts_with("HTTP/1.1 101"),
        "{}",
        String::from_utf8_lossy(&head)
    );
    tcp.write_all(b"ping-bytes").await.unwrap();
    let mut echo = [0u8; 10];
    tokio::time::timeout(Duration::from_secs(5), tcp.read_exact(&mut echo))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&echo, b"ping-bytes");
}

#[tokio::test]
async fn test_fr_m9_ticket_query_redirects_once_and_sets_the_cookie() {
    let e = Editor::start(Mode::Dedicated, true, |_| {}).await;
    let l = e.s.admin().post("/api/editor/ose/launch", json!({})).await;
    assert_eq!(l.status, 200, "{}", l.text);
    assert_eq!(l.body["path"], "/ide/ose/");
    assert_eq!(l.body["commit"], "c0ffee");
    let ticket = l.body["ticket"].as_str().unwrap().to_string();
    let c = e.client();
    let r = c
        .get(e.url(&format!(
            "/ide/ose/?folder=%2Fwork&dpx-ticket={ticket}&egw=1"
        )))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 302);
    assert_eq!(r.headers()["location"], "/ide/ose/?folder=%2Fwork&egw=1");
    let cookie = r.headers()["set-cookie"].to_str().unwrap().to_string();
    assert!(
        cookie.starts_with(&format!("dpx-editor={ticket};")),
        "{cookie}"
    );
    for part in ["HttpOnly", "SameSite=Strict", "Path=/ide/ose/"] {
        assert!(cookie.contains(part), "{cookie}");
    }
    assert!(
        e.lines("requests.jsonl").is_empty(),
        "the redirect does not reach the editor"
    );
    let r = c
        .get(e.url("/ide/ose/?folder=%2Fwork"))
        .header("cookie", format!("theme=dark; dpx-editor={ticket}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: Value = r.json().await.unwrap();
    assert_eq!(
        v["headers"]["cookie"], "theme=dark",
        "the ticket never reaches the editor: {v}"
    );
}

#[tokio::test]
async fn test_fr_m9_stop_revokes_the_tickets() {
    let e = Editor::start(Mode::Dedicated, true, |_| {}).await;
    let ticket =
        e.s.admin()
            .post("/api/editor/ose/launch", json!({}))
            .await
            .body["ticket"]
            .as_str()
            .unwrap()
            .to_string();
    let pid = e.pid();
    let r = e.s.admin().post("/api/editor/ose/stop", json!({})).await;
    assert_eq!(r.status, 204);
    assert_eq!(e.state().await, "stopped");
    assert!(!alive(pid));
    let r = e
        .client()
        .get(e.url("/ide/ose/"))
        .header("cookie", format!("dpx-editor={ticket}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
}

#[tokio::test]
async fn test_fr_m10_editor_lists_installed_runtimes_without_the_socket() {
    let e = Editor::start(Mode::Dedicated, true, |_| {}).await;
    e.wait_state("running").await;
    let r = e.s.admin().get("/api/editor").await;
    assert_eq!(r.status, 200);
    let rts = r.body["runtimes"].as_array().unwrap();
    assert_eq!(rts.len(), 1, "only ose is installed: {}", r.text);
    assert_eq!(rts[0]["runtime"], "ose");
    assert_eq!(rts[0]["version"], "1.0.0");
    assert_eq!(rts[0]["commit"], "c0ffee");
    assert_eq!(rts[0]["path"], "/ide/ose/");
    assert_eq!(r.body["default_runtime"], "ose");
    assert!(!r.text.contains(".sock"), "{}", r.text);
}

#[tokio::test]
async fn test_fr_m10_launch_is_idempotent_and_returns_a_fresh_ticket() {
    let e = Editor::start(Mode::Dedicated, false, |_| {}).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(e.lines("starts.jsonl").is_empty(), "autostart off");
    let a = e.s.admin().post("/api/editor/ose/launch", json!({})).await;
    let b = e.s.admin().post("/api/editor/ose/launch", json!({})).await;
    assert_eq!((a.status, b.status), (200, 200));
    assert_ne!(a.body["ticket"], b.body["ticket"]);
    assert_eq!(e.lines("starts.jsonl").len(), 1);
}

#[tokio::test]
async fn test_fr_m10_launch_of_an_uninstalled_runtime_is_409() {
    let e = Editor::start(Mode::Dedicated, false, |_| {}).await;
    e.s.admin()
        .post("/api/editor/vsc/launch", json!({}))
        .await
        .error(409, "conflict");
    e.s.admin()
        .post("/api/editor/script/launch", json!({}))
        .await
        .error(409, "conflict");
    e.s.admin()
        .post("/api/editor/nope/launch", json!({}))
        .await
        .error(404, "not_found");
}

#[tokio::test]
async fn test_fr_m10_stop_ends_the_editor_until_the_next_launch() {
    let e = Editor::start(Mode::Dedicated, true, |_| {}).await;
    e.wait_state("running").await;
    e.s.admin().post("/api/editor/ose/stop", json!({})).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(e.state().await, "stopped");
    assert_eq!(e.lines("starts.jsonl").len(), 1);
    assert_eq!(
        e.s.admin()
            .post("/api/editor/ose/launch", json!({}))
            .await
            .status,
        200
    );
    assert_eq!(e.state().await, "running");
}

#[tokio::test]
async fn test_fr_m10_extensions_use_the_runtimes_data_dir() {
    let e = Editor::start(Mode::Dedicated, false, |_| {}).await;
    let r =
        e.s.admin()
            .post(
                "/api/editor/ose/extensions",
                json!({"id": "redhat.vscode-yaml", "version": "1.2.3"}),
            )
            .await;
    assert_eq!(r.status, 200, "{}", r.text);
    let cli = e.lines("cli.jsonl");
    let argv: Vec<String> = serde_json::from_value(cli[0].clone()).unwrap();
    assert!(
        argv.windows(2)
            .any(|w| w[0] == "--install-extension" && w[1] == "redhat.vscode-yaml@1.2.3"),
        "{argv:?}"
    );
    assert!(
        argv.windows(2)
            .any(|w| w[0] == "--server-data-dir" && Path::new(&w[1]) == e.data()),
        "{argv:?}"
    );
    let f =
        e.s.admin()
            .post("/api/editor/ose/extensions", json!({"id": "fail.this"}))
            .await;
    let err = f.error(422, "extension_failed");
    assert!(
        err["data"]["output"]
            .as_str()
            .unwrap()
            .contains("not found"),
        "{err}"
    );
    e.s.admin()
        .post("/api/editor/ose/extensions", json!({"id": "../../x.vsix"}))
        .await
        .error(400, "bad_request");
    e.s.admin()
        .post("/api/editor/vsc/extensions", json!({"id": "a.b"}))
        .await
        .error(409, "conflict");
}

#[tokio::test]
async fn test_fr_m10_presets_install_only_missing_extensions() {
    let e = Editor::start(Mode::Dedicated, false, |home| {
        let data = editor::data_dir(home, "ose");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("installed.txt"), "redhat.vscode-yaml@1.0.0\n").unwrap();
    })
    .await;
    let r = e
        .s
        .admin()
        .call(
            "PUT",
            "/api/editor/ose/presets",
            Some(json!({"extensions": [{"id": "redhat.vscode-yaml"}, {"id": "ms-python.python"}]})),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text);
    let installed = std::fs::read_to_string(e.data().join("installed.txt")).unwrap();
    assert_eq!(
        installed.lines().collect::<Vec<_>>(),
        ["redhat.vscode-yaml@1.0.0", "ms-python.python"]
    );
}

#[tokio::test]
async fn test_fr_m10_presets_put_the_theme_in_configuration_defaults() {
    let e = Editor::start(Mode::Dedicated, true, |_| {}).await;
    e.wait_state("running").await;
    let body = json!({"theme": "DarkPyonix Phoenix", "settings": {"editor.fontSize": 13}});
    let r =
        e.s.admin()
            .call("PUT", "/api/editor/ose/presets", Some(body))
            .await;
    assert_eq!(r.status, 200, "{}", r.text);
    let manifest: Value = serde_json::from_str(
        &std::fs::read_to_string(
            editor::presets_extension_dir(&e.home, "ose").join("package.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let d = &manifest["contributes"]["configurationDefaults"];
    assert_eq!(d["workbench.colorTheme"], "DarkPyonix Phoenix");
    assert_eq!(d["editor.fontSize"], 13);
    e.wait_state("running").await;
    assert_eq!(
        e.lines("starts.jsonl").len(),
        2,
        "changed defaults restart the server"
    );
}

#[tokio::test]
async fn test_fr_m10_presets_extension_is_installed_through_the_cli() {
    let e = Editor::start(Mode::Dedicated, false, |_| {}).await;
    let put = |body: Value| {
        let e = &e;
        async move {
            let r = e.s.admin().call("PUT", "/api/editor/ose/presets", Some(body)).await;
            assert_eq!(r.status, 200, "{}", r.text);
        }
    };
    let vsix_installs = || {
        e.lines("cli.jsonl")
            .into_iter()
            .map(|v| serde_json::from_value::<Vec<String>>(v).unwrap())
            .filter(|a| {
                a.windows(2).any(|w| w[0] == "--install-extension" && w[1].ends_with(".vsix"))
                    && a.iter().any(|x| x == "--force")
            })
            .count()
    };
    put(json!({"theme": "Default Light Modern"})).await;
    assert_eq!(vsix_installs(), 1);
    let installed = std::fs::read_to_string(e.data().join("installed.txt")).unwrap();
    assert!(installed.contains("darkpyonix.presets@1.0.0"), "{installed}");
    // The same presets again: nothing to install.
    put(json!({"theme": "Default Light Modern"})).await;
    assert_eq!(vsix_installs(), 1);
    // No defaults: the extension is uninstalled through the CLI.
    put(json!({})).await;
    let installed = std::fs::read_to_string(e.data().join("installed.txt")).unwrap();
    assert!(!installed.contains("darkpyonix.presets"), "{installed}");
    assert!(!editor::presets_extension_dir(&e.home, "ose").exists());
}

#[tokio::test]
async fn test_fr_m10_presets_are_listed_as_applied() {
    let e = Editor::start(Mode::Dedicated, false, |_| {}).await;
    let body = json!({"extensions": [{"id": "redhat.vscode-yaml", "version": "1.2.3"}], "theme": "Default Light Modern"});
    assert_eq!(
        e.s.admin()
            .call("PUT", "/api/editor/ose/presets", Some(body.clone()))
            .await
            .status,
        200
    );
    let r = e.s.admin().get("/api/editor").await;
    let p = &r.body["runtimes"][0]["presets"];
    assert_eq!(p["extensions"], body["extensions"]);
    assert_eq!(p["theme"], "Default Light Modern");
}

#[tokio::test]
async fn test_fr_m10_extension_delete_uninstalls_from_the_data_dir() {
    let e = Editor::start(Mode::Dedicated, false, |_| {}).await;
    for id in ["redhat.vscode-yaml", "stuck.ext"] {
        let r = e.s.admin().post("/api/editor/ose/extensions", json!({"id": id})).await;
        assert_eq!(r.status, 200, "{}", r.text);
    }
    let r = e.s.admin().delete("/api/editor/ose/extensions/redhat.vscode-yaml").await;
    assert_eq!(r.status, 204, "{}", r.text);
    let installed = std::fs::read_to_string(e.data().join("installed.txt")).unwrap();
    assert!(!installed.contains("redhat.vscode-yaml"), "{installed}");
    let uninstall: Vec<Vec<String>> = e
        .lines("cli.jsonl")
        .into_iter()
        .map(|v| serde_json::from_value(v).unwrap())
        .filter(|a: &Vec<String>| a.iter().any(|x| x == "--uninstall-extension"))
        .collect();
    assert_eq!(uninstall.len(), 1, "{uninstall:?}");
    assert!(
        uninstall[0]
            .windows(2)
            .any(|w| w[0] == "--server-data-dir" && Path::new(&w[1]) == e.data()),
        "{uninstall:?}"
    );
    // Absent: nothing to do, no CLI call.
    let r = e.s.admin().delete("/api/editor/ose/extensions/redhat.vscode-yaml").await;
    assert_eq!(r.status, 204, "{}", r.text);
    assert_eq!(
        e.lines("cli.jsonl")
            .iter()
            .filter(|v| v.to_string().contains("--uninstall-extension"))
            .count(),
        1
    );
    let err = e
        .s
        .admin()
        .delete("/api/editor/ose/extensions/stuck.ext")
        .await
        .error(422, "extension_failed");
    assert!(err["data"]["output"].as_str().unwrap().contains("in use"), "{err}");
    e.s.admin()
        .delete("/api/editor/ose/extensions/..%2Fx")
        .await
        .error(400, "bad_request");
    e.s.admin()
        .delete("/api/editor/vsc/extensions/a.b")
        .await
        .error(409, "conflict");
}

#[tokio::test]
async fn test_fr_m10_extension_delete_refuses_the_presets_extension() {
    let e = Editor::start(Mode::Dedicated, false, |_| {}).await;
    let r = e
        .s
        .admin()
        .call(
            "PUT",
            "/api/editor/ose/presets",
            Some(json!({"theme": "Default Dark Modern"})),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text);
    e.s.admin()
        .delete("/api/editor/ose/extensions/darkpyonix.presets")
        .await
        .error(409, "conflict");
    assert!(e.data().join("extensions/darkpyonix.presets-1.0.0/package.json").is_file());
}

#[tokio::test]
async fn test_fr_m10_ose_turns_off_extension_signature_checks() {
    let e = Editor::start(Mode::Dedicated, true, |home| {
        let user = editor::data_dir(home, "ose").join("data/User");
        std::fs::create_dir_all(&user).unwrap();
        std::fs::write(user.join("settings.json"), r#"{"editor.tabSize": 2}"#).unwrap();
    })
    .await;
    e.wait_state("running").await;
    let read = |scope: &str| -> Value {
        serde_json::from_str(&std::fs::read_to_string(e.data().join("data").join(scope).join("settings.json")).unwrap())
            .unwrap()
    };
    // The running server reads the Machine settings.
    assert_eq!(read("Machine")["extensions.verifySignature"], false);
    // The CLI reads the User settings; the user's own keys stay.
    let r = e.s.admin().post("/api/editor/ose/extensions", json!({"id": "redhat.vscode-yaml"})).await;
    assert_eq!(r.status, 200, "{}", r.text);
    let user = read("User");
    assert_eq!(user["extensions.verifySignature"], false);
    assert_eq!(user["editor.tabSize"], 2);
}
