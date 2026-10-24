//! Every operation of manager.openapi.yaml against a fake backend (SPEC FR-M1, FR-M2, FR-X3,
//! FR-X4, FR-A2, FR-A3). Every response is also checked against the statuses the YAML
//! documents for its operation (NFR-M3, see common::Contract::check).

mod common;

use common::*;
use dpx_core::DpxError;
use dpx_server::Mode;
use serde_json::json;

#[tokio::test]
async fn test_fr_m1_health_and_manager_info() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let h = s.anon().get("/health").await;
    assert_eq!(h.status, 200);
    assert_eq!(h.body, json!({"status": "ok", "version": "0.1.0"}));
    let info = s.admin().get("/api/manager").await;
    assert_eq!(info.status, 200);
    assert_eq!(info.body["mode"], "ephemeral");
    assert_eq!(info.body["capabilities"], json!(["read", "history", "execute", "edit", "manage"]));
    assert_eq!(info.body["idle_timeout"], 120);
    assert_eq!(info.body["pid"], std::process::id());
    for k in ["version", "started_at", "host"] {
        assert!(info.body[k].is_string(), "{k}");
    }
}

#[tokio::test]
async fn test_fr_m1_docs_page_is_served_outside_the_schema() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let anon = s.anon();
    let page = anon.get("/docs/").await;
    assert_eq!(page.status, 200);
    assert!(page.text.to_lowercase().contains("swagger"));
    let no_redirect = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let r = no_redirect.get(format!("{}/docs", s.url)).send().await.unwrap();
    assert!(matches!(r.status().as_u16(), 302 | 307));
    let spec = anon.get("/docs/manager.openapi.yaml").await;
    assert_eq!(spec.status, 200);
    let on_disk = std::fs::read_to_string(repo_root().join("docs/api/manager.openapi.yaml")).unwrap();
    assert_eq!(spec.text, on_disk);
    assert_eq!(anon.get("/docs/hub.openapi.yaml").await.status, 200);
    anon.get("/docs/CLAUDE.md").await.error(404, "not_found");
    for p in ["/openapi.json", "/redoc", "/api/not-in-the-contract"] {
        s.admin().get(p).await.error(404, "not_found");
    }
}

#[tokio::test]
async fn test_nfr_v1_versioned_manager_path_is_not_served() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, _path) = s.kernel("train.py");
    let a = s.admin();
    assert_eq!(a.get("/api/manager").await.status, 200);
    for p in [
        "/api/v1/manager".to_string(),
        "/api/v1/kernels".to_string(),
        format!("/api/v1/kernels/{kid}/events"),
    ] {
        a.get(&p).await.error(404, "not_found");
    }
}

#[tokio::test]
async fn test_fr_m1_list_and_get_kernels() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, path) = s.kernel("train.py");
    let a = s.admin();
    let list = a.get("/api/kernels?refresh=true").await;
    assert_eq!(list.status, 200);
    let ks = list.body["kernels"].as_array().unwrap();
    assert_eq!(ks.len(), 1);
    assert_eq!(ks[0]["kernel_id"], kid);
    assert!(ks[0].get("port").is_none() && ks[0].get("control").is_none());
    assert_eq!(ks[0]["run_id"], json!(null));
    let runs_dir = std::path::Path::new(&path).parent().unwrap().join("__runs__").join("train.py");
    assert_eq!(ks[0]["runs_dir"], runs_dir.to_string_lossy().as_ref());
    a.get("/api/kernels?refresh=maybe").await.error(400, "bad_request");

    let k = a.get(&format!("/api/kernels/{kid}")).await;
    assert_eq!(k.status, 200);
    assert_eq!(k.body["status"], "idle");
    assert_eq!(k.body["queue"], json!([]));
    assert_eq!(k.body["kernel_version"], "0.1.0");
    a.get("/api/kernels/k_00000000000000000000").await.error(404, "not_found");
    a.get("/api/kernels/not-a-kernel").await.error(404, "not_found");

    s.backend.fail_on("get", DpxError::new("kernel_unreachable", "no route"));
    a.get(&format!("/api/kernels/{kid}")).await.error(502, "kernel_unreachable");
    s.backend.fail_on("get", DpxError::new("shutting_down", "bye"));
    a.get(&format!("/api/kernels/{kid}")).await.error(502, "shutting_down");
    s.backend.fail_on("get", DpxError::new("weird", "?"));
    let e = a.get(&format!("/api/kernels/{kid}")).await.error(500, "internal");
    assert_eq!(e["data"]["cause"], "weird");
    s.backend.clear_failures();
    s.backend.fail_on("list", DpxError::new("internal", "boom"));
    a.get("/api/kernels").await.error(500, "internal");
}

#[tokio::test]
async fn test_fr_m2_start_kernel_is_idempotent() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let path = s.notebook("train.py");
    let a = s.admin();
    let first = a.post("/api/kernels", json!({"path": path, "env": {"A": "1"}})).await;
    assert_eq!(first.status, 201, "{}", first.text);
    assert_eq!(first.body["kernel_id"], kernel_id_for(&path));
    let second = a.post("/api/kernels", json!({"path": path})).await;
    assert_eq!(second.status, 200);
    assert_eq!(second.body["kernel_id"], first.body["kernel_id"]);
    assert_eq!(s.backend.launches.lock().unwrap().len(), 1);
    assert_eq!(s.backend.launches.lock().unwrap()[0].env.get("A").map(String::as_str), Some("1"));

    // A missing file is a bad request for this operation (the contract has no 404 here).
    a.post("/api/kernels", json!({"path": format!("{path}.missing.py")})).await.error(400, "bad_request");
    s.backend.fail_on("ensure", DpxError::new("start_timeout", "no announce").with_data(json!({"kernel_id": "k_x"})));
    let e = a.post("/api/kernels", json!({"path": path})).await.error(504, "start_timeout");
    assert_eq!(e["data"]["kernel_id"], "k_x");
    s.backend.fail_on("ensure", DpxError::new("kernel_unreachable", "?"));
    // 502 is not documented for startKernel, so it becomes 500 with the cause kept.
    let e = a.post("/api/kernels", json!({"path": path})).await.error(500, "internal");
    assert_eq!(e["data"]["cause"], "kernel_unreachable");
}

#[tokio::test]
async fn test_fr_m1_validation_errors_are_400_bad_request() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, _) = s.kernel("train.py");
    let a = s.admin();
    let e = a.post("/api/kernels", json!({"python": "x"})).await.error(400, "bad_request");
    assert!(!e["data"]["errors"].as_array().unwrap().is_empty());
    a.raw("POST", "/api/kernels", b"{not json").await.error(400, "bad_request");
    a.raw("POST", "/api/kernels", b"").await.error(400, "bad_request");
    a.post("/api/kernels", json!({"path": 3})).await.error(400, "bad_request");
    a.post("/api/kernels", json!({"path": "x.py", "env": {"A": 1}})).await.error(400, "bad_request");
    let runs = format!("/api/kernels/{kid}/runs");
    a.post(&runs, json!({"mode": "some"})).await.error(400, "bad_request");
    a.post(&runs, json!({"mode": "cells"})).await.error(400, "bad_request");
    a.post(&runs, json!({"mode": "cells", "cells": []})).await.error(400, "bad_request");
    a.post(&runs, json!({"mode": "cells", "cells": [-1]})).await.error(400, "bad_request");
    a.post(&runs, json!({"mode": "all", "on_busy": "later"})).await.error(400, "bad_request");
    a.post(&runs, json!({"mode": "all", "params": [1]})).await.error(400, "bad_request");
    a.raw("POST", &format!("/api/kernels/{kid}/restart"), b"{bad").await.error(400, "bad_request");
    a.get("/api/documents").await.error(400, "bad_request");
    a.get("/api/documents?path=notes.txt").await.error(400, "bad_request");
    a.get(&format!("/api/kernels/{kid}/runs/latest?format=xml")).await.error(400, "bad_request");
    a.call("DELETE", &format!("/api/kernels/{kid}?force=perhaps"), None).await.error(400, "bad_request");
    a.get(&format!("/api/kernels/{kid}/events?since=-1")).await.error(400, "bad_request");
    // limit is lenient (falls back to the default), as the pytest oracle expects.
    assert_eq!(a.get(&format!("/api/kernels/{kid}/runs?limit=abc")).await.status, 200);
    assert_eq!(a.get(&format!("/api/kernels/{kid}/namespace?limit=0")).await.status, 200);
    assert_eq!(s.backend.last_call(&kid).unwrap().1["limit"], 1);
}

#[tokio::test]
async fn test_fr_x3_busy_run_is_409_with_busy_error() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, _) = s.kernel("train.py");
    let a = s.admin();
    let url = format!("/api/kernels/{kid}/runs");
    let first = a.post(&url, json!({"mode": "all"})).await;
    assert_eq!(first.status, 202);
    assert_eq!(first.body["state"], "running");
    let e = a.post(&url, json!({"mode": "cells", "cells": [1]})).await.error(409, "busy");
    assert_eq!(e["data"]["current"]["run_id"], first.body["run_id"]);
    assert_eq!(e["data"]["current"]["status"], "running");
    assert_eq!(e["data"]["queue_length"], 0);
    let q = a.post(&url, json!({"mode": "all", "on_busy": "queue", "params": {"lr": 0.1}, "source": "x=1"})).await;
    assert_eq!(q.status, 202);
    assert_eq!(q.body["state"], "queued");
    assert_eq!(q.body["position"], 1);
    let (m, mut p) = s.backend.last_call(&kid).unwrap();
    assert_eq!(m, "run");
    // `client` (FR-S6) is checked in collab.rs.
    let client = p.as_object_mut().unwrap().remove("client").expect("run carries the client");
    assert_eq!(client["capabilities"], json!(["read", "history", "execute", "edit", "manage"]));
    assert_eq!(p, json!({"mode": "all", "on_busy": "queue", "params": {"lr": 0.1}, "source": "x=1"}));
    // busy data without `current` is wrapped so the BusyError shape holds.
    s.backend.fail_on("run", DpxError::new("busy", "busy").with_data(json!({"run_id": "r"})));
    let e = a.post(&url, json!({"mode": "all"})).await.error(409, "busy");
    assert_eq!(e["data"]["current"]["run_id"], "r");
    s.backend.fail_on("run", DpxError::new("kernel_unreachable", "lost"));
    a.post(&url, json!({"mode": "all"})).await.error(502, "kernel_unreachable");
    a.post("/api/kernels/k_00000000000000000000/runs", json!({"mode": "all"})).await.error(404, "not_found");
}

#[tokio::test]
async fn test_fr_x4_interrupt_maps_to_kernel_interrupt() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, _) = s.kernel("train.py");
    let a = s.admin();
    let url = format!("/api/kernels/{kid}/interrupt");
    let r = a.call("POST", &url, None).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body, json!({"interrupted": false}));
    let run_id = a.post(&format!("/api/kernels/{kid}/runs"), json!({"mode": "all"})).await.body["run_id"].clone();
    assert_eq!(a.call("POST", &url, None).await.body, json!({"interrupted": true, "run_id": run_id}));
    let summary = a.get(&format!("/api/kernels/{kid}/runs/{}?format=summary", run_id.as_str().unwrap())).await;
    assert_eq!(summary.body["status"], "interrupted");
    let calls = s.backend.method_calls(&kid);
    assert_eq!(calls.iter().filter(|m| *m == "interrupt").count(), 2);
    assert!(!calls.contains(&"shutdown".to_string()) && !calls.contains(&"restart".to_string()));
    a.call("POST", "/api/kernels/k_00000000000000000000/interrupt", None).await.error(404, "not_found");
    s.backend.fail_on("interrupt", DpxError::new("kernel_unreachable", "lost"));
    a.call("POST", &url, None).await.error(502, "kernel_unreachable");
}

#[tokio::test]
async fn test_fr_m1_runs_namespace_document_restart_shutdown() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, path) = s.kernel("train.py");
    let a = s.admin();
    let base = format!("/api/kernels/{kid}");
    let run_id = a.post(&format!("{base}/runs"), json!({"mode": "all"})).await.body["run_id"].as_str().unwrap().to_string();
    let queued = a.post(&format!("{base}/runs"), json!({"mode": "all", "on_busy": "queue"})).await.body["run_id"]
        .as_str()
        .unwrap()
        .to_string();
    let c = a.delete(&format!("{base}/runs/{queued}")).await;
    assert_eq!((c.status, c.body.clone()), (200, json!({"cancelled": true})));
    assert_eq!(a.delete(&format!("{base}/runs/{queued}")).await.body, json!({"cancelled": false}));
    assert_eq!(a.delete(&format!("{base}/runs/current")).await.body, json!({"cancelled": false}));
    assert_eq!(a.delete(&format!("{base}/runs/latest")).await.body, json!({"cancelled": false}));
    a.delete(&format!("{base}/runs/bogus")).await.error(404, "not_found");

    let cur = a.get(&format!("{base}/runs/current")).await;
    assert_eq!(cur.status, 200);
    assert_eq!(cur.body["metadata"]["darkpyonix"]["run_id"], run_id);
    s.backend.finish_run(&kid);
    let runs = a.get(&format!("{base}/runs?limit=1")).await;
    assert_eq!(runs.status, 200);
    assert_eq!(runs.body["runs"].as_array().unwrap().len(), 1);
    assert_eq!(runs.body["runs"][0]["run_id"], run_id);
    let nb = a.get(&format!("{base}/runs/latest")).await;
    assert_eq!(nb.body["nbformat"], 4);
    assert_eq!(nb.body["metadata"]["darkpyonix"]["run_id"], run_id);
    let sum = a.get(&format!("{base}/runs/{run_id}?format=summary")).await;
    assert_eq!(sum.body["status"], "ok");
    assert_eq!(sum.body["duration"], 2.0);
    assert!(sum.body["path"].as_str().unwrap().ends_with(&format!("__runs__/train.py/{run_id}.ipynb")));
    a.get(&format!("{base}/runs/current")).await.error(404, "not_found");
    a.get(&format!("{base}/runs/20990101-000000-0000")).await.error(404, "not_found");
    a.get(&format!("{base}/runs/nope")).await.error(404, "not_found");

    let ns = a.get(&format!("{base}/namespace")).await;
    assert_eq!(ns.body["variables"][0]["name"], "x");
    assert_eq!(s.backend.last_call(&kid).unwrap().1, json!({"limit": 200}));

    let doc = a.get(&format!("{base}/document")).await;
    assert_eq!(doc.status, 200);
    assert_eq!(doc.body["kernel_id"], kid);
    assert!(doc.body["cells"][0]["outputs"].is_array());
    let d = a.get(&format!("/api/documents?path={path}")).await;
    assert_eq!(d.status, 200);
    assert_eq!(d.body["cells"][0]["type"], "preamble");
    a.get(&format!("/api/documents?path={path}.nope.py")).await.error(404, "not_found");

    let r = a.post(&format!("{base}/restart"), json!({"hard": false})).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body["kernel_id"], kid);
    assert_eq!(s.backend.last_call(&kid).unwrap(), ("restart".into(), json!({"hard": false})));
    let r = a.call("POST", &format!("{base}/restart"), None).await;
    assert_eq!(r.status, 200);
    let r = a.post(&format!("{base}/restart"), json!({"hard": true})).await;
    assert_eq!(r.status, 200);
    assert_eq!(s.backend.last_call(&kid).unwrap(), ("restart".into(), json!({"hard": true})));

    let d = a.delete(&base).await;
    assert_eq!((d.status, d.body.clone()), (202, json!({"shutting_down": true})));
    assert!(s.backend.method_calls(&kid).contains(&"shutdown".to_string()));
    a.delete(&base).await.error(404, "not_found");
    assert!(s.backend.kills.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_fr_k8_forced_shutdown_kills_only_after_grace() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, _) = s.kernel("stuck.py");
    s.backend.set_linger(&kid);
    s.backend.fail_on("shutdown", DpxError::new("kernel_unreachable", "hung"));
    let a = s.admin();
    a.delete(&format!("/api/kernels/{kid}")).await.error(502, "kernel_unreachable");
    let d = a.delete(&format!("/api/kernels/{kid}?force=true")).await;
    assert_eq!(d.status, 202);
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert!(s.backend.kills.lock().unwrap().is_empty(), "killed before the 5 s grace");
    assert!(wait_for(|| s.backend.kills.lock().unwrap().len() == 1, std::time::Duration::from_secs(5)).await);
}

#[tokio::test]
async fn test_fr_a2_requests_without_token_are_401() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, _) = s.kernel("train.py");
    let calls = [
        ("GET", "/api/manager".to_string()),
        ("GET", "/api/kernels".into()),
        ("POST", "/api/kernels".into()),
        ("GET", format!("/api/kernels/{kid}")),
        ("POST", format!("/api/kernels/{kid}/interrupt")),
        ("POST", format!("/api/kernels/{kid}/runs")),
        ("GET", format!("/api/kernels/{kid}/events")),
        ("GET", "/api/documents?path=x.py".into()),
        ("GET", format!("/api/kernels/{kid}/shares")),
        ("GET", "/api/nowhere".into()),
    ];
    for token in [None, Some("wrong"), Some("")] {
        let api = match token {
            None => s.anon(),
            Some(t) => s.with_token(t),
        };
        for (m, p) in &calls {
            let r = api.raw(m, p, b"{bad").await;
            r.error(401, "unauthorized");
            assert_eq!(r.headers["www-authenticate"], "Bearer");
        }
    }
    let anon = s.anon();
    assert_eq!(anon.get("/health").await.status, 200);
    // ?token= only on the events stream.
    anon.get(&format!("/api/kernels?token={}", s.token)).await.error(401, "unauthorized");
    let r = reqwest::get(format!("{}/api/kernels/{kid}/events?token={}", s.url, s.token)).await.unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.headers()["content-type"].to_str().unwrap().starts_with("text/event-stream"));
    drop(r);
    let calls = s.backend.method_calls(&kid);
    assert!(!calls.contains(&"interrupt".to_string()) && !calls.contains(&"run".to_string()));
}

#[tokio::test]
async fn test_fr_a3_ephemeral_manager_refuses_share_creation() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, _) = s.kernel("train.py");
    let a = s.admin();
    a.post(&format!("/api/kernels/{kid}/shares"), json!({"capabilities": ["read"]})).await.error(403, "forbidden");
    let l = a.get(&format!("/api/kernels/{kid}/shares")).await;
    assert_eq!((l.status, l.body.clone()), (200, json!({"shares": []})));
    a.delete(&format!("/api/kernels/{kid}/shares/s_0000000000000000")).await.error(404, "not_found");
}

const CAPS: [&str; 5] = ["read", "history", "execute", "edit", "manage"];

/// The FR-A3 matrix: every operation x a token holding only that capability, plus kernel scoping.
#[tokio::test]
async fn test_fr_a3_each_operation_needs_its_capability() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (kid, path) = s.kernel("train.py");
    let (other, _) = s.kernel("other.py");
    let a = s.admin();
    let mut tokens = vec![];
    for cap in CAPS {
        let c = a.post(&format!("/api/kernels/{kid}/shares"), json!({"capabilities": [cap], "label": cap})).await;
        assert_eq!(c.status, 201, "{}", c.text);
        assert_eq!(c.body["capabilities"], json!([cap]));
        tokens.push((cap, c.body["token"].as_str().unwrap().to_string()));
    }
    // (method, path, body, the one capability it needs)
    let b = format!("/api/kernels/{kid}");
    let ops: Vec<(&str, String, Option<serde_json::Value>, &str)> = vec![
        ("GET", "/api/kernels".into(), None, "read"),
        ("GET", b.clone(), None, "read"),
        ("GET", format!("{b}/document"), None, "read"),
        ("GET", format!("{b}/namespace"), None, "history"),
        ("GET", format!("{b}/runs"), None, "history"),
        ("GET", format!("{b}/runs/latest"), None, "history"),
        ("POST", format!("{b}/interrupt"), None, "execute"),
        ("DELETE", format!("{b}/runs/20261003-120000-ffff"), None, "execute"),
        ("POST", format!("{b}/runs"), Some(json!({"mode": "all", "on_busy": "queue"})), "execute"),
        ("POST", format!("{b}/restart"), Some(json!({})), "execute"),
        ("GET", format!("{b}/shares"), None, "manage"),
        ("POST", format!("{b}/shares"), Some(json!({"capabilities": ["read"]})), "manage"),
        ("DELETE", format!("{b}/shares/s_0000000000000000"), None, "manage"),
    ];
    for (cap, token) in &tokens {
        let v = s.with_token(token);
        assert_eq!(v.get("/api/manager").await.body["capabilities"], json!([cap]));
        for (m, p, body, need) in &ops {
            let r = v.call(m, p, body.clone()).await;
            if need == cap {
                let missing = r.status == 404 && (p.contains("/runs/") || p.contains("/shares/"));
                assert!(r.status < 400 || missing, "{cap} {m} {p}: {}", r.text);
            } else {
                r.error(403, "forbidden");
            }
        }
        // A token bound to one kernel cannot start kernels or read files by path.
        if *cap == "execute" || *cap == "read" {
            v.post("/api/kernels", json!({"path": path})).await.error(403, "forbidden");
            v.get(&format!("/api/documents?path={path}")).await.error(403, "forbidden");
        }
        // Scoped to one kernel: other kernels are 404, never 403.
        for (m, p) in [
            ("GET", format!("/api/kernels/{other}")),
            ("GET", format!("/api/kernels/{other}/document")),
            ("GET", format!("/api/kernels/{other}/events")),
            ("POST", format!("/api/kernels/{other}/interrupt")),
            ("DELETE", format!("/api/kernels/{other}")),
            ("GET", format!("/api/kernels/{other}/shares")),
        ] {
            v.call(m, &p, None).await.error(404, "not_found");
        }
        if *cap == "read" {
            let ks = v.get("/api/kernels").await;
            assert_eq!(ks.body["kernels"].as_array().unwrap().len(), 1);
            assert_eq!(ks.body["kernels"][0]["kernel_id"], kid);
            // read without history: cells without outputs.
            let doc = v.get(&format!("{b}/document")).await;
            assert!(doc.body["cells"][0].get("outputs").is_none());
        }
    }
    // Shutdown needs execute; only the execute token (and the master) may.
    for (cap, token) in &tokens {
        if *cap != "execute" {
            s.with_token(token).delete(&b).await.error(403, "forbidden");
        }
    }
    assert_eq!(a.delete(&b).await.status, 202);
}

#[tokio::test]
async fn test_fr_a3_capabilities_have_no_order() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (kid, _) = s.kernel("train.py");
    let a = s.admin();
    let b = format!("/api/kernels/{kid}");
    // manage alone is not read: it makes shares but cannot see the document.
    let m = a.post(&format!("{b}/shares"), json!({"capabilities": ["manage"]})).await;
    let v = s.with_token(m.body["token"].as_str().unwrap());
    assert_eq!(v.post(&format!("{b}/shares"), json!({"capabilities": ["read"]})).await.status, 201);
    v.get(&format!("{b}/document")).await.error(403, "forbidden");
    // read + history sees outputs.
    let h = a.post(&format!("{b}/shares"), json!({"capabilities": ["history", "read"]})).await;
    assert_eq!(h.body["capabilities"], json!(["read", "history"]), "canonical order");
    let v = s.with_token(h.body["token"].as_str().unwrap());
    let doc = v.get(&format!("{b}/document")).await;
    assert!(doc.body["cells"][0].get("outputs").is_some());
}

#[tokio::test]
async fn test_fr_a3_tier_names_are_refused() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (kid, _) = s.kernel("train.py");
    let a = s.admin();
    let url = format!("/api/kernels/{kid}/shares");
    for tier in ["viewer1", "viewer2", "viewer3", "admin", "editor", "write"] {
        a.post(&url, json!({"capabilities": [tier]})).await.error(400, "bad_request");
        a.post(&url, json!({"permission": tier})).await.error(400, "bad_request");
    }
    a.post(&url, json!({"capabilities": []})).await.error(400, "bad_request");
    a.post(&format!("/api/kernels/{kid}/tokens/share"), json!({"capabilities": ["viewer2"]}))
        .await
        .error(400, "bad_request");
}

#[tokio::test]
async fn test_fr_a3_user_permission_names_the_needed_capability() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (kid, _) = s.kernel("train.py");
    let a = s.admin();
    let b = format!("/api/kernels/{kid}");
    // The right name passes (the token's set decides), a wrong or unknown one is 400.
    let ok = a.post(&format!("{b}/runs"), json!({"mode": "all", "user_permission": "execute"})).await;
    assert_eq!(ok.status, 202, "{}", ok.text);
    for bad in ["edit", "write", "viewer3", "admin"] {
        a.post(&format!("{b}/runs"), json!({"mode": "all", "user_permission": bad})).await.error(400, "bad_request");
    }
    a.post(&format!("{b}/shares"), json!({"capabilities": ["read"], "user_permission": "manage"})).await;
    a.post(&format!("{b}/shares"), json!({"capabilities": ["read"], "user_permission": "edit"}))
        .await
        .error(400, "bad_request");
    // A token without the capability is 403 even with the right name.
    let c = a.post(&format!("{b}/shares"), json!({"capabilities": ["read"]})).await;
    let v = s.with_token(c.body["token"].as_str().unwrap());
    v.post(&format!("{b}/runs"), json!({"mode": "all", "user_permission": "execute"})).await.error(403, "forbidden");
}

#[tokio::test]
async fn test_fr_a3_token_share_is_issued_by_the_kernel() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (kid, _) = s.kernel("train.py");
    let a = s.admin();
    let url = format!("/api/kernels/{kid}/tokens/share");
    let c = a
        .post(&url, json!({"capabilities": ["read", "history"], "kernel_id": kid, "user_permission": "manage"}))
        .await;
    assert_eq!(c.status, 201, "{}", c.text);
    let sid = c.body["share_id"].as_str().unwrap();
    let token = c.body["share_token"].as_str().unwrap();
    assert!(dpx_server::util::is_share_id(sid));
    assert_eq!(c.body["capabilities"], json!(["read", "history"]));
    assert_eq!(c.body["share_url"], format!("https://darkpyonix.dev/s/{sid}#{token}"));
    let issued = s.backend.calls_of(&kid, "tokens.issue").pop().unwrap();
    assert_eq!(issued["kind"], "share");
    assert_eq!(issued["capabilities"], json!(["read", "history"]));
    // The kernel holds it: it works as a bearer token and lists, then revokes through the kernel.
    let v = s.with_token(token);
    let runs = format!("/api/kernels/{kid}/runs");
    assert_eq!(v.get(&runs).await.status, 200);
    v.post(&runs, json!({"mode": "all"})).await.error(403, "forbidden");
    let listed = a.get(&format!("/api/kernels/{kid}/shares")).await.body["shares"].clone();
    assert!(listed.as_array().unwrap().iter().any(|x| x["share_id"] == sid && x["kernel_id"] == kid.as_str()));
    assert_eq!(a.delete(&format!("/api/kernels/{kid}/shares/{sid}")).await.status, 204);
    v.get(&runs).await.error(401, "token_revoked");
    // Validation.
    a.post(&url, json!({"capabilities": ["read"], "kernel_id": "k_00000000000000000000"})).await.error(400, "bad_request");
    a.post(&url, json!({"capabilities": ["read"], "user_permission": "read"})).await.error(400, "bad_request");
    let m = a.post(&url, json!({"capabilities": ["read"]})).await;
    let r = s.with_token(m.body["share_token"].as_str().unwrap());
    r.post(&url, json!({"capabilities": ["read"]})).await.error(403, "forbidden");
}

#[tokio::test]
async fn test_fr_a3_ephemeral_manager_refuses_token_share() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let (kid, _) = s.kernel("train.py");
    s.admin()
        .post(&format!("/api/kernels/{kid}/tokens/share"), json!({"capabilities": ["read"]}))
        .await
        .error(403, "forbidden");
}

#[tokio::test]
async fn test_fr_a3_share_tokens_are_scoped_to_one_kernel_and_capabilities() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (kid, path) = s.kernel("train.py");
    let a = s.admin();
    let created = a.post(&format!("/api/kernels/{kid}/shares"), json!({"capabilities": ["read"], "label": "demo"})).await;
    assert_eq!(created.status, 201);
    let share = created.body;
    let sid = share["share_id"].as_str().unwrap();
    let token = share["token"].as_str().unwrap();
    assert!(dpx_server::util::is_share_id(sid));
    assert_eq!(share["url"], format!("https://darkpyonix.dev/s/{sid}#{token}"));
    assert_eq!(share["kernel_id"], kid);
    assert_eq!(share["label"], "demo");
    assert_eq!(share["expires_at"], json!(null));
    let listed = a.get(&format!("/api/kernels/{kid}/shares")).await.body["shares"].clone();
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["share_id"], sid);
    assert!(listed[0].get("token").is_none());

    let v = s.with_token(token);
    assert_eq!(v.get("/api/manager").await.body["capabilities"], json!(["read"]));
    v.post("/api/kernels", json!({"path": path})).await.error(403, "forbidden");

    // Validation of createShare.
    let url = format!("/api/kernels/{kid}/shares");
    a.post(&url, json!({"capabilities": ["admin"]})).await.error(400, "bad_request");
    a.post(&url, json!({"label": "x"})).await.error(400, "bad_request");
    a.post(&url, json!({"capabilities": ["read"], "label": "x".repeat(121)})).await.error(400, "bad_request");
    a.post(&url, json!({"capabilities": ["read"], "expires_at": "tomorrow"})).await.error(400, "bad_request");
    a.post("/api/kernels/k_00000000000000000000/shares", json!({"capabilities": ["read"]})).await.error(404, "not_found");

    // Expired shares do not authenticate.
    let expired = a.post(&url, json!({"capabilities": ["read", "history"], "expires_at": "2000-01-01T00:00:00Z"})).await;
    assert_eq!(expired.status, 201);
    assert_eq!(expired.body["expires_at"], "2000-01-01T00:00:00Z");
    s.with_token(expired.body["token"].as_str().unwrap()).get("/api/manager").await.error(401, "unauthorized");

    let r = a.delete(&format!("{url}/{sid}")).await;
    assert_eq!(r.status, 204);
    a.delete(&format!("{url}/{sid}")).await.error(404, "not_found");
    a.delete(&format!("{url}/not-a-share")).await.error(404, "not_found");
    v.get("/api/manager").await.error(401, "unauthorized");
}

#[tokio::test]
async fn test_fr_m4_shares_are_stored_hashed_and_survive_restart() {
    let scratch = scratch("m4");
    let home = scratch.join("home");
    let backend = FakeBackend::new();
    let mut cfg = dpx_server::ServerConfig::dedicated(&home, "127.0.0.1", 0);
    cfg.master_token = Some("master-secret".into());
    cfg.share_base = "https://example.test/share/".into();
    let mut s = TestServer::start_cfg(cfg.clone(), backend.clone(), scratch.clone()).await;
    let (kid, _) = s.kernel("train.py");
    assert_eq!(s.token, "master-secret");
    let c = s.admin().post(&format!("/api/kernels/{kid}/shares"), json!({"capabilities": ["read", "history"]})).await;
    let token = c.body["token"].as_str().unwrap().to_string();
    assert!(c.body["url"].as_str().unwrap().starts_with("https://example.test/share/s_"));
    s.stop().await;

    let db = std::fs::read(home.join("manager.db")).unwrap();
    let hay = String::from_utf8_lossy(&db);
    assert!(!hay.contains(&token), "token stored in clear");
    assert!(!hay.contains("master-secret"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(std::fs::metadata(home.join("manager.db")).unwrap().permissions().mode() & 0o777, 0o600);
    }

    let s2 = TestServer::start_cfg(cfg, backend, scratch.clone()).await;
    let v = s2.with_token(&token);
    assert_eq!(v.get("/api/manager").await.body["capabilities"], json!(["read", "history"]));
    let info = s2.admin().get("/api/manager").await;
    assert_eq!(info.body["mode"], "dedicated");
    assert_eq!(info.body["idle_timeout"], json!(null));
    drop(s2);
    drop(s);
}
