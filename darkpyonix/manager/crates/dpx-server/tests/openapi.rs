//! NFR-M3: the router answers every (path, method) of docs/api/manager.openapi.yaml, only
//! with statuses the YAML documents, and serves nothing else under /api.

mod common;

use common::*;
use dpx_server::Mode;
use serde_json::{json, Value};

fn probe_url(path: &str) -> String {
    path.replace("{kernel_id}", "k_00000000000000000000")
        .replace("{run_ref}", "20260101-000000-0000")
        .replace("{share_id}", "s_0000000000000000")
        .replace("{cell_id}", "c_probe")
}

fn probe_body(op: &str) -> Option<Value> {
    match op {
        "startKernel" => Some(json!({"path": "/nonexistent/darkpyonix/x.py"})),
        "startRun" => Some(json!({"mode": "all"})),
        "restartKernel" => Some(json!({})),
        "createShare" => Some(json!({"permission": "viewer1"})),
        _ => None,
    }
}

async fn probe_all(s: &TestServer, kernel: Option<&str>) -> usize {
    let raw = &contract().raw;
    let admin = s.admin();
    let anon = s.anon();
    let mut n = 0;
    for (path, item) in raw["paths"].as_mapping().unwrap() {
        let path = path.as_str().unwrap();
        for (method, op) in item.as_mapping().unwrap() {
            let method = method.as_str().unwrap();
            if !["get", "put", "post", "delete", "patch"].contains(&method) {
                continue;
            }
            let op_id = op["operationId"].as_str().unwrap();
            let documented: Vec<u16> = contract().documented(&method.to_uppercase(), path).unwrap().clone();
            let mut url = probe_url(path);
            if let Some(k) = kernel {
                url = url.replace("k_00000000000000000000", k);
            }
            if op_id == "getDocument" {
                url.push_str("?path=/nonexistent/darkpyonix/x.py");
            }
            if op_id == "waitRun" {
                // An unknown run long-polls until the timeout before answering 404.
                url.push_str("?timeout=1");
            }
            let m = method.to_uppercase();
            let body = probe_body(op_id);
            if op_id == "streamEvents" {
                // Do not read an endless stream: check the status only.
                let r = admin.req(&m, &url).send().await.unwrap();
                assert!(documented.contains(&r.status().as_u16()), "{op_id} {}", r.status());
            } else {
                let r = admin.call(&m, &url, body.clone()).await;
                assert!(documented.contains(&r.status), "{op_id}: {} {}", r.status, r.text);
                if r.status >= 400 {
                    assert_eq!(r.body.as_object().unwrap().keys().collect::<Vec<_>>(), ["error"], "{op_id}");
                }
            }
            let secured = op.get("security").and_then(|s| s.as_sequence()).is_none_or(|s| !s.is_empty());
            if secured {
                anon.call(&m, &url, body).await.error(401, "unauthorized");
            }
            n += 1;
        }
    }
    n
}

#[tokio::test]
async fn test_nfr_m3_every_operation_answers_with_a_documented_status() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let n = probe_all(&s, None).await;
    assert_eq!(n, contract().operations().count());
    assert_eq!(n, 28, "operation count changed; update the router and the tests");
    s.admin().get("/api/not-in-the-contract").await.error(404, "not_found");
}

#[tokio::test]
async fn test_nfr_m3_documented_statuses_with_a_live_kernel_and_dedicated_mode() {
    let s = TestServer::start(Mode::Dedicated).await;
    let (kid, _) = s.kernel("train.py");
    assert_eq!(probe_all(&s, Some(&kid)).await, 28);
}

#[tokio::test]
async fn test_nfr_m3_undocumented_methods_are_not_served() {
    let s = TestServer::start(Mode::Ephemeral).await;
    let a = s.admin();
    for (m, p) in [("PUT", "/api/kernels"), ("PATCH", "/api/manager"), ("POST", "/api/documents")] {
        let r = a.call(m, p, Some(json!({}))).await;
        assert_eq!(r.status, 405, "{m} {p}");
        assert!(r.body["error"]["message"].is_string());
    }
}
