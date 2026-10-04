//! The relay host's operations match docs/api/hub.openapi.yaml (CLAUDE.md: the OpenAPI file
//! is the SPEC). Since INTENT D15 the hub API runs on Cloudflare Workers (hub/worker, which
//! checks the rest); this binary serves only the operations with a path-level
//! `servers: relay.darkpyonix.dev` entry.

use iroh::SecretKey;
use serde_yaml_ng::Value as Yaml;

use crate::common::*;

fn spec() -> Yaml {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/api/hub.openapi.yaml"
    );
    let text = std::fs::read_to_string(path).expect("read hub.openapi.yaml");
    serde_yaml_ng::from_str(&text).expect("parse hub.openapi.yaml")
}

/// A value that satisfies each documented path parameter's pattern.
fn fill(path: &str) -> String {
    let key = SecretKey::generate().public();
    path.replace("{endpoint_id}", &key.to_string())
        .replace("{key}", &key.to_z32())
        .replace("{share_id}", "s_0123456789abcdef")
        .replace("{name}", "example-name")
}

#[tokio::test]
async fn test_hub_every_operation_answers_with_a_documented_status() {
    let hub = start_hub("openapi").await;
    let spec = spec();
    let paths = spec["paths"].as_mapping().expect("paths");
    let mut checked = 0;
    for (path, item) in paths {
        let path = path.as_str().unwrap();
        // Only the relay host's operations; the Worker serves the rest.
        if item.get("servers").is_none() {
            continue;
        }
        // Not served until the relay trim (SPEC FR-H3: the relay asks the Worker whom to admit
        // and takes disconnects at /admin/disconnect).
        if path.starts_with("/admin/") {
            continue;
        }
        for method in ["get", "put", "post", "delete"] {
            let Some(op) = item.get(method) else { continue };
            let documented: Vec<u16> = op["responses"]
                .as_mapping()
                .expect("responses")
                .keys()
                .map(|k| k.as_str().unwrap().parse().unwrap())
                .collect();
            let url = hub.url(&fill(path));
            let request = match method {
                "get" => hub.client.get(&url),
                "put" => hub.client.put(&url),
                "post" => hub.client.post(&url),
                _ => hub.client.delete(&url),
            };
            // No credentials and an empty body: every operation must still answer with a
            // status its documentation lists.
            let status = request.send().await.unwrap().status().as_u16();
            assert!(
                documented.contains(&status),
                "{} {path} answered {status}, documented {documented:?}",
                method.to_uppercase()
            );
            checked += 1;
        }
    }
    assert!(checked >= 3, "only {checked} operations checked");
}
