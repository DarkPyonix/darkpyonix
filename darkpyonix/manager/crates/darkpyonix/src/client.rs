//! Minimal client for `docs/api/manager.openapi.yaml`.

use std::time::Duration;

use reqwest::{Method, StatusCode};
use serde_json::{json, Value};

/// A failure to show the user: an API `Error` object, or a local problem in the same shape.
#[derive(Debug, Clone)]
pub struct CliError {
    pub code: String,
    pub message: String,
    pub data: Option<Value>,
}

impl CliError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self { code: code.into(), message: message.into(), data: None }
    }

    pub fn is(&self, code: &str) -> bool {
        self.code == code
    }

    /// The OpenAPI `Error` body, for `--json`.
    pub fn to_json(&self) -> Value {
        let mut e = json!({"code": self.code, "message": self.message});
        if let Some(d) = &self.data {
            e["data"] = d.clone();
        }
        json!({ "error": e })
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

pub fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(3))
        .build()
        .expect("HTTP client")
}

/// One manager reached over HTTP with its master token.
pub struct Api {
    pub base: String,
    pub token: String,
    http: reqwest::Client,
}

impl Api {
    pub fn new(http: reqwest::Client, base: &str, token: &str) -> Self {
        Self {
            base: base.trim_end_matches('/').to_string(),
            token: token.to_string(),
            http,
        }
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        self.http.request(method, format!("{}{}", self.base, path)).bearer_auth(&self.token)
    }

    /// Send a request; a non-2xx answer becomes a `CliError` carrying the API error.
    pub async fn call(&self, method: Method, path: &str, body: Option<&Value>) -> Result<(u16, Value), CliError> {
        let mut req = self.request(method, path);
        if let Some(b) = body {
            req = req.header("content-type", "application/json").body(b.to_string());
        }
        let resp = req.send().await.map_err(|e| transport(&self.base, e))?;
        let status = resp.status();
        let bytes = resp.bytes().await.map_err(|e| transport(&self.base, e))?;
        let value: Value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()))
        };
        if status.is_success() {
            Ok((status.as_u16(), value))
        } else {
            Err(api_error(status, value))
        }
    }

    pub async fn get(&self, path: &str) -> Result<Value, CliError> {
        self.call(Method::GET, path, None).await.map(|(_, v)| v)
    }

    pub async fn post(&self, path: &str, body: &Value) -> Result<(u16, Value), CliError> {
        self.call(Method::POST, path, Some(body)).await
    }

    /// Open the SSE stream of a kernel. Resolves once the response headers arrived, so
    /// the manager is subscribed before the caller starts a run.
    pub async fn events(&self, kernel_id: &str, since: Option<u64>) -> Result<reqwest::Response, CliError> {
        let mut path = format!("/api/kernels/{kernel_id}/events");
        if let Some(s) = since {
            path.push_str(&format!("?since={s}"));
        }
        let mut req = self.request(Method::GET, &path).header("accept", "text/event-stream");
        if let Some(s) = since {
            req = req.header("last-event-id", s.to_string());
        }
        let resp = req.send().await.map_err(|e| transport(&self.base, e))?;
        let status = resp.status();
        if status.is_success() {
            Ok(resp)
        } else {
            let body = resp.bytes().await.unwrap_or_default();
            Err(api_error(status, serde_json::from_slice(&body).unwrap_or(Value::Null)))
        }
    }
}

fn transport(base: &str, e: reqwest::Error) -> CliError {
    CliError::new("manager_unreachable", format!("cannot reach the manager at {base}: {e}"))
}

fn api_error(status: StatusCode, body: Value) -> CliError {
    let err = body.get("error");
    let field = |k: &str| err.and_then(|e| e.get(k)).and_then(Value::as_str).map(str::to_string);
    CliError {
        code: field("code").unwrap_or_else(|| format!("http_{}", status.as_u16())),
        message: field("message").unwrap_or_else(|| format!("manager answered {status}")),
        data: err.and_then(|e| e.get("data")).cloned(),
    }
}
