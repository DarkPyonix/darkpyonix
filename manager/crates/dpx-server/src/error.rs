//! The OpenAPI `Error` / `BusyError` responses and the mapping from [`DpxError`] codes.

use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use dpx_core::DpxError;
use serde_json::{json, Value};

/// An HTTP error in the contract's `{"error": {"code", "message", "data"?}}` shape.
#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: String,
    pub message: String,
    pub data: Option<Value>,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &str, message: impl Into<String>) -> Self {
        Self { status, code: code.to_string(), message: message.into(), data: None }
    }
    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }
    /// A validation failure; `data.errors` lists what was wrong (one entry per problem).
    pub fn invalid(message: impl Into<String>) -> Self {
        let message = message.into();
        Self::bad_request("request validation failed")
            .with_data(json!({"errors": [{"msg": message}]}))
    }
    pub fn unauthorized() -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", "missing or invalid token")
    }
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", message)
    }
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }

    /// Keep the status only if the operation documents it (NFR-M3); otherwise answer
    /// `500 internal` and keep the original code in `data.cause`.
    pub fn documented(self, statuses: &[u16]) -> Self {
        if statuses.contains(&self.status.as_u16()) {
            return self;
        }
        let mut data = serde_json::Map::new();
        data.insert("cause".into(), Value::String(self.code.clone()));
        if let Some(d) = self.data {
            data.insert("data".into(), d);
        }
        ApiError::internal(self.message).with_data(Value::Object(data))
    }
}

impl From<DpxError> for ApiError {
    /// not_found→404, busy/conflict/locked→409, start_timeout→504, kernel_unreachable/shutting_down→502,
    /// forbidden→403, unauthorized→401, bad_request→400, anything else→500 internal.
    fn from(e: DpxError) -> Self {
        let status = match e.code.as_str() {
            "bad_request" => StatusCode::BAD_REQUEST,
            "unauthorized" => StatusCode::UNAUTHORIZED,
            "forbidden" => StatusCode::FORBIDDEN,
            "not_found" => StatusCode::NOT_FOUND,
            // FR-S2/S3: `conflict` carries `data.cell`, `locked` carries `data.locked_by`.
            "busy" | "conflict" | "locked" => StatusCode::CONFLICT,
            "start_timeout" => StatusCode::GATEWAY_TIMEOUT,
            "kernel_unreachable" | "shutting_down" => StatusCode::BAD_GATEWAY,
            _ => {
                let mut data = serde_json::Map::new();
                data.insert("cause".into(), Value::String(e.code.clone()));
                if let Some(d) = e.data {
                    data.insert("data".into(), d);
                }
                return ApiError::internal(e.message).with_data(Value::Object(data));
            }
        };
        let data = if e.code == "busy" {
            // BusyError requires data.current; the kernel's `busy` data is passed through.
            match e.data {
                Some(Value::Object(m)) if m.contains_key("current") => Some(Value::Object(m)),
                Some(other) => Some(json!({ "current": other })),
                None => Some(json!({ "current": {} })),
            }
        } else {
            e.data
        };
        ApiError { status, code: e.code, message: e.message, data }
    }
}

pub fn error_body(code: &str, message: &str, data: Option<&Value>) -> Value {
    let mut err = serde_json::Map::new();
    err.insert("code".into(), Value::String(code.into()));
    err.insert("message".into(), Value::String(message.into()));
    if let Some(d) = data {
        err.insert("data".into(), d.clone());
    }
    json!({ "error": Value::Object(err) })
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = error_body(&self.code, &self.message, self.data.as_ref());
        let mut resp = (self.status, axum::Json(body)).into_response();
        if self.status == StatusCode::UNAUTHORIZED {
            resp.headers_mut().insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        resp
    }
}

pub type ApiResult<T> = std::result::Result<T, ApiError>;
