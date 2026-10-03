//! The API reference page (`/docs/`) and the two OpenAPI files it renders. These are
//! static files outside the contract (SPEC NFR-M3 exception), embedded in the binary.

use axum::extract::Path;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};

use crate::error::ApiError;

pub const INDEX_HTML: &str = include_str!("../../../../../docs/api/index.html");
pub const MANAGER_OPENAPI: &str = include_str!("../../../../../docs/api/manager.openapi.yaml");
pub const HUB_OPENAPI: &str = include_str!("../../../../../docs/api/hub.openapi.yaml");

pub async fn redirect() -> Redirect {
    Redirect::temporary("/docs/")
}

pub async fn index() -> Response {
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], INDEX_HTML).into_response()
}

pub async fn file(Path(name): Path<String>) -> Response {
    let body = match name.as_str() {
        "manager.openapi.yaml" => MANAGER_OPENAPI,
        "hub.openapi.yaml" => HUB_OPENAPI,
        _ => return ApiError::not_found(format!("no such document: {name}")).into_response(),
    };
    (StatusCode::OK, [(header::CONTENT_TYPE, "application/yaml")], body).into_response()
}
