//! HTTP probes and relay administration.
use std::sync::Arc;
use axum::{extract::{Request, State}, http::{header, HeaderMap, HeaderValue, StatusCode, Uri}, middleware::{self, Next}, response::{IntoResponse, Redirect, Response}, routing::{get, post}, Json, Router};
use iroh_base::EndpointId;
use serde::Deserialize;
use url::Url;
use crate::state::State as RelayState;

pub fn router(state: Arc<RelayState>) -> Router {
    Router::new().route("/health", get(health)).route("/ping", get(ping))
        .route("/generate_204", get(generate_204))
        .route("/admin/disconnect", post(disconnect).route_layer(middleware::from_fn_with_state(state.clone(), authorize)))
        .with_state(state)
}
pub fn side_router(state: Arc<RelayState>, public_url: Url) -> Router {
    Router::new().route("/health", get(health)).route("/ping", get(ping))
        .route("/generate_204", get(generate_204))
        .fallback(move |uri: Uri| async move {
            let mut target = public_url.clone();
            target.set_path(uri.path());
            target.set_query(uri.query());
            Redirect::permanent(target.as_str())
        }).with_state(state)
}
async fn health() -> impl IntoResponse { Json(serde_json::json!({"status":"ok", "version":env!("CARGO_PKG_VERSION")})) }
async fn ping() -> impl IntoResponse { ([(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")], StatusCode::OK) }
async fn generate_204(headers: HeaderMap) -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    if let Some(challenge) = headers.get("x-iroh-challenge").and_then(|v| v.to_str().ok()).filter(|c| !c.is_empty() && c.len() < 64 && c.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))) {
        if let Ok(value) = HeaderValue::from_str(&format!("response {challenge}")) { response.headers_mut().insert("x-iroh-response", value); }
    }
    response
}
async fn authorize(State(state): State<Arc<RelayState>>, request: Request, next: Next) -> Response {
    let expected = format!("Bearer {}", state.secret);
    let given = request.headers().get(header::AUTHORIZATION).map(|v| v.as_bytes()).unwrap_or_default();
    let expected = expected.as_bytes();
    let mut difference = given.len() ^ expected.len();
    for i in 0..expected.len() { difference |= usize::from(expected[i] ^ given.get(i).copied().unwrap_or(0)); }
    if difference != 0 { return StatusCode::UNAUTHORIZED.into_response(); }
    next.run(request).await
}
#[derive(Deserialize)]
struct Disconnect { endpoint_id: String }
async fn disconnect(State(state): State<Arc<RelayState>>, body: Result<Json<Disconnect>, axum::extract::rejection::JsonRejection>) -> StatusCode {
    let Ok(Json(body)) = body else { return StatusCode::BAD_REQUEST };
    if body.endpoint_id.len() != 64 || !body.endpoint_id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) { return StatusCode::BAD_REQUEST; }
    let Ok(endpoint) = body.endpoint_id.parse::<EndpointId>() else { return StatusCode::BAD_REQUEST };
    state.disconnect(endpoint);
    StatusCode::NO_CONTENT
}
