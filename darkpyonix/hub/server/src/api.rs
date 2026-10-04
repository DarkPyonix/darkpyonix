//! The hub's HTTP API (docs/api/hub.openapi.yaml). `/relay` is not here: the front end
//! hands it to the iroh relay service before this router sees it (see `serve`).

use std::{collections::HashMap, sync::Arc};

use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, State as AxState},
    http::{header, HeaderMap, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use data_encoding::BASE64URL_NOPAD;
use iroh_base::{PublicKey, Signature};
use iroh_dns::{endpoint_info::EndpointInfo, pkarr::SignedPacket};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::warn;

use crate::{
    db::{Claim, DeviceRow, NameRow, TokenOwner},
    state::State,
    util,
};

type Shared = Arc<State>;
type ApiResult = Result<Response, ApiError>;

/// How long a registration challenge is valid.
const CHALLENGE_TTL_SECS: i64 = 300;
/// How long a guest relay pass from a share resolution is valid.
const RELAY_PASS_TTL_SECS: i64 = 600;

/// An error answered as `{"error": "..."}` with a documented status.
#[derive(Debug)]
pub(crate) struct ApiError(StatusCode, String);

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self(status, message.into())
    }
    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }
    fn unauthorized() -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "missing or invalid token")
    }
    fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "not found")
    }
}

impl From<rusqlite::Error> for ApiError {
    fn from(err: rusqlite::Error) -> Self {
        warn!(%err, "database error");
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

pub(crate) fn router(state: Shared) -> Router {
    let mut router = Router::new()
        .route("/health", get(health))
        .route("/accounts", post(create_account))
        .route("/challenges", post(create_challenge))
        .route("/devices", get(list_devices).post(register_device))
        .route(
            "/devices/{endpoint_id}",
            get(get_device).delete(remove_device),
        )
        .route("/devices/{endpoint_id}/addresses", get(device_addresses))
        .route("/pkarr/{key}", put(pkarr_put).get(pkarr_get))
        .route("/ping", get(ping))
        .route("/generate_204", get(generate_204))
        .route("/shares", post(publish_share))
        .route(
            "/shares/{share_id}",
            get(resolve_share).delete(unpublish_share),
        )
        .route("/s/{share_id}", get(share_page))
        .route("/ash/", get(ash_index))
        .route("/names", get(list_names))
        .route("/names/{name}", put(reserve_name).delete(release_name))
        .route(
            "/names/{name}/acme-challenge",
            put(set_acme_challenge).delete(clear_acme_challenge),
        );
    if state.ash_dir.is_some() {
        router = router.route("/ash/{*path}", get(ash_file));
    }
    router.with_state(state)
}

/// The router for the plain-HTTP side port when the main listener uses TLS: the iroh
/// captive-portal probe, health, and a redirect to HTTPS for everything else.
pub(crate) fn side_router(state: Shared) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/generate_204", get(generate_204))
        .fallback(redirect_to_https)
        .with_state(state)
}

// ---------------------------------------------------------------- helpers

fn json_response(status: StatusCode, value: Value) -> Response {
    (status, Json(value)).into_response()
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim().to_string())
}

/// Resolves the caller from `Authorization: Bearer` or, where allowed, `?token=`.
fn caller(
    state: &State,
    headers: &HeaderMap,
    query_token: Option<&str>,
) -> Result<TokenOwner, ApiError> {
    let token = bearer(headers)
        .or_else(|| query_token.map(str::to_string))
        .ok_or_else(ApiError::unauthorized)?;
    state
        .db
        .token_owner(&util::hash_token(&token))?
        .ok_or_else(ApiError::unauthorized)
}

fn account_of(owner: &TokenOwner) -> &str {
    match owner {
        TokenOwner::Account { account_id } | TokenOwner::Device { account_id, .. } => account_id,
    }
}

fn require_account(owner: TokenOwner) -> Result<String, ApiError> {
    match owner {
        TokenOwner::Account { account_id } => Ok(account_id),
        TokenOwner::Device { .. } => Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "this operation needs the account token",
        )),
    }
}

fn parse_body<T: for<'de> Deserialize<'de>>(body: &Bytes) -> Result<T, ApiError> {
    serde_json::from_slice(body)
        .map_err(|err| ApiError::bad_request(format!("invalid body: {err}")))
}

fn parse_endpoint_id(s: &str) -> Option<PublicKey> {
    if !util::is_lower_hex(s, 64) {
        return None;
    }
    let mut bytes = [0u8; 32];
    hex::decode_to_slice(s, &mut bytes).ok()?;
    PublicKey::from_bytes(&bytes).ok()
}

fn device_json(state: &State, d: &DeviceRow) -> Value {
    json!({
        "endpoint_id": d.endpoint_id,
        "name": d.name,
        "role": d.role,
        "created_at": d.created_at,
        "last_seen": d.last_seen,
        "online": state.is_online(&d.endpoint_id),
    })
}

fn name_json(state: &State, n: &NameRow) -> Value {
    json!({
        "name": n.name,
        "fqdn": format!("{}.{}", n.name, state.zone),
        "endpoint_id": n.endpoint_id,
    })
}

fn html(status: StatusCode, body: String) -> Response {
    (
        status,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        )],
        body,
    )
        .into_response()
}

fn acme_fqdn(state: &State, name: &str) -> String {
    format!("_acme-challenge.{name}.{}", state.zone)
}

/// The relay URL to dial a device on: its published home relay, or this hub.
fn relay_url_of(state: &State, endpoint_id: &str) -> Result<String, ApiError> {
    let published = state
        .db
        .record(endpoint_id)?
        .and_then(|bytes| SignedPacket::from_bytes(&bytes).ok())
        .and_then(|packet| EndpointInfo::from_pkarr_signed_packet(&packet).ok())
        .and_then(|info| info.relay_urls().next().map(|url| url.to_string()));
    Ok(published.unwrap_or_else(|| state.public_url.to_string()))
}

// ---------------------------------------------------------------- system

async fn health() -> Response {
    json_response(
        StatusCode::OK,
        json!({ "status": "ok", "version": env!("CARGO_PKG_VERSION") }),
    )
}

/// iroh's HTTPS latency probe.
async fn ping() -> Response {
    (
        StatusCode::OK,
        [(
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("*"),
        )],
    )
        .into_response()
}

/// iroh's captive portal check: echo `X-Iroh-Challenge` as `X-Iroh-Response`.
async fn generate_204(headers: HeaderMap) -> Response {
    let challenge = headers
        .get("x-iroh-challenge")
        .and_then(|v| v.to_str().ok())
        .filter(|c| {
            !c.is_empty()
                && c.len() < 64
                && c.bytes().all(|b| {
                    b.is_ascii_lowercase()
                        || b.is_ascii_uppercase()
                        || b.is_ascii_digit()
                        || matches!(b, b'.' | b'-' | b'_')
                })
        });
    let mut response = StatusCode::NO_CONTENT.into_response();
    if let Some(c) = challenge {
        if let Ok(value) = HeaderValue::from_str(&format!("response {c}")) {
            response.headers_mut().insert("x-iroh-response", value);
        }
    }
    response
}

async fn redirect_to_https(AxState(state): AxState<Shared>, uri: Uri) -> Response {
    let path = uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
    let mut target = state.public_url.clone();
    target.set_path("");
    let location = format!("{}{}", target.as_str().trim_end_matches('/'), path);
    match HeaderValue::from_str(&location) {
        Ok(value) => (StatusCode::MOVED_PERMANENTLY, [(header::LOCATION, value)]).into_response(),
        Err(_) => StatusCode::BAD_REQUEST.into_response(),
    }
}

// ---------------------------------------------------------------- accounts (FR-H1)

async fn create_account(AxState(state): AxState<Shared>, headers: HeaderMap) -> ApiResult {
    if let Some(secret) = &state.signup_secret {
        let given = headers
            .get("x-hub-signup-secret")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !util::ct_eq(given, secret) {
            return Err(ApiError::new(
                StatusCode::FORBIDDEN,
                "signup secret missing or wrong",
            ));
        }
    }
    let account_id = format!("a_{}", util::random_hex(8));
    let token = util::new_token("dpa_");
    state
        .db
        .insert_account(&account_id, &util::hash_token(&token), util::now())?;
    Ok(json_response(
        StatusCode::CREATED,
        json!({ "account_id": account_id, "account_token": token }),
    ))
}

async fn create_challenge(AxState(state): AxState<Shared>, headers: HeaderMap) -> ApiResult {
    let account_id = require_account(caller(&state, &headers, None)?)?;
    let challenge = util::random_hex(32);
    let now = util::now();
    let expires_at = now + CHALLENGE_TTL_SECS;
    state
        .db
        .insert_challenge(&challenge, &account_id, expires_at, now)?;
    Ok(json_response(
        StatusCode::CREATED,
        json!({ "challenge": challenge, "expires_at": expires_at }),
    ))
}

// ---------------------------------------------------------------- devices (FR-H1)

#[derive(Deserialize)]
struct Registration {
    endpoint_id: String,
    name: String,
    role: String,
    challenge: String,
    signature: String,
}

async fn register_device(
    AxState(state): AxState<Shared>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult {
    let account_id = require_account(caller(&state, &headers, None)?)?;
    let reg: Registration = parse_body(&body)?;

    // The challenge is consumed whatever happens next.
    let expires_at = state.db.take_challenge(&reg.challenge, &account_id)?;
    let key = parse_endpoint_id(&reg.endpoint_id)
        .ok_or_else(|| ApiError::bad_request("endpoint_id is not an ed25519 public key in hex"))?;
    if reg.name.is_empty() || reg.name.chars().count() > 64 {
        return Err(ApiError::bad_request("name must be 1 to 64 characters"));
    }
    if reg.role != "main_server" && reg.role != "computer" {
        return Err(ApiError::bad_request(
            "role must be main_server or computer",
        ));
    }
    match expires_at {
        Some(t) if t >= util::now() => {}
        _ => return Err(ApiError::bad_request("unknown, used or expired challenge")),
    }
    let mut sig = [0u8; 64];
    if !util::is_lower_hex(&reg.signature, 128)
        || hex::decode_to_slice(&reg.signature, &mut sig).is_err()
    {
        return Err(ApiError::bad_request(
            "signature must be 128 lowercase hex characters",
        ));
    }
    let message = util::registration_message(&account_id, &reg.challenge);
    if key
        .verify(message.as_bytes(), &Signature::from_bytes(&sig))
        .is_err()
    {
        return Err(ApiError::bad_request("signature does not verify"));
    }
    if state.db.device_exists_any_state(&reg.endpoint_id)? {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "endpoint id already registered",
        ));
    }

    let token = util::new_token("dpd_");
    let device = DeviceRow {
        endpoint_id: reg.endpoint_id,
        account_id,
        name: reg.name,
        role: reg.role,
        created_at: util::now(),
        last_seen: None,
        revoked: false,
    };
    state.db.insert_device(&device, &util::hash_token(&token))?;
    Ok(json_response(
        StatusCode::CREATED,
        json!({ "device": device_json(&state, &device), "device_token": token }),
    ))
}

async fn list_devices(AxState(state): AxState<Shared>, headers: HeaderMap) -> ApiResult {
    let owner = caller(&state, &headers, None)?;
    let devices: Vec<Value> = state
        .db
        .list_devices(account_of(&owner))?
        .iter()
        .map(|d| device_json(&state, d))
        .collect();
    Ok(json_response(StatusCode::OK, json!({ "devices": devices })))
}

async fn get_device(
    AxState(state): AxState<Shared>,
    headers: HeaderMap,
    Path(endpoint_id): Path<String>,
) -> ApiResult {
    let owner = caller(&state, &headers, None)?;
    let device = state
        .db
        .account_device(account_of(&owner), &endpoint_id)?
        .ok_or_else(ApiError::not_found)?;
    Ok(json_response(StatusCode::OK, device_json(&state, &device)))
}

async fn remove_device(
    AxState(state): AxState<Shared>,
    headers: HeaderMap,
    Path(endpoint_id): Path<String>,
) -> ApiResult {
    let account_id = require_account(caller(&state, &headers, None)?)?;
    let key = parse_endpoint_id(&endpoint_id).ok_or_else(ApiError::not_found)?;
    let names = state.db.names_of_device(&endpoint_id)?;
    if !state
        .db
        .revoke_device(&account_id, &endpoint_id, util::now())?
    {
        return Err(ApiError::not_found());
    }
    state.disconnect_from_relay(key);
    for name in names {
        state.db.delete_name(&name)?;
        let fqdn = acme_fqdn(&state, &name);
        if let Err(err) = state.dns.clear_txt(&fqdn).await {
            warn!(%err, %fqdn, "could not clear challenge records of a removed device");
        }
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

// ---------------------------------------------------------------- directory (FR-H2)

async fn device_addresses(
    AxState(state): AxState<Shared>,
    headers: HeaderMap,
    Path(endpoint_id): Path<String>,
) -> ApiResult {
    let owner = caller(&state, &headers, None)?;
    state
        .db
        .account_device(account_of(&owner), &endpoint_id)?
        .ok_or_else(ApiError::not_found)?;
    let bytes = state
        .db
        .record(&endpoint_id)?
        .ok_or_else(ApiError::not_found)?;
    let packet = SignedPacket::from_bytes(&bytes).map_err(|_| ApiError::not_found())?;
    let info =
        EndpointInfo::from_pkarr_signed_packet(&packet).map_err(|_| ApiError::not_found())?;
    let relay_urls: Vec<String> = info.relay_urls().map(|u| u.to_string()).collect();
    let direct: Vec<String> = info.ip_addrs().map(|a| a.to_string()).collect();
    Ok(json_response(
        StatusCode::OK,
        json!({
            "endpoint_id": endpoint_id,
            "relay_urls": relay_urls,
            "direct_addresses": direct,
            "published_at_us": packet.timestamp().as_micros(),
            "signed_packet": BASE64URL_NOPAD.encode(&packet.to_relay_payload()),
        }),
    ))
}

async fn pkarr_put(
    AxState(state): AxState<Shared>,
    Path(key): Path<String>,
    body: Bytes,
) -> ApiResult {
    let key = PublicKey::from_z32(&key).map_err(|_| ApiError::bad_request("malformed key"))?;
    let packet = SignedPacket::from_relay_payload(&key, &body)
        .map_err(|_| ApiError::bad_request("malformed payload or bad signature"))?;
    let endpoint_id = key.to_string();
    if !state.db.device_active(&endpoint_id)? {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "not a registered device",
        ));
    }
    let timestamp_us = i64::try_from(packet.timestamp().as_micros())
        .map_err(|_| ApiError::bad_request("timestamp out of range"))?;
    if let Some(stored) = state.db.record_timestamp(&endpoint_id)? {
        if stored >= timestamp_us {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "not newer than the stored packet",
            ));
        }
    }
    state
        .db
        .put_record(&endpoint_id, packet.as_bytes(), timestamp_us)?;
    state.db.touch_device(&endpoint_id, util::now())?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn pkarr_get(
    AxState(state): AxState<Shared>,
    headers: HeaderMap,
    Path(key): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> ApiResult {
    let key = PublicKey::from_z32(&key).map_err(|_| ApiError::bad_request("malformed key"))?;
    let owner = caller(&state, &headers, query.get("token").map(String::as_str))?;
    let endpoint_id = key.to_string();
    state
        .db
        .account_device(account_of(&owner), &endpoint_id)?
        .ok_or_else(ApiError::not_found)?;
    let bytes = state
        .db
        .record(&endpoint_id)?
        .ok_or_else(ApiError::not_found)?;
    let packet = SignedPacket::from_bytes(&bytes).map_err(|_| ApiError::not_found())?;
    Ok((
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        )],
        packet.to_relay_payload(),
    )
        .into_response())
}

// ---------------------------------------------------------------- shares (FR-H4)

#[derive(Deserialize)]
struct ShareBody {
    share_id: String,
}

fn share_url(state: &State, share_id: &str) -> String {
    format!("{}s/{share_id}", state.public_url)
}

async fn publish_share(
    AxState(state): AxState<Shared>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult {
    let TokenOwner::Device { endpoint_id, .. } = caller(&state, &headers, None)? else {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "this operation needs a device token",
        ));
    };
    let share: ShareBody = parse_body(&body)?;
    if !util::is_share_id(&share.share_id) {
        return Err(ApiError::bad_request(
            "share_id must match ^s_[0-9a-f]{16}$",
        ));
    }
    match state
        .db
        .claim_share(&share.share_id, &endpoint_id, util::now())?
    {
        Claim::Taken => Err(ApiError::new(
            StatusCode::CONFLICT,
            "published by another device",
        )),
        Claim::Created | Claim::AlreadyYours => Ok(json_response(
            StatusCode::CREATED,
            json!({ "share_id": share.share_id, "url": share_url(&state, &share.share_id) }),
        )),
    }
}

async fn resolve_share(AxState(state): AxState<Shared>, Path(share_id): Path<String>) -> ApiResult {
    if !util::is_share_id(&share_id) {
        return Err(ApiError::not_found());
    }
    let host = state
        .db
        .share_host(&share_id)?
        .ok_or_else(ApiError::not_found)?;
    let relay_url = relay_url_of(&state, &host.endpoint_id)?;
    let token = util::new_token("dpg_");
    let now = util::now();
    let expires_at = now + RELAY_PASS_TTL_SECS;
    state
        .db
        .insert_pass(&util::hash_token(&token), &share_id, expires_at, now)?;
    Ok(json_response(
        StatusCode::OK,
        json!({
            "share_id": share_id,
            "endpoint_id": host.endpoint_id,
            "relay_url": relay_url,
            "relay_token": token,
            "relay_token_expires_at": expires_at,
        }),
    ))
}

async fn unpublish_share(
    AxState(state): AxState<Shared>,
    headers: HeaderMap,
    Path(share_id): Path<String>,
) -> ApiResult {
    let owner = caller(&state, &headers, None)?;
    if !util::is_share_id(&share_id) {
        return Err(ApiError::not_found());
    }
    let host = state
        .db
        .share_host(&share_id)?
        .ok_or_else(ApiError::not_found)?;
    let allowed = match &owner {
        TokenOwner::Account { account_id } => *account_id == host.account_id,
        TokenOwner::Device { endpoint_id, .. } => *endpoint_id == host.endpoint_id,
    };
    if !allowed {
        return Err(ApiError::not_found());
    }
    state.db.delete_share(&share_id)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

const SHARE_PAGE: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DarkPyonix share</title>
</head>
<body>
<main>
<h1>DarkPyonix shared notebook</h1>
<p>The ash viewer is not published on this hub yet. This page resolves the share
<code id="share">__SHARE_ID__</code>; the access token after <code>#</code> stays in your browser.</p>
<pre id="route"></pre>
</main>
<script>
fetch("/shares/__SHARE_ID__").then(function (r) { return r.json(); }).then(function (j) {
  document.getElementById("route").textContent = "host: " + j.endpoint_id + "\nrelay: " + j.relay_url;
});
</script>
</body>
</html>
"#;

const ASH_PLACEHOLDER: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ash</title>
</head>
<body>
<main>
<h1>ash</h1>
<p>The official ash viewer will be served here.</p>
</main>
</body>
</html>
"#;

async fn share_page(AxState(state): AxState<Shared>, Path(share_id): Path<String>) -> ApiResult {
    if !util::is_share_id(&share_id) || state.db.share_host(&share_id)?.is_none() {
        return Ok(html(
            StatusCode::NOT_FOUND,
            "<!doctype html><p>Unknown share.</p>\n".to_string(),
        ));
    }
    // If the ash viewer is installed, the share link opens it; the fragment survives.
    if let Some(dir) = &state.ash_dir {
        if let Ok(index) = tokio::fs::read_to_string(dir.join("index.html")).await {
            return Ok(html(StatusCode::OK, index));
        }
    }
    Ok(html(
        StatusCode::OK,
        SHARE_PAGE.replace("__SHARE_ID__", &share_id),
    ))
}

async fn ash_index(AxState(state): AxState<Shared>) -> Response {
    if let Some(dir) = &state.ash_dir {
        if let Ok(index) = tokio::fs::read_to_string(dir.join("index.html")).await {
            return html(StatusCode::OK, index);
        }
    }
    html(StatusCode::OK, ASH_PLACEHOLDER.to_string())
}

fn content_type_for(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" | "map" => "application/json",
        "wasm" => "application/wasm",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    }
}

/// Static assets of the ash viewer (only when an ash directory is configured).
async fn ash_file(AxState(state): AxState<Shared>, Path(path): Path<String>) -> Response {
    let Some(dir) = &state.ash_dir else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let safe = !path.is_empty()
        && path
            .split('/')
            .all(|seg| !seg.is_empty() && seg != "." && seg != ".." && !seg.contains('\\'));
    if !safe {
        return StatusCode::NOT_FOUND.into_response();
    }
    match tokio::fs::read(dir.join(&path)).await {
        Ok(bytes) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static(content_type_for(&path)),
            )],
            Body::from(bytes),
        )
            .into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

// ---------------------------------------------------------------- names (FR-H5)

async fn list_names(AxState(state): AxState<Shared>, headers: HeaderMap) -> ApiResult {
    let owner = caller(&state, &headers, None)?;
    let names: Vec<Value> = state
        .db
        .list_names(account_of(&owner))?
        .iter()
        .map(|n| name_json(&state, n))
        .collect();
    Ok(json_response(StatusCode::OK, json!({ "names": names })))
}

async fn reserve_name(
    AxState(state): AxState<Shared>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult {
    let TokenOwner::Device {
        account_id,
        endpoint_id,
        role,
    } = caller(&state, &headers, None)?
    else {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "this operation needs a device token",
        ));
    };
    if !util::is_valid_name(&name) {
        return Err(ApiError::bad_request("malformed or reserved name"));
    }
    if role != "main_server" {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "only a main server can hold a name",
        ));
    }
    let status = match state
        .db
        .claim_name(&name, &endpoint_id, &account_id, util::now())?
    {
        Claim::Taken => return Err(ApiError::new(StatusCode::CONFLICT, "taken")),
        Claim::AlreadyYours => StatusCode::OK,
        Claim::Created => StatusCode::CREATED,
    };
    let row = NameRow {
        name,
        endpoint_id,
        account_id,
    };
    Ok(json_response(status, name_json(&state, &row)))
}

/// The name if the caller may manage it: the owning device, or (for release) its account.
fn owned_name(
    state: &State,
    owner: &TokenOwner,
    name: &str,
    account_may: bool,
) -> Result<NameRow, ApiError> {
    let row = state.db.name(name)?.ok_or_else(ApiError::not_found)?;
    let allowed = match owner {
        TokenOwner::Device { endpoint_id, .. } => *endpoint_id == row.endpoint_id,
        TokenOwner::Account { account_id } => account_may && *account_id == row.account_id,
    };
    if allowed {
        Ok(row)
    } else {
        Err(ApiError::not_found())
    }
}

async fn release_name(
    AxState(state): AxState<Shared>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult {
    let owner = caller(&state, &headers, None)?;
    let row = owned_name(&state, &owner, &name, true)?;
    state.db.delete_name(&row.name)?;
    let fqdn = acme_fqdn(&state, &row.name);
    if let Err(err) = state.dns.clear_txt(&fqdn).await {
        warn!(%err, %fqdn, "could not clear challenge records of a released name");
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Deserialize)]
struct AcmeBody {
    values: Vec<String>,
}

async fn set_acme_challenge(
    AxState(state): AxState<Shared>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult {
    let owner = caller(&state, &headers, None)?;
    if matches!(owner, TokenOwner::Account { .. }) {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "this operation needs a device token",
        ));
    }
    let row = owned_name(&state, &owner, &name, false)?;
    let acme: AcmeBody = parse_body(&body)?;
    if acme.values.is_empty()
        || acme.values.len() > 4
        || !acme.values.iter().all(|v| util::is_acme_txt_value(v))
    {
        return Err(ApiError::bad_request(
            "values must be 1 to 4 base64url SHA-256 digests",
        ));
    }
    let fqdn = acme_fqdn(&state, &row.name);
    state
        .dns
        .set_txt(&fqdn, &acme.values)
        .await
        .map_err(|err| ApiError::new(StatusCode::BAD_GATEWAY, err.to_string()))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn clear_acme_challenge(
    AxState(state): AxState<Shared>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult {
    let owner = caller(&state, &headers, None)?;
    if matches!(owner, TokenOwner::Account { .. }) {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "this operation needs a device token",
        ));
    }
    let row = owned_name(&state, &owner, &name, false)?;
    let fqdn = acme_fqdn(&state, &row.name);
    state
        .dns
        .clear_txt(&fqdn)
        .await
        .map_err(|err| ApiError::new(StatusCode::BAD_GATEWAY, err.to_string()))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
