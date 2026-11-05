#![allow(dead_code)]
use std::{sync::{Arc, Mutex}, time::Duration};
use axum::{extract::State, http::{HeaderMap, StatusCode}, routing::post, Json, Router};
use darkpyonix_hub::{HubConfig, RunningHub};
use iroh::{RelayUrl, SecretKey};
use iroh_relay::client::{Client, ClientBuilder};
use serde_json::{json, Value};
use tokio::task::JoinHandle;
use url::Url;

pub const SECRET: &str = "test-secret";
#[derive(Default)]
pub struct Calls { pub admissions: Vec<Value>, pub presence: Vec<Value> }
pub struct TestHub { pub hub: RunningHub, pub client: reqwest::Client, pub calls: Arc<Mutex<Calls>>, _worker: JoinHandle<()> }
impl TestHub { pub fn url(&self, path: &str) -> String { format!("http://{}{}", self.hub.addr(), path) } }
async fn admit(State(calls): State<Arc<Mutex<Calls>>>, headers: HeaderMap, Json(body): Json<Value>) -> (StatusCode, Json<Value>) {
    assert_eq!(headers.get("authorization").unwrap(), "Bearer test-secret");
    let guest = body["token"] == "guest-pass";
    let allow = body["token"].is_null() || guest;
    calls.lock().unwrap().admissions.push(body);
    (StatusCode::OK, Json(json!({"allow": allow, "cache_secs": if guest { 0 } else { 1 }})))
}
async fn presence(State(calls): State<Arc<Mutex<Calls>>>, headers: HeaderMap, Json(body): Json<Value>) -> StatusCode {
    assert_eq!(headers.get("authorization").unwrap(), "Bearer test-secret");
    calls.lock().unwrap().presence.push(body);
    StatusCode::NO_CONTENT
}
pub async fn start_hub(_: &str) -> TestHub {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let calls = Arc::new(Mutex::new(Calls::default()));
    let app = Router::new().route("/internal/relay/admit", post(admit)).route("/internal/relay/presence", post(presence)).with_state(calls.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let worker = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
    let hub = darkpyonix_hub::start(HubConfig { listen: "127.0.0.1:0".parse().unwrap(), tls: None, http_listen: None, qad_listen: None, public_url: None, api_url: Url::parse(&format!("http://{address}")).unwrap(), relay_shared_secret: SECRET.to_owned() }).await.unwrap();
    TestHub { hub, client: reqwest::Client::new(), calls, _worker: worker }
}
impl Drop for TestHub { fn drop(&mut self) { self._worker.abort(); } }
pub async fn connect(hub: &TestHub, key: SecretKey, token: Option<&str>) -> Result<Client, iroh_relay::client::ConnectError> {
    let url: RelayUrl = hub.hub.public_url().as_str().parse().unwrap();
    let config = iroh_relay::tls::CaTlsConfig::default().client_config(Arc::new(rustls::crypto::ring::default_provider())).unwrap();
    let mut builder = ClientBuilder::new(url, key, iroh_dns::dns::DnsResolver::default()).tls_client_config(config);
    if let Some(token) = token { builder = builder.auth_token(token); }
    builder.connect().await
}
pub async fn wait_for(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async { while !condition() { tokio::time::sleep(Duration::from_millis(20)).await; } }).await.unwrap();
}
