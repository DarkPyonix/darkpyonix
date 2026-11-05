//! Relay admission and presence (SPEC FR-H3).
use std::{collections::{HashMap, HashSet}, fmt, sync::{Arc, Mutex, OnceLock}, time::{Duration, Instant}};
use iroh_base::EndpointId;
use iroh_relay::server::{Access, AccessControl, ClientRequest, ConnectionId, RelayService};
use serde::Deserialize;
use tokio::sync::mpsc;
use url::Url;

pub(crate) struct State {
    client: reqwest::Client,
    api_url: Url,
    pub secret: String,
    pub relay: OnceLock<RelayService>,
    online: Mutex<HashMap<String, HashSet<ConnectionId>>>,
    cache: Mutex<HashMap<String, Instant>>,
    generations: Mutex<HashMap<String, u64>>,
    presence: mpsc::UnboundedSender<(String, bool)>,
}
#[derive(Deserialize)]
struct Admission { allow: bool, cache_secs: u64 }
impl State {
    pub fn new(api_url: Url, secret: String) -> Arc<Self> {
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().expect("HTTP client");
        let (tx, mut rx) = mpsc::unbounded_channel::<(String, bool)>();
        let worker_client = client.clone();
        let worker_url = api_url.join("/internal/relay/presence").expect("API URL");
        let worker_secret = secret.clone();
        tokio::spawn(async move {
            while let Some((endpoint_id, online)) = rx.recv().await {
                if let Err(err) = worker_client.post(worker_url.clone()).bearer_auth(&worker_secret)
                    .json(&serde_json::json!({"endpoint_id": endpoint_id, "online": online})).send().await {
                    tracing::warn!(%err, "presence report failed");
                }
            }
        });
        Arc::new(Self { client, api_url, secret, relay: OnceLock::new(), online: Mutex::new(HashMap::new()), cache: Mutex::new(HashMap::new()), generations: Mutex::new(HashMap::new()), presence: tx })
    }
    async fn admits(&self, endpoint: &str, token: Option<String>) -> bool {
        if self.cache.lock().unwrap().get(endpoint).is_some_and(|expiry| *expiry > Instant::now()) { return true; }
        let generation = *self.generations.lock().unwrap().get(endpoint).unwrap_or(&0);
        let result = self.client.post(self.api_url.join("/internal/relay/admit").expect("API URL"))
            .bearer_auth(&self.secret).json(&serde_json::json!({"endpoint_id": endpoint, "token": token}))
            .send().await;
        let response = match result { Ok(r) if r.status().is_success() => r, _ => return false };
        let admission: Admission = match response.json().await { Ok(a) => a, Err(_) => return false };
        let generations = self.generations.lock().unwrap();
        if *generations.get(endpoint).unwrap_or(&0) != generation { return false; }
        if admission.allow && admission.cache_secs > 0 {
            if let Some(expiry) = Instant::now().checked_add(Duration::from_secs(admission.cache_secs)) {
                self.cache.lock().unwrap().insert(endpoint.to_owned(), expiry);
            }
        }
        admission.allow
    }
    pub fn disconnect(&self, endpoint: EndpointId) {
        let endpoint_hex = endpoint.to_string();
        let mut generations = self.generations.lock().unwrap();
        *generations.entry(endpoint_hex.clone()).or_default() += 1;
        self.cache.lock().unwrap().remove(&endpoint_hex);
        if let Some(relay) = self.relay.get() { relay.clients().disconnect(endpoint, None); }
    }
    fn connected(&self, endpoint: &str, id: ConnectionId) {
        let mut online = self.online.lock().unwrap();
        let connections = online.entry(endpoint.to_owned()).or_default();
        let first = connections.is_empty();
        connections.insert(id);
        if first { let _ = self.presence.send((endpoint.to_owned(), true)); }
    }
    fn disconnected(&self, endpoint: &str, id: ConnectionId) {
        let mut online = self.online.lock().unwrap();
        if let Some(connections) = online.get_mut(endpoint) {
            if connections.remove(&id) && connections.is_empty() {
                online.remove(endpoint);
                let _ = self.presence.send((endpoint.to_owned(), false));
            }
        }
    }
}
#[derive(Clone)]
pub(crate) struct HubAccess(pub Arc<State>);
impl fmt::Debug for HubAccess { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("HubAccess") } }
impl AccessControl for HubAccess {
    async fn on_connect(&self, request: &ClientRequest) -> Access {
        let endpoint = request.endpoint_id().to_string();
        if self.0.admits(&endpoint, request.auth_token()).await {
            self.0.connected(&endpoint, request.connection_id());
            Access::Allow
        } else { Access::Deny { reason: Some("relay admission denied".to_owned()) } }
    }
    fn on_disconnect(&self, endpoint: EndpointId, id: ConnectionId) { self.0.disconnected(&endpoint.to_string(), id); }
}
