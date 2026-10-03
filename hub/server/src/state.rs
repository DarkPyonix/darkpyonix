//! Shared hub state and the relay access policy (SPEC FR-H3).

use std::{
    collections::{HashMap, HashSet},
    fmt,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
};

use iroh_base::EndpointId;
use iroh_relay::server::{Access, AccessControl, ClientRequest, ConnectionId, RelayService};
use tracing::{debug, warn};
use url::Url;

use crate::{db::Db, dns::DnsProvider, util};

pub(crate) struct State {
    pub db: Db,
    /// `https://darkpyonix.dev/` (with the trailing slash `Url` gives it).
    pub public_url: Url,
    /// `darkpyonix.dev`
    pub zone: String,
    pub signup_secret: Option<String>,
    pub ash_dir: Option<PathBuf>,
    pub dns: Arc<dyn DnsProvider>,
    /// Set once the relay service exists; used to drop a removed device's connections.
    pub relay: OnceLock<RelayService>,
    /// Relay connections per endpoint id (hex).
    online: Mutex<HashMap<String, HashSet<ConnectionId>>>,
}

impl State {
    pub(crate) fn new(
        db: Db,
        public_url: Url,
        zone: String,
        signup_secret: Option<String>,
        ash_dir: Option<PathBuf>,
        dns: Arc<dyn DnsProvider>,
    ) -> Self {
        Self {
            db,
            public_url,
            zone,
            signup_secret,
            ash_dir,
            dns,
            relay: OnceLock::new(),
            online: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn is_online(&self, endpoint_id: &str) -> bool {
        self.online
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(endpoint_id)
            .is_some_and(|set| !set.is_empty())
    }

    fn connected(&self, endpoint_id: &str, connection: ConnectionId) {
        self.online
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(endpoint_id.to_string())
            .or_default()
            .insert(connection);
    }

    fn disconnected(&self, endpoint_id: &str, connection: ConnectionId) {
        let mut online = self.online.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(set) = online.get_mut(endpoint_id) {
            set.remove(&connection);
            if set.is_empty() {
                online.remove(endpoint_id);
            }
        }
    }

    /// Drops every relay connection of `endpoint_id` (used when a device is removed).
    pub(crate) fn disconnect_from_relay(&self, endpoint_id: EndpointId) {
        if let Some(relay) = self.relay.get() {
            relay.clients().disconnect(endpoint_id, None);
        }
    }

    /// Whether the relay admits this endpoint: an active registered device, or anyone with
    /// an unexpired guest relay pass.
    fn admits(&self, endpoint_hex: &str, auth_token: Option<String>) -> bool {
        match self.db.device_active(endpoint_hex) {
            Ok(true) => return true,
            Ok(false) => {}
            Err(err) => {
                warn!(%err, "relay access check failed");
                return false;
            }
        }
        match auth_token {
            Some(token) => self
                .db
                .pass_valid(&util::hash_token(&token), util::now())
                .unwrap_or(false),
            None => false,
        }
    }
}

/// The relay's access policy (SPEC FR-H3).
#[derive(Clone)]
pub(crate) struct HubAccess(pub(crate) Arc<State>);

impl fmt::Debug for HubAccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HubAccess")
    }
}

impl AccessControl for HubAccess {
    async fn on_connect(&self, request: &ClientRequest) -> Access {
        let endpoint_hex = request.endpoint_id().to_string();
        if self.0.admits(&endpoint_hex, request.auth_token()) {
            debug!(endpoint = %endpoint_hex, "relay: admitted");
            self.0.connected(&endpoint_hex, request.connection_id());
            let _ = self.0.db.touch_device(&endpoint_hex, util::now());
            Access::Allow
        } else {
            debug!(endpoint = %endpoint_hex, "relay: denied");
            Access::Deny {
                reason: Some("not a registered device and no valid relay pass".to_string()),
            }
        }
    }

    fn on_disconnect(&self, endpoint_id: EndpointId, connection_id: ConnectionId) {
        let endpoint_hex = endpoint_id.to_string();
        self.0.disconnected(&endpoint_hex, connection_id);
        let _ = self.0.db.touch_device(&endpoint_hex, util::now());
    }
}
