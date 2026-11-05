//! darkpyonix.dev, the DarkPyonix hub (SPEC §10, docs/api/hub.openapi.yaml).
//!
//! **Partly superseded (INTENT D15, 2026-10-03).** The hub API now runs on Cloudflare
//! Workers (`darkpyonix/hub/worker/`): GitHub accounts and device links (FR-H6, FR-H1), the address
//! directory (FR-H2), shares (FR-H4) and names (FR-H5). This crate stays as the **relay
//! host** at `relay.darkpyonix.dev`: the iroh relay with its probes and QUIC address
//! discovery on UDP 7842 (FR-H3), which Workers cannot serve.
//!
mod routes;
mod serve;
mod state;

use std::{net::SocketAddr, sync::Arc};

use iroh_relay::{
    defaults::DEFAULT_KEY_CACHE_CAPACITY,
    server::{Handlers, Metrics, QuicConfig, RelayService, Server as QadServer, ServerConfig},
    KeyCache,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_rustls::TlsAcceptor;
use url::Url;


/// A certificate chain and its private key for the hub's own TLS.
#[derive(Debug)]
pub struct TlsCert {
    pub chain: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
}

/// How to run a hub.
pub struct HubConfig {
    /// Main listener: HTTPS when `tls` is set, plain HTTP otherwise (development, tests).
    pub listen: SocketAddr,
    /// TLS for the main listener and for QUIC address discovery.
    pub tls: Option<TlsCert>,
    /// Plain-HTTP side port (normally 80) when `tls` is set: captive-portal probe, health,
    /// redirect to HTTPS.
    pub http_listen: Option<SocketAddr>,
    /// UDP address for iroh QUIC address discovery (normally port 7842). Needs `tls`.
    pub qad_listen: Option<SocketAddr>,
    /// The URL the hub is reached at, e.g. `https://darkpyonix.dev`. Defaults to the main
    /// listener's address (`http(s)://<ip>:<port>`).
    pub public_url: Option<Url>,
    /// Worker API base URL.
    pub api_url: Url,
    /// Shared secret for relay callbacks and administrative requests.
    pub relay_shared_secret: String,
}

/// Errors starting a hub.
#[derive(Debug, thiserror::Error)]
pub enum HubError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls: {0}")]
    Tls(#[from] rustls::Error),
    #[error("public url: {0}")]
    Url(#[from] url::ParseError),
    #[error("quic address discovery: {0}")]
    Qad(String),
    #[error("configuration: {0}")]
    Config(String),
}

/// A running hub. Dropping it stops every listener.
pub struct RunningHub {
    addr: SocketAddr,
    http_addr: Option<SocketAddr>,
    qad_addr: Option<SocketAddr>,
    public_url: Url,
    tasks: Vec<JoinHandle<()>>,
    _qad: Option<QadServer>,
}

impl RunningHub {
    /// The main listener's address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The plain-HTTP side listener's address, if any.
    pub fn http_addr(&self) -> Option<SocketAddr> {
        self.http_addr
    }

    /// The QUIC address discovery socket, if any.
    pub fn qad_addr(&self) -> Option<SocketAddr> {
        self.qad_addr
    }

    /// The hub's public URL; also its relay URL.
    pub fn public_url(&self) -> &Url {
        &self.public_url
    }
}

impl Drop for RunningHub {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

fn server_tls_config(cert: TlsCert) -> Result<rustls::ServerConfig, HubError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(cert.chain, cert.key)?;
    Ok(config)
}

/// Starts a hub and returns once every listener is bound.
pub async fn start(config: HubConfig) -> Result<RunningHub, HubError> {
    if config.qad_listen.is_some() && config.tls.is_none() {
        return Err(HubError::Config(
            "QUIC address discovery needs a TLS certificate".to_string(),
        ));
    }
    if config.relay_shared_secret.is_empty() || !matches!(config.api_url.scheme(), "http" | "https") || config.api_url.host().is_none() {
        return Err(HubError::Config("a valid API URL and nonempty relay secret are required".to_owned()));
    }
    let listener = TcpListener::bind(config.listen).await?;
    let addr = listener.local_addr()?;
    let public_url = match config.public_url {
        Some(url) => url,
        None => {
            let scheme = if config.tls.is_some() {
                "https"
            } else {
                "http"
            };
            Url::parse(&format!("{scheme}://{addr}"))?
        }
    };

    let _ = rustls::crypto::ring::default_provider().install_default();
    let state = state::State::new(config.api_url, config.relay_shared_secret);

    let relay = RelayService::new(
        Handlers::default(),
        Default::default(),
        None,
        KeyCache::new(DEFAULT_KEY_CACHE_CAPACITY),
        Arc::new(state::HubAccess(state.clone())),
        Arc::new(Metrics::default()),
    );
    // Only fails if already set, which cannot happen here.
    let _ = state.relay.set(relay.clone());

    let mut tasks = Vec::new();
    let mut qad = None;
    let mut qad_addr = None;
    let mut http_addr = None;

    let acceptor = match config.tls {
        Some(cert) => {
            let base = server_tls_config(cert)?;
            if let Some(qad_listen) = config.qad_listen {
                // The QUIC server sets its own ALPN.
                let mut qad_config = ServerConfig::default();
                let mut quic = QuicConfig::new(qad_listen);
                quic.server_config = Some(base.clone());
                qad_config.quic = Some(quic);
                let server = QadServer::spawn(qad_config)
                    .await
                    .map_err(|err| HubError::Qad(err.to_string()))?;
                qad_addr = server.quic_addr();
                qad = Some(server);
            }
            let mut https = base;
            https.alpn_protocols = vec![b"http/1.1".to_vec()];
            Some(TlsAcceptor::from(Arc::new(https)))
        }
        None => None,
    };

    if let (Some(side), Some(_)) = (config.http_listen, acceptor.as_ref()) {
        let side_listener = TcpListener::bind(side).await?;
        http_addr = Some(side_listener.local_addr()?);
        let front = serve::Frontend {
            router: routes::side_router(state.clone(), public_url.clone()),
            relay: None,
        };
        tasks.push(tokio::spawn(serve::run(side_listener, None, front)));
    }

    let front = serve::Frontend {
        router: routes::router(state.clone()),
        relay: Some(relay),
    };
    tasks.push(tokio::spawn(serve::run(listener, acceptor, front)));

    Ok(RunningHub {
        addr,
        http_addr,
        qad_addr,
        public_url,
        tasks,
        _qad: qad,
    })
}
