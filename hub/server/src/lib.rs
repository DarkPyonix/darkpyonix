//! darkpyonix.dev, the DarkPyonix hub (SPEC §10, docs/api/hub.openapi.yaml).
//!
//! One process serves the device registry and address directory (FR-H1, FR-H2), the iroh
//! relay with its probes and QUIC address discovery (FR-H3), share resolution and the ash
//! viewer (FR-H4), and HTTPS names (FR-H5, Draft).

mod api;
mod db;
pub mod dns;
mod serve;
mod state;
mod util;

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use iroh_relay::{
    defaults::DEFAULT_KEY_CACHE_CAPACITY,
    server::{Handlers, Metrics, QuicConfig, RelayService, Server as QadServer, ServerConfig},
    KeyCache,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_rustls::TlsAcceptor;
use url::Url;

pub use crate::dns::{DnsError, DnsProvider, MemoryDns};

/// A certificate chain and its private key for the hub's own TLS.
#[derive(Debug)]
pub struct TlsCert {
    pub chain: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
}

/// How to run a hub.
pub struct HubConfig {
    /// SQLite database file.
    pub db_path: PathBuf,
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
    /// DNS zone for names, e.g. `darkpyonix.dev`.
    pub zone: String,
    /// If set, `POST /v1/accounts` requires this in `X-Hub-Signup-Secret`.
    pub signup_secret: Option<String>,
    /// Directory with the built ash viewer, served under `/ash/`.
    pub ash_dir: Option<PathBuf>,
    /// Where ACME challenge TXT records go.
    pub dns: Arc<dyn DnsProvider>,
}

/// Errors starting a hub.
#[derive(Debug, thiserror::Error)]
pub enum HubError {
    #[error("database: {0}")]
    Db(#[from] rusqlite::Error),
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
    if let Some(parent) = config.db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let db = db::Db::open(&config.db_path)?;

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

    let state = Arc::new(state::State::new(
        db,
        public_url.clone(),
        config.zone,
        config.signup_secret,
        config.ash_dir,
        config.dns,
    ));

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
            router: api::side_router(state.clone()),
            relay: None,
        };
        tasks.push(tokio::spawn(serve::run(side_listener, None, front)));
    }

    let front = serve::Frontend {
        router: api::router(state.clone()),
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
