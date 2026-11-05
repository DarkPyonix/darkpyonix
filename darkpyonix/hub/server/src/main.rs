//! `darkpyonix-hub`: the darkpyonix.dev server binary.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

use clap::Parser;
use darkpyonix_hub::{HubConfig, TlsCert};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use tracing::info;
use tracing_subscriber::EnvFilter;
use url::Url;

/// darkpyonix.dev: device registry, address directory, iroh relay, share links, HTTPS names.
#[derive(Debug, Parser)]
#[command(name = "darkpyonix-hub", version)]
struct Args {
    /// Main listener (HTTPS with --tls-cert, plain HTTP without).
    #[arg(long, env = "HUB_LISTEN", default_value = "0.0.0.0:443")]
    listen: SocketAddr,
    /// Plain-HTTP side listener used with TLS: captive-portal probe and redirect.
    #[arg(long, env = "HUB_HTTP_LISTEN")]
    http_listen: Option<SocketAddr>,
    /// UDP address for iroh QUIC address discovery (needs TLS), normally 0.0.0.0:7842.
    #[arg(long, env = "HUB_QAD_LISTEN")]
    qad_listen: Option<SocketAddr>,
    /// PEM certificate chain for the hub's own name.
    #[arg(long, env = "HUB_TLS_CERT", requires = "tls_key")]
    tls_cert: Option<PathBuf>,
    /// PEM private key for --tls-cert.
    #[arg(long, env = "HUB_TLS_KEY", requires = "tls_cert")]
    tls_key: Option<PathBuf>,
    /// Public URL, e.g. https://darkpyonix.dev
    #[arg(long, env = "HUB_PUBLIC_URL")]
    public_url: Option<Url>,
    /// Worker API base URL.
    #[arg(long, env = "API_URL", default_value = "https://api.darkpyonix.dev")]
    api_url: Url,
    /// Shared secret used for relay admission and administration.
    #[arg(long, env = "RELAY_SHARED_SECRET")]
    relay_shared_secret: String,
}

fn load_tls(cert: &Path, key: &Path) -> Result<TlsCert, Box<dyn std::error::Error>> {
    let chain = CertificateDer::pem_file_iter(cert)?.collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(key)?;
    Ok(TlsCert { chain, key })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();

    let tls = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => Some(load_tls(cert, key)?),
        _ => None,
    };
    let hub = darkpyonix_hub::start(HubConfig {
        listen: args.listen,
        tls,
        http_listen: args.http_listen,
        qad_listen: args.qad_listen,
        public_url: args.public_url,
        api_url: args.api_url,
        relay_shared_secret: args.relay_shared_secret,
    })
    .await?;
    info!(
        addr = %hub.addr(),
        http = ?hub.http_addr(),
        qad = ?hub.qad_addr(),
        url = %hub.public_url(),
        "darkpyonix-hub listening"
    );
    tokio::signal::ctrl_c().await?;
    info!("shutting down");
    Ok(())
}
