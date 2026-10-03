//! TLS through rustls (INTENT D10: no nginx). HTTP/2 is negotiated with ALPN.
//!
//! Certificates are served through a swappable resolver ([`CertStore`]) so that ACME
//! (automatic issuance and renewal for a dedicated manager) can replace the certificate
//! at run time without rebinding. ACME itself is not implemented yet.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::{ClientHello, ResolvesServerCert};
use tokio_rustls::rustls::sign::CertifiedKey;
use tokio_rustls::rustls::ServerConfig as RustlsConfig;
use tokio_rustls::TlsAcceptor;

#[derive(Debug, Clone)]
pub enum TlsConfig {
    /// A PEM certificate chain and its PEM private key (PKCS#8, PKCS#1 or SEC1).
    PemFiles { cert: PathBuf, key: PathBuf },
    /// Automatic certificates from an ACME directory (e.g. Let's Encrypt) for `domains`,
    /// cached in `cache_dir`. TODO: not implemented; `serve` refuses it for now.
    Acme { domains: Vec<String>, contact: Option<String>, cache_dir: PathBuf, directory_url: String },
}

/// The current certificate; ACME renewal will call [`CertStore::set`].
#[derive(Debug)]
pub struct CertStore {
    current: RwLock<Arc<CertifiedKey>>,
}

impl CertStore {
    pub fn set(&self, key: Arc<CertifiedKey>) {
        *self.current.write().unwrap_or_else(|p| p.into_inner()) = key;
    }
}

impl ResolvesServerCert for CertStore {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.current.read().unwrap_or_else(|p| p.into_inner()).clone())
    }
}

fn io_err(msg: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, msg.into())
}

pub fn certified_key_from_pem(cert_pem: &[u8], key_pem: &[u8]) -> std::io::Result<CertifiedKey> {
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut &cert_pem[..]).collect::<Result<_, _>>()?;
    if certs.is_empty() {
        return Err(io_err("TLS: no certificate in the PEM file"));
    }
    let key: PrivateKeyDer<'static> =
        rustls_pemfile::private_key(&mut &key_pem[..])?.ok_or_else(|| io_err("TLS: no private key in the PEM file"))?;
    let signing = ring::sign::any_supported_type(&key).map_err(|e| io_err(format!("TLS: {e}")))?;
    Ok(CertifiedKey::new(certs, signing))
}

/// Builds the acceptor and the certificate store behind it.
pub fn acceptor(cfg: &TlsConfig) -> std::io::Result<(TlsAcceptor, Arc<CertStore>)> {
    let key = match cfg {
        TlsConfig::PemFiles { cert, key } => certified_key_from_pem(&std::fs::read(cert)?, &std::fs::read(key)?)?,
        TlsConfig::Acme { .. } => {
            return Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "TLS: ACME is not implemented yet"))
        }
    };
    let store = Arc::new(CertStore { current: RwLock::new(Arc::new(key)) });
    let mut config = RustlsConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| io_err(format!("TLS: {e}")))?
        .with_no_client_auth()
        .with_cert_resolver(store.clone());
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok((TlsAcceptor::from(Arc::new(config)), store))
}
