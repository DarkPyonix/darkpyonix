//! Test harness: a hub in this process, HTTP helpers, and real iroh endpoints.
//!
//! State goes under the repository's `.scratch/hub-tests/` (CLAUDE.md "Where files go").

#![allow(dead_code)]

use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use darkpyonix_hub::{HubConfig, MemoryDns, RunningHub, TlsCert};
use iroh::{
    address_lookup::{AddrFilter, PkarrPublisher, PkarrResolver},
    endpoint::{presets, Connection},
    tls::CaTlsConfig,
    Endpoint, EndpointAddr, EndpointId, RelayConfig, RelayMap, RelayMode, RelayUrl, SecretKey,
};
use iroh_relay::RelayQuicConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use serde_json::{json, Value};
use tokio::task::JoinHandle;
use url::Url;

pub const ALPN: &[u8] = b"darkpyonix/hub-test/1";

/// Stream mode bytes understood by [`spawn_server`].
pub const MODE_ECHO: u8 = b'E';
pub const MODE_SINK: u8 = b'S';

pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// A fresh directory under `<repo>/.scratch/hub-tests/`.
pub fn scratch_dir(name: &str) -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.scratch/hub-tests")
        .join(format!(
            "{name}-{}-{:08x}",
            std::process::id(),
            rand::random::<u32>()
        ));
    std::fs::create_dir_all(&root).expect("create scratch dir");
    root
}

pub struct TestHub {
    pub hub: RunningHub,
    /// Base URL without the trailing slash, e.g. `http://127.0.0.1:1234`.
    pub base: String,
    /// The relay URL endpoints use (the hub's public URL).
    pub relay_url: RelayUrl,
    pub client: reqwest::Client,
    pub dns: MemoryDns,
    pub root_cert: Option<CertificateDer<'static>>,
    pub dir: PathBuf,
}

#[derive(Default)]
pub struct HubOptions {
    pub tls: bool,
    pub signup_secret: Option<String>,
    /// Advertise this URL instead of the listener's (e.g. a recording proxy).
    pub public_url: Option<Url>,
}

pub async fn start_hub(name: &str) -> TestHub {
    start_hub_with(name, HubOptions::default()).await
}

pub async fn start_hub_with(name: &str, opts: HubOptions) -> TestHub {
    install_crypto_provider();
    let dir = scratch_dir(name);
    let dns = MemoryDns::default();
    let (tls, root_cert) = if opts.tls {
        let certified = rcgen::generate_simple_self_signed(vec![
            "127.0.0.1".to_string(),
            "localhost".to_string(),
        ])
        .expect("self-signed certificate");
        let cert = certified.cert.der().clone();
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            certified.signing_key.serialize_der(),
        ));
        (
            Some(TlsCert {
                chain: vec![cert.clone()],
                key,
            }),
            Some(cert),
        )
    } else {
        (None, None)
    };
    let qad_listen: Option<SocketAddr> = opts.tls.then(|| "127.0.0.1:0".parse().unwrap());
    let hub = darkpyonix_hub::start(HubConfig {
        db_path: dir.join("hub.db"),
        listen: "127.0.0.1:0".parse().unwrap(),
        tls,
        http_listen: None,
        qad_listen,
        public_url: opts.public_url,
        zone: "darkpyonix.test".to_string(),
        signup_secret: opts.signup_secret,
        ash_dir: None,
        dns: Arc::new(dns.clone()),
    })
    .await
    .expect("start hub");

    let scheme = if root_cert.is_some() { "https" } else { "http" };
    let base = format!("{scheme}://{}", hub.addr());
    let relay_url: RelayUrl = hub.public_url().as_str().parse().expect("relay url");

    let client = match &root_cert {
        Some(cert) => {
            let mut roots = rustls::RootCertStore::empty();
            roots.add(cert.clone()).expect("add root");
            let config = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .expect("tls versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
            reqwest::Client::builder()
                .tls_backend_preconfigured(config)
                .build()
                .expect("http client")
        }
        None => reqwest::Client::builder().build().expect("http client"),
    };

    TestHub {
        hub,
        base,
        relay_url,
        client,
        dns,
        root_cert,
        dir,
    }
}

impl TestHub {
    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    pub fn ca_tls(&self) -> CaTlsConfig {
        match &self.root_cert {
            Some(cert) => CaTlsConfig::custom_roots([cert.clone()]),
            None => CaTlsConfig::embedded(),
        }
    }

    /// The hub's relay as iroh relay configuration, with QUIC address discovery when the
    /// hub runs it.
    pub fn relay_config(&self) -> RelayConfig {
        let quic = self.hub.qad_addr().map(|a| RelayQuicConfig::new(a.port()));
        RelayConfig::new(self.relay_url.clone(), quic)
    }

    /// `(account_id, account_token)`
    pub async fn create_account(&self) -> (String, String) {
        let res = self
            .client
            .post(self.url("/v1/accounts"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 201);
        let body: Value = res.json().await.unwrap();
        (
            body["account_id"].as_str().unwrap().to_string(),
            body["account_token"].as_str().unwrap().to_string(),
        )
    }

    pub async fn challenge(&self, account_token: &str) -> String {
        let res = self
            .client
            .post(self.url("/v1/challenges"))
            .bearer_auth(account_token)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 201);
        let body: Value = res.json().await.unwrap();
        body["challenge"].as_str().unwrap().to_string()
    }

    /// Registers `key` and returns the raw response.
    pub async fn register_raw(
        &self,
        account_token: &str,
        endpoint_id: &str,
        challenge: &str,
        signature_hex: &str,
        role: &str,
    ) -> reqwest::Response {
        self.client
            .post(self.url("/v1/devices"))
            .bearer_auth(account_token)
            .json(&json!({
                "endpoint_id": endpoint_id,
                "name": "test device",
                "role": role,
                "challenge": challenge,
                "signature": signature_hex,
            }))
            .send()
            .await
            .unwrap()
    }

    /// Registers the endpoint key and returns its device token.
    pub async fn register(
        &self,
        account: &(String, String),
        key: &SecretKey,
        role: &str,
    ) -> String {
        let (account_id, account_token) = account;
        let challenge = self.challenge(account_token).await;
        let signature = sign_registration(key, account_id, &challenge);
        let res = self
            .register_raw(
                account_token,
                &key.public().to_string(),
                &challenge,
                &signature,
                role,
            )
            .await;
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        assert_eq!(status, 201, "register: {text}");
        let body: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(body["device"]["endpoint_id"], key.public().to_string());
        body["device_token"].as_str().unwrap().to_string()
    }

    pub async fn get_json(&self, path: &str, token: &str) -> (u16, Value) {
        let res = self
            .client
            .get(self.url(path))
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        let status = res.status().as_u16();
        let body = res.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    /// `true` once the device shows `online` (connected to this hub's relay).
    pub async fn wait_online(&self, token: &str, endpoint_id: EndpointId, want: bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            let (status, body) = self
                .get_json(&format!("/v1/devices/{endpoint_id}"), token)
                .await;
            if status == 200 && body["online"].as_bool() == Some(want) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    /// Waits until the device's published record matches `pred`; returns it.
    pub async fn wait_record(
        &self,
        token: &str,
        endpoint_id: EndpointId,
        pred: impl Fn(&Value) -> bool,
    ) -> Value {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let (status, body) = self
                .get_json(&format!("/v1/devices/{endpoint_id}/addresses"), token)
                .await;
            if status == 200 && pred(&body) {
                return body;
            }
            assert!(
                Instant::now() < deadline,
                "no matching record for {endpoint_id}: {status} {body}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

pub fn sign_registration(key: &SecretKey, account_id: &str, challenge: &str) -> String {
    let message = format!("darkpyonix-hub/v1/register\n{account_id}\n{challenge}");
    hex::encode(key.sign(message.as_bytes()).to_bytes())
}

/// How a test endpoint reaches the network.
pub struct EndpointOptions {
    /// Use this relay (`None`: relay disabled).
    pub relay: Option<RelayConfig>,
    /// Bind an IP socket on 127.0.0.1 (`false`: relay only).
    pub loopback_ip: bool,
    /// Publish to and resolve from the hub's pkarr directory with this device token.
    pub directory_token: Option<String>,
}

pub async fn endpoint(hub: &TestHub, key: SecretKey, opts: EndpointOptions) -> Endpoint {
    let mut builder = Endpoint::builder(presets::Minimal)
        .secret_key(key)
        .alpns(vec![ALPN.to_vec()])
        .ca_tls_config(hub.ca_tls())
        .clear_ip_transports();
    builder = match opts.relay {
        Some(config) => builder.relay_mode(RelayMode::Custom(RelayMap::from(config))),
        None => builder.relay_mode(RelayMode::Disabled),
    };
    if opts.loopback_ip {
        builder = builder.bind_addr("127.0.0.1:0").expect("bind addr");
    }
    if let Some(token) = opts.directory_token {
        let url: Url = format!("{}/pkarr?token={token}", hub.base).parse().unwrap();
        builder = builder
            .address_lookup(
                PkarrPublisher::builder(url.clone()).addr_filter(AddrFilter::unfiltered()),
            )
            .address_lookup(PkarrResolver::builder(url));
    }
    builder.bind().await.expect("bind endpoint")
}

/// Accepts connections; each bi stream starts with a mode byte: [`MODE_ECHO`] echoes
/// everything back, [`MODE_SINK`] reads to the end and answers the byte count (u64 BE).
pub fn spawn_server(ep: Endpoint) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let Ok(conn) = incoming.await else { continue };
            tokio::spawn(async move {
                while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                    tokio::spawn(async move {
                        let mut mode = [0u8; 1];
                        if recv.read_exact(&mut mode).await.is_err() {
                            return;
                        }
                        let mut buf = vec![0u8; 64 * 1024];
                        let mut total: u64 = 0;
                        loop {
                            match recv.read(&mut buf).await {
                                Ok(Some(n)) => {
                                    total += n as u64;
                                    if mode[0] == MODE_ECHO
                                        && send.write_all(&buf[..n]).await.is_err()
                                    {
                                        return;
                                    }
                                }
                                Ok(None) => break,
                                Err(_) => return,
                            }
                        }
                        if mode[0] == MODE_SINK {
                            let _ = send.write_all(&total.to_be_bytes()).await;
                        }
                        let _ = send.finish();
                    });
                }
            });
        }
    })
}

/// Sends `payload` on an echo stream and returns what came back.
pub async fn echo(conn: &Connection, payload: &[u8]) -> Vec<u8> {
    let (mut send, mut recv) = conn.open_bi().await.expect("open stream");
    send.write_all(&[MODE_ECHO]).await.unwrap();
    send.write_all(payload).await.unwrap();
    send.finish().unwrap();
    recv.read_to_end(payload.len() + 1024)
        .await
        .expect("read echo")
}

/// Waits until the connection's selected path satisfies `pred` (`is_relay` / `is_ip`).
pub async fn wait_selected_path(conn: &Connection, relay: bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let selected = conn
            .paths()
            .iter()
            .any(|p| p.is_selected() && if relay { p.is_relay() } else { p.is_ip() });
        if selected {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Round-trip times of `n` 32-byte ping-pongs on one stream, sorted, in microseconds.
pub async fn measure_rtts(conn: &Connection, n: usize) -> Vec<u128> {
    let (mut send, mut recv) = conn.open_bi().await.expect("open stream");
    send.write_all(&[MODE_ECHO]).await.unwrap();
    let ping = [7u8; 32];
    let mut pong = [0u8; 32];
    let mut rtts = Vec::with_capacity(n);
    for _ in 0..n {
        let start = Instant::now();
        send.write_all(&ping).await.unwrap();
        recv.read_exact(&mut pong).await.unwrap();
        rtts.push(start.elapsed().as_micros());
    }
    send.finish().unwrap();
    rtts.sort_unstable();
    rtts
}

/// Sends `bytes` on a sink stream; returns throughput in MiB/s.
pub async fn measure_throughput(conn: &Connection, bytes: usize) -> f64 {
    let (mut send, mut recv) = conn.open_bi().await.expect("open stream");
    let chunk = vec![0x5au8; 64 * 1024];
    let start = Instant::now();
    send.write_all(&[MODE_SINK]).await.unwrap();
    let mut sent = 0;
    while sent < bytes {
        let n = chunk.len().min(bytes - sent);
        send.write_all(&chunk[..n]).await.unwrap();
        sent += n;
    }
    send.finish().unwrap();
    let ack = recv.read_to_end(8).await.expect("read count");
    let elapsed = start.elapsed().as_secs_f64();
    let count = u64::from_be_bytes(ack.as_slice().try_into().expect("8 bytes"));
    assert_eq!(count as usize, bytes);
    bytes as f64 / (1024.0 * 1024.0) / elapsed
}

pub fn percentile(sorted: &[u128], p: f64) -> u128 {
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

/// Address to dial a peer through a relay only.
pub fn via_relay(id: EndpointId, relay: &RelayUrl) -> EndpointAddr {
    EndpointAddr::new(id).with_relay_url(relay.clone())
}
