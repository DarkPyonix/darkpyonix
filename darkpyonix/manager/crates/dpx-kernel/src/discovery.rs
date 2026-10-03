//! Manager side of PROTOCOL §2: loopback UDP multicast query/announce/bye, merged with the
//! registry (FR-D1, FR-D2). A background listener keeps a live cache of unsolicited
//! announces and byes so that `list` without `refresh` costs no network round trip.

use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::Notify;

use crate::process::pid_alive;
use crate::registry::{self, entry_pid, Entry};

pub const GROUP: Ipv4Addr = Ipv4Addr::new(239, 255, 68, 80);
pub const PORT: u16 = 46880;
pub const INTERFACE: Ipv4Addr = Ipv4Addr::LOCALHOST;
pub const QUERY_TIMEOUT: Duration = Duration::from_millis(200);
pub const STALE_AFTER: Duration = Duration::from_secs(15);
const MAX_DATAGRAM: usize = 65507;

struct Cached {
    entry: Entry,
    seen: Instant,
}

pub struct Discovery {
    user_tag: String,
    kernels_dir: PathBuf,
    multicast: bool,
    cache: Mutex<HashMap<String, Cached>>,
    listening: Mutex<bool>,
    queried: Mutex<bool>,
    /// Woken on every announce/bye seen by the listener.
    pub changed: Notify,
}

fn set_send_options(sock: &Socket) -> std::io::Result<()> {
    sock.set_multicast_if_v4(&INTERFACE)?;
    sock.set_multicast_ttl_v4(0)?;
    sock.set_multicast_loop_v4(true)?;
    Ok(())
}

/// A socket that receives datagrams sent to the discovery group on loopback (§2.1).
fn open_group_socket() -> std::io::Result<UdpSocket> {
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_reuse_address(true)?;
    #[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
    let _ = sock.set_reuse_port(true);
    // Binding the group address (POSIX) keeps plain unicast to the port out; Windows
    // cannot bind a multicast address and binds the wildcard instead.
    let group = SocketAddr::V4(SocketAddrV4::new(GROUP, PORT));
    let any = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, PORT));
    if cfg!(windows) || sock.bind(&group.into()).is_err() {
        sock.bind(&any.into())?;
    }
    sock.join_multicast_v4(&GROUP, &INTERFACE)?;
    set_send_options(&sock)?;
    sock.set_nonblocking(true)?;
    UdpSocket::from_std(sock.into())
}

/// An ephemeral loopback socket that sends to the group and receives unicast answers.
fn open_send_socket() -> std::io::Result<UdpSocket> {
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    set_send_options(&sock)?;
    sock.bind(&SocketAddr::V4(SocketAddrV4::new(INTERFACE, 0)).into())?;
    sock.set_nonblocking(true)?;
    UdpSocket::from_std(sock.into())
}

fn decode(data: &[u8]) -> Option<Entry> {
    match serde_json::from_slice::<Value>(data).ok()? {
        Value::Object(m) if m.get("dkp").and_then(Value::as_u64) == Some(1) => Some(m),
        _ => None,
    }
}

fn str_field<'a>(e: &'a Entry, k: &str) -> Option<&'a str> {
    e.get(k).and_then(Value::as_str)
}

impl Discovery {
    pub fn new(user_tag: String, kernels_dir: PathBuf, multicast: bool) -> Arc<Self> {
        Arc::new(Self {
            user_tag,
            kernels_dir,
            multicast,
            cache: Mutex::new(HashMap::new()),
            listening: Mutex::new(false),
            queried: Mutex::new(false),
            changed: Notify::new(),
        })
    }

    pub fn multicast(&self) -> bool {
        self.multicast
    }

    /// Start the background listener once (needs a tokio runtime).
    pub fn ensure_listening(self: &Arc<Self>) {
        if !self.multicast {
            return;
        }
        let mut started = self.listening.lock().unwrap();
        if *started {
            return;
        }
        *started = true;
        let sock = match open_group_socket() {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("multicast unavailable ({e}); not listening");
                return;
            }
        };
        let weak = Arc::downgrade(self);
        tokio::spawn(listen(weak, sock));
    }

    fn remember(&self, mut entry: Entry) {
        entry.remove("nonce");
        let Some(kid) = str_field(&entry, "kernel_id").map(str::to_string) else {
            return;
        };
        self.cache.lock().unwrap().insert(
            kid,
            Cached {
                entry,
                seen: Instant::now(),
            },
        );
    }

    fn handle_datagram(&self, data: &[u8]) {
        let Some(msg) = decode(data) else { return };
        if str_field(&msg, "user_tag") != Some(self.user_tag.as_str()) {
            return;
        }
        match str_field(&msg, "op") {
            Some("announce") => self.remember(msg),
            Some("bye") => {
                if let Some(kid) = str_field(&msg, "kernel_id") {
                    self.cache.lock().unwrap().remove(kid);
                }
            }
            _ => return,
        }
        self.changed.notify_waiters();
    }

    /// Drop a kernel from the live cache only (the registry is left alone).
    pub fn forget_cached(&self, kernel_id: &str) {
        self.cache.lock().unwrap().remove(kernel_id);
    }

    /// Forget a kernel (after a force kill).
    pub fn forget(&self, kernel_id: &str) {
        self.cache.lock().unwrap().remove(kernel_id);
        let _ = std::fs::remove_file(self.kernels_dir.join(format!("{kernel_id}.json")));
    }

    /// Registry (pruned) merged with the live cache; a fresh announce beats the file.
    pub fn snapshot(&self, kernel_id: Option<&str>) -> Vec<Entry> {
        let mut merged: HashMap<String, Entry> = HashMap::new();
        match kernel_id {
            Some(kid) => {
                if let Some(e) = registry::read(&self.kernels_dir, &self.user_tag, kid) {
                    merged.insert(kid.to_string(), e);
                }
            }
            None => {
                for e in registry::scan(&self.kernels_dir, &self.user_tag, true) {
                    if let Some(kid) = str_field(&e, "kernel_id") {
                        merged.insert(kid.to_string(), e.clone());
                    }
                }
            }
        }
        {
            let mut cache = self.cache.lock().unwrap();
            cache.retain(|_, c| c.seen.elapsed() < STALE_AFTER && pid_alive(entry_pid(&c.entry)));
            for (kid, c) in cache.iter() {
                if kernel_id.is_none_or(|k| k == kid) {
                    merged.insert(kid.clone(), c.entry.clone());
                }
            }
        }
        let mut out: Vec<Entry> = merged.into_values().collect();
        out.sort_by(|a, b| str_field(a, "kernel_id").cmp(&str_field(b, "kernel_id")));
        out
    }

    /// Send one query and collect answers for up to `timeout`. Returns early once every id in
    /// `expect` (and at least one kernel) has answered. Answers update the cache.
    pub async fn query(
        &self,
        kernel_id: Option<&str>,
        timeout: Duration,
        expect: &HashSet<String>,
    ) -> usize {
        if !self.multicast {
            return 0;
        }
        let nonce = crate::home::random_hex(8);
        let mut msg = json!({"dkp": 1, "op": "query", "user_tag": self.user_tag, "nonce": nonce});
        if let Some(kid) = kernel_id {
            msg["kernel_id"] = json!(kid);
        }
        let sock = match open_send_socket() {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("multicast unavailable ({e})");
                return 0;
            }
        };
        let deadline = tokio::time::Instant::now() + timeout;
        let group = SocketAddr::V4(SocketAddrV4::new(GROUP, PORT));
        if let Err(e) = sock
            .send_to(&serde_json::to_vec(&msg).unwrap(), group)
            .await
        {
            tracing::warn!("discovery query failed: {e}");
            return 0;
        }
        *self.queried.lock().unwrap() = true;
        let mut answered: HashSet<String> = HashSet::new();
        let mut buf = vec![0u8; MAX_DATAGRAM];
        loop {
            let done = match kernel_id {
                Some(k) => answered.contains(k),
                None => !answered.is_empty() && !expect.is_empty() && expect.is_subset(&answered),
            };
            if done {
                break;
            }
            let n = match tokio::time::timeout_at(deadline, sock.recv_from(&mut buf)).await {
                Ok(Ok((n, _))) => n,
                Ok(Err(_)) => continue,
                Err(_) => break,
            };
            let Some(reply) = decode(&buf[..n]) else {
                continue;
            };
            if str_field(&reply, "op") != Some("announce")
                || str_field(&reply, "nonce") != Some(nonce.as_str())
                || str_field(&reply, "user_tag") != Some(self.user_tag.as_str())
            {
                continue;
            }
            let Some(kid) = str_field(&reply, "kernel_id").map(str::to_string) else {
                continue;
            };
            if kernel_id.is_some_and(|k| k != kid) || !pid_alive(entry_pid(&reply)) {
                continue;
            }
            self.remember(reply);
            answered.insert(kid);
        }
        answered.len()
    }

    /// FR-D1/D2: live kernels of this user. Queries the group when `refresh` is set or no
    /// query was ever made; otherwise relies on the listener's cache and the registry.
    pub async fn discover(self: &Arc<Self>, refresh: bool) -> Vec<Entry> {
        self.ensure_listening();
        let queried = *self.queried.lock().unwrap();
        if self.multicast && (refresh || !queried) {
            let known: HashSet<String> = self
                .snapshot(None)
                .iter()
                .filter_map(|e| str_field(e, "kernel_id").map(str::to_string))
                .collect();
            self.query(None, QUERY_TIMEOUT, &known).await;
        }
        self.snapshot(None)
    }

    /// One kernel: cache/registry first, then a targeted query.
    pub async fn find(self: &Arc<Self>, kernel_id: &str) -> Option<Entry> {
        self.ensure_listening();
        if let Some(e) = self.snapshot(Some(kernel_id)).pop() {
            return Some(e);
        }
        if self.multicast {
            self.query(Some(kernel_id), QUERY_TIMEOUT, &HashSet::new())
                .await;
        }
        self.snapshot(Some(kernel_id)).pop()
    }
}

async fn listen(weak: Weak<Discovery>, sock: UdpSocket) {
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        let got = tokio::time::timeout(Duration::from_secs(1), sock.recv_from(&mut buf)).await;
        let Some(disc) = weak.upgrade() else { return };
        match got {
            Ok(Ok((n, _))) => disc.handle_datagram(&buf[..n]),
            Ok(Err(e)) => {
                tracing::debug!("discovery listener: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(_) => {}
        }
    }
}
