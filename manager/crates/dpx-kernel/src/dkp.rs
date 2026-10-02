//! DKP/1 client (PROTOCOL §3): length-prefixed JSON frames, HMAC handshake, request/response
//! by id and event fan-out.
//!
//! One [`Connection`] per kernel is shared by every request and every event subscriber of
//! the manager (FR-M5). The connection subscribes to the kernel once with `since: 0` and
//! mirrors the kernel's replay ring, so each subscriber can start from its own `since`
//! without another kernel connection. Live events fan out over a `broadcast` channel; a
//! subscriber that lags behind the channel is resumed from the mirror (or told
//! `replay_truncated`), so a slow reader never blocks the others.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use dpx_core::{DpxError, EventStream, KernelEvent, Result};
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, oneshot};

pub const MAX_FRAME: usize = 64 * 1024 * 1024;
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub const RING_MAX_BYTES: usize = 16 * 1024 * 1024;

/// Tunables of the event mirror and fan-out.
#[derive(Debug, Clone, Copy)]
pub struct FanoutConfig {
    /// Events kept in the manager's mirror of the kernel ring (kernel default: 10,000).
    pub ring_max_events: usize,
    /// Per-subscriber backlog in the broadcast channel before it counts as lagging.
    pub channel_capacity: usize,
}

impl Default for FanoutConfig {
    fn default() -> Self {
        Self {
            ring_max_events: 10_000,
            channel_capacity: 4096,
        }
    }
}

fn unreachable(msg: impl Into<String>) -> DpxError {
    DpxError::new("kernel_unreachable", msg)
}

pub fn encode(msg: &Value) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(msg).map_err(|e| DpxError::new("internal", e.to_string()))?;
    if payload.len() > MAX_FRAME {
        return Err(DpxError::new(
            "frame_too_large",
            format!("frame of {} bytes exceeds {MAX_FRAME}", payload.len()),
        ));
    }
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Read one frame; returns the parsed object and its payload size.
async fn read_frame(
    reader: &mut OwnedReadHalf,
) -> std::io::Result<(serde_json::Map<String, Value>, usize)> {
    let len = reader.read_u32().await? as usize;
    if len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame of {len} bytes exceeds {MAX_FRAME}"),
        ));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    match serde_json::from_slice::<Value>(&buf) {
        Ok(Value::Object(m)) => Ok((m, len)),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame payload is not a JSON object",
        )),
    }
}

/// `hex(HMAC-SHA256(user.key, nonce || kernel_id))` (PROTOCOL §3.2).
pub fn auth_mac(key: &[u8], nonce: &[u8], kernel_id: &str) -> String {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(nonce);
    mac.update(kernel_id.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// UTC now as `YYYY-MM-DDTHH:MM:SS.mmmZ`.
pub fn now_iso() -> String {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = d.as_secs() as i64;
    let (days, sod) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60,
        d.subsec_millis()
    )
}

fn truncated_event(oldest: u64) -> KernelEvent {
    KernelEvent {
        seq: None,
        kind: "replay_truncated".into(),
        time: now_iso(),
        data: json!({"oldest_seq": oldest}),
    }
}

struct Events {
    ring: VecDeque<(KernelEvent, usize)>,
    ring_bytes: usize,
    /// Highest seq evicted from the mirror (0 if none).
    evicted_upto: u64,
    /// `oldest_seq` reported by the kernel's own `replay_truncated`.
    kernel_oldest: Option<u64>,
    last_seq: u64,
    /// `None` once the connection is closed: subscriber streams then end.
    sender: Option<broadcast::Sender<KernelEvent>>,
}

impl Events {
    fn oldest_available(&self) -> u64 {
        self.kernel_oldest.unwrap_or(1).max(self.evicted_upto + 1)
    }
}

struct Shared {
    kernel_id: String,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value>>>>,
    events: Mutex<Events>,
    closed: AtomicBool,
    fanout: FanoutConfig,
}

impl Shared {
    fn close(&self, why: &str) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        tracing::debug!(kernel_id = %self.kernel_id, "DKP connection closed: {why}");
        let pending: Vec<_> = self.pending.lock().unwrap().drain().collect();
        for (_, tx) in pending {
            let _ = tx.send(Err(unreachable(format!(
                "connection to kernel lost: {why}"
            ))));
        }
        self.events.lock().unwrap().sender = None;
    }

    fn on_event(&self, frame: serde_json::Map<String, Value>, size: usize) {
        let kind = frame
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let data = frame.get("data").cloned().unwrap_or_else(|| json!({}));
        let mut ev = self.events.lock().unwrap();
        if kind == "replay_truncated" {
            ev.kernel_oldest = data.get("oldest_seq").and_then(Value::as_u64);
            return;
        }
        let Some(seq) = frame.get("seq").and_then(Value::as_u64) else {
            return;
        };
        if ev
            .ring
            .back()
            .is_some_and(|(e, _)| e.seq.unwrap_or(0) >= seq)
        {
            return; // duplicate of a mirrored event
        }
        let time = frame
            .get("time")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let event = KernelEvent {
            seq: Some(seq),
            kind,
            time,
            data,
        };
        ev.ring.push_back((event.clone(), size));
        ev.ring_bytes += size;
        while ev.ring.len() > self.fanout.ring_max_events
            || (ev.ring_bytes > RING_MAX_BYTES && ev.ring.len() > 1)
        {
            if let Some((old, sz)) = ev.ring.pop_front() {
                ev.ring_bytes -= sz;
                ev.evicted_upto = old.seq.unwrap_or(0);
            }
        }
        ev.last_seq = ev.last_seq.max(seq);
        if let Some(tx) = &ev.sender {
            let _ = tx.send(event);
        }
    }

    fn on_response(&self, frame: serde_json::Map<String, Value>) {
        let Some(id) = frame.get("id").and_then(Value::as_u64) else {
            return;
        };
        let Some(tx) = self.pending.lock().unwrap().remove(&id) else {
            return;
        };
        let result = if frame.get("ok").and_then(Value::as_bool) == Some(true) {
            Ok(frame.get("result").cloned().unwrap_or(Value::Null))
        } else {
            let err = frame.get("error").cloned().unwrap_or(Value::Null);
            let code = err
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or("internal");
            let message = err.get("message").and_then(Value::as_str).unwrap_or(code);
            let mut e = DpxError::new(code, message);
            if let Some(data) = err.get("data").filter(|d| !d.is_null()) {
                e = e.with_data(data.clone());
            }
            Err(e)
        };
        let _ = tx.send(result);
    }

    /// Snapshot of mirrored events after `cursor` plus a receiver for everything later,
    /// taken atomically so that nothing is lost or duplicated in between.
    fn attach(
        &self,
        cursor: Option<u64>,
    ) -> Option<(VecDeque<KernelEvent>, u64, broadcast::Receiver<KernelEvent>)> {
        let ev = self.events.lock().unwrap();
        let rx = ev.sender.as_ref()?.subscribe();
        let cursor = cursor.unwrap_or(ev.last_seq);
        let mut backlog = VecDeque::new();
        let oldest = ev.oldest_available();
        if cursor + 1 < oldest {
            backlog.push_back(truncated_event(oldest));
        }
        backlog.extend(
            ev.ring
                .iter()
                .filter(|(e, _)| e.seq.unwrap_or(0) > cursor)
                .map(|(e, _)| e.clone()),
        );
        Some((backlog, cursor, rx))
    }
}

/// An authenticated control connection to one kernel.
pub struct Connection {
    shared: Arc<Shared>,
    tx: mpsc::UnboundedSender<Vec<u8>>,
    next_id: AtomicU64,
    subscribed: tokio::sync::Mutex<bool>,
    pub session: Option<String>,
}

impl Connection {
    /// Connect to `127.0.0.1:port` and complete hello → auth → welcome (PROTOCOL §3.2).
    pub async fn connect(
        port: u16,
        kernel_id: &str,
        key: &[u8],
        fanout: FanoutConfig,
    ) -> Result<Arc<Connection>> {
        let stream =
            tokio::time::timeout(HANDSHAKE_TIMEOUT, TcpStream::connect(("127.0.0.1", port)))
                .await
                .map_err(|_| unreachable("connect timed out"))?
                .map_err(|e| unreachable(format!("cannot connect to kernel: {e}")))?;
        let _ = stream.set_nodelay(true);
        let (mut reader, mut writer) = stream.into_split();
        let handshake = async {
            let (hello, _) = read_frame(&mut reader)
                .await
                .map_err(|e| unreachable(format!("handshake: {e}")))?;
            if hello.get("op").and_then(Value::as_str) != Some("hello") {
                return Err(unreachable(format!(
                    "expected hello, got {:?}",
                    hello.get("op")
                )));
            }
            let kid = hello.get("kernel_id").and_then(Value::as_str).unwrap_or("");
            if kid != kernel_id {
                return Err(unreachable(format!(
                    "port {port} belongs to {kid}, not {kernel_id}"
                )));
            }
            let nonce = base64::engine::general_purpose::STANDARD
                .decode(hello.get("nonce").and_then(Value::as_str).unwrap_or(""))
                .map_err(|e| unreachable(format!("bad hello nonce: {e}")))?;
            let auth = json!({
                "dkp": 1, "op": "auth",
                "client": {"name": "darkpyonix-manager", "kind": "manager", "pid": std::process::id()},
                "mac": auth_mac(key, &nonce, kernel_id),
            });
            writer
                .write_all(&encode(&auth)?)
                .await
                .map_err(|e| unreachable(format!("handshake: {e}")))?;
            let (welcome, _) = read_frame(&mut reader)
                .await
                .map_err(|e| unreachable(format!("handshake: {e}")))?;
            match welcome.get("op").and_then(Value::as_str) {
                Some("welcome") => Ok(welcome),
                Some("error") => {
                    let code = welcome
                        .get("code")
                        .and_then(Value::as_str)
                        .unwrap_or("auth_failed");
                    let msg = welcome
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or(code);
                    Err(DpxError::new(code, msg))
                }
                other => Err(unreachable(format!("expected welcome, got {other:?}"))),
            }
        };
        let welcome = tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake)
            .await
            .map_err(|_| unreachable("handshake timed out"))??;
        let seq = welcome.get("seq").and_then(Value::as_u64).unwrap_or(0);
        let session = welcome
            .get("session")
            .and_then(Value::as_str)
            .map(str::to_string);

        let (sender, _) = broadcast::channel(fanout.channel_capacity.max(1));
        let shared = Arc::new(Shared {
            kernel_id: kernel_id.to_string(),
            pending: Mutex::new(HashMap::new()),
            events: Mutex::new(Events {
                ring: VecDeque::new(),
                ring_bytes: 0,
                evicted_upto: 0,
                kernel_oldest: None,
                last_seq: seq,
                sender: Some(sender),
            }),
            closed: AtomicBool::new(false),
            fanout,
        });
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(write_loop(writer, rx, shared.clone()));
        tokio::spawn(read_loop(reader, shared.clone()));
        Ok(Arc::new(Connection {
            shared,
            tx,
            next_id: AtomicU64::new(1),
            subscribed: tokio::sync::Mutex::new(false),
            session,
        }))
    }

    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::SeqCst)
    }

    pub fn close(&self) {
        self.shared.close("closed by manager");
    }

    pub async fn request(&self, method: &str, params: Value) -> Result<Value> {
        self.request_timeout(method, params, REQUEST_TIMEOUT).await
    }

    pub async fn request_timeout(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value> {
        if self.is_closed() {
            return Err(unreachable("connection to kernel lost"));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let params = if params.is_null() { json!({}) } else { params };
        let frame = encode(
            &json!({"dkp": 1, "op": "request", "id": id, "method": method, "params": params}),
        )?;
        let (otx, orx) = oneshot::channel();
        self.shared.pending.lock().unwrap().insert(id, otx);
        if self.tx.send(frame).is_err() {
            self.shared.pending.lock().unwrap().remove(&id);
            return Err(unreachable("connection to kernel lost"));
        }
        // The reader may have closed between the check and the insert.
        if self.is_closed() {
            self.shared.pending.lock().unwrap().remove(&id);
            return Err(unreachable("connection to kernel lost"));
        }
        match tokio::time::timeout(timeout, orx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(unreachable("connection to kernel lost")),
            Err(_) => {
                self.shared.pending.lock().unwrap().remove(&id);
                Err(unreachable(format!(
                    "kernel did not answer {method} within {} s",
                    timeout.as_secs()
                )))
            }
        }
    }

    async fn ensure_subscribed(&self) -> Result<()> {
        let mut done = self.subscribed.lock().await;
        if !*done {
            self.request("subscribe", json!({"since": 0})).await?;
            *done = true;
        }
        Ok(())
    }

    /// Events after `since` (from the mirror) then live; `None` means live only.
    pub async fn subscribe(&self, since: Option<u64>) -> Result<EventStream> {
        self.ensure_subscribed().await?;
        let (backlog, cursor, rx) = self
            .shared
            .attach(since)
            .ok_or_else(|| unreachable("connection to kernel lost"))?;
        let state = SubState {
            shared: self.shared.clone(),
            backlog,
            cursor,
            rx,
        };
        Ok(Box::pin(futures::stream::unfold(state, next_event)))
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.shared.close("connection dropped");
    }
}

struct SubState {
    shared: Arc<Shared>,
    backlog: VecDeque<KernelEvent>,
    cursor: u64,
    rx: broadcast::Receiver<KernelEvent>,
}

async fn next_event(mut st: SubState) -> Option<(KernelEvent, SubState)> {
    loop {
        if let Some(e) = st.backlog.pop_front() {
            if let Some(seq) = e.seq {
                if seq <= st.cursor {
                    continue;
                }
                st.cursor = seq;
            }
            return Some((e, st));
        }
        match st.rx.recv().await {
            Ok(e) => {
                let seq = e.seq.unwrap_or(0);
                if seq > st.cursor {
                    st.cursor = seq;
                    return Some((e, st));
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                // Too slow for the live channel: resume from the mirror after our cursor.
                let (backlog, cursor, rx) = st.shared.attach(Some(st.cursor))?;
                st.backlog = backlog;
                st.cursor = cursor;
                st.rx = rx;
            }
            Err(broadcast::error::RecvError::Closed) => return None,
        }
    }
}

async fn write_loop(
    mut writer: OwnedWriteHalf,
    mut rx: mpsc::UnboundedReceiver<Vec<u8>>,
    shared: Arc<Shared>,
) {
    while let Some(frame) = rx.recv().await {
        if shared.closed.load(Ordering::SeqCst) {
            break;
        }
        if let Err(e) = writer.write_all(&frame).await {
            shared.close(&format!("write failed: {e}"));
            break;
        }
    }
    let _ = writer.shutdown().await;
}

async fn read_loop(mut reader: OwnedReadHalf, shared: Arc<Shared>) {
    loop {
        if shared.closed.load(Ordering::SeqCst) {
            return;
        }
        let (frame, size) = match read_frame(&mut reader).await {
            Ok(f) => f,
            Err(e) => {
                shared.close(&e.to_string());
                return;
            }
        };
        match frame.get("op").and_then(Value::as_str) {
            Some("response") => shared.on_response(frame),
            Some("event") => shared.on_event(frame, size),
            Some("error") => {
                let code = frame
                    .get("code")
                    .and_then(Value::as_str)
                    .unwrap_or("internal")
                    .to_string();
                shared.close(&format!("kernel sent error {code}"));
                return;
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_iso_has_the_protocol_shape() {
        let t = now_iso();
        assert_eq!(t.len(), 24, "{t}");
        assert!(
            t.ends_with('Z') && &t[4..5] == "-" && &t[10..11] == "T",
            "{t}"
        );
    }

    #[test]
    fn pr_2_auth_mac_matches_python_vector() {
        // python -c "import hmac,hashlib;print(hmac.new(b'k'*32,b'n'*32+b'k_abc',hashlib.sha256).hexdigest())"
        let mac = auth_mac(&[b'k'; 32], &[b'n'; 32], "k_abc");
        assert_eq!(
            mac,
            "90e6e83290e15a1069fa5e223f8ae795a1f1d1ac20c9909659ac6c4299c01910"
        );
    }
}
