//! Long-lived streams go to the kernel (SPEC FR-M6, PROTOCOL §3.7, INTENT D6).
//!
//! The manager authenticates and authorizes, then the kernel answers the client itself:
//!
//! * **Plain TCP** (HTTP/1.1, no TLS): [`intercept`] peeks the request head before hyper sees
//!   it. If it is a stream request that passes every check, the head is consumed, the socket is
//!   duplicated into the kernel with `SCM_RIGHTS`, and the manager drops its copy. The stream
//!   then survives the manager (a `SIGKILL` included). Anything else, including every failed
//!   check, goes on to hyper untouched, so the error answers are the ordinary ones.
//! * **TLS, HTTP/2, tunnels**: the request reaches an axum handler. [`pump_response`] hands one
//!   end of a socket pair to the kernel and pumps bytes between the other end and the response
//!   body. The kernel's status line and headers become the response head; the body is never
//!   translated. These streams end with the manager.
//!
//! The request bytes the kernel gets are rebuilt without the token (`Authorization`, `?token=`):
//! the kernel never verifies tokens (FR-K9). Unix only; Windows (`WSADuplicateSocketW`) is a
//! follow-up of #47, and without it streams stay on the manager.

use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Uri};

use crate::api::HandoffKind;

/// Marks a response whose stream the kernel owns: the idle timer does not count it (FR-M3).
#[derive(Clone, Copy)]
pub struct Handoff;

const MAX_HEAD: usize = 16 * 1024;

/// A stream request parsed from the head bytes of a plain connection.
#[derive(Debug)]
pub struct Parsed {
    pub head_len: usize,
    pub kind: HandoffKind,
    pub kernel_id: String,
    pub run_ref: Option<String>,
    pub uri: Uri,
    pub headers: HeaderMap,
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// `Some` only for `GET` HTTP/1.1 requests without a body on one of the three stream paths.
pub fn parse_head(buf: &[u8]) -> Option<Parsed> {
    let end = find(buf, b"\r\n\r\n")?;
    let text = std::str::from_utf8(&buf[..end]).ok()?;
    let mut lines = text.split("\r\n");
    let mut first = lines.next()?.split(' ');
    let (method, target, version) = (first.next()?, first.next()?, first.next()?);
    if method != "GET" || version != "HTTP/1.1" || first.next().is_some() {
        return None;
    }
    let uri: Uri = target.parse().ok()?;
    let mut headers = HeaderMap::new();
    for line in lines {
        let (name, value) = line.split_once(':')?;
        let name = HeaderName::from_bytes(name.trim().as_bytes()).ok()?;
        headers.append(name, HeaderValue::from_str(value.trim()).ok()?);
    }
    if headers.contains_key(header::TRANSFER_ENCODING)
        || headers.get(header::CONTENT_LENGTH).is_some_and(|v| v.as_bytes() != b"0")
    {
        return None;
    }
    let segs: Vec<&str> = uri.path().trim_start_matches('/').split('/').collect();
    let (kind, kernel_id, run_ref) = match segs.as_slice() {
        ["api", "kernels", kid, "events"] => (HandoffKind::Events, *kid, None),
        ["api", "ws", "kernels", kid] => (HandoffKind::Ws, *kid, None),
        ["api", "kernels", kid, "runs", rr, "wait"] => (HandoffKind::Wait, *kid, Some((*rr).to_string())),
        _ => return None,
    };
    if kernel_id.is_empty() {
        return None;
    }
    if kind == HandoffKind::Ws {
        let upgrade = headers
            .get(header::UPGRADE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
        if !upgrade || !headers.contains_key("sec-websocket-key") {
            return None;
        }
    }
    Some(Parsed { head_len: end + 4, kind, kernel_id: kernel_id.to_string(), run_ref, uri, headers })
}

/// The bearer token, or for events and WebSocket (which cannot set headers in a browser) `?token=`.
pub fn token_of(kind: HandoffKind, uri: &Uri, headers: &HeaderMap) -> Option<String> {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            let (scheme, rest) = v.split_once(' ')?;
            scheme.eq_ignore_ascii_case("bearer").then(|| rest.trim().to_string())
        });
    bearer.or_else(|| {
        if kind == HandoffKind::Wait {
            return None;
        }
        let pairs: Vec<(String, String)> = serde_urlencoded::from_str(uri.query()?).ok()?;
        pairs.into_iter().find(|(k, _)| k == "token").map(|(_, v)| v)
    })
}

/// The request as the kernel gets it: same path and headers, no token anywhere.
pub fn build_request(uri: &Uri, headers: &HeaderMap) -> Vec<u8> {
    let pairs: Vec<(String, String)> =
        serde_urlencoded::from_str(uri.query().unwrap_or("")).unwrap_or_default();
    let kept: Vec<(String, String)> = pairs.into_iter().filter(|(k, _)| k != "token").collect();
    let mut target = uri.path().to_string();
    if !kept.is_empty() {
        target.push('?');
        target.push_str(&serde_urlencoded::to_string(&kept).unwrap_or_default());
    }
    let mut out = format!("GET {target} HTTP/1.1\r\n").into_bytes();
    let mut has_host = false;
    for (name, value) in headers {
        if *name == header::AUTHORIZATION || *name == header::COOKIE || *name == header::PROXY_AUTHORIZATION {
            continue;
        }
        has_host |= *name == header::HOST;
        out.extend_from_slice(name.as_str().as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    if !has_host {
        let host = uri.authority().map_or("localhost", |a| a.as_str());
        out.extend_from_slice(format!("host: {host}\r\n").as_bytes());
    }
    out.extend_from_slice(b"\r\n");
    out
}

// ------------------------------------------------------------------ plain TCP: SCM_RIGHTS

#[cfg(unix)]
pub use unix::{intercept, pump_response};

#[cfg(not(unix))]
pub async fn pump_response(
    _st: &std::sync::Arc<crate::api::AppState>,
    _plan: crate::api::HandoffPlan,
    _uri: &Uri,
    _headers: &HeaderMap,
    _upgrade: Option<hyper::upgrade::OnUpgrade>,
) -> crate::error::ApiResult<axum::response::Response> {
    Err(crate::error::ApiError::internal("stream handoff needs Unix sockets"))
}

#[cfg(unix)]
mod unix {
    use std::os::fd::OwnedFd;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use axum::body::{Body, Bytes};
    use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Uri};
    use axum::response::Response;
    use dpx_core::AdoptRequest;
    use serde_json::json;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpStream, UnixStream};

    use super::{build_request, find, parse_head, token_of, Handoff, MAX_HEAD};
    use crate::auth::Lookup;
    use crate::api::{plan_handoff, AppState, HandoffKind, HandoffPlan};
    use crate::error::{ApiError, ApiResult};
    use crate::Activity;

    const PEEK_TIMEOUT: Duration = Duration::from_secs(2);
    const PUMP_HEAD_TIMEOUT: Duration = Duration::from_secs(10);
    const PUMP_WAIT_TIMEOUT: Duration = Duration::from_secs(330);

    /// The request head of a connection, still unread, if it starts like a `GET`.
    async fn peek_head(stream: &TcpStream) -> Option<Vec<u8>> {
        let mut buf = vec![0u8; MAX_HEAD];
        let deadline = Instant::now() + PEEK_TIMEOUT;
        let mut last = 0;
        loop {
            let n = stream.peek(&mut buf).await.ok()?;
            if n == 0 || !b"GET ".starts_with(&buf[..n.min(4)]) {
                return None; // closed, or not a GET (h2c preface, POST, ...)
            }
            if let Some(i) = find(&buf[..n], b"\r\n\r\n") {
                buf.truncate(i + 4);
                return Some(buf);
            }
            if n == buf.len() || Instant::now() >= deadline {
                return None;
            }
            if n == last {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            last = n;
        }
    }

    /// Hand a plain connection to the kernel if it is a valid stream request. `Some(stream)`
    /// means "not handed off, serve it as usual" and nothing has been read from it.
    pub async fn intercept(stream: TcpStream, st: &Arc<AppState>, activity: &Arc<Activity>) -> Option<TcpStream> {
        if !st.backend.supports_handoff() {
            return Some(stream);
        }
        let Some(head) = peek_head(&stream).await else { return Some(stream) };
        let Some(parsed) = parse_head(&head) else { return Some(stream) };
        let token = token_of(parsed.kind, &parsed.uri, &parsed.headers);
        let principal = match st.auth.lookup(token.as_deref()) {
            Lookup::Found(p) => p,
            // A kernel-held token (FR-A6): ask the kernel the way the auth gate does.
            Lookup::Unknown => match token.as_deref().filter(|t| !t.is_empty()) {
                Some(t) => match crate::kernel_principal(st, &parsed.kernel_id, t).await {
                    Ok(Some(p)) => p,
                    _ => return Some(stream),
                },
                None => return Some(stream),
            },
            Lookup::Revoked => return Some(stream),
        };
        let plan = match plan_handoff(
            st,
            &principal,
            parsed.kind,
            &parsed.kernel_id,
            parsed.run_ref.as_deref(),
            &parsed.uri,
            &parsed.headers,
        )
        .await
        {
            Ok(plan) => plan,
            Err(_) => return Some(stream), // hyper answers 400/403/404 the usual way
        };
        let mut stream = stream;
        let mut consumed = vec![0u8; parsed.head_len];
        if stream.read_exact(&mut consumed).await.is_err() {
            return None;
        }
        activity.touch(); // the request counts; the stream does not (FR-M3)
        let std_stream = stream.into_std().ok()?;
        let _ = std_stream.set_nonblocking(false);
        let dup = std_stream.try_clone().ok()?;
        let req = AdoptRequest {
            kind: plan.kind.as_str().to_string(),
            transport: "fd".into(),
            request: build_request(&parsed.uri, &parsed.headers),
            label: plan.label,
            fd: OwnedFd::from(dup),
        };
        if let Err(e) = st.backend.adopt(&plan.kernel_id, req).await {
            use std::io::Write;
            let body = json!({"error": {"code": e.code, "message": e.message}}).to_string();
            let reply = format!(
                "HTTP/1.1 502 Bad Gateway\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = std_stream.set_write_timeout(Some(Duration::from_secs(2)));
            let _ = (&std_stream).write_all(reply.as_bytes());
        }
        None // our copy of the socket closes here; the kernel's lives on
    }

    // -------------------------------------------------------------- socket pair and pump

    struct Head {
        status: u16,
        headers: Vec<(String, String)>,
        rest: Vec<u8>,
    }

    async fn read_response_head<R: AsyncRead + Unpin>(r: &mut R) -> std::io::Result<Head> {
        let bad = |m: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, m.to_string());
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            if let Some(i) = find(&buf, b"\r\n\r\n") {
                let text = String::from_utf8_lossy(&buf[..i]).into_owned();
                let mut lines = text.split("\r\n");
                let status = lines
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|c| c.parse::<u16>().ok())
                    .ok_or_else(|| bad("bad status line"))?;
                let headers = lines
                    .filter_map(|l| l.split_once(':'))
                    .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                    .collect();
                return Ok(Head { status, headers, rest: buf[i + 4..].to_vec() });
            }
            if buf.len() > 64 * 1024 {
                return Err(bad("response head too large"));
            }
            let n = r.read(&mut chunk).await?;
            if n == 0 {
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
            }
            buf.extend_from_slice(&chunk[..n]);
        }
    }

    struct PumpState {
        sock: UnixStream,
        first: Option<Vec<u8>>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    }

    /// Answer a stream request the kernel owns from a TLS / HTTP/2 / tunnel connection: one end
    /// of a socket pair goes to the kernel, bytes from the other end become the response body.
    pub async fn pump_response(
        st: &Arc<AppState>,
        plan: HandoffPlan,
        uri: &Uri,
        headers: &HeaderMap,
        upgrade: Option<hyper::upgrade::OnUpgrade>,
    ) -> ApiResult<Response> {
        // A WebSocket needs hyper's upgrade (HTTP/1.1). HTTP/2 would need RFC 8441 extended
        // CONNECT, which is not implemented (SPEC FR-S9); refuse before the kernel is involved.
        if plan.kind == HandoffKind::Ws && upgrade.is_none() {
            return Err(ApiError::bad_request(
                "WebSocket sync needs an HTTP/1.1 upgrade; HTTP/2 (RFC 8441) is not supported, use HTTP/1.1",
            ));
        }
        let (ours, theirs) =
            std::os::unix::net::UnixStream::pair().map_err(|e| ApiError::internal(format!("socket pair: {e}")))?;
        ours.set_nonblocking(true).map_err(|e| ApiError::internal(e.to_string()))?;
        let req = AdoptRequest {
            kind: plan.kind.as_str().to_string(),
            transport: "pump".into(),
            request: build_request(uri, headers),
            label: plan.label,
            fd: OwnedFd::from(theirs),
        };
        st.backend.adopt(&plan.kernel_id, req).await?;
        let mut sock =
            UnixStream::from_std(ours).map_err(|e| ApiError::internal(format!("socket pair: {e}")))?;
        let limit = if plan.kind == HandoffKind::Wait { PUMP_WAIT_TIMEOUT } else { PUMP_HEAD_TIMEOUT };
        let head = tokio::time::timeout(limit, read_response_head(&mut sock))
            .await
            .map_err(|_| ApiError::internal("the kernel did not answer the handed-off stream"))?
            .map_err(|e| ApiError::internal(format!("kernel answer: {e}")))?;
        if head.status == 101 {
            if let Some(on) = upgrade {
                // Hand the upgraded connection to a task that copies both ways (hyper drops the
                // stream otherwise); the response itself carries no body.
                let mut shutdown = st.shutdown.clone();
                let rest = head.rest;
                tokio::spawn(async move {
                    let Ok(up) = on.await else { return };
                    let mut up = hyper_util::rt::TokioIo::new(up);
                    if !rest.is_empty() && up.write_all(&rest).await.is_err() {
                        return;
                    }
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut up, &mut sock) => {}
                        _ = shutdown.wait_for(|v| *v) => {}
                    }
                });
                let mut resp = Response::new(Body::empty());
                *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
                for (k, v) in head.headers {
                    if let (Ok(name), Ok(value)) =
                        (HeaderName::from_bytes(k.to_ascii_lowercase().as_bytes()), HeaderValue::from_str(&v))
                    {
                        resp.headers_mut().append(name, value);
                    }
                }
                resp.extensions_mut().insert(Handoff);
                return Ok(resp);
            }
        }
        let state = PumpState { sock, first: Some(head.rest), shutdown: st.shutdown.clone() };
        let body = futures::stream::unfold(state, |mut s| async move {
            if let Some(first) = s.first.take() {
                if !first.is_empty() {
                    return Some((Ok::<Bytes, std::io::Error>(Bytes::from(first)), s));
                }
            }
            let mut buf = vec![0u8; 16 * 1024];
            // Finish the select (and its borrows of `s`) before `s` moves into the result.
            let read = tokio::select! {
                r = s.sock.read(&mut buf) => Some(r),
                _ = s.shutdown.wait_for(|v| *v) => None,
            };
            match read {
                Some(Ok(n)) if n > 0 => {
                    buf.truncate(n);
                    Some((Ok(Bytes::from(buf)), s))
                }
                _ => None,
            }
        });
        let mut resp = Response::new(Body::from_stream(body));
        *resp.status_mut() = StatusCode::from_u16(head.status).unwrap_or(StatusCode::BAD_GATEWAY);
        for (k, v) in head.headers {
            let lower = k.to_ascii_lowercase();
            // The body is re-framed by the transport (chunked, DATA frames); the bytes are not.
            if matches!(lower.as_str(), "connection" | "content-length" | "transfer-encoding" | "keep-alive") {
                continue;
            }
            if let (Ok(name), Ok(value)) = (HeaderName::from_bytes(lower.as_bytes()), HeaderValue::from_str(&v)) {
                resp.headers_mut().append(name, value);
            }
        }
        resp.extensions_mut().insert(Handoff);
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head(target: &str, extra: &str) -> Vec<u8> {
        format!("GET {target} HTTP/1.1\r\nHost: x\r\n{extra}\r\n").into_bytes()
    }

    #[test]
    fn test_fr_m6_parse_recognises_the_three_stream_paths() {
        let p = parse_head(&head("/api/kernels/k_1/events?since=3", "")).unwrap();
        assert_eq!((p.kind, p.kernel_id.as_str()), (HandoffKind::Events, "k_1"));
        let p = parse_head(&head("/api/kernels/k_1/runs/latest/wait?timeout=5", "")).unwrap();
        assert_eq!((p.kind, p.run_ref.as_deref()), (HandoffKind::Wait, Some("latest")));
        let ws = "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: abc\r\n";
        let p = parse_head(&head("/api/ws/kernels/k_1", ws)).unwrap();
        assert_eq!(p.kind, HandoffKind::Ws);
    }

    #[test]
    fn test_fr_m6_parse_leaves_everything_else_to_the_manager() {
        assert!(parse_head(&head("/api/kernels/k_1", "")).is_none());
        assert!(parse_head(&head("/api/kernels/k_1/runs", "")).is_none());
        assert!(parse_head(&head("/api/ws/kernels/k_1", "")).is_none(), "ws needs an upgrade");
        assert!(parse_head(&head("/api/kernels/k_1/events", "Content-Length: 5\r\n")).is_none());
        assert!(parse_head(&head("/api/kernels/k_1/events", "Transfer-Encoding: chunked\r\n")).is_none());
        assert!(parse_head(b"POST /api/kernels/k_1/events HTTP/1.1\r\n\r\n").is_none());
        assert!(parse_head(b"GET /api/kernels/k_1/events HTTP/1.0\r\n\r\n").is_none());
        assert!(parse_head(b"GET /api/kernels/k_1/events HTTP/1.1\r\nHost: x\r\n").is_none(), "incomplete head");
    }

    #[test]
    fn test_fr_m6_token_comes_from_the_header_or_the_query_but_never_for_wait_query() {
        let p = parse_head(&head("/api/kernels/k_1/events?token=q", "Authorization: Bearer h\r\n")).unwrap();
        assert_eq!(token_of(p.kind, &p.uri, &p.headers).as_deref(), Some("h"));
        let p = parse_head(&head("/api/kernels/k_1/events?token=q", "")).unwrap();
        assert_eq!(token_of(p.kind, &p.uri, &p.headers).as_deref(), Some("q"));
        let p = parse_head(&head("/api/kernels/k_1/runs/latest/wait?token=q", "")).unwrap();
        assert_eq!(token_of(p.kind, &p.uri, &p.headers), None);
    }

    #[test]
    fn test_fr_k9_kernel_never_receives_the_token() {
        let h = head(
            "/api/kernels/k_1/events?since=3&token=secret&client_id=dev_8f2c1a",
            "Authorization: Bearer secret\r\nCookie: a=b\r\nLast-Event-ID: 7\r\n",
        );
        let p = parse_head(&h).unwrap();
        let out = String::from_utf8(build_request(&p.uri, &p.headers)).unwrap();
        assert!(!out.contains("secret") && !out.to_lowercase().contains("authorization"), "{out}");
        assert!(!out.to_lowercase().contains("cookie"), "{out}");
        assert!(out.starts_with("GET /api/kernels/k_1/events?since=3&client_id=dev_8f2c1a HTTP/1.1\r\n"), "{out}");
        assert!(out.contains("last-event-id: 7\r\n") && out.ends_with("\r\n\r\n"), "{out}");
    }
}
