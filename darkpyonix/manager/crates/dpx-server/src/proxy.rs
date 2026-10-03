//! Generic reverse proxy routes (INTENT D10: the manager replaces nginx in front of
//! VS Code `serve-web`, ember, ...). Outside the OpenAPI contract, like `/docs/`.
//!
//! Plain HTTP requests are forwarded with a pooled client; HTTP/1.1 `Upgrade` requests
//! (WebSocket) are forwarded on a dedicated upstream connection and then spliced byte for
//! byte. The client's `Host` header is preserved (serve-web checks it).

use std::net::SocketAddr;

use axum::body::Body;
use axum::http::uri::{Authority, PathAndQuery, Scheme};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Request, Response, StatusCode, Uri, Version};
use axum::response::IntoResponse;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};

use crate::error::ApiError;

#[derive(Debug, Clone)]
pub struct ProxyRoute {
    /// Path prefix on this server, e.g. `/vscode`. Matches the prefix itself and below it.
    pub prefix: String,
    /// Upstream base URL, e.g. `http://127.0.0.1:8000` (HTTP only).
    pub upstream: String,
    /// Remove `prefix` from the forwarded path (keep it when the upstream is configured
    /// with the same base path, e.g. `code serve-web --server-base-path /vscode`).
    pub strip_prefix: bool,
}

impl ProxyRoute {
    pub fn new(prefix: impl Into<String>, upstream: impl Into<String>) -> Self {
        Self { prefix: prefix.into(), upstream: upstream.into(), strip_prefix: false }
    }

    pub fn matches(&self, path: &str) -> bool {
        let p = self.prefix.trim_end_matches('/');
        if p.is_empty() {
            return true;
        }
        path == p || (path.starts_with(p) && path.as_bytes().get(p.len()) == Some(&b'/'))
    }
}

/// The peer and transport of the accepted connection (set by the accept loop).
#[derive(Debug, Clone, Copy)]
pub struct ConnInfo {
    pub peer: SocketAddr,
    pub tls: bool,
}

pub struct Proxy {
    routes: Vec<(ProxyRoute, Scheme, Authority, String)>,
    client: Client<HttpConnector, Body>,
}

const HOP_BY_HOP: [&str; 8] = [
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

fn strip_hop_by_hop(h: &mut HeaderMap) {
    let listed: Vec<HeaderName> = h
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|t| HeaderName::from_bytes(t.trim().as_bytes()).ok())
        .collect();
    for name in listed {
        h.remove(name);
    }
    for name in HOP_BY_HOP {
        h.remove(name);
    }
}

fn is_upgrade(h: &HeaderMap) -> bool {
    h.contains_key(header::UPGRADE)
        && h.get_all(header::CONNECTION)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .any(|v| v.split(',').any(|t| t.trim().eq_ignore_ascii_case("upgrade")))
}

fn bad_gateway(msg: impl std::fmt::Display) -> Response<Body> {
    ApiError::new(StatusCode::BAD_GATEWAY, "internal", format!("proxy upstream: {msg}")).into_response()
}

impl Proxy {
    pub fn new(routes: &[ProxyRoute]) -> std::io::Result<Self> {
        let mut out = Vec::new();
        for r in routes {
            let uri: Uri = r
                .upstream
                .parse()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("proxy {}: {e}", r.upstream)))?;
            let (Some(scheme), Some(auth)) = (uri.scheme().cloned(), uri.authority().cloned()) else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("proxy {}: need http://host:port", r.upstream),
                ));
            };
            if scheme != Scheme::HTTP {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("proxy {}: only http:// upstreams are supported", r.upstream),
                ));
            }
            let base = uri.path().trim_end_matches('/').to_string();
            out.push((r.clone(), scheme, auth, base));
        }
        let client = Client::builder(TokioExecutor::new()).build_http::<Body>();
        Ok(Self { routes: out, client })
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    pub fn route_for(&self, path: &str) -> Option<usize> {
        self.routes.iter().position(|(r, ..)| r.matches(path))
    }

    pub async fn forward(&self, idx: usize, mut req: Request<Body>) -> Response<Body> {
        let (route, scheme, authority, base) = &self.routes[idx];
        let conn = req.extensions().get::<ConnInfo>().copied();
        let activity = req.extensions().get::<std::sync::Arc<crate::Activity>>().cloned();
        let path = req.uri().path();
        let rest = if route.strip_prefix { &path[route.prefix.trim_end_matches('/').len()..] } else { path };
        let rest = if rest.starts_with('/') { rest.to_string() } else { format!("/{rest}") };
        let pq = match req.uri().query() {
            Some(q) => format!("{base}{rest}?{q}"),
            None => format!("{base}{rest}"),
        };
        let Ok(pq) = PathAndQuery::try_from(pq) else { return bad_gateway("bad path") };
        // Origin-form for the dedicated upgrade connection (hyper's conn API writes the URI
        // verbatim); absolute-form for the pooled client, which rewrites it itself.
        let origin_form = Uri::from(pq.clone());
        let target = Uri::builder().scheme(scheme.clone()).authority(authority.clone()).path_and_query(pq).build();
        let Ok(target) = target else { return bad_gateway("bad target") };

        let upgrade = req.version() == Version::HTTP_11 && is_upgrade(req.headers());
        let upgrade_proto = req.headers().get(header::UPGRADE).cloned();
        // HTTP/2 carries the host in :authority; HTTP/1.1 in Host. Preserve whichever came.
        let host = req
            .headers()
            .get(header::HOST)
            .cloned()
            .or_else(|| req.uri().authority().and_then(|a| HeaderValue::from_str(a.as_str()).ok()));

        let mut headers = req.headers().clone();
        strip_hop_by_hop(&mut headers);
        if let Some(h) = &host {
            headers.insert(header::HOST, h.clone());
            headers.insert("x-forwarded-host", h.clone());
        }
        if let Some(c) = conn {
            let ip = c.peer.ip().to_string();
            let xff = match headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
                Some(prev) => format!("{prev}, {ip}"),
                None => ip,
            };
            if let Ok(v) = HeaderValue::from_str(&xff) {
                headers.insert("x-forwarded-for", v);
            }
            headers.insert("x-forwarded-proto", HeaderValue::from_static(if c.tls { "https" } else { "http" }));
        }
        if upgrade {
            headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
            if let Some(p) = upgrade_proto {
                headers.insert(header::UPGRADE, p);
            }
            let on_client_upgrade = hyper::upgrade::on(&mut req);
            let mut out = Request::builder().method(req.method().clone()).uri(origin_form).version(Version::HTTP_11);
            *out.headers_mut().expect("builder") = headers;
            let Ok(out) = out.body(Body::empty()) else { return bad_gateway("bad request") };
            return self.forward_upgrade(authority, out, on_client_upgrade, activity).await;
        }

        let (parts, body) = req.into_parts();
        let mut out = Request::builder().method(parts.method).uri(target).version(Version::HTTP_11);
        *out.headers_mut().expect("builder") = headers;
        let Ok(out) = out.body(body) else { return bad_gateway("bad request") };
        match self.client.request(out).await {
            Ok(resp) => {
                let (mut parts, body) = resp.into_parts();
                strip_hop_by_hop(&mut parts.headers);
                Response::from_parts(parts, Body::new(body))
            }
            Err(e) => bad_gateway(e),
        }
    }

    async fn forward_upgrade(
        &self,
        authority: &Authority,
        out: Request<Body>,
        on_client_upgrade: hyper::upgrade::OnUpgrade,
        activity: Option<std::sync::Arc<crate::Activity>>,
    ) -> Response<Body> {
        let addr = match authority.port_u16() {
            Some(_) => authority.as_str().to_string(),
            None => format!("{}:80", authority.host()),
        };
        let stream = match tokio::net::TcpStream::connect(&addr).await {
            Ok(s) => s,
            Err(e) => return bad_gateway(e),
        };
        let _ = stream.set_nodelay(true);
        let (mut sender, conn) = match hyper::client::conn::http1::handshake(TokioIo::new(stream)).await {
            Ok(x) => x,
            Err(e) => return bad_gateway(e),
        };
        tokio::spawn(async move {
            let _ = conn.with_upgrades().await;
        });
        let mut resp = match sender.send_request(out).await {
            Ok(r) => r,
            Err(e) => return bad_gateway(e),
        };
        if resp.status() == StatusCode::SWITCHING_PROTOCOLS {
            let on_upstream_upgrade = hyper::upgrade::on(&mut resp);
            tokio::spawn(async move {
                // An open tunnel keeps an ephemeral manager alive like an open stream.
                let _active = activity.as_ref().map(|a| a.enter());
                let (Ok(client), Ok(upstream)) = tokio::join!(on_client_upgrade, on_upstream_upgrade) else {
                    return;
                };
                let mut client = TokioIo::new(client);
                let mut upstream = TokioIo::new(upstream);
                let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
            });
            let (parts, _) = resp.into_parts();
            return Response::from_parts(parts, Body::empty());
        }
        let (mut parts, body) = resp.into_parts();
        strip_hop_by_hop(&mut parts.headers);
        Response::from_parts(parts, Body::new(body))
    }
}
