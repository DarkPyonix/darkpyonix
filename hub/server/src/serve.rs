//! One listener for everything: TLS (optional), then HTTP/1.1. `GET /relay` goes to the
//! embedded iroh relay service, every other request to the axum router.

use std::{convert::Infallible, sync::Arc, time::Duration};

use axum::{body::Body, http::StatusCode, Router};
use hyper::{
    body::Incoming,
    server::conn::http1,
    service::{service_fn, Service as _},
    Request, Response,
};
use hyper_util::rt::TokioIo;
use iroh_relay::server::{
    http_server::RelayServiceWithNotify, streams::MaybeTlsStream, RelayService,
};
use tokio::{net::TcpListener, sync::Notify};
use tokio_rustls::TlsAcceptor;
use tower::ServiceExt as _;
use tracing::{debug, warn};

/// How long a client may take to finish the TLS handshake.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub(crate) struct Frontend {
    pub router: Router,
    pub relay: Option<RelayService>,
}

impl Frontend {
    async fn handle(self, req: Request<Incoming>) -> Result<Response<Body>, Infallible> {
        if req.uri().path() == iroh_relay::http::RELAY_PATH {
            if let Some(relay) = self.relay {
                // The relay service takes over the upgraded connection itself.
                let service = RelayServiceWithNotify::new(relay, Arc::new(Notify::new()));
                return Ok(match service.call(req).await {
                    Ok(response) => response.map(Body::new),
                    Err(err) => {
                        warn!(%err, "relay upgrade failed");
                        let mut response = Response::new(Body::empty());
                        *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                        response
                    }
                });
            }
        }
        self.router.oneshot(req.map(Body::new)).await
    }
}

/// Accepts connections until the task is aborted.
pub(crate) async fn run(listener: TcpListener, tls: Option<TlsAcceptor>, front: Frontend) {
    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                warn!(%err, "accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let tls = tls.clone();
        let front = front.clone();
        tokio::spawn(async move {
            let stream = match tls {
                Some(acceptor) => {
                    match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
                        Ok(Ok(stream)) => MaybeTlsStream::Tls(stream),
                        Ok(Err(err)) => {
                            debug!(%err, %peer, "tls handshake failed");
                            return;
                        }
                        Err(_) => {
                            debug!(%peer, "tls handshake timed out");
                            return;
                        }
                    }
                }
                None => MaybeTlsStream::Plain(tcp),
            };
            let service = service_fn(move |req| front.clone().handle(req));
            if let Err(err) = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades()
                .await
            {
                debug!(%err, %peer, "connection ended with an error");
            }
        });
    }
}
