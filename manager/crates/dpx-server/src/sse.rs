//! `GET /kernels/{id}/events` as Server-Sent Events (SPEC FR-M1, PROTOCOL §3.4).
//!
//! Frames are `id: <seq>`, `event: <type>`, `data: <json of data>`; `replay_truncated` has no
//! `id` so it never moves the client's `Last-Event-ID`. A comment is sent every
//! `keepalive` while nothing happens. Dropping the client drops only this subscription.

use std::convert::Infallible;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::http::{header, HeaderValue};
use axum::response::Response;
use dpx_core::{EventStream, KernelEvent};
use futures::StreamExt;
use tokio::sync::watch;

const OUTPUT_EVENTS: [&str; 2] = ["output", "output.clear"];

pub fn frame(e: &KernelEvent) -> String {
    let data = serde_json::to_string(&e.data).unwrap_or_else(|_| "null".into());
    // `type` comes from the kernel; keep it on one line whatever it holds.
    let kind: String = e.kind.chars().filter(|c| *c != '\n' && *c != '\r').collect();
    match e.seq {
        Some(seq) => format!("id: {seq}\nevent: {kind}\ndata: {data}\n\n"),
        None => format!("event: {kind}\ndata: {data}\n\n"),
    }
}

struct Feed {
    events: EventStream,
    hide_outputs: bool,
    keepalive: Duration,
    shutdown: watch::Receiver<bool>,
    hello: Option<String>,
}

pub fn response(
    events: EventStream,
    kernel_id: &str,
    hide_outputs: bool,
    keepalive: Duration,
    shutdown: watch::Receiver<bool>,
) -> Response {
    let feed = Feed {
        events,
        hide_outputs,
        keepalive,
        shutdown,
        hello: Some(format!(": darkpyonix events {kernel_id}\n\n")),
    };
    let stream = futures::stream::unfold(feed, |mut f| async move {
        if let Some(hello) = f.hello.take() {
            return Some((Ok::<_, Infallible>(Bytes::from(hello)), f));
        }
        loop {
            if *f.shutdown.borrow() {
                return None;
            }
            tokio::select! {
                next = tokio::time::timeout(f.keepalive, f.events.next()) => match next {
                    Ok(Some(e)) => {
                        if f.hide_outputs && OUTPUT_EVENTS.contains(&e.kind.as_str()) {
                            continue;
                        }
                        return Some((Ok(Bytes::from(frame(&e))), f));
                    }
                    Ok(None) => return None,
                    Err(_) => return Some((Ok(Bytes::from_static(b": keepalive\n\n")), f)),
                },
                changed = f.shutdown.changed() => {
                    if changed.is_err() || *f.shutdown.borrow() {
                        return None;
                    }
                }
            }
        }
    });
    let mut resp = Response::new(Body::from_stream(stream));
    let h = resp.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    h.insert("x-accel-buffering", HeaderValue::from_static("no"));
    resp
}
