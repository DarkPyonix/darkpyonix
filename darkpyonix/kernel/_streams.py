"""Streams the manager handed to the kernel (PROTOCOL §3.7; SPEC FR-K9, FR-S9, PR-5, PR-6).

The manager authenticates and authorizes, then passes a client connection (SSE, WebSocket,
run-wait long poll) with the request bytes it already read and a label. The kernel answers the
request itself. It never verifies a token: the label's ``capabilities`` only decide what is sent
(FR-K9). Each stream has one thread, so a slow client blocks only itself.

Standard library only; Python 3.8+.
"""
from __future__ import annotations

import base64
import hashlib
import json
import queue
import secrets
import select
import socket
import threading
import time
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import parse_qsl, quote, urlsplit

from . import _wsync
from ._protocol import DKPError

Handler = Callable[[Dict[str, Any]], Any]

KINDS = ("events", "ws", "wait")
MAX_REQUEST = 64 * 1024
MAX_QUEUED = 64 * 1024 * 1024   # a stream further behind than this is dropped (PROTOCOL §3.7.5)
SEND_TIMEOUT = 10.0
KEEPALIVE = 15.0
PRESENCE_HEARTBEAT = 10.0
POLL = 0.25
MAX_POLLS = 100                 # SPEC FR-S7: 2025 ``max_polls``
RECOMMENDED_DELAY = 5           # seconds between polls of an alarm client
WS_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
OUTPUT_EVENTS = ("output", "output.clear")
FINISHED = ("ok", "error", "interrupted", "cancelled", "crashed")
_STOP = object()
_CAPABILITY_ORDER = ("read", "history", "execute", "edit", "manage")


def parse_request(raw: bytes):
    """``(method, path, query, headers)`` of the request head the manager already read."""
    head, sep, _ = raw.partition(b"\r\n\r\n")
    if not sep:
        raise ValueError("request head is not complete")
    lines = head.decode("latin-1").split("\r\n")
    method, target, version = lines[0].split(" ")
    if not version.startswith("HTTP/"):
        raise ValueError("bad request line")
    headers = {}  # type: Dict[str, str]
    for line in lines[1:]:
        k, _, v = line.partition(":")
        headers[k.strip().lower()] = v.strip()
    parts = urlsplit(target)
    return method, parts.path, dict(parse_qsl(parts.query)), headers


class _Stream:
    def __init__(self, stream_id: str, kind: str, sock: socket.socket, label: Dict[str, Any],
                 request) -> None:
        self.id = stream_id
        self.kind = kind
        self.sock = sock
        self.label = label
        self.caps = set(c for c in label.get("capabilities") or [] if isinstance(c, str))
        self.share_id = label.get("share_id") if isinstance(label.get("share_id"), str) else None
        self.client_id = label.get("client_id") if isinstance(label.get("client_id"), str) else None
        self.method, self.path, self.query, self.headers = request
        self.q = queue.Queue()  # type: queue.Queue
        self.queued = 0
        self.closed = False
        self.lock = threading.Lock()

    def close(self) -> bool:
        """Close now; True if this call did it."""
        with self.lock:
            if self.closed:
                return False
            self.closed = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass
        self.q.put(_STOP)
        return True


def _http(status: int, reason: str, headers: List[str], body: bytes = b"") -> bytes:
    lines = ["HTTP/1.1 %d %s" % (status, reason)] + headers
    return ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1") + body


def _json_response(status: int, reason: str, obj: Any) -> bytes:
    body = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return _http(status, reason, ["Content-Type: application/json", "Cache-Control: no-store",
                                  "Content-Length: %d" % len(body), "Connection: close"], body)


def _error_response(status: int, reason: str, code: str, message: str) -> bytes:
    return _json_response(status, reason, {"code": code, "message": message})


_STATUS = {"bad_request": (400, "Bad Request"), "not_found": (404, "Not Found"),
           "busy": (409, "Conflict"), "forbidden": (403, "Forbidden"),
           "poll_limit_exceeded": (429, "Too Many Requests")}


def sse_frame(event: Dict[str, Any]) -> bytes:
    kind = str(event.get("type", "")).replace("\n", "").replace("\r", "")
    data = json.dumps(event.get("data"), ensure_ascii=False, separators=(",", ":"))
    seq = event.get("seq")
    head = "id: %d\n" % seq if isinstance(seq, int) else ""
    return ("%sevent: %s\ndata: %s\n\n" % (head, kind, data)).encode("utf-8")


class StreamHub:
    """Owns every adopted stream of one kernel."""

    def __init__(self, kernel_id: str, events, handlers: Dict[str, Handler]) -> None:
        self.kernel_id = kernel_id
        self._events = events
        self._handlers = handlers
        self._lock = threading.Lock()
        self._streams = {}  # type: Dict[str, _Stream]
        self._running = True
        events.add_listener(self._on_event)

    # ------------------------------------------------------------ API used by the server

    def adopt(self, sock: socket.socket, kind: str, raw: bytes, label: Any) -> str:
        """Take ``sock`` and answer ``raw`` on it. Raises ``DKPError`` (the caller closes)."""
        if kind not in KINDS:
            raise DKPError("bad_request", "kind must be one of %s" % ", ".join(KINDS))
        if not isinstance(label, dict):
            raise DKPError("bad_request", "label must be an object")
        if len(raw) > MAX_REQUEST:
            raise DKPError("bad_request", "request is larger than %d bytes" % MAX_REQUEST)
        try:
            request = parse_request(raw)
        except (ValueError, UnicodeError) as e:
            raise DKPError("bad_request", "cannot parse the handed-off request: %s" % (e,))
        if kind == "ws" and not request[3].get("sec-websocket-key"):
            raise DKPError("bad_request", "a WebSocket request needs Sec-WebSocket-Key")
        if not self._running:
            raise DKPError("shutting_down", "the kernel is stopping")
        st = _Stream("st_" + secrets.token_hex(6), kind, sock, label, request)
        with self._lock:
            self._streams[st.id] = st
        threading.Thread(target=self._run, args=(st,), name="dkp-stream", daemon=True).start()
        return st.id

    def close_share(self, share_id: str) -> int:
        with self._lock:
            targets = [s for s in self._streams.values() if s.share_id == share_id]
        return sum(1 for s in targets if s.close())

    def count(self) -> int:
        with self._lock:
            return len(self._streams)

    def stop(self) -> None:
        self._running = False
        self._events.remove_listener(self._on_event)
        with self._lock:
            streams = list(self._streams.values())
        for s in streams:
            s.close()

    # ------------------------------------------------------------ events fan-out

    def _on_event(self, event: Dict[str, Any]) -> None:
        # Called under the event log's lock, in seq order: stay quick, never block.
        with self._lock:
            targets = [s for s in self._streams.values()
                       if s.kind in ("events", "ws") and not s.closed]
        if not targets:
            return
        size = len(json.dumps(event, ensure_ascii=False, separators=(",", ":")))
        for s in targets:
            s.queued += size
            if s.queued > MAX_QUEUED:
                threading.Thread(target=s.close, daemon=True).start()
            else:
                s.q.put((event, size))

    # ------------------------------------------------------------ one thread per stream

    def _run(self, st: _Stream) -> None:
        try:
            st.sock.settimeout(SEND_TIMEOUT)
            if st.kind == "events":
                self._run_events(st)
            elif st.kind == "ws":
                self._run_ws(st)
            else:
                self._run_wait(st)
        except Exception:
            pass
        finally:
            st.close()
            with self._lock:
                self._streams.pop(st.id, None)

    def _send(self, st: _Stream, data: bytes) -> bool:
        try:
            st.sock.sendall(data)
            return True
        except (OSError, ValueError):
            return False

    def _peer_open(self, st: _Stream) -> bool:
        """Drain what the client sent; False once it hung up."""
        try:
            while True:
                ready, _, _ = select.select([st.sock], [], [], 0)
                if not ready:
                    return True
                data = st.sock.recv(65536)
                if not data:
                    return False
        except (OSError, ValueError):
            return False

    def _client_of(self, st: _Stream) -> Dict[str, Any]:
        """The ``client`` object of PROTOCOL §4 for the label of ``st``."""
        caps = [c for c in _CAPABILITY_ORDER if c in st.caps]
        return {"client_id": st.client_id or st.id, "capabilities": caps,
                "user": st.label.get("user") or "", "nickname": st.label.get("nickname") or ""}

    def _heartbeat_presence(self, st: _Stream) -> None:
        handler = self._handlers.get("presence.update")
        if handler is None or st.client_id is None:
            return
        try:
            handler({"client": self._client_of(st)})
        except Exception:
            pass

    # ------------------------------------------------------------ events (SSE)

    def _run_events(self, st: _Stream) -> None:
        since = None
        raw = st.headers.get("last-event-id") or st.query.get("since")
        if raw not in (None, ""):
            try:
                since = int(str(raw).strip())
                if since < 0:
                    raise ValueError
            except ValueError:
                self._send(st, _error_response(400, "Bad Request", "bad_request",
                                               "since: expected an integer >= 0"))
                return
        head = _http(200, "OK", ["Content-Type: text/event-stream", "Cache-Control: no-cache",
                                 "X-Accel-Buffering: no", "Connection: close"])
        if not self._send(st, head + (": darkpyonix events %s\n\n" % self.kernel_id).encode()):
            return
        # The listener already queues live events; the replay below is merged by seq.
        if since is None:
            cursor = self._events.seq
            replay, oldest = [], None  # type: List[Dict[str, Any]], Optional[int]
        else:
            replay, oldest = self._events.since(since)
            cursor = replay[-1]["seq"] if replay else since
        if oldest is not None:
            if not self._send(st, sse_frame({"type": "replay_truncated", "seq": None,
                                             "data": {"oldest_seq": oldest}})):
                return
        for ev in replay:
            if not self._deliver(st, ev):
                return
        history = "history" in st.caps
        now = time.monotonic()
        next_ka = now + KEEPALIVE
        next_presence = now
        while not st.closed:
            try:
                item = st.q.get(timeout=POLL)
            except queue.Empty:
                item = None
            if item is _STOP:
                return
            if item is not None:
                event, size = item
                st.queued -= size
                seq = event.get("seq")
                if isinstance(seq, int) and seq <= cursor:
                    continue
                if isinstance(seq, int):
                    cursor = seq
                if not self._deliver(st, event, history):
                    return
                next_ka = time.monotonic() + KEEPALIVE
                continue
            if not self._peer_open(st):
                return
            now = time.monotonic()
            if now >= next_presence:
                self._heartbeat_presence(st)
                next_presence = now + PRESENCE_HEARTBEAT
            if now >= next_ka:
                if not self._send(st, b": keepalive\n\n"):
                    return
                next_ka = now + KEEPALIVE

    def _deliver(self, st: _Stream, event: Dict[str, Any], history: Optional[bool] = None) -> bool:
        if history is None:
            history = "history" in st.caps
        if not history and event.get("type") in OUTPUT_EVENTS:
            return True  # FR-K9: no history capability, no outputs
        return self._send(st, sse_frame(event))

    # ------------------------------------------------------------ run-wait long poll

    def _run_wait(self, st: _Stream) -> None:
        parts = st.path.rstrip("/").split("/")
        # /api/kernels/{kernel_id}/runs/{run_ref}/wait
        if len(parts) < 3 or parts[-1] != "wait" or parts[-3] != "runs" or not parts[-2]:
            self._send(st, _error_response(404, "Not Found", "not_found", "not a run-wait path"))
            return
        ref = parts[-2]
        try:
            timeout = max(1, min(int(st.query.get("timeout", 60)), 300))
            poll = int(st.query.get("poll_sequence", 1))
            if poll < 1:
                raise ValueError
        except ValueError:
            self._send(st, _error_response(400, "Bad Request", "bad_request",
                                           "timeout and poll_sequence: expected integers (>= 1 for poll_sequence)"))
            return
        if poll > MAX_POLLS:
            self._send(st, _error_response(429, "Too Many Requests", "poll_limit_exceeded",
                                           "poll_sequence %d is above max_polls %d" % (poll, MAX_POLLS)))
            return
        handler = self._handlers.get("runs.wait")
        if handler is None:
            self._send(st, _error_response(404, "Not Found", "not_found", "no run waiting here"))
            return
        try:
            out = dict(handler({"run_id": ref, "timeout": timeout}))
        except DKPError as e:
            status, reason = _STATUS.get(e.code, (500, "Internal Server Error"))
            self._send(st, _error_response(status, reason, e.code, e.message))
            return
        except Exception as e:
            self._send(st, _error_response(500, "Internal Server Error", "internal", repr(e)))
            return
        if st.closed:
            return
        status = str(out.get("status", "running"))
        run_id = out.get("run_id") or ref
        out["status"], out["run_id"] = status, run_id
        out.setdefault("run", None)
        if "history" not in st.caps:
            out["run"] = None  # FR-A3: run history needs the history capability
        out["poll_sequence"] = poll
        if status in FINISHED:
            out["next"], out["next_poll_sequence"], out["auto_repoll"] = None, None, None
        else:
            nxt = "/api/kernels/%s/runs/%s/wait?timeout=%d&poll_sequence=%d" % (
                self.kernel_id, run_id, timeout, poll + 1)
            if st.query.get("client_id"):
                nxt += "&client_id=" + quote(st.query["client_id"], safe="")
            out["next"], out["next_poll_sequence"] = nxt, poll + 1
            out["auto_repoll"] = {"enabled": poll < MAX_POLLS, "next_poll_url": nxt,
                                  "recommended_delay": RECOMMENDED_DELAY, "max_polls": MAX_POLLS}
        self._send(st, _json_response(200, "OK", out))

    # ------------------------------------------------------------ WebSocket (PROTOCOL §5)

    def _run_ws(self, st: _Stream) -> None:
        key = st.headers.get("sec-websocket-key", "").encode("latin-1")
        accept = base64.b64encode(hashlib.sha1(key + WS_GUID).digest()).decode("ascii")
        if not self._send(st, _http(101, "Switching Protocols", [
                "Upgrade: websocket", "Connection: Upgrade", "Sec-WebSocket-Accept: " + accept])):
            return
        sync = _wsync.WsSync(self._handlers, st.caps, self._client_of(st))
        reader = _wsync.FrameReader()
        self._heartbeat_presence(st)   # joins the document before the first snapshot
        try:
            if not self._ws_send(st, sync.start()):
                return
        except DKPError:
            return
        now = time.monotonic()
        last_rx, next_ping, next_presence = now, now + _wsync.PING_INTERVAL, now + PRESENCE_HEARTBEAT
        while not st.closed:
            try:
                item = st.q.get(timeout=POLL)
            except queue.Empty:
                item = None
            if item is _STOP:
                return
            if item is not None:
                event, size = item
                st.queued -= size
                if not self._ws_send(st, sync.on_event(event)):
                    return
            got = self._ws_read(st, reader, sync)
            if got is None:
                return
            now = time.monotonic()
            if got:
                last_rx = now
            if now - last_rx > _wsync.IDLE_TIMEOUT:
                self._send(st, _wsync.close_frame(1001, "idle"))
                return
            if now >= next_ping:
                if not self._send(st, _wsync.frame(_wsync.OP_PING)):
                    return
                next_ping = now + _wsync.PING_INTERVAL
            if now >= next_presence:
                self._heartbeat_presence(st)
                next_presence = now + PRESENCE_HEARTBEAT

    def _ws_send(self, st: _Stream, messages: List[Dict[str, Any]]) -> bool:
        for m in messages:
            if not self._send(st, _wsync.text_frame(m)):
                return False
        return True

    def _ws_read(self, st: _Stream, reader: Any, sync: Any) -> Optional[bool]:
        """Handle what the client sent. ``None``: finished (closed, or it broke the protocol);
        else whether anything arrived."""
        arrived = False
        try:
            while True:
                ready, _, _ = select.select([st.sock], [], [], 0)
                if not ready:
                    return arrived
                data = st.sock.recv(65536)
                if not data:
                    return None
                arrived = True
                for opcode, payload in reader.feed(data):
                    if opcode == _wsync.OP_CLOSE:
                        self._send(st, _wsync.frame(_wsync.OP_CLOSE, payload[:2]))
                        return None
                    if opcode == _wsync.OP_PING:
                        self._send(st, _wsync.frame(_wsync.OP_PONG, payload))
                    elif opcode == _wsync.OP_TEXT:
                        try:
                            msg = json.loads(payload.decode("utf-8"))
                        except ValueError:
                            msg = None
                        if not self._ws_send(st, self._ws_answer(sync, msg)):
                            return None
                    elif opcode == _wsync.OP_BINARY:
                        self._send(st, _wsync.close_frame(1003, "text only"))
                        return None
        except _wsync.ProtocolError as e:
            self._send(st, _wsync.close_frame(e.code, str(e)))
            return None
        except (OSError, ValueError):
            return None

    @staticmethod
    def _ws_answer(sync: Any, msg: Any) -> List[Dict[str, Any]]:
        try:
            return sync.on_message(msg)
        except Exception as e:  # a broken handler must not drop the connection
            return [{"type": "error", "error": "INTERNAL", "message": repr(e)}]
