"""The kernel's DKP/1 control channel server (PROTOCOL §3; FR-A1, PR-1..PR-4).

One selector thread serves every client with non-blocking sockets. Outgoing bytes are
buffered per client, so a slow reader never blocks other clients or the thread calling
``EventLog.append``. Request handlers may block (they call into the executor), so they run
on a small worker pool, never on the selector thread.

Standard library only; Python 3.8+.
"""
from __future__ import annotations

import base64
import json
import secrets
import selectors
import socket
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional

from . import protocol as _p
from .events import EventLog
from .protocol import DKPError, FrameTooLarge, encode

Handler = Callable[[Dict[str, Any]], Any]

MAX_OUTBUF = 64 * 1024 * 1024    # a client further behind than this is dropped
MAX_PREAUTH_FRAME = 64 * 1024    # an unauthenticated peer cannot make us buffer more
RECV_CHUNK = 256 * 1024
HANDLER_WORKERS = 4


class _Conn:
    __slots__ = ("sock", "inbuf", "outbuf", "nonce", "deadline", "authed", "session",
                 "subscribed", "pending", "closing", "dead", "closed", "mask")

    def __init__(self, sock: socket.socket, deadline: float) -> None:
        self.sock = sock
        self.inbuf = bytearray()
        self.outbuf = bytearray()
        self.nonce = _p.new_nonce()
        self.deadline = deadline
        self.authed = False
        self.session = ""
        self.subscribed = False
        self.pending = None  # type: Optional[List[Any]]  # events seen while subscribing
        self.closing = False  # close once outbuf is flushed; stop reading
        self.dead = False     # close now
        self.closed = False
        self.mask = selectors.EVENT_READ


def _error_frame(code: str, message: str) -> bytes:
    return encode({"op": "error", "code": code, "message": message})


def _response(rid: Any, result: Any = None, error: Optional[DKPError] = None) -> bytes:
    if error is not None:
        return encode({"op": "response", "id": rid, "ok": False, "error": error.to_dict()})
    try:
        return encode({"op": "response", "id": rid, "ok": True, "result": result})
    except FrameTooLarge as e:
        return _response(rid, error=e)
    except (TypeError, ValueError) as e:
        return _response(rid, error=DKPError("internal", "result is not JSON: %r" % (e,)))


class ControlServer:
    def __init__(self, kernel_id: str, key: bytes, events: EventLog,
                 handlers: Dict[str, Handler], host: str = "127.0.0.1", port: int = 0) -> None:
        self.kernel_id = kernel_id
        self._key = key
        self._events = events
        self._handlers = handlers
        self._host = host
        self._port = port
        self._lock = threading.Lock()
        self._conns = []  # type: List[_Conn]
        self._sel = None  # type: Optional[selectors.BaseSelector]
        self._listen = None  # type: Optional[socket.socket]
        self._wake_r = None  # type: Optional[socket.socket]
        self._wake_w = None  # type: Optional[socket.socket]
        self._pool = None  # type: Optional[ThreadPoolExecutor]
        self._thread = None  # type: Optional[threading.Thread]
        self._running = False

    # ------------------------------------------------------------ lifecycle

    @property
    def port(self) -> int:
        return self._port

    def start(self) -> None:
        listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen.bind((self._host, self._port))
        listen.listen(64)
        listen.setblocking(False)
        self._listen = listen
        self._port = listen.getsockname()[1]
        self._wake_r, self._wake_w = socket.socketpair()
        self._wake_r.setblocking(False)
        self._wake_w.setblocking(False)
        self._sel = selectors.DefaultSelector()
        self._sel.register(listen, selectors.EVENT_READ, None)
        self._sel.register(self._wake_r, selectors.EVENT_READ, None)
        self._pool = ThreadPoolExecutor(max_workers=HANDLER_WORKERS,
                                        thread_name_prefix="dkp-handler")
        self._running = True
        self._events.add_listener(self._on_event)
        self._thread = threading.Thread(target=self._loop, name="dkp-control", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        self._events.remove_listener(self._on_event)
        self._wake()
        if self._thread is not None:
            self._thread.join(5)
        with self._lock:
            for c in self._conns:
                self._close(c)
            self._conns = []
        for s in (self._listen, self._wake_r, self._wake_w):
            try:
                s.close()
            except Exception:
                pass
        if self._sel is not None:
            self._sel.close()
        if self._pool is not None:
            self._pool.shutdown(wait=False)

    def client_count(self) -> int:
        with self._lock:
            return sum(1 for c in self._conns if c.authed and not c.closed)

    # ------------------------------------------------------------ sending (any thread)

    def _wake(self) -> None:
        try:
            self._wake_w.send(b"x")
        except (BlockingIOError, OSError, AttributeError):
            pass

    def _send_locked(self, c: _Conn, data: bytes) -> None:
        """Queue ``data`` for ``c``; write directly when nothing is queued. Hold ``_lock``."""
        if c.closed or c.dead:
            return
        if not c.outbuf:
            try:
                n = c.sock.send(data)
            except (BlockingIOError, InterruptedError):
                n = 0
            except OSError:
                c.dead = True
                self._wake()
                return
            if n == len(data):
                return
            data = data[n:]
        c.outbuf += data
        if len(c.outbuf) > MAX_OUTBUF:
            c.dead = True
            c.outbuf = bytearray()
        self._wake()

    def _send(self, c: _Conn, data: bytes) -> None:
        with self._lock:
            self._send_locked(c, data)

    def _on_event(self, event: Dict[str, Any]) -> None:
        # Called by EventLog.append under the log's lock, in seq order.
        with self._lock:
            targets = [c for c in self._conns if c.subscribed or c.pending is not None]
            if not targets:
                return
            try:
                data = encode(event)
            except (FrameTooLarge, TypeError, ValueError):
                return
            for c in targets:
                if c.subscribed:
                    self._send_locked(c, data)
                else:
                    c.pending.append((event["seq"], data))

    # ------------------------------------------------------------ selector thread

    def _loop(self) -> None:
        sel = self._sel
        while self._running:
            timeout = None
            now = time.monotonic()
            with self._lock:
                deadlines = [c.deadline for c in self._conns if not c.authed and not c.closed]
            if deadlines:
                timeout = max(0.0, min(deadlines) - now)
            try:
                ready = sel.select(timeout)
            except OSError:
                if not self._running:
                    break
                raise
            for key, mask in ready:
                if key.fileobj is self._listen:
                    self._accept()
                elif key.fileobj is self._wake_r:
                    try:
                        while self._wake_r.recv(4096):
                            pass
                    except (BlockingIOError, OSError):
                        pass
                else:
                    c = key.data
                    if mask & selectors.EVENT_WRITE:
                        self._flush(c)
                    if mask & selectors.EVENT_READ:
                        self._readable(c)
            self._housekeeping()

    def _accept(self) -> None:
        while True:
            try:
                sock, _ = self._listen.accept()
            except (BlockingIOError, InterruptedError):
                return
            except OSError:
                return
            sock.setblocking(False)
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
            c = _Conn(sock, time.monotonic() + _p.HANDSHAKE_TIMEOUT)
            with self._lock:
                self._conns.append(c)
                self._sel.register(sock, selectors.EVENT_READ, c)
                self._send_locked(c, encode({
                    "op": "hello", "kernel_id": self.kernel_id,
                    "nonce": base64.b64encode(c.nonce).decode("ascii"),
                    "kernel_version": _p.KERNEL_VERSION,
                }))

    def _flush(self, c: _Conn) -> None:
        with self._lock:
            if c.closed or not c.outbuf:
                return
            try:
                n = c.sock.send(c.outbuf)
            except (BlockingIOError, InterruptedError):
                return
            except OSError:
                c.dead = True
                return
            del c.outbuf[:n]

    def _housekeeping(self) -> None:
        now = time.monotonic()
        with self._lock:
            keep = []
            for c in self._conns:
                if not c.authed and now >= c.deadline:
                    c.dead = True
                if c.dead or (c.closing and not c.outbuf):
                    self._close(c)
                    continue
                keep.append(c)
                mask = 0 if c.closing else selectors.EVENT_READ
                if c.outbuf:
                    mask |= selectors.EVENT_WRITE
                if mask != c.mask:
                    self._sel.modify(c.sock, mask, c)
                    c.mask = mask
            self._conns = keep

    def _close(self, c: _Conn) -> None:
        if c.closed:
            return
        c.closed = True
        c.subscribed = False
        c.pending = None
        try:
            self._sel.unregister(c.sock)
        except Exception:
            pass
        try:
            c.sock.close()
        except OSError:
            pass

    def _readable(self, c: _Conn) -> None:
        if c.closed or c.closing or c.dead:
            return
        try:
            data = c.sock.recv(RECV_CHUNK)
        except (BlockingIOError, InterruptedError):
            return
        except OSError:
            data = b""
        if not data:
            c.dead = True
            return
        c.inbuf += data
        while len(c.inbuf) >= 4 and not (c.closing or c.dead):
            (length,) = struct.unpack(">I", bytes(c.inbuf[:4]))
            limit = _p.MAX_FRAME if c.authed else MAX_PREAUTH_FRAME
            if length > limit:
                self._send(c, _error_frame("frame_too_large",
                                           "frame of %d bytes exceeds %d" % (length, limit)))
                c.closing = True
                c.inbuf = bytearray()
                return
            if len(c.inbuf) < 4 + length:
                return
            payload = bytes(c.inbuf[4:4 + length])
            del c.inbuf[:4 + length]
            try:
                msg = json.loads(payload.decode("utf-8"))
                if not isinstance(msg, dict):
                    raise ValueError("frame payload is not a JSON object")
            except ValueError as e:  # includes JSONDecodeError and UnicodeDecodeError
                self._bad_frame(c, repr(e))
                continue
            if c.authed:
                self._on_message(c, msg)
            else:
                self._on_auth(c, msg)

    # ------------------------------------------------------------ protocol

    def _bad_frame(self, c: _Conn, message: str) -> None:
        if c.authed:
            self._send(c, _response(None, error=DKPError("bad_request", message)))
        else:
            self._send(c, _error_frame("bad_request", message))
            c.closing = True

    def _on_auth(self, c: _Conn, msg: Dict[str, Any]) -> None:
        if msg.get("op") == "auth" and _p.verify_mac(self._key, c.nonce, self.kernel_id,
                                                     msg.get("mac", "")):
            c.authed = True
            c.session = "s_" + secrets.token_hex(8)
            self._send(c, encode({"op": "welcome", "session": c.session,
                                  "seq": self._events.seq}))
        else:
            self._send(c, _error_frame("auth_failed", "authentication failed"))
            c.closing = True

    def _on_message(self, c: _Conn, msg: Dict[str, Any]) -> None:
        rid = msg.get("id")
        if msg.get("op") != "request":
            self._send(c, _response(rid, error=DKPError("bad_request", "expected a request")))
            return
        method = msg.get("method")
        params = msg.get("params")
        if params is None:
            params = {}
        if not isinstance(method, str) or not isinstance(params, dict):
            self._send(c, _response(rid, error=DKPError(
                "bad_request", "request needs a string method and object params")))
            return
        if method == "subscribe":
            self._subscribe(c, rid, params)
        elif method == "unsubscribe":
            with self._lock:
                c.subscribed = False
                c.pending = None
                self._send_locked(c, _response(rid, {}))
        else:
            handler = self._handlers.get(method)
            if handler is None:
                self._send(c, _response(rid, error=DKPError(
                    "unknown_method", "unknown method %r" % (method,))))
                return
            self._pool.submit(self._call, c, rid, handler, params)

    def _call(self, c: _Conn, rid: Any, handler: Handler, params: Dict[str, Any]) -> None:
        try:
            data = _response(rid, handler(params))
        except DKPError as e:
            data = _response(rid, error=e)
        except Exception as e:
            data = _response(rid, error=DKPError("internal", repr(e)))
        self._send(c, data)

    def _subscribe(self, c: _Conn, rid: Any, params: Dict[str, Any]) -> None:
        since = params.get("since")
        if since is not None and (isinstance(since, bool) or not isinstance(since, int)):
            self._send(c, _response(rid, error=DKPError("bad_request", "since must be an int")))
            return
        # Start capturing live events before taking the snapshot, then merge by seq, so no
        # event is lost or duplicated between replay and live streaming.
        with self._lock:
            c.subscribed = False
            c.pending = []
        if since is None:
            replay, oldest = [], None  # type: List[Dict[str, Any]], Optional[int]
            last = self._events.seq
        else:
            replay, oldest = self._events.since(since)
            last = replay[-1]["seq"] if replay else since
        frames = []
        if oldest is not None:
            frames.append(encode({"op": "event", "seq": None, "type": "replay_truncated",
                                  "time": _p.now_iso(), "data": {"oldest_seq": oldest}}))
        for ev in replay:
            try:
                frames.append(encode(ev))
            except (FrameTooLarge, TypeError, ValueError):
                pass
        with self._lock:
            if c.closed or c.pending is None:
                return
            live = [(s, d) for s, d in c.pending if s > last]
            seq = max([last] + [s for s, _ in live])
            frames.append(_response(rid, {"seq": seq, "replayed": len(replay)}))
            frames.extend(d for _, d in live)
            c.pending = None
            c.subscribed = True
            self._send_locked(c, b"".join(frames))
