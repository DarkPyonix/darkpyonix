"""A blocking DKP/1 client for one kernel (PROTOCOL §3).

Used by the CLI, the manager and tests. A background reader thread demultiplexes responses
(by request id) and events (into a queue), so ``request`` is safe to call from many threads.

Standard library only; Python 3.8+.
"""
from __future__ import annotations

import base64
import itertools
import os
import queue
import socket
import threading
from typing import Any, Dict, Iterator, Optional

from . import _home
from ._protocol import DKPError, auth_mac, encode, recv_frame

_CLOSED = object()


class _Pending:
    __slots__ = ("done", "frame")

    def __init__(self) -> None:
        self.done = threading.Event()
        self.frame = None  # type: Optional[Dict[str, Any]]


class KernelClient:
    def __init__(self, port: int, kernel_id: str, key: Optional[bytes] = None,
                 host: str = "127.0.0.1", name: str = "client", kind: str = "cli") -> None:
        self.port = port
        self.kernel_id = kernel_id
        self.host = host
        self.name = name
        self.kind = kind
        self._key = key
        self.welcome = None  # type: Optional[Dict[str, Any]]
        self._sock = None  # type: Optional[socket.socket]
        self._send_lock = threading.Lock()
        self._lock = threading.Lock()
        self._ids = itertools.count(1)
        self._pending = {}  # type: Dict[Any, _Pending]
        self._events = queue.Queue()  # type: queue.Queue
        self._closed = False
        self._finished = False
        self._error = None  # type: Optional[DKPError]
        self._reader = None  # type: Optional[threading.Thread]

    # ------------------------------------------------------------ connection

    def connect(self, timeout: float = 5.0) -> "KernelClient":
        key = self._key if self._key is not None else _home.user_key()
        sock = socket.create_connection((self.host, self.port), timeout=timeout)
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            hello = recv_frame(sock)
            if hello.get("op") != "hello":
                raise DKPError("bad_request", "expected hello, got %r" % (hello.get("op"),))
            if hello.get("kernel_id") != self.kernel_id:
                raise DKPError("auth_failed", "port belongs to kernel %r, not %r"
                               % (hello.get("kernel_id"), self.kernel_id))
            nonce = base64.b64decode(hello["nonce"])
            sock.sendall(encode({
                "op": "auth",
                "client": {"name": self.name, "kind": self.kind, "pid": os.getpid()},
                "mac": auth_mac(key, nonce, self.kernel_id),
            }))
            reply = recv_frame(sock)
            if reply.get("op") != "welcome":
                raise DKPError(reply.get("code") or "auth_failed", reply.get("message") or "")
        except BaseException:
            sock.close()
            raise
        sock.settimeout(None)
        self.welcome = reply
        self._sock = sock
        self._reader = threading.Thread(target=self._read_loop, name="dkp-client-reader",
                                        daemon=True)
        self._reader.start()
        return self

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        if self._sock is not None:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._sock.close()
        self._finish()

    def __enter__(self) -> "KernelClient":
        if self._sock is None:
            self.connect()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------ requests

    def request(self, method: str, params: Optional[Dict[str, Any]] = None,
                timeout: Optional[float] = None) -> Any:
        rid = next(self._ids)
        pending = _Pending()
        with self._lock:
            if self._closed or self._sock is None:
                raise ConnectionError("not connected")
            self._pending[rid] = pending
        frame = encode({"op": "request", "id": rid, "method": method, "params": params or {}})
        try:
            with self._send_lock:
                self._sock.sendall(frame)
        except OSError:
            with self._lock:
                self._pending.pop(rid, None)
            raise ConnectionError("connection closed")
        if not pending.done.wait(timeout):
            with self._lock:
                self._pending.pop(rid, None)
            raise TimeoutError("no response to %r within %ss" % (method, timeout))
        resp = pending.frame
        if resp is None:
            if self._error is not None:
                raise self._error
            raise ConnectionError("connection closed")
        if resp.get("ok"):
            return resp.get("result")
        err = resp.get("error") or {}
        raise DKPError(err.get("code", "internal"), err.get("message", ""), err.get("data"))

    def subscribe(self, since: Optional[int] = None) -> Dict[str, Any]:
        params = {} if since is None else {"since": since}
        return self.request("subscribe", params)

    # ------------------------------------------------------------ events

    def next_event(self, timeout: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """The next event, or ``None`` on timeout or once the connection is closed."""
        try:
            ev = self._events.get(timeout=timeout)
        except queue.Empty:
            return None
        if ev is _CLOSED:
            self._events.put(_CLOSED)  # keep later callers from blocking
            return None
        return ev

    def events(self) -> Iterator[Dict[str, Any]]:
        while True:
            ev = self.next_event()
            if ev is None:
                return
            yield ev

    # ------------------------------------------------------------ reader thread

    def _read_loop(self) -> None:
        sock = self._sock
        try:
            while True:
                frame = recv_frame(sock)
                op = frame.get("op")
                if op == "response":
                    with self._lock:
                        pending = self._pending.pop(frame.get("id"), None)
                    if pending is not None:
                        pending.frame = frame
                        pending.done.set()
                elif op == "event":
                    self._events.put(frame)
                elif op == "error":
                    self._error = DKPError(frame.get("code") or "internal",
                                           frame.get("message") or "")
                # PR-4: other ops are ignored
        except (OSError, ConnectionError, ValueError, DKPError):
            pass
        with self._lock:
            self._closed = True
        self._finish()

    def _finish(self) -> None:
        with self._lock:
            pending = list(self._pending.values())
            self._pending.clear()
            finished = self._finished
            self._finished = True
        for p in pending:
            p.done.set()
        if not finished:
            self._events.put(_CLOSED)
