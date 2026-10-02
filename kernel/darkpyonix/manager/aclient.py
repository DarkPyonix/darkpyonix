"""Asyncio DKP/1 client used by the manager (docs/PROTOCOL.md §3).

One ``KernelConnection`` per kernel is shared by every HTTP request of a manager. It
subscribes to the kernel's event stream once (``since: 0``), mirrors the kernel's replay
ring locally and fans events out to any number of listeners (SSE streams), each of which
may resume from its own sequence number (SPEC FR-M1, FR-M5, PR-3).

SUPERSEDED PROTOTYPE: the manager is being rewritten in Rust. This Python module is kept as
a reference for the contract's behaviour; the language-neutral oracle is tests/ (fake DKP/1
kernel in tests/helpers, HTTP-level tests runnable via DARKPYONIX_MANAGER_CMD).
"""
from __future__ import annotations

import asyncio
import base64
import collections
import itertools
import json
import os
import struct
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

from darkpyonix.kernel import protocol
from darkpyonix.kernel.protocol import DKPError

REQUEST_TIMEOUT = 30.0
LISTENER_QUEUE_MAX = 10000


async def _read_frame(reader: asyncio.StreamReader) -> Dict[str, Any]:
    header = await reader.readexactly(4)
    (length,) = struct.unpack(">I", header)
    if length > protocol.MAX_FRAME:
        raise protocol.FrameTooLarge(length)
    message = json.loads((await reader.readexactly(length)).decode("utf-8"))
    if not isinstance(message, dict):
        raise DKPError("bad_request", "frame payload is not a JSON object")
    return message


class Listener:
    """One subscriber's view of the event stream. Iterate with ``async for``."""

    def __init__(self, conn: "KernelConnection", cursor: int) -> None:
        self._conn = conn
        self.cursor = cursor
        self.queue = asyncio.Queue(maxsize=LISTENER_QUEUE_MAX)  # type: asyncio.Queue

    def offer(self, event: Optional[Dict[str, Any]]) -> None:
        if event is None:
            self._force(None)
            return
        if event["seq"] <= self.cursor:
            return
        self.cursor = event["seq"]
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            # A client too slow to keep up is cut off; it resumes with Last-Event-ID.
            self._conn.detach(self)
            self._force(None)

    def _force(self, item: Any) -> None:
        while True:
            try:
                self.queue.put_nowait(item)
                return
            except asyncio.QueueFull:
                self.queue.get_nowait()

    def close(self) -> None:
        self._conn.detach(self)

    def __aiter__(self):
        return self

    async def __anext__(self) -> Dict[str, Any]:
        item = await self.queue.get()
        if item is None:
            raise StopAsyncIteration
        return item


class KernelConnection:
    """An authenticated control connection to one kernel."""

    def __init__(self, kernel_id: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 session: Optional[str], seq: int) -> None:
        self.kernel_id = kernel_id
        self.session = session
        self.last_seq = seq
        self._reader = reader
        self._writer = writer
        self._ids = itertools.count(1)
        self._pending = {}  # type: Dict[int, asyncio.Future]
        self._listeners = set()  # type: Set[Listener]
        self._mirror = collections.deque(maxlen=protocol.EVENT_RING_MAX_EVENTS)  # type: Deque[Dict[str, Any]]
        self._kernel_oldest = None  # type: Optional[int]
        self._subscribed = False
        self._sub_lock = asyncio.Lock()
        self.closed = asyncio.Event()
        self._task = asyncio.ensure_future(self._read_loop())

    # ------------------------------------------------------------ connect / close

    @classmethod
    async def connect(cls, host: str, port: int, kernel_id: Optional[str], key: bytes,
                      timeout: float = protocol.HANDSHAKE_TIMEOUT) -> "KernelConnection":
        """Open a connection and complete the hello/auth/welcome handshake (PROTOCOL §3.2)."""
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
        except (OSError, asyncio.TimeoutError) as exc:
            raise DKPError("kernel_unreachable", "cannot connect to kernel: %s" % (exc,))
        try:
            hello = await asyncio.wait_for(_read_frame(reader), timeout)
            if hello.get("op") != "hello":
                raise DKPError("kernel_unreachable", "expected hello, got %r" % (hello.get("op"),))
            kid = hello.get("kernel_id")
            if kernel_id is not None and kid != kernel_id:
                raise DKPError("kernel_unreachable", "port belongs to %s, not %s" % (kid, kernel_id))
            nonce = base64.b64decode(hello.get("nonce", ""))
            writer.write(protocol.encode({
                "op": "auth",
                "client": {"name": "manager", "kind": "manager", "pid": os.getpid()},
                "mac": protocol.auth_mac(key, nonce, kid),
            }))
            await writer.drain()
            reply = await asyncio.wait_for(_read_frame(reader), timeout)
        except DKPError:
            writer.close()
            raise
        except (OSError, ValueError, asyncio.IncompleteReadError, asyncio.TimeoutError) as exc:
            writer.close()
            raise DKPError("kernel_unreachable", "handshake failed: %s" % (exc,))
        if reply.get("op") != "welcome":
            writer.close()
            raise DKPError(reply.get("code", "auth_failed"), reply.get("message", "handshake refused"))
        return cls(kid, reader, writer, reply.get("session"), int(reply.get("seq") or 0))

    async def close(self) -> None:
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):
            pass
        self._shutdown()

    def _shutdown(self) -> None:
        if self.closed.is_set():
            return
        self.closed.set()
        try:
            self._writer.close()
        except Exception:
            pass
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(DKPError("kernel_unreachable", "connection to kernel lost"))
        self._pending.clear()
        for listener in list(self._listeners):
            listener.offer(None)
        self._listeners.clear()

    # ------------------------------------------------------------ reading

    async def _read_loop(self) -> None:
        try:
            while True:
                frame = await _read_frame(self._reader)
                op = frame.get("op")
                if op == "response":
                    fut = self._pending.pop(frame.get("id"), None)
                    if fut is not None and not fut.done():
                        if frame.get("ok"):
                            fut.set_result(frame.get("result"))
                        else:
                            err = frame.get("error") or {}
                            fut.set_exception(DKPError(err.get("code", "internal"),
                                                       err.get("message", ""), err.get("data")))
                elif op == "event":
                    self._on_event(frame)
                elif op == "error":
                    break
        except (asyncio.IncompleteReadError, OSError, ValueError, DKPError):
            pass
        finally:
            self._shutdown()

    def _on_event(self, frame: Dict[str, Any]) -> None:
        if frame.get("type") == "replay_truncated":
            oldest = (frame.get("data") or {}).get("oldest_seq")
            if isinstance(oldest, int):
                self._kernel_oldest = oldest
            return
        try:
            seq = int(frame["seq"])
        except (KeyError, TypeError, ValueError):
            return
        event = {"seq": seq, "type": frame.get("type"), "time": frame.get("time"),
                 "data": frame.get("data") or {}}
        if self._mirror and seq <= self._mirror[-1]["seq"]:
            return
        self._mirror.append(event)
        if seq > self.last_seq:
            self.last_seq = seq
        for listener in list(self._listeners):
            listener.offer(event)

    # ------------------------------------------------------------ requests

    async def request(self, method: str, params: Optional[Dict[str, Any]] = None,
                      timeout: float = REQUEST_TIMEOUT) -> Any:
        if self.closed.is_set():
            raise DKPError("kernel_unreachable", "connection to kernel lost")
        rid = next(self._ids)
        fut = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        try:
            self._writer.write(protocol.encode({"op": "request", "id": rid, "method": method,
                                                "params": params or {}}))
            await self._writer.drain()
        except OSError as exc:
            self._pending.pop(rid, None)
            self._shutdown()
            raise DKPError("kernel_unreachable", str(exc))
        try:
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            self._pending.pop(rid, None)
            raise DKPError("kernel_unreachable", "kernel did not answer %s within %.0f s" % (method, timeout))

    # ------------------------------------------------------------ events

    async def ensure_subscribed(self) -> None:
        async with self._sub_lock:
            if not self._subscribed:
                await self.request("subscribe", {"since": 0})
                self._subscribed = True

    def oldest_available(self) -> int:
        oldest = self._kernel_oldest or 1
        if self._mirror and len(self._mirror) == self._mirror.maxlen:
            oldest = max(oldest, self._mirror[0]["seq"])
        return oldest

    async def attach(self, since: Optional[int]) -> Tuple[List[Dict[str, Any]], Optional[int], Listener]:
        """Subscribe a listener.

        Returns ``(backlog, truncated_oldest, listener)``: the mirrored events after ``since``,
        the oldest sequence number still available when ``since`` is older than that (else
        ``None``), and a listener that yields every later event exactly once.
        ``since=None`` means live events only.
        """
        await self.ensure_subscribed()
        if self.closed.is_set():
            raise DKPError("kernel_unreachable", "connection to kernel lost")
        if since is None:
            since = self.last_seq
        truncated = None
        oldest = self.oldest_available()
        if since + 1 < oldest:
            truncated = oldest
        backlog = [e for e in self._mirror if e["seq"] > since]
        listener = Listener(self, backlog[-1]["seq"] if backlog else since)
        self._listeners.add(listener)
        return backlog, truncated, listener

    def detach(self, listener: Listener) -> None:
        self._listeners.discard(listener)
