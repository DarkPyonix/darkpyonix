"""The 2025 WebSocket synchronization (SPEC FR-S9; PROTOCOL §5).

Two parts, both standard library only:

* RFC 6455 framing: ``FrameReader`` (client frames in, masked, fragments joined) and the
  ``*_frame`` encoders (server frames out, never masked);
* ``WsSync``: the messages of one connection, as pure functions of kernel events and client
  messages. It does no I/O: the stream thread (``_streams.StreamHub``) sends what it returns.

The kernel never checks a capability to refuse a request (FR-K9, INTENT D5); the label's
capabilities only decide what is *sent* (FR-S9) and, for ``start_typing``, whether the kernel
asks the document to lock at all, as the 2025 protocol answered ``INSUFFICIENT_PERMISSION``.
"""
from __future__ import annotations

import json
import struct
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import _document
from ._protocol import DKPError

PING_INTERVAL = 30.0     # PROTOCOL §5.1 (leader proposal; the 2025 spec has no value)
IDLE_TIMEOUT = 60.0
MAX_MESSAGE = 1024 * 1024

OP_CONT, OP_TEXT, OP_BINARY, OP_CLOSE, OP_PING, OP_PONG = 0x0, 0x1, 0x2, 0x8, 0x9, 0xA

Handler = Callable[[Dict[str, Any]], Any]


# ---------------------------------------------------------------- framing

class ProtocolError(Exception):
    """The peer broke RFC 6455; ``code`` is the close code to answer with."""

    def __init__(self, code: int, message: str) -> None:
        Exception.__init__(self, message)
        self.code = code


def frame(opcode: int, payload: bytes = b"") -> bytes:
    """One unmasked, final server frame."""
    n = len(payload)
    if n < 126:
        head = struct.pack(">BB", 0x80 | opcode, n)
    elif n < 65536:
        head = struct.pack(">BBH", 0x80 | opcode, 126, n)
    else:
        head = struct.pack(">BBQ", 0x80 | opcode, 127, n)
    return head + payload


def text_frame(obj: Any) -> bytes:
    return frame(OP_TEXT, json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def close_frame(code: int = 1000, reason: str = "") -> bytes:
    return frame(OP_CLOSE, struct.pack(">H", code) + reason.encode("utf-8")[:120])


class FrameReader(object):
    """Incremental parser of client frames. ``feed`` returns complete messages as
    ``(opcode, payload)``: text/binary messages joined from fragments, and control frames."""

    def __init__(self, max_message: int = MAX_MESSAGE) -> None:
        self.buf = bytearray()
        self.max_message = max_message
        self._opcode = None  # type: Optional[int]
        self._parts = []     # type: List[bytes]
        self._size = 0

    def feed(self, data: bytes) -> List[Tuple[int, bytes]]:
        self.buf += data
        out = []  # type: List[Tuple[int, bytes]]
        buf = self.buf
        while len(buf) >= 2:
            b0, b1 = buf[0], buf[1]
            fin, rsv, opcode = bool(b0 & 0x80), b0 & 0x70, b0 & 0x0F
            masked, length, pos = bool(b1 & 0x80), b1 & 0x7F, 2
            if rsv:
                raise ProtocolError(1002, "reserved bits are set")
            if not masked:
                raise ProtocolError(1002, "client frames must be masked")
            if length == 126:
                if len(buf) < 4:
                    break
                length, pos = struct.unpack(">H", bytes(buf[2:4]))[0], 4
            elif length == 127:
                if len(buf) < 10:
                    break
                length, pos = struct.unpack(">Q", bytes(buf[2:10]))[0], 10
            control = bool(opcode & 0x8)
            if control and (length > 125 or not fin):
                raise ProtocolError(1002, "bad control frame")
            if length > self.max_message:
                raise ProtocolError(1009, "message too big")
            if len(buf) < pos + 4 + length:
                break
            mask = bytes(buf[pos:pos + 4])
            payload = bytearray(buf[pos + 4:pos + 4 + length])
            del buf[:pos + 4 + length]
            for i in range(length):
                payload[i] ^= mask[i & 3]
            payload = bytes(payload)
            if control:
                out.append((opcode, payload))
                continue
            if opcode in (OP_TEXT, OP_BINARY):
                if self._opcode is not None:
                    raise ProtocolError(1002, "new message inside a fragmented one")
                self._opcode, self._parts, self._size = opcode, [payload], len(payload)
            elif opcode == OP_CONT:
                if self._opcode is None:
                    raise ProtocolError(1002, "continuation without a message")
                self._parts.append(payload)
                self._size += len(payload)
            else:
                raise ProtocolError(1002, "unknown opcode")
            if self._size > self.max_message:
                raise ProtocolError(1009, "message too big")
            if fin:
                out.append((self._opcode, b"".join(self._parts)))
                self._opcode, self._parts, self._size = None, [], 0
        return out


# ---------------------------------------------------------------- messages

def _name(who: Any) -> str:
    """2025 ``user_name``: the user, else the nickname."""
    if isinstance(who, dict):
        return str(who.get("user") or who.get("nickname") or "")
    return ""


def _dp(cell: Dict[str, Any]) -> Dict[str, Any]:
    md = cell.get("metadata") if isinstance(cell, dict) else None
    dp = md.get("darkpyonix") if isinstance(md, dict) else None
    return dp if isinstance(dp, dict) else {}


class _Doc(object):
    """A snapshot cell with the attributes ``_document._map_run`` reads."""

    def __init__(self, c: Dict[str, Any], pos: int) -> None:
        self.id = c.get("cell_id")
        self.source_sha256 = c.get("source_sha256")
        self.index = c.get("index", pos)


def match_run_cells(snap_cells: List[Dict[str, Any]],
                    log: Dict[str, Any]) -> List[Tuple[Optional[Dict[str, Any]], Dict[str, Any], bool]]:
    """Match the records of run ``log`` to the cells of a snapshot (SPEC FR-S9, K2).

    Uses the FR-R4 rule (``_document._map_run``: id, then source hash, then index, each
    record once). Returns ``(snapshot_cell, record, stale)`` per record; the cell is None
    and ``stale`` true when no rule holds, and ``stale`` is true when the hashes differ.
    """
    cells = [_Doc(c, i) for i, c in enumerate(snap_cells)]
    records = _document._records(log)
    found = _document._map_run(cells, list(range(len(cells))), records)
    by_rec = dict((id(rec), pos) for pos, rec in found.items())
    out = []
    for rec in records:
        pos = by_rec.get(id(rec))
        if pos is None:
            out.append((None, rec, True))
        else:
            out.append((snap_cells[pos], rec,
                        rec.get("source_sha256") != cells[pos].source_sha256))
    return out


def _blank_sources(event: Dict[str, Any]) -> Dict[str, Any]:
    """Copy of ``event`` without cell code (no ``read``)."""
    data = event.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("cell"), dict):
        return event
    cell = dict(data["cell"], source="")
    if isinstance(cell.get("conflict"), dict):
        cell["conflict"] = dict(cell["conflict"], disk_source=None)
    return dict(event, data=dict(data, cell=cell))


class WsSync(object):
    """The messages of one WebSocket connection (PROTOCOL §5)."""

    def __init__(self, handlers: Dict[str, Handler], caps: Any, client: Dict[str, Any]) -> None:
        self.h = handlers
        self.caps = set(caps)
        self.client = client
        self.me = client["client_id"]
        self.cursor = 0                       # last event seq already reflected in a message
        self.focus = {}                       # type: Dict[str, Optional[str]]
        self._interrupted = {}                # type: Dict[str, Dict[str, Any]]

    # ------------------------------------------------------------ helpers

    def _call(self, method: str, params: Dict[str, Any]) -> Any:
        handler = self.h.get(method)
        if handler is None:
            raise DKPError("not_found", "no method %s" % method)
        return handler(params)

    def _snapshot(self) -> Optional[Dict[str, Any]]:
        try:
            return self._call("doc.snapshot", {})
        except DKPError:
            return None

    def _run_cell(self, run_id: Any, index: Any) -> Optional[Dict[str, Any]]:
        """The cell record of ``run_id`` at file index ``index`` (from the run log)."""
        if not isinstance(run_id, str):
            return None
        try:
            log = self._call("runs.get", {"run_id": run_id})
        except DKPError:
            return None
        for c in log.get("cells") or []:
            if _dp(c).get("index") == index:
                return c
        return None

    def _outputs(self, cell: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if cell is None or "history" not in self.caps:
            return []
        return list(cell.get("outputs") or [])

    @staticmethod
    def _stamp(msg: Dict[str, Any], seq: Any) -> Dict[str, Any]:
        if isinstance(seq, int):
            msg["seq"] = seq
        return msg

    # ------------------------------------------------------------ connection start

    def start(self) -> List[Dict[str, Any]]:
        """``users_focus_data`` (once, right after the upgrade). Sets the event cursor, so
        events the snapshot already reflects are not sent twice."""
        snap = self._snapshot()
        users = []  # type: List[Dict[str, Any]]
        if snap is not None:
            self.cursor = snap.get("seq") if isinstance(snap.get("seq"), int) else 0
            for p in snap.get("presence") or []:
                self.focus[p["client_id"]] = p.get("focused_cell_id")
                if p.get("focused_cell_id") and "read" in self.caps:
                    users.append({"user_id": p["client_id"], "user_name": _name(p),
                                  "user_avatar": p.get("avatar"),
                                  "focused_cell_id": p["focused_cell_id"],
                                  "focused_at": p.get("focused_at")})
        return [self._stamp({"type": "users_focus_data", "users_focus": users}, self.cursor)]

    # ------------------------------------------------------------ kernel events -> messages

    def on_event(self, ev: Dict[str, Any]) -> List[Dict[str, Any]]:
        seq = ev.get("seq")
        if isinstance(seq, int):
            if seq <= self.cursor:
                return []
            self.cursor = seq
        t = ev.get("type")
        d = ev.get("data") if isinstance(ev.get("data"), dict) else {}
        when = ev.get("time")
        out = []  # type: List[Dict[str, Any]]
        history = "history" in self.caps
        read = "read" in self.caps
        if t == "doc.lock":
            lk = d.get("lock") or {}
            out.append({"type": "cell_locked", "cell_id": d.get("cell_id"),
                        "locked_by": lk.get("locked_by"), "user_name": _name(lk),
                        "locked_at": lk.get("locked_at")})
        elif t == "doc.unlock":
            by = d.get("by") or {}
            code = ""
            if read:
                snap = self._snapshot()
                for c in (snap or {}).get("cells") or []:
                    if c.get("cell_id") == d.get("cell_id"):
                        code = c.get("source") or ""
            out.append({"type": "cell_unlocked_with_code", "cell_id": d.get("cell_id"),
                        "previously_locked_by": by.get("client_id"), "user_name": _name(by),
                        "updated_code": code})
        elif t == "presence.update":
            out.extend(self._presence_messages(d, when, ev))
        elif t == "presence.leave":
            prev = self.focus.pop(d.get("client_id"), None)
            if prev and d.get("client_id") != self.me and read:
                out.append({"type": "other_user_blur", "cell_id": prev,
                            "user_id": d.get("client_id"), "user_name": _name(d),
                            "timestamp": when})
            out.append({"type": "event", "event": ev})
        elif t == "cell.started":
            cell = self._run_cell(d.get("run_id"), d.get("index"))
            by = d.get("started_by") or {}
            out.append({"type": "execution_started", "cell_id": d.get("cell_id"),
                        "execution_id": d.get("run_id"), "started_by": by.get("client_id"),
                        "user_name": _name(by),
                        "code": (cell or {}).get("source", "") if read else "",
                        "timestamp": when})
        elif t == "output":
            if history:
                out.append({"type": "execution_output", "cell_id": d.get("cell_id"),
                            "execution_id": d.get("run_id"), "outputs": [d.get("output")],
                            "is_complete": False, "timestamp": when})
        elif t == "cell.finished":
            out.extend(self._finished_messages(d, when))
        elif t == "run.finished":
            pending = self._interrupted.pop(d.get("run_id"), None)
            if pending is not None:
                by = d.get("interrupted_by") or {}
                pending.update({"interrupted_by": by.get("client_id"), "user_name": _name(by)})
                out.append(pending)
            out.append({"type": "event", "event": ev})
        elif t in ("output.clear",):
            if history:
                out.append({"type": "event", "event": ev})
        else:
            out.append({"type": "event", "event": ev if read else _blank_sources(ev)})
        return [self._stamp(m, seq) for m in out]

    def _presence_messages(self, d: Dict[str, Any], when: Any, ev: Dict[str, Any]) -> List[Dict[str, Any]]:
        cid = d.get("client_id")
        cur = d.get("focused_cell_id")
        prev = self.focus.get(cid)
        self.focus[cid] = cur
        if cur == prev:
            return [{"type": "event", "event": ev}]  # cursor move, join, avatar change
        out = []  # type: List[Dict[str, Any]]
        if cid != self.me and "read" in self.caps:
            if prev:
                out.append({"type": "other_user_blur", "cell_id": prev, "user_id": cid,
                            "user_name": _name(d), "timestamp": when})
            if cur:
                out.append({"type": "other_user_focus", "cell_id": cur, "user_id": cid,
                            "user_name": _name(d), "user_avatar": d.get("avatar"),
                            "timestamp": when})
        return out

    def _finished_messages(self, d: Dict[str, Any], when: Any) -> List[Dict[str, Any]]:
        status = d.get("status")
        cell = self._run_cell(d.get("run_id"), d.get("index"))
        outputs = self._outputs(cell)
        base = {"cell_id": d.get("cell_id"), "execution_id": d.get("run_id")}
        if status == "ok":
            m = dict(base, type="execution_complete", final_outputs=outputs,
                     execution_time=d.get("duration"), completed_at=when)
            return [m]
        if status in ("interrupted", "cancelled"):
            # `interrupted_by` is known only at run.finished: send then (see on_event).
            m = dict(base, type="execution_interrupted", interrupted_by=None, user_name="",
                     interrupted_at=when, partial_outputs=outputs)
            self._interrupted[d.get("run_id")] = m
            return []
        error = {"ename": "", "evalue": "", "traceback": []}  # type: Dict[str, Any]
        for o in reversed(outputs):
            if o.get("output_type") == "error":
                error = {"ename": o.get("ename", ""), "evalue": o.get("evalue", ""),
                         "traceback": list(o.get("traceback") or [])}
                break
        return [dict(base, type="execution_error", error=error, failed_at=when)]

    # ------------------------------------------------------------ client messages -> replies

    def on_message(self, msg: Any) -> List[Dict[str, Any]]:
        if not isinstance(msg, dict) or not isinstance(msg.get("type"), str):
            return [{"type": "error", "error": "BAD_REQUEST"}]
        t = msg["type"]
        read = "read" in self.caps
        if t == "request_code":
            return [self._code_data(read)]
        if t == "request_history":
            return [self._history_data()]
        if t == "request_locks":
            return [self._locks_data(read)]
        if t == "start_typing":
            return self._start_typing(msg)
        if t == "cell_focus":
            return self._focus(msg.get("cell_id"), read)
        if t == "cell_blur":
            return self._focus(None, read)
        return [{"type": "error", "error": "UNKNOWN_TYPE"}]

    def _code_data(self, read: bool) -> Dict[str, Any]:
        snap = self._snapshot() or {}
        counts = {}  # type: Dict[Any, Any]
        snap_cells = snap.get("cells") or []
        try:
            log = self._call("runs.get", {"run_id": "latest"})
            for cell, rec, _stale in match_run_cells(snap_cells, log):
                if cell is not None:
                    counts[cell["cell_id"]] = rec.get("execution_count")
        except DKPError:
            pass
        cells = [{"cell_id": c["cell_id"], "cell_type": c.get("type"),
                  "source": c.get("source", "") if read else "",
                  "execution_count": counts.get(c["cell_id"])} for c in snap_cells]
        return self._stamp({"type": "code_data", "cells": cells,
                            "doc_version": snap.get("doc_version")}, snap.get("seq"))

    def _history_data(self) -> Dict[str, Any]:
        history = []  # type: List[Dict[str, Any]]
        if "history" in self.caps:
            snap = self._snapshot() or {}
            try:
                log = self._call("runs.get", {"run_id": "latest"})
            except DKPError:
                log = {}
            for cell, rec, stale in match_run_cells(snap.get("cells") or [], log):
                history.append({"cell_id": cell["cell_id"] if cell else None,
                                "execution_count": rec.get("execution_count"),
                                "outputs": list(rec.get("outputs") or []),
                                "executed_at": rec.get("ended_at"),
                                "stale": stale})
        return {"type": "history_data", "history": history}

    def _locks_data(self, read: bool) -> Dict[str, Any]:
        locks = []  # type: List[Dict[str, Any]]
        if read:
            for c in (self._snapshot() or {}).get("cells") or []:
                lk = c.get("lock")
                if lk:
                    locks.append({"cell_id": c["cell_id"], "is_locked": True,
                                  "locked_by": lk.get("locked_by"), "user_name": _name(lk),
                                  "locked_at": lk.get("locked_at")})
                else:
                    locks.append({"cell_id": c["cell_id"], "is_locked": False})
        return {"type": "locks_data", "locks": locks}

    def _start_typing(self, msg: Dict[str, Any]) -> List[Dict[str, Any]]:
        if "edit" not in self.caps:
            return [{"type": "error", "error": "INSUFFICIENT_PERMISSION"}]
        cell_id = msg.get("cell_id")
        try:
            self._call("doc.lock", {"client": self.client, "cell_id": cell_id})
        except DKPError as e:
            if e.code == "locked":
                return [{"type": "error", "error": "CELL_ALREADY_LOCKED", "cell_id": cell_id,
                         "locked_by": (e.data or {}).get("locked_by")}]
            if e.code == "not_found":
                return [{"type": "error", "error": "CELL_NOT_FOUND", "cell_id": cell_id}]
            return [{"type": "error", "error": "BAD_REQUEST", "cell_id": cell_id}]
        return []  # the lock reaches everyone, this client included, as cell_locked

    def _focus(self, cell_id: Any, read: bool) -> List[Dict[str, Any]]:
        if not read:
            return [{"type": "error", "error": "INSUFFICIENT_PERMISSION"}]
        try:
            self._call("presence.update", {"client": self.client, "focused_cell_id": cell_id})
        except DKPError as e:
            return [{"type": "error", "error": "CELL_NOT_FOUND" if e.code == "not_found"
                     else "BAD_REQUEST", "cell_id": cell_id}]
        return []
