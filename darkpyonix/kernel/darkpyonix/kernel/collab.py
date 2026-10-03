"""The kernel's shared document: cells, locks and presence (SPEC §10a FR-S1..S5, S8; PROTOCOL §4).

Standard library only; Python 3.8+.

``DocumentState`` holds the parsed file in memory and serves the PROTOCOL §4 methods. The
kernel registers ``handlers()`` on its control server; every state change is announced
through ``emit(type, data)`` (normally ``EventLog.append``).

Concurrency: one re-entrant lock guards all state, and events are emitted while it is held.
Events therefore leave in the order the state changed, and ``doc.snapshot`` can report the
event ``seq`` that matches the state it returns. ``emit`` must not call back into this object
(``EventLog.append`` only notifies listeners, which is fine).

Identity (FR-S1): a cell keeps its ``cell_id`` for the kernel's lifetime. The id is the
cell's ``# @id`` when the file has one, else ``c_<hex>`` generated here and never written
to the file. ``version`` (per cell) starts at 1 and grows with every content change;
``doc_version`` grows only on cell create/update/delete/move and on reload. Lock, unlock,
conflict and presence events carry the current ``doc_version`` without changing it.

Snapshot ``seq`` is the seq of the last event already reflected in the snapshot: state changes,
their events and snapshots all happen under one lock, so a client that subscribes with
``since=seq`` sees every later change exactly once.

``request_id`` (optional, client-chosen, 1..64 chars) on a ``doc.*`` / ``presence.*`` request
is echoed by every event that request causes.

Disk (FR-S5): edits are written atomically ``save_debounce`` seconds after the last one,
through ``darkpyonix.format`` so untouched cells keep their exact bytes. The background
thread polls the file's ``(mtime_ns, size)``; on change it hashes the bytes, and a hash other
than the last one this object read or wrote means an external edit: the file is re-parsed,
cells are matched ``cell_id`` → ``source_sha256`` → position, and ``doc.reloaded`` is sent.
A locked cell that changed on disk keeps the lock holder's content and is marked
``conflict`` (``doc.conflict``) until the holder updates or the lock is released. While a
cell is in conflict, saves write the disk version of that cell, so an unrelated edit does
not overwrite the external change before the conflict is resolved.

Presence (FR-S4): a client joins with its first ``presence.update`` and leaves with
``presence.leave`` or when no ``presence.update`` arrived for ``presence_grace_seconds``.
The manager keeps a client present by sending ``presence.update`` (with no focus/cursor
fields, a heartbeat that emits nothing) while that client's event stream is open.
"""
from __future__ import annotations

import datetime
import hashlib
import os
import re
import secrets
import sys
import tempfile
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from darkpyonix import format as dpformat
from darkpyonix.kernel.protocol import DKPError, now_iso

Emit = Callable[[str, Dict[str, Any]], None]
Handler = Callable[[Dict[str, Any]], Dict[str, Any]]

# FR-A3 / FR-S8 permission ladder.
PERMISSION_RANK = {"viewer1": 1, "viewer2": 2, "viewer3": 3, "editor": 4, "admin": 5}
EDIT_PERMISSION = "editor"      # cell edits and locks (FR-S8)
PRESENCE_PERMISSION = "viewer1"  # presence, focus, cursor (FR-S8)

CURSOR_MIN_INTERVAL = 1.0 / 20  # FR-S4: at most 20 cursor events per second per client

_TYPE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")  # FORMAT §2.2 ``[type]``


def by_of(client: Dict[str, Any]) -> Dict[str, Any]:
    """``{client_id, user, nickname}``: the ``by`` / ``started_by`` attribution (PROTOCOL §4)."""
    return {"client_id": client.get("client_id"), "user": client.get("user", ""),
            "nickname": client.get("nickname", "")}


def _iso_after(seconds: float) -> str:
    t = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=seconds)
    return t.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (t.microsecond // 1000)


def _new_cell_id() -> str:
    return "c_" + secrets.token_hex(6)


def _int_param(params: Dict[str, Any], name: str, required: bool = True) -> Optional[int]:
    value = params.get(name)
    if value is None and not required:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise DKPError("bad_request", "%s must be an integer" % name)
    return value


def _str_param(params: Dict[str, Any], name: str, required: bool = True) -> Optional[str]:
    value = params.get(name)
    if value is None and not required:
        return None
    if not isinstance(value, str):
        raise DKPError("bad_request", "%s must be a string" % name)
    return value


def _request(fn: Handler) -> Handler:  # type: ignore[misc]
    """Serve a ``DocumentState`` method under its lock with ``params["request_id"]`` set, so
    every event the request causes echoes it (PROTOCOL §4)."""
    def wrapper(self: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            rid = params.get("request_id") if isinstance(params, dict) else None
            if rid is not None and (not isinstance(rid, str) or not 1 <= len(rid) <= 64):
                raise DKPError("bad_request", "request_id must be a string of 1 to 64 characters")
            outer = self._rid
            self._rid = rid
            try:
                return fn(self, params)  # type: ignore[call-arg]
            finally:
                self._rid = outer
    wrapper.__name__ = fn.__name__
    wrapper.__doc__ = fn.__doc__
    return wrapper  # type: ignore[return-value]


class _Entry(object):
    """One cell of the shared document."""

    __slots__ = ("cell", "cell_id", "version", "disk_cell", "conflict")

    def __init__(self, cell: Any, cell_id: str, version: int = 1) -> None:
        self.cell = cell              # darkpyonix.format.Cell (the content clients see)
        self.cell_id = cell_id
        self.version = version
        self.conflict = False         # FR-S5: changed on disk while locked
        self.disk_cell = None         # type: Any  # the disk version while in conflict (None: deleted)


class _Lock(object):
    __slots__ = ("cell_id", "client", "locked_at", "last_activity", "last_activity_iso")

    def __init__(self, cell_id: str, client: Dict[str, Any], now: float) -> None:
        self.cell_id = cell_id
        self.client = client
        self.locked_at = now_iso()
        self.last_activity = now
        self.last_activity_iso = self.locked_at


class _Presence(object):
    __slots__ = ("client", "focused_cell_id", "focused_at", "cursor", "last_seen",
                 "last_seen_iso", "last_emit", "pending", "pending_rid")

    def __init__(self, client: Dict[str, Any], now: float) -> None:
        self.client = client
        self.focused_cell_id = None  # type: Optional[str]
        self.focused_at = None  # type: Optional[str]
        self.cursor = None  # type: Optional[Dict[str, Any]]
        self.last_seen = now
        self.last_seen_iso = now_iso()
        self.last_emit = None  # type: Optional[float]
        self.pending = False  # a coalesced cursor change waits to be emitted
        self.pending_rid = None  # type: Optional[str]  # request_id of the last coalesced update


class DocumentState(object):
    def __init__(self, path: str, emit: Emit, clock: Callable[[], float] = time.monotonic,
                 idle_lock_seconds: float = 180, presence_grace_seconds: float = 30,
                 save_debounce: float = 0.3, poll_interval: float = 0.5,
                 seq_provider: Optional[Callable[[], int]] = None) -> None:
        self.path = os.path.abspath(path)
        self._emit_cb = emit
        self._clock = clock
        self.idle_lock_seconds = float(idle_lock_seconds)
        self.presence_grace_seconds = float(presence_grace_seconds)
        self.save_debounce = float(save_debounce)
        self.poll_interval = float(poll_interval)
        self._seq = seq_provider if seq_provider is not None else (lambda: 0)

        self._lock = threading.RLock()
        self._entries = []  # type: List[_Entry]
        self._bom = False
        self._locks = {}  # type: Dict[str, _Lock]
        self._presence = {}  # type: Dict[str, _Presence]
        self.doc_version = 0
        self._dirty = False
        self._save_due = 0.0
        self._next_poll = 0.0
        self._known_stat = None  # type: Optional[Tuple[int, int]]
        self._known_sha = None  # type: Optional[str]
        self._rid = None  # type: Optional[str]  # request_id of the request being served

        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = None  # type: Optional[threading.Thread]

        data = self._read_bytes()
        text = "" if data is None else data.decode("utf-8")
        doc = dpformat.parse(text, self.path)
        self._bom = doc.bom
        taken = set()  # type: set
        for cell in doc.cells:
            cid = cell.id if cell.id and cell.id not in taken else _new_cell_id()
            taken.add(cid)
            self._entries.append(_Entry(cell, cid))
        if data is not None:
            self._known_sha = hashlib.sha256(data).hexdigest()
            self._known_stat = self._stat()

    # ------------------------------------------------------------ lifecycle

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="dp-collab", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the background thread and write any pending edit."""
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(5)
            self._thread = None
        self.flush()

    def flush(self) -> None:
        """Write pending edits now (e.g. before a run reads the file)."""
        with self._lock:
            if self._dirty:
                self._save()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception as e:  # never let the thread die
                sys.stderr.write("darkpyonix collab: %r\n" % (e,))
            delay = max(0.005, min(self.poll_interval, self._next_due() - self._clock()))
            self._wake.wait(delay)
            self._wake.clear()

    def _next_due(self) -> float:
        with self._lock:
            due = [self._next_poll]
            if self._dirty:
                due.append(self._save_due)
            for lk in self._locks.values():
                due.append(lk.last_activity + self.idle_lock_seconds)
            for p in self._presence.values():
                due.append(p.last_seen + self.presence_grace_seconds)
                if p.pending and p.last_emit is not None:
                    due.append(p.last_emit + CURSOR_MIN_INTERVAL)
            return min(due)

    def tick(self) -> None:
        """Do whatever is due at ``clock()``: file poll, debounced save, coalesced cursors,
        idle locks, presence grace. Called by the background thread; tests call it directly."""
        with self._lock:
            now = self._clock()
            if now >= self._next_poll:
                self._next_poll = now + self.poll_interval
                self._check_disk()
            if self._dirty and now >= self._save_due:
                self._save()
            for p in list(self._presence.values()):
                if p.pending and (p.last_emit is None
                                  or now - p.last_emit >= CURSOR_MIN_INTERVAL):
                    self._emit_presence(p, now, p.pending_rid)
            for cell_id, lk in list(self._locks.items()):
                if now - lk.last_activity >= self.idle_lock_seconds:
                    self._release(cell_id, "idle", lk.client)
            for cid, p in list(self._presence.items()):
                if now - p.last_seen >= self.presence_grace_seconds:
                    self._leave(cid, p.client)

    # ------------------------------------------------------------ public helpers for the kernel

    def handlers(self) -> Dict[str, Handler]:
        return {
            "doc.snapshot": self.snapshot,
            "doc.cell.create": self.cell_create,
            "doc.cell.update": self.cell_update,
            "doc.cell.delete": self.cell_delete,
            "doc.cell.move": self.cell_move,
            "doc.lock": self.lock,
            "doc.unlock": self.unlock,
            "presence.update": self.presence_update,
            "presence.leave": self.presence_leave,
        }

    def indices_for(self, cell_ids: List[str]) -> List[int]:
        """Current indices of ``cell_ids`` (for ``run`` with ``cell_ids``); ``not_found`` if any is unknown."""
        with self._lock:
            pos = dict((e.cell_id, i) for i, e in enumerate(self._entries))
            out = []
            for cid in cell_ids:
                if cid not in pos:
                    raise DKPError("not_found", "no cell %r" % (cid,), {"cell_id": cid})
                out.append(pos[cid])
            return out

    def cell_ids(self) -> List[str]:
        with self._lock:
            return [e.cell_id for e in self._entries]

    # ------------------------------------------------------------ helpers

    def _emit(self, type_: str, data: Dict[str, Any], rid: Optional[str] = None) -> None:
        rid = rid if rid is not None else self._rid
        if rid is not None:
            data["request_id"] = rid
        self._emit_cb(type_, data)

    def _client(self, params: Dict[str, Any], need: str) -> Dict[str, Any]:
        client = params.get("client")
        if not isinstance(client, dict) or not isinstance(client.get("client_id"), str) \
                or not client.get("client_id"):
            raise DKPError("bad_request", "client with a client_id is required")
        perm = client.get("permission")
        if perm not in PERMISSION_RANK:
            raise DKPError("forbidden", "unknown permission %r" % (perm,))
        if PERMISSION_RANK[perm] < PERMISSION_RANK[need]:
            raise DKPError("forbidden", "%s permission is required" % need,
                           {"permission": perm, "required": need})
        out = {"client_id": client["client_id"], "permission": perm,
               "user": client.get("user") if isinstance(client.get("user"), str) else "",
               "nickname": client.get("nickname") if isinstance(client.get("nickname"), str) else ""}
        if isinstance(client.get("avatar"), str):
            out["avatar"] = client["avatar"]
        # Any request from a present client counts as a sign of life.
        p = self._presence.get(out["client_id"])
        if p is not None:
            p.last_seen = self._clock()
            p.last_seen_iso = now_iso()
        return out

    def _find(self, cell_id: Any) -> int:
        if not isinstance(cell_id, str):
            raise DKPError("bad_request", "cell_id must be a string")
        for i, e in enumerate(self._entries):
            if e.cell_id == cell_id:
                return i
        raise DKPError("not_found", "no cell %r" % (cell_id,), {"cell_id": cell_id})

    def _reindex(self) -> None:
        for i, e in enumerate(self._entries):
            e.cell.index = i

    def _lock_dict(self, lk: _Lock) -> Dict[str, Any]:
        remaining = max(0.0, lk.last_activity + self.idle_lock_seconds - self._clock())
        return {"cell_id": lk.cell_id, "locked_by": lk.client["client_id"],
                "user": lk.client.get("user", ""), "nickname": lk.client.get("nickname", ""),
                "locked_at": lk.locked_at, "last_activity": lk.last_activity_iso,
                "expires_at": _iso_after(remaining)}

    def _cell_dict(self, e: _Entry) -> Dict[str, Any]:
        c = e.cell
        out = {"cell_id": e.cell_id, "index": c.index, "type": c.type, "raw_type": c.raw_type,
               "title": c.title,
               "metadata": dict(c.metadata), "source": c.source,
               "source_sha256": c.source_sha256, "version": e.version}  # type: Dict[str, Any]
        lk = self._locks.get(e.cell_id)
        if lk is not None:
            out["lock"] = self._lock_dict(lk)
        if e.conflict:
            out["conflict"] = {"disk_source": None if e.disk_cell is None else e.disk_cell.source}
        return out

    def _presence_dict(self, p: _Presence) -> Dict[str, Any]:
        c = p.client
        out = {"client_id": c["client_id"], "nickname": c.get("nickname", ""),
               "user": c.get("user", ""), "permission": c["permission"],
               "focused_cell_id": p.focused_cell_id, "focused_at": p.focused_at,
               "cursor": p.cursor, "last_seen": p.last_seen_iso}  # type: Dict[str, Any]
        if "avatar" in c:
            out["avatar"] = c["avatar"]
        return out

    def _touch_doc(self) -> None:
        self.doc_version += 1
        self._dirty = True
        self._save_due = self._clock() + self.save_debounce
        self._wake.set()

    def _check_lock(self, cell_id: str, client: Dict[str, Any]) -> Optional[_Lock]:
        lk = self._locks.get(cell_id)
        if lk is not None and lk.client["client_id"] != client["client_id"]:
            raise DKPError("locked", "cell %r is locked by %s" % (cell_id, lk.client["client_id"]),
                           {"locked_by": lk.client["client_id"], "lock": self._lock_dict(lk)})
        return lk

    def _renew(self, lk: Optional[_Lock]) -> None:
        if lk is not None:
            lk.last_activity = self._clock()
            lk.last_activity_iso = now_iso()

    @staticmethod
    def _metadata_param(params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        md = params.get("metadata")
        if md is None:
            return None
        if not isinstance(md, dict) or not all(isinstance(k, str) for k in md):
            raise DKPError("bad_request", "metadata must be an object")
        return md

    @staticmethod
    def _type_param(params: Dict[str, Any], required: bool) -> Optional[str]:
        t = _str_param(params, "type", required)
        if t is None:
            return None
        if not _TYPE_RE.match(t) or t.lower() == dpformat.PREAMBLE:
            raise DKPError("bad_request", "invalid cell type %r" % (t,))
        return t

    # ------------------------------------------------------------ doc.snapshot

    def snapshot(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        with self._lock:
            return {
                "doc_version": self.doc_version,
                "seq": self._seq(),
                "cells": [self._cell_dict(e) for e in self._entries],
                "presence": [self._presence_dict(p) for p in self._presence.values()],
            }

    # ------------------------------------------------------------ edits (FR-S2)

    @_request
    def cell_create(self, params: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            client = self._client(params, EDIT_PERMISSION)
            type_ = self._type_param(params, False) or dpformat.CODE
            source = _str_param(params, "source", False) or ""
            title = _str_param(params, "title", False) or None
            metadata = self._metadata_param(params) or {}
            if params.get("after") is not None and params.get("before") is not None:
                raise DKPError("bad_request", "give after or before, not both")
            if params.get("after") is not None:
                pos = self._find(params["after"]) + 1
            elif params.get("before") is not None:
                pos = self._find(params["before"])
                if pos == 0:
                    raise DKPError("bad_request", "nothing can precede the preamble")
            else:
                pos = len(self._entries)
            mid = metadata.get("id")
            taken = set(e.cell_id for e in self._entries)
            cell_id = mid if isinstance(mid, str) and mid and mid not in taken else _new_cell_id()
            lowered = type_.lower()
            cell = dpformat.Cell(
                pos, dpformat.TYPE_ALIASES.get(lowered, lowered), source, raw_type=type_,
                title=title, metadata=dict(metadata), header="",
                source_sha256=dpformat.source_sha256(source),
                id=mid if isinstance(mid, str) else None)
            entry = _Entry(cell, cell_id)
            self._entries.insert(pos, entry)
            self._reindex()
            self._touch_doc()
            out = self._cell_dict(entry)
            self._emit("doc.cell.created", {"doc_version": self.doc_version, "cell": out,
                                            "by": by_of(client)})
            return {"cell": out}

    def _apply_update(self, entry: _Entry, params: Dict[str, Any]) -> bool:
        """Apply source/type/title/metadata from ``params``. True if anything changed."""
        cell = entry.cell
        source = _str_param(params, "source", False)
        type_ = self._type_param(params, False)
        title = _str_param(params, "title", False) if "title" in params else None
        metadata = self._metadata_param(params)
        if cell.type == dpformat.PREAMBLE and (type_ is not None or metadata is not None
                                               or title is not None):
            raise DKPError("bad_request", "the preamble has only a source")
        regen = False
        changed = False
        if source is not None and source != cell.source:
            cell.source = source
            cell.source_sha256 = dpformat.source_sha256(source)
            changed = True
        if type_ is not None and type_ != (cell.raw_type or cell.type):
            lowered = type_.lower()
            cell.type = dpformat.TYPE_ALIASES.get(lowered, lowered)
            cell.raw_type = type_
            regen = True
        if "title" in params and (title or None) != cell.title:
            cell.title = title or None
            regen = True
        if metadata is not None and metadata != cell.metadata:
            cell.metadata = dict(metadata)
            mid = metadata.get("id")
            cell.id = mid if isinstance(mid, str) else None
            regen = True
        if regen:
            cell.header = ""  # serialize generates a fresh marker + metadata block
            changed = True
        if changed:
            entry.version += 1
        return changed

    def _resolve_conflict(self, entry: _Entry) -> None:
        entry.conflict = False
        entry.disk_cell = None

    @_request
    def cell_update(self, params: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            client = self._client(params, EDIT_PERMISSION)
            i = self._find(params.get("cell_id"))
            entry = self._entries[i]
            base = _int_param(params, "base_version")
            lk = self._check_lock(entry.cell_id, client)
            if base != entry.version:
                raise DKPError("conflict", "cell %r is at version %d, not %d"
                               % (entry.cell_id, entry.version, base),
                               {"cell": self._cell_dict(entry)})
            changed = self._apply_update(entry, params)
            self._renew(lk)
            if lk is not None and entry.conflict:
                self._resolve_conflict(entry)
                if not changed:
                    entry.version += 1
                changed = True
            out = self._cell_dict(entry)
            if changed:
                self._touch_doc()
                out = self._cell_dict(entry)
                self._emit("doc.cell.updated", {"doc_version": self.doc_version, "cell": out,
                                                "by": by_of(client)})
            return {"cell": out}

    @_request
    def cell_delete(self, params: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            client = self._client(params, EDIT_PERMISSION)
            i = self._find(params.get("cell_id"))
            entry = self._entries[i]
            if i == 0:
                raise DKPError("bad_request", "the preamble cannot be deleted")
            base = _int_param(params, "base_version")
            self._check_lock(entry.cell_id, client)
            if base != entry.version:
                raise DKPError("conflict", "cell %r is at version %d, not %d"
                               % (entry.cell_id, entry.version, base),
                               {"cell": self._cell_dict(entry)})
            del self._entries[i]
            self._locks.pop(entry.cell_id, None)
            for p in self._presence.values():
                if p.focused_cell_id == entry.cell_id:
                    p.focused_cell_id = None
                if p.cursor and p.cursor.get("cell_id") == entry.cell_id:
                    p.cursor = None
            self._reindex()
            self._touch_doc()
            self._emit("doc.cell.deleted", {"doc_version": self.doc_version,
                                            "cell_id": entry.cell_id, "by": by_of(client)})
            return {"deleted": True}

    @_request
    def cell_move(self, params: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            client = self._client(params, EDIT_PERMISSION)
            i = self._find(params.get("cell_id"))
            to = _int_param(params, "to_index")
            if i == 0 or to < 1 or to >= len(self._entries):
                raise DKPError("bad_request", "to_index must be between 1 and %d and the preamble "
                               "stays first" % (len(self._entries) - 1))
            entry = self._entries.pop(i)
            self._entries.insert(to, entry)
            self._reindex()
            if i != to:
                self._touch_doc()
            out = self._cell_dict(entry)
            if i != to:
                self._emit("doc.cell.moved", {"doc_version": self.doc_version, "cell": out,
                                              "by": by_of(client)})
            return {"cell": out}

    # ------------------------------------------------------------ locks (FR-S3)

    @_request
    def lock(self, params: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            client = self._client(params, EDIT_PERMISSION)
            entry = self._entries[self._find(params.get("cell_id"))]
            lk = self._check_lock(entry.cell_id, client)
            if lk is not None:  # already ours: renew
                self._renew(lk)
                return {"lock": self._lock_dict(lk)}
            lk = _Lock(entry.cell_id, client, self._clock())
            self._locks[entry.cell_id] = lk
            out = self._lock_dict(lk)
            self._emit("doc.lock", {"doc_version": self.doc_version, "cell_id": entry.cell_id,
                                    "lock": out, "by": by_of(client)})
            self._wake.set()
            return {"lock": out}

    @_request
    def unlock(self, params: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            client = self._client(params, EDIT_PERMISSION)
            entry = self._entries[self._find(params.get("cell_id"))]
            lk = self._check_lock(entry.cell_id, client)
            if params.get("source") is not None:
                base = _int_param(params, "base_version", required=False)
                if base is not None and base != entry.version:
                    raise DKPError("conflict", "cell %r is at version %d, not %d"
                                   % (entry.cell_id, entry.version, base),
                                   {"cell": self._cell_dict(entry)})
                _str_param(params, "source")
                if self._apply_update(entry, {"source": params["source"]}):
                    if entry.conflict:
                        self._resolve_conflict(entry)
                    self._touch_doc()
                    self._emit("doc.cell.updated", {"doc_version": self.doc_version,
                                                    "cell": self._cell_dict(entry),
                                                    "by": by_of(client)})
            if lk is not None:
                self._release(entry.cell_id, "released", client)
            return {"cell": self._cell_dict(entry)}

    def _release(self, cell_id: str, reason: str, by: Dict[str, Any]) -> None:
        """Drop the lock on ``cell_id``. A conflict resolves to the holder's content."""
        lk = self._locks.pop(cell_id, None)
        if lk is None:
            return
        for e in self._entries:
            if e.cell_id == cell_id and e.conflict:
                self._resolve_conflict(e)
                e.version += 1
                self._touch_doc()
                self._emit("doc.cell.updated", {"doc_version": self.doc_version,
                                                "cell": self._cell_dict(e),
                                                "by": by_of(lk.client)})
        self._emit("doc.unlock", {"doc_version": self.doc_version, "cell_id": cell_id,
                                  "by": by_of(by), "reason": reason})

    # ------------------------------------------------------------ presence (FR-S4)

    def _cursor_param(self, value: Any) -> Optional[Dict[str, Any]]:
        if value is None:
            return None
        if not isinstance(value, dict):
            raise DKPError("bad_request", "cursor must be an object")
        cid = value.get("cell_id")
        self._find(cid)
        out = {"cell_id": cid}  # type: Dict[str, Any]
        for k in ("line", "column"):
            v = value.get(k)
            if isinstance(v, bool) or not isinstance(v, int) or v < 0:
                raise DKPError("bad_request", "cursor.%s must be a non-negative integer" % k)
            out[k] = v
        sel = value.get("selection")
        if sel is not None:
            ok = (isinstance(sel, list) and len(sel) == 2 and all(
                isinstance(pt, list) and len(pt) == 2
                and all(isinstance(n, int) and not isinstance(n, bool) and n >= 0 for n in pt)
                for pt in sel))
            if not ok:
                raise DKPError("bad_request", "cursor.selection must be [[line, col], [line, col]]")
            out["selection"] = [list(pt) for pt in sel]
        return out

    def _emit_presence(self, p: _Presence, now: float, rid: Optional[str] = None) -> None:
        p.pending = False
        p.pending_rid = None
        p.last_emit = now
        data = self._presence_dict(p)
        data["doc_version"] = self.doc_version
        self._emit("presence.update", data, rid)

    @_request
    def presence_update(self, params: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            client = self._client(params, PRESENCE_PERMISSION)
            now = self._clock()
            cid = client["client_id"]
            p = self._presence.get(cid)
            joined = p is None
            if joined:
                p = _Presence(client, now)
                self._presence[cid] = p
            urgent = joined or p.client != client
            p.client = client
            p.last_seen = now
            p.last_seen_iso = now_iso()
            if "focused_cell_id" in params:
                focus = params["focused_cell_id"]
                if focus is not None:
                    self._find(focus)
                if focus != p.focused_cell_id:
                    p.focused_cell_id = focus
                    p.focused_at = now_iso() if focus is not None else None
                    urgent = True
            cursor_changed = False
            if "cursor" in params:
                cursor = self._cursor_param(params["cursor"])
                if cursor != p.cursor:
                    p.cursor = cursor
                    cursor_changed = True
            if urgent:
                self._emit_presence(p, now)
            elif cursor_changed:
                if p.last_emit is None or now - p.last_emit >= CURSOR_MIN_INTERVAL:
                    self._emit_presence(p, now)
                else:
                    p.pending = True
                    p.pending_rid = self._rid
            self._wake.set()
            return {}

    @_request
    def presence_leave(self, params: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            client = self._client(params, PRESENCE_PERMISSION)
            self._leave(client["client_id"], client)
            return {}

    def _leave(self, client_id: str, client: Dict[str, Any]) -> None:
        p = self._presence.pop(client_id, None)
        for cell_id, lk in list(self._locks.items()):
            if lk.client["client_id"] == client_id:
                self._release(cell_id, "disconnected", lk.client)
        if p is not None:
            data = self._presence_dict(p)
            data["focused_cell_id"] = None
            data["cursor"] = None
            data["doc_version"] = self.doc_version
            self._emit("presence.leave", data)

    # ------------------------------------------------------------ disk (FR-S5)

    def _read_bytes(self) -> Optional[bytes]:
        try:
            with open(self.path, "rb") as f:
                return f.read()
        except (FileNotFoundError, NotADirectoryError):
            return None

    def _stat(self) -> Optional[Tuple[int, int]]:
        try:
            st = os.stat(self.path)
        except OSError:
            return None
        return (st.st_mtime_ns, st.st_size)

    def _check_disk(self) -> bool:
        """Reload if the file changed outside this object. True if it did."""
        st = self._stat()
        if st is None or st == self._known_stat:
            return False
        data = self._read_bytes()
        if data is None:
            return False
        sha = hashlib.sha256(data).hexdigest()
        self._known_stat = st
        if sha == self._known_sha:
            return False
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            return False  # half-written or not a notebook; look again on the next change
        self._known_sha = sha
        self._reload(text)
        return True

    def _reload(self, text: str) -> None:
        new = dpformat.parse(text, self.path)
        old = self._entries
        assign = [None] * len(new.cells)  # type: List[Optional[int]]
        used = set()  # type: set
        assign[0] = 0
        used.add(0)
        by_id = dict((e.cell_id, i) for i, e in enumerate(old))
        for j in range(1, len(new.cells)):  # 1. cell_id
            c = new.cells[j]
            i = by_id.get(c.id) if c.id else None
            if i is not None and i not in used and i != 0:
                assign[j] = i
                used.add(i)
        for j in range(1, len(new.cells)):  # 2. source_sha256
            if assign[j] is None:
                sha = new.cells[j].source_sha256
                for i in range(1, len(old)):
                    if i not in used and old[i].cell.source_sha256 == sha \
                            and not (old[i].cell.id and new.cells[j].id):
                        assign[j] = i
                        used.add(i)
                        break
        # 3. order: the remaining cells pair up in file order, same type only, never crossing
        # (an edited cell keeps its id; a deleted-and-added one does not steal another's).
        rest = [i for i in range(1, len(old)) if i not in used and not old[i].cell.id]
        k = 0
        for j in range(1, len(new.cells)):
            if assign[j] is not None or new.cells[j].id:
                continue
            for m in range(k, len(rest)):
                if old[rest[m]].cell.type == new.cells[j].type:
                    assign[j] = rest[m]
                    used.add(rest[m])
                    k = m + 1
                    break

        taken = set(old[i].cell_id for i in used)
        entries = []  # type: List[_Entry]
        conflicts = []  # type: List[_Entry]
        for j, c in enumerate(new.cells):
            i = assign[j]
            if i is None:
                cid = c.id if c.id and c.id not in taken else _new_cell_id()
                taken.add(cid)
                entries.append(_Entry(c, cid))
                continue
            e = old[i]
            mine = e.cell
            same = (mine.source_sha256 == c.source_sha256 and mine.type == c.type
                    and mine.title == c.title and mine.metadata == c.metadata)
            if same:
                e.cell = c  # adopt the disk bytes
                self._resolve_conflict(e)
            elif e.cell_id in self._locks:
                e.conflict = True
                e.disk_cell = c
                conflicts.append(e)
            else:
                e.cell = c
                e.version += 1
                self._resolve_conflict(e)
            entries.append(e)
        for i, e in enumerate(old):  # a locked cell deleted on disk survives, in conflict
            if i not in used and e.cell_id in self._locks:
                e.conflict = True
                e.disk_cell = None
                entries.insert(max(1, min(i, len(entries))), e)
                conflicts.append(e)

        self._entries = entries
        self._bom = new.bom
        self._reindex()
        live = set(e.cell_id for e in entries)
        for p in self._presence.values():
            if p.focused_cell_id not in live:
                p.focused_cell_id = None
            if p.cursor and p.cursor.get("cell_id") not in live:
                p.cursor = None
        self.doc_version += 1
        self._dirty = False  # memory now matches disk, except cells in conflict
        self._emit("doc.reloaded", {"doc_version": self.doc_version,
                                    "cells": [self._cell_dict(e) for e in entries],
                                    "cause": "external"})
        for e in conflicts:
            holder = self._locks[e.cell_id].client
            self._emit("doc.conflict", {
                "doc_version": self.doc_version, "cell_id": e.cell_id,
                "local": {"source": e.cell.source, "version": e.version, "by": by_of(holder)},
                "disk": {"source": None if e.disk_cell is None else e.disk_cell.source},
            })

    def _serialize(self) -> str:
        cells = []
        for e in self._entries:
            if e.conflict:
                if e.disk_cell is not None:
                    cells.append(e.disk_cell)
            else:
                cells.append(e.cell)
        return dpformat.serialize(dpformat.NotebookDocument(self.path, cells, "", bom=self._bom))

    def _save(self) -> None:
        # Never overwrite an external change we have not seen yet.
        if self._check_disk():
            return
        data = self._serialize().encode("utf-8")
        directory = os.path.dirname(self.path)
        try:
            mode = os.stat(self.path).st_mode & 0o7777
        except OSError:
            mode = None
        fd, tmp = tempfile.mkstemp(prefix="." + os.path.basename(self.path) + ".",
                                   suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            if mode is not None:
                os.chmod(tmp, mode)
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        self._known_sha = hashlib.sha256(data).hexdigest()
        self._known_stat = self._stat()
        self._dirty = False
