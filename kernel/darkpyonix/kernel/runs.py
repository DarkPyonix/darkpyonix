"""Run logs beside the notebook file and the ``__runs__`` magic (SPEC FR-R1, FR-R2, FR-R3, FR-R5).

Standard library only; Python 3.8+.

Layout (INTENT D8)::

    <dir of file>/__runs__/<file name>/<run_id>.ipynb         one run = one nbformat 4.5 notebook
    <dir of file>/__runs__/<file name>/<run_id>.cell<i>.log   FR-R5 overflow of cell i's streams
    <dir of file>/__runs__/<file name>/index.json             run summaries, newest first

``__runs__/`` is tracked by Git by default; this module never writes a ``.gitignore``.

Threading: ``RunStore.update`` only marks the run dirty; a daemon writer thread rewrites the
log at most once per second. ``begin`` and ``finish`` write synchronously. Every write is
atomic (temporary file in the same directory, then ``os.replace``). The writer snapshots the
``Run`` while the executor may still append outputs; output dicts must therefore not be
mutated in place after they are appended (replace the list item instead, or append a new one).
A snapshot that races with such a mutation is retried on the next tick.
"""
from __future__ import annotations

import copy
import json
import os
import re
import secrets
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from darkpyonix.kernel.model import Run
from darkpyonix.kernel.protocol import DKPError

RUNS_DIRNAME = "__runs__"
INDEX_NAME = "index.json"
WRITE_INTERVAL = 1.0                     # FR-R2: at most one rewrite per second
DEFAULT_OUTPUT_LIMIT = 16 * 1024 * 1024  # FR-R5
OUTPUT_LIMIT_ENV = "DARKPYONIX_RUN_OUTPUT_LIMIT"
UNFINISHED = ("queued", "running")

_RUN_ID_RE = re.compile(r"^[0-9]{8}-[0-9]{6}-[0-9a-f]{4}$")
_TMP_RE = re.compile(r"^.+\.[0-9a-f]{8}\.tmp$")


def runs_dir_for(path: str) -> str:
    """``<dir of file>/__runs__/<file name>/`` (INTENT D8). Not created."""
    full = os.path.abspath(path)
    return os.path.join(os.path.dirname(full), RUNS_DIRNAME, os.path.basename(full)) + os.sep


def output_limit() -> int:
    """``DARKPYONIX_RUN_OUTPUT_LIMIT`` in bytes (default 16 MiB)."""
    raw = os.environ.get(OUTPUT_LIMIT_ENV)
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return DEFAULT_OUTPUT_LIMIT


# ---------------------------------------------------------------- notebook form (FR-R1)

def _cell_id(run_id: str, index: int, used: set) -> str:
    """A valid nbformat 4.5 cell id ([a-zA-Z0-9-_]{1,64}), deterministic from run_id+index."""
    base = "%s-c%d" % (run_id, index)
    cid, n = base, 1
    while cid in used:
        cid = "%s-%d" % (base, n)
        n += 1
    used.add(cid)
    return cid


def _summary(run_meta: Dict[str, Any]) -> Dict[str, Any]:
    """RunSummary (without ``path``) from a run's ``metadata.darkpyonix``."""
    started, ended = run_meta.get("started_at"), run_meta.get("ended_at")
    return {
        "run_id": run_meta.get("run_id"), "status": run_meta.get("status"),
        "mode": run_meta.get("mode"), "cells": list(run_meta.get("cells") or []),
        "params": dict(run_meta.get("params") or {}),
        "started_at": started, "ended_at": ended, "duration": _duration(started, ended),
    }


def _parse_iso(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        import datetime
        t = datetime.datetime.strptime(value.rstrip("Z")[:23], "%Y-%m-%dT%H:%M:%S.%f")
        return (t - datetime.datetime(1970, 1, 1)).total_seconds()
    except ValueError:
        return None


def _duration(started: Optional[str], ended: Optional[str]) -> Optional[float]:
    a, b = _parse_iso(started), _parse_iso(ended)
    if a is None or b is None:
        return None
    return round(b - a, 3)


def _iso_from_epoch(t: float) -> str:
    import datetime
    d = datetime.datetime.fromtimestamp(t, datetime.timezone.utc)
    return d.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (d.microsecond // 1000)


def _spill(run_id: str, index: int, outputs: List[Dict[str, Any]],
           limit: int) -> Tuple[List[Dict[str, Any]], str]:
    """Keep at most ``limit`` UTF-8 bytes of stream text; return (outputs, excess text).

    Outputs are already copies. The kept part is cut at a character boundary, followed by
    one stream line naming the sidecar file. Non-stream outputs are kept in place.
    """
    total = 0
    for o in outputs:
        if o.get("output_type") == "stream":
            total += len(_text(o).encode("utf-8"))
    if total <= limit:
        return outputs, ""
    kept = []          # type: List[Dict[str, Any]]
    excess = []        # type: List[str]
    budget = limit
    notice_at = None   # type: Optional[int]
    notice_name = "stdout"
    for o in outputs:
        if o.get("output_type") != "stream":
            kept.append(o)
            continue
        text = _text(o)
        if budget <= 0:
            excess.append(text)
            continue
        data = text.encode("utf-8")
        if len(data) <= budget:
            kept.append(o)
            budget -= len(data)
            continue
        head = data[:budget].decode("utf-8", "ignore")
        budget = 0
        if head:
            o = dict(o, text=head)
            kept.append(o)
        excess.append(text[len(head):])
        notice_at = len(kept)
        notice_name = o.get("name", "stdout")
    rest = "".join(excess)
    notice = {
        "output_type": "stream", "name": notice_name,
        "text": "\n[darkpyonix] stream output over %d bytes; the remaining %d bytes are in %s\n"
                % (limit, len(rest.encode("utf-8")), sidecar_name(run_id, index)),
    }
    kept.insert(notice_at if notice_at is not None else len(kept), notice)
    return kept, rest


def _text(output: Dict[str, Any]) -> str:
    text = output.get("text", "")
    return "".join(text) if isinstance(text, list) else str(text)


def sidecar_name(run_id: str, index: int) -> str:
    """``<run_id>.cell<index>.log`` (FR-R5)."""
    return "%s.cell%d.log" % (run_id, index)


def _render(run: Run, limit: Optional[int]) -> Tuple[Dict[str, Any], Dict[int, str]]:
    """The run log notebook, and per cell index the stream text spilled past ``limit``."""
    python = copy.deepcopy(run.python) if run.python else {}
    meta = {
        "run_id": run.run_id, "kernel_id": run.kernel_id, "file": run.file,
        "file_sha256": run.file_sha256, "mode": run.mode, "cells": list(run.cell_indexes),
        "params": copy.deepcopy(run.params), "status": run.status,
        "started_at": run.started_at, "ended_at": run.ended_at,
        "python": python, "host": run.host,
    }
    cells = []
    spilled = {}  # type: Dict[int, str]
    used = set()  # type: set
    for rec in list(run.cells):
        outputs = copy.deepcopy(list(rec.outputs))
        if limit is not None:
            outputs, rest = _spill(run.run_id, rec.index, outputs, limit)
            if rest:
                spilled[rec.index] = spilled.get(rec.index, "") + rest
        dp = {
            "index": rec.index, "type": rec.type, "title": rec.title,
            "source_sha256": rec.source_sha256, "status": rec.status,
            "started_at": rec.started_at, "ended_at": rec.ended_at,
        }  # type: Dict[str, Any]
        if rec.cell_id:
            dp["id"] = rec.cell_id
        cells.append({
            "cell_type": "code",
            "id": _cell_id(run.run_id, rec.index, used),
            "metadata": {"darkpyonix": dp},
            "source": rec.source,
            "execution_count": rec.execution_count,
            "outputs": outputs,
        })
    nb = {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"},
            "language_info": {"name": "python", "version": str(python.get("version", ""))},
            "darkpyonix": meta,
        },
        "cells": cells,
    }
    return nb, spilled


def to_notebook(run: Run) -> Dict[str, Any]:
    """The nbformat 4.5 form of ``run`` (FR-R1), outputs in full (no FR-R5 spill)."""
    return _render(run, None)[0]


# ---------------------------------------------------------------- files

def _write_atomic(path: str, data: bytes) -> None:
    tmp = "%s.%s.tmp" % (path, secrets.token_hex(4))
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        view = memoryview(data)
        while view:
            n = os.write(fd, view)
            view = view[n:]
    except BaseException:
        os.close(fd)
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    os.close(fd)
    os.replace(tmp, path)


def _dumps(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, indent=1).encode("utf-8")


def _read_json(path: str) -> Any:
    with open(path, "rb") as f:
        return json.loads(f.read().decode("utf-8"))


def _sort_key(summary: Dict[str, Any]) -> Tuple[str, str]:
    return (summary.get("started_at") or "", summary.get("run_id") or "")


# ---------------------------------------------------------------- store (FR-R1, FR-R2, FR-R5)

class RunStore(object):
    """Run logs of one notebook file, written by the kernel that owns the file."""

    def __init__(self, file_path: str, kernel_id: str) -> None:
        self.file_path = os.path.abspath(file_path)
        self.kernel_id = kernel_id
        self.dir = runs_dir_for(self.file_path)
        self.limit = output_limit()
        self._lock = threading.RLock()        # guards the fields below and every file write
        self._cond = threading.Condition(threading.Lock())
        self._active = {}                     # type: Dict[str, Run]   begun, not finished
        self._dirty = {}                      # type: Dict[str, Run]
        self._spilled = {}                    # type: Dict[Tuple[str, int], int]  chars on disk
        self._last_write = 0.0
        self._writer = None                   # type: Optional[threading.Thread]

    # ---- paths

    def path_of(self, run_id: str) -> str:
        return os.path.join(self.dir, run_id + ".ipynb")

    def _ensure_dir(self) -> None:
        os.makedirs(self.dir, exist_ok=True)

    # ---- writing

    def begin(self, run: Run) -> None:
        """Write the log now and add the run to index.json."""
        with self._lock:
            self._active[run.run_id] = run
            self._dirty.pop(run.run_id, None)
            self._write(run)
            self._index_put(_summary(to_notebook_meta(run)))

    def update(self, run: Run) -> None:
        """Mark ``run`` changed. The log is rewritten within ~1 s, never more than once a second."""
        with self._cond:
            self._dirty[run.run_id] = run
            if self._writer is None or not self._writer.is_alive():
                self._writer = threading.Thread(target=self._write_loop,
                                                name="darkpyonix-runlog", daemon=True)
                self._writer.start()
            self._cond.notify()

    def finish(self, run: Run) -> None:
        """Write the final log now and update index.json."""
        with self._lock:
            with self._cond:
                self._dirty.pop(run.run_id, None)
            self._write(run)
            self._active.pop(run.run_id, None)
            for key in [k for k in self._spilled if k[0] == run.run_id]:
                del self._spilled[key]
            self._index_put(_summary(to_notebook_meta(run)))

    def flush(self) -> None:
        """Write every pending update now (e.g. before the kernel exits)."""
        with self._cond:
            pending = list(self._dirty.values())
            self._dirty.clear()
        with self._lock:
            for run in pending:
                if run.run_id in self._active:
                    self._write(run)

    def _write_loop(self) -> None:
        while True:
            with self._cond:
                while not self._dirty:
                    if not self._cond.wait(timeout=30.0) and not self._dirty:
                        self._writer = None
                        return
                wait = self._last_write + WRITE_INTERVAL - time.monotonic()
                if wait > 0:
                    self._cond.wait(timeout=wait)
                    continue
                pending = list(self._dirty.values())
                self._dirty.clear()
            with self._lock:
                for run in pending:
                    if run.run_id not in self._active:
                        continue          # finished (or never begun): finish() owns the file
                    try:
                        self._write(run)
                    except RuntimeError:  # a container changed during the snapshot
                        with self._cond:
                            self._dirty.setdefault(run.run_id, run)
                    except OSError:
                        pass              # disk trouble: the next update retries

    def _write(self, run: Run) -> None:
        """Render and write ``run`` atomically, appending FR-R5 overflow first. Holds _lock."""
        nb, spilled = _render(run, self.limit)
        self._ensure_dir()
        for index, rest in spilled.items():
            key = (run.run_id, index)
            done = self._spilled.get(key, 0)
            if len(rest) > done:
                with open(os.path.join(self.dir, sidecar_name(run.run_id, index)),
                          "a" if done else "w", encoding="utf-8", newline="") as f:
                    f.write(rest[done:])
                self._spilled[key] = len(rest)
        _write_atomic(self.path_of(run.run_id), _dumps(nb))
        self._last_write = time.monotonic()

    # ---- index.json

    def _index_path(self) -> str:
        return os.path.join(self.dir, INDEX_NAME)

    def _log_ids(self) -> List[str]:
        try:
            names = os.listdir(self.dir)
        except OSError:
            return []
        return [n[:-6] for n in names if n.endswith(".ipynb") and _RUN_ID_RE.match(n[:-6])]

    def _index_load(self) -> List[Dict[str, Any]]:
        """Summaries newest first; rebuilt from the logs if index.json is missing, corrupt or
        does not list exactly the logs on disk."""
        ids = set(self._log_ids())
        runs = None  # type: Optional[List[Dict[str, Any]]]
        try:
            data = _read_json(self._index_path())
            if isinstance(data, dict) and isinstance(data.get("runs"), list):
                runs = [r for r in data["runs"] if isinstance(r, dict) and r.get("run_id")]
        except (OSError, ValueError):
            runs = None
        if runs is not None and set(r["run_id"] for r in runs) == ids:
            return sorted(runs, key=_sort_key, reverse=True)
        known = dict((r["run_id"], r) for r in (runs or []) if r["run_id"] in ids)
        rebuilt = []
        for run_id in ids:
            if run_id in known:
                rebuilt.append(known[run_id])
                continue
            try:
                meta = _read_json(self.path_of(run_id))["metadata"]["darkpyonix"]
            except (OSError, ValueError, KeyError, TypeError):
                continue
            rebuilt.append(_summary(meta))
        rebuilt.sort(key=_sort_key, reverse=True)
        if ids:
            try:
                self._index_save(rebuilt)
            except OSError:
                pass
        return rebuilt

    def _index_save(self, runs: List[Dict[str, Any]]) -> None:
        self._ensure_dir()
        body = {"file": os.path.basename(self.file_path), "runs": runs}
        _write_atomic(self._index_path(), _dumps(body))

    def _index_put(self, summary: Dict[str, Any]) -> None:
        runs = [r for r in self._index_load() if r.get("run_id") != summary["run_id"]]
        runs.append(summary)
        runs.sort(key=_sort_key, reverse=True)
        self._index_save(runs)

    # ---- reading

    def list(self, limit: int = 20) -> List[Dict[str, Any]]:
        """RunSummary dicts newest first, each with the absolute ``path`` of its log."""
        with self._lock:
            runs = self._index_load()
        out = []
        for r in runs[:max(0, int(limit))]:
            s = copy.deepcopy(r)
            s["path"] = self.path_of(s["run_id"])
            out.append(s)
        return out

    def current_run(self) -> Optional[Run]:
        with self._lock:
            active = list(self._active.values())
        if not active:
            return None
        running = [r for r in active if r.status == "running"]
        pool = running or active
        return max(pool, key=lambda r: (r.started_at or "", r.run_id))

    def get(self, ref: str) -> Dict[str, Any]:
        """The run log notebook for a run id, ``"latest"`` (newest finished) or ``"current"``."""
        if ref == "current":
            run = self.current_run()
            if run is None:
                raise DKPError("not_found", "no run is in progress")
            with self._lock:
                return _render(run, self.limit)[0]
        if ref == "latest":
            for s in self.list(limit=1 << 30):
                if s.get("status") not in UNFINISHED:
                    return self.get(s["run_id"])
            raise DKPError("not_found", "no finished run")
        if not isinstance(ref, str) or not _RUN_ID_RE.match(ref):
            raise DKPError("not_found", "no run %r" % (ref,))
        run = self._active.get(ref)
        if run is not None:
            with self._lock:
                return _render(run, self.limit)[0]
        try:
            return _read_json(self.path_of(ref))
        except FileNotFoundError:
            raise DKPError("not_found", "no run %r" % (ref,))
        except (OSError, ValueError) as e:
            raise DKPError("internal", "cannot read run %s: %s" % (ref, e))

    # ---- recovery (FR-R2)

    def recover_crashed(self) -> List[str]:
        """Mark logs left ``running``/``queued`` by a dead kernel as ``crashed``.

        Runs this process has begun and not finished are left alone. ``ended_at`` is set to
        the log's last modification time (the last moment the old kernel wrote it). Stale
        temporary files from interrupted atomic writes are removed. Returns the run ids.
        """
        crashed = []
        with self._lock:
            try:
                names = os.listdir(self.dir)
            except OSError:
                return crashed
            for n in names:
                if _TMP_RE.match(n):
                    try:
                        os.unlink(os.path.join(self.dir, n))
                    except OSError:
                        pass
            runs = self._index_load()
            for s in runs:
                run_id = s.get("run_id")
                if s.get("status") not in UNFINISHED or run_id in self._active:
                    continue
                path = self.path_of(run_id)
                try:
                    nb = _read_json(path)
                    meta = nb["metadata"]["darkpyonix"]
                    mtime = os.stat(path).st_mtime
                except (OSError, ValueError, KeyError, TypeError):
                    continue
                if meta.get("status") in UNFINISHED:
                    meta["status"] = "crashed"
                    if not meta.get("ended_at"):
                        meta["ended_at"] = _iso_from_epoch(mtime)
                    _write_atomic(path, _dumps(nb))
                s.clear()
                s.update(_summary(meta))
                crashed.append(run_id)
            if crashed:
                runs.sort(key=_sort_key, reverse=True)
                self._index_save(runs)
        return crashed

    def magic(self) -> "RunsMagic":
        return RunsMagic(self)


def to_notebook_meta(run: Run) -> Dict[str, Any]:
    """The run fields of ``metadata.darkpyonix`` without rendering cells."""
    return {
        "run_id": run.run_id, "status": run.status, "mode": run.mode,
        "cells": list(run.cell_indexes), "params": copy.deepcopy(run.params),
        "started_at": run.started_at, "ended_at": run.ended_at,
    }


# ---------------------------------------------------------------- __runs__ (FR-R3)

class RunsMagic(object):
    """The ``__runs__`` object in the kernel namespace.

    Every value is a fresh JSON-serialisable dict/list; changing it never changes a log.
    ``__runs__[-1]`` is the newest *finished* run (the same as ``.latest``), ``[-2]`` the one
    before it; non-negative integers count from the oldest finished run.
    """

    __slots__ = ("_store",)

    def __init__(self, store: RunStore) -> None:
        self._store = store

    @property
    def dir(self) -> str:
        return self._store.dir

    @property
    def current(self) -> Optional[Dict[str, Any]]:
        try:
            return self._store.get("current")
        except DKPError:
            return None

    @property
    def latest(self) -> Optional[Dict[str, Any]]:
        try:
            return self._store.get("latest")
        except DKPError:
            return None

    def list(self, limit: int = 20) -> List[Dict[str, Any]]:
        return self._store.list(limit)

    def __getitem__(self, key: Union[str, int]) -> Dict[str, Any]:
        if isinstance(key, bool):
            raise TypeError("__runs__ keys are run ids or integers")
        if isinstance(key, int):
            finished = [s for s in reversed(self._store.list(limit=1 << 30))
                        if s.get("status") not in UNFINISHED]
            try:
                run_id = finished[key]["run_id"]
            except IndexError:
                raise IndexError("__runs__[%d]: %d finished run(s)" % (key, len(finished)))
            return self._store.get(run_id)
        if isinstance(key, str):
            try:
                return self._store.get(key)
            except DKPError as e:
                raise KeyError(key) if e.code == "not_found" else e
        raise TypeError("__runs__ keys are run ids or integers")

    def __len__(self) -> int:
        return len(self._store.list(limit=1 << 30))

    def __repr__(self) -> str:
        runs = self._store.list(limit=1 << 30)
        cur = self._store.current_run()
        latest = next((s["run_id"] for s in runs if s.get("status") not in UNFINISHED), None)
        return "<__runs__ %d run(s) in %r current=%s latest=%s>" % (
            len(runs), self._store.dir, cur.run_id if cur else None, latest)
