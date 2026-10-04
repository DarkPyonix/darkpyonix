"""The DarkPyonix kernel process (INTENT D1–D3, D12; PROTOCOL §2–§3).

Started by ``launcher.bootstrap_command``. Owns the per-file lock, the shared document
(SPEC §10a, ``collab.DocumentState``), the run store, the executor (user code on the main
thread), the DKP/1 control server and discovery. Standard
library only; Python 3.8+.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import signal
import socket
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from darkpyonix import _home
from darkpyonix.kernel import lock, registry
from darkpyonix.kernel.collab import DocumentState
from darkpyonix.kernel.discovery import DiscoveryResponder
from darkpyonix.kernel.events import EventLog
from darkpyonix.kernel.executor import Executor
from darkpyonix.kernel.launcher import bootstrap_command
from darkpyonix.kernel.protocol import (
    EXIT_ALREADY_RUNNING, KERNEL_VERSION, DKPError, canonical_path, kernel_id_for, now_iso,
)
from darkpyonix.kernel.runs import RunStore, runs_dir_for

FINISHED = ("ok", "error", "interrupted", "cancelled", "crashed")


def _log(stream, message: str) -> None:
    try:
        stream.write("[darkpyonix-kernel %s] %s\n" % (now_iso(), message))
        stream.flush()
    except Exception:
        pass


class Kernel(object):
    def __init__(self, path: str) -> None:
        self.path = canonical_path(path)
        self.kernel_id = kernel_id_for(self.path)
        self.key = _home.user_key()
        self.user_tag = _home.user_tag(self.key)
        self.lock = lock.FileLock(lock.lock_path(self.kernel_id))
        self.events = EventLog()
        self.collab = None  # type: Optional[DocumentState]
        self.store = None  # type: Optional[RunStore]
        self.executor = None  # type: Optional[Executor]
        self.server = None
        self.discovery = None  # type: Optional[DiscoveryResponder]
        self.extra_handlers = {}  # type: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]]
        self.started_at = now_iso()
        self.python = {
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "executable": sys.executable,
        }
        self.host = socket.gethostname()
        self.diag = sys.stderr
        self._shutdown_started = threading.Event()

    # -------------------------------------------------------------- discovery info

    def info(self) -> Dict[str, Any]:
        st = self.executor.status() if self.executor is not None else {"status": "starting"}
        return {
            "kernel_id": self.kernel_id,
            "path": self.path,
            "pid": os.getpid(),
            "port": self.server.port if self.server is not None else 0,
            "status": st.get("status", "starting"),
            "run_id": st.get("run_id"),
            "python": self.python,
            "kernel_version": KERNEL_VERSION,
            "runs_dir": runs_dir_for(self.path),
            "started_at": self.started_at,
            "host": self.host,
        }

    def emit(self, type: str, data: Dict[str, Any]) -> None:
        self.events.append(type, data)
        if type == "kernel.status" and self.discovery is not None:
            self.discovery.announce_now()

    # -------------------------------------------------------------- handlers

    def _status(self, params: Dict[str, Any]) -> Dict[str, Any]:
        out = self.info()
        out.update(self.executor.status())
        return out

    def _runs_list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        limit = int(params.get("limit", 20))
        return {"runs": self.store.list(limit)}

    def _runs_get(self, params: Dict[str, Any]) -> Dict[str, Any]:
        ref = params.get("run_id")
        if not isinstance(ref, str):
            raise DKPError("bad_request", "run_id is required")
        return self.store.get(ref)

    def _runs_wait(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """SPEC FR-S7: return when the run finishes, or after ``timeout`` seconds."""
        ref = params.get("run_id")
        timeout = max(1.0, min(float(params.get("timeout", 60)), 300.0))
        if ref == "current":
            ref = self.executor.status().get("run_id")
            if ref is None:
                raise DKPError("not_found", "no run is executing")
        deadline = time.monotonic() + timeout
        while True:
            summary = self._summary(ref)
            if summary is not None and summary.get("status") in FINISHED:
                return {"status": summary["status"], "run_id": summary["run_id"], "run": summary}
            if time.monotonic() >= deadline:
                if summary is None:
                    raise DKPError("not_found", "unknown run %r" % ref)
                return {"status": summary.get("status", "running"), "run_id": summary["run_id"],
                        "run": summary}
            time.sleep(0.1)

    def _summary(self, ref: str) -> Optional[Dict[str, Any]]:
        for s in self.store.list(500):
            if ref == "latest" and s.get("status") in FINISHED:
                return s
            if s.get("run_id") == ref:
                return s
        st = self.executor.status()
        if ref in st.get("queue", []):
            return {"run_id": ref, "status": "queued"}
        return None

    def _run(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """PROTOCOL §3.3 ``run``; ``cell_ids`` (FR-S6) name cells of the shared document."""
        params = dict(params or {}) if isinstance(params, dict) else params
        if isinstance(params, dict) and params.get("cell_ids") is not None:
            ids = params.pop("cell_ids")
            if not isinstance(ids, list) or not ids or not all(isinstance(c, str) for c in ids):
                raise DKPError("bad_request", "cell_ids must be a non-empty list of strings")
            if params.get("cells") is not None:
                raise DKPError("bad_request", "give cells or cell_ids, not both")
            params["cells"] = self.collab.indices_for(ids)
            params["mode"] = "cells"
        self.collab.flush()  # debounced edits reach the file before the run reads it
        return self.executor.submit(params)

    def _before_load(self) -> List[str]:
        """Executor hook, right before a (possibly queued) run reads the file."""
        self.collab.flush()
        return self.collab.cell_ids()

    def _shutdown(self, params: Dict[str, Any]) -> Dict[str, Any]:
        self.request_shutdown()
        return {"shutting_down": True}

    def handlers(self) -> Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]]:
        ex = self.executor
        h = {
            "status": self._status,
            "run": self._run,
            "cancel": lambda p: ex.cancel(p.get("run_id")),
            "interrupt": lambda p: ex.interrupt(p.get("client")),
            "restart": lambda p: ex.restart(bool(p.get("hard", False))),
            "shutdown": self._shutdown,
            "namespace": lambda p: ex.namespace(int(p.get("limit", 200))),
            "runs.list": self._runs_list,
            "runs.get": self._runs_get,
            "runs.wait": self._runs_wait,
        }  # type: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]]
        h.update(self.extra_handlers)
        return h

    def request_shutdown(self) -> None:
        if self._shutdown_started.is_set():
            return
        self._shutdown_started.set()
        threading.Thread(target=self.executor.shutdown, name="dpx-shutdown", daemon=True).start()

    # -------------------------------------------------------------- lifecycle

    def run(self) -> int:
        if not self.lock.acquire():
            existing = registry.read(self.kernel_id) or {"kernel_id": self.kernel_id}
            sys.stderr.write(json.dumps(existing, ensure_ascii=False) + "\n")
            sys.stderr.flush()
            return EXIT_ALREADY_RUNNING
        try:
            self.collab = DocumentState(self.path, self.events.append,
                                        seq_provider=lambda: self.events.seq)
            self.extra_handlers.update(self.collab.handlers())
            self.store = RunStore(self.path, self.kernel_id)
            crashed = self.store.recover_crashed()
            if crashed:
                _log(self.diag, "marked crashed runs: %s" % ", ".join(crashed))
            self.executor = Executor(self.path, self.kernel_id, self.emit, self.store,
                                     self.python, self.host)
            self.executor.before_load = self._before_load
            self.diag = getattr(self.executor, "original_stderr", sys.stderr)

            from darkpyonix.kernel.server import ControlServer
            self.server = ControlServer(self.kernel_id, self.key, self.events, self.handlers())
            self.collab.start()
            self.server.start()
            self.discovery = DiscoveryResponder(self.info, self.user_tag)
            self.discovery.start()

            signal.signal(signal.SIGTERM, lambda signum, frame: self.request_shutdown())
            if hasattr(signal, "SIGHUP"):
                signal.signal(signal.SIGHUP, signal.SIG_IGN)
            _log(self.diag, "kernel %s for %s on port %d" % (self.kernel_id, self.path, self.server.port))

            self.executor.run_forever()      # main thread until shutdown / hard restart
        finally:
            hard = bool(self.executor is not None and self.executor.hard_restart_requested)
            self._teardown()
        if hard:
            _log(sys.stderr, "hard restart")
            cmd = bootstrap_command(sys.executable, self.path)
            os.execv(cmd[0], cmd)
        return 0

    def _teardown(self) -> None:
        for step in (
            lambda: self.store.flush() if self.store is not None else None,
            lambda: self.discovery.stop(bye=True) if self.discovery is not None else None,
            lambda: registry.remove(self.kernel_id),
            lambda: self.server.stop() if self.server is not None else None,
            lambda: self.collab.stop() if self.collab is not None else None,
            lambda: self.lock.release(),
        ):
            try:
                step()
            except Exception as e:  # never let teardown keep the lock
                _log(sys.stderr, "teardown: %r" % (e,))


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="darkpyonix-kernel")
    parser.add_argument("--file", required=True)
    args, _unknown = parser.parse_known_args(argv)
    if not os.path.isfile(args.file):
        sys.stderr.write("darkpyonix-kernel: no such file: %s\n" % args.file)
        return 2
    return Kernel(args.file).run()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
