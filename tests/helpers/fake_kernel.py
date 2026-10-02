"""A tiny fake DKP/1 kernel for manager tests (PROTOCOL §3).

It speaks the real handshake and frames from ``darkpyonix.kernel.protocol`` and answers
status, run, cancel, interrupt, restart, shutdown, namespace, runs.list, runs.get, subscribe
and unsubscribe with canned data. A started run stays ``running`` until the test calls
``finish_run()`` or a client interrupts it.

It also writes the FR-D2 registry entry ``kernels/<kernel_id>.json`` under
``DARKPYONIX_HOME``, so a manager written in any language finds it with
``DARKPYONIX_DISCOVERY=registry``.

Runs on its own event loop thread, or standalone: ``python fake_kernel.py FILE ANNOUNCE_JSON``
serves until SIGTERM and writes its announce body to ANNOUNCE_JSON.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import struct
import sys
import threading
from typing import Any, Dict, List, Optional

if __name__ == "__main__":
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "kernel"))

from darkpyonix.kernel import protocol  # noqa: E402


async def _read(reader: asyncio.StreamReader) -> Dict[str, Any]:
    (n,) = struct.unpack(">I", await reader.readexactly(4))
    return json.loads((await reader.readexactly(n)).decode("utf-8"))


class FakeKernel:
    def __init__(self, path: str, key: bytes, ring_max: int = 10000) -> None:
        self.path = protocol.canonical_path(path)
        self.kernel_id = protocol.kernel_id_for(self.path)
        self.key = key
        self.ring_max = ring_max
        self.pid = os.getpid()
        self.port = 0
        self.started_at = protocol.now_iso()
        self.calls = []  # type: List[tuple]
        self.connections = 0
        self.status = "idle"
        self.run_id = None  # type: Optional[str]
        self.queue = []  # type: List[str]
        self.runs = {}  # type: Dict[str, Dict[str, Any]]
        self.order = []  # type: List[str]
        self.seq = 0
        self.ring = []  # type: List[Dict[str, Any]]
        self._subscribers = set()  # type: set
        self._loop = None  # type: Optional[asyncio.AbstractEventLoop]
        self._server = None
        self._thread = None  # type: Optional[threading.Thread]

    # ------------------------------------------------------------ lifecycle

    def start(self) -> "FakeKernel":
        ready = threading.Event()

        def run() -> None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._server = self._loop.run_until_complete(
                asyncio.start_server(self._handle, "127.0.0.1", 0))
            self.port = self._server.sockets[0].getsockname()[1]
            ready.set()
            self._loop.run_forever()
            self._server.close()
            self._loop.run_until_complete(self._server.wait_closed())
            self._loop.close()

        self._thread = threading.Thread(target=run, name="fake-kernel", daemon=True)
        self._thread.start()
        ready.wait(5)
        self._write_registry()
        return self

    def _registry_path(self) -> str:
        from darkpyonix import _home
        return os.path.join(_home.kernels_dir(), "%s.json" % self.kernel_id)

    def _write_registry(self) -> None:
        """FR-D2 registry entry, so any manager (DARKPYONIX_DISCOVERY=registry) can find us."""
        from darkpyonix import _home
        self._registry = self._registry_path()
        _home.write_public(self._registry, json.dumps(self.announce()).encode("utf-8"))

    def stop(self) -> None:
        if self._loop is not None:
            def close_all() -> None:
                for w in list(self._subscribers):
                    w.close()
                self._loop.stop()
            self._loop.call_soon_threadsafe(close_all)
            self._thread.join(5)
        try:
            os.unlink(self._registry)
        except (AttributeError, OSError):
            pass

    def _sync(self, fn, *args):
        """Run ``fn`` on the kernel loop from a test thread and return its result."""
        done = threading.Event()
        box = {}

        def call() -> None:
            try:
                box["v"] = fn(*args)
            finally:
                done.set()
        self._loop.call_soon_threadsafe(call)
        done.wait(5)
        return box.get("v")

    def announce(self) -> Dict[str, Any]:
        return {"dkp": 1, "op": "announce", "kernel_id": self.kernel_id, "path": self.path,
                "pid": self.pid, "port": self.port, "status": self.status, "run_id": self.run_id,
                "python": {"version": "3.11.9", "implementation": "CPython", "executable": sys.executable},
                "dkp_kernel_version": protocol.KERNEL_VERSION, "started_at": self.started_at, "host": "fake"}

    # ------------------------------------------------------------ test controls

    def emit(self, type_: str, data: Dict[str, Any]) -> int:
        return self._sync(self._emit, type_, data)

    def finish_run(self, status: str = "ok") -> None:
        self._sync(self._finish, status)

    def method_calls(self) -> List[str]:
        return [c[0] for c in self.calls]

    # ------------------------------------------------------------ internals (kernel loop)

    def _emit(self, type_: str, data: Dict[str, Any]) -> int:
        self.seq += 1
        event = {"dkp": 1, "op": "event", "seq": self.seq, "type": type_, "time": protocol.now_iso(),
                 "data": data}
        self.ring.append(event)
        del self.ring[:-self.ring_max]
        frame = protocol.encode(event)
        for w in list(self._subscribers):
            w.write(frame)
        return self.seq

    def _summary(self, run_id: str) -> Dict[str, Any]:
        meta = self.runs[run_id]["metadata"]["darkpyonix"]
        return {k: meta[k] for k in ("run_id", "status", "mode", "started_at", "ended_at", "params")}

    def _start(self, run_id: str) -> None:
        self.status, self.run_id = "busy", run_id
        self.runs[run_id]["metadata"]["darkpyonix"]["status"] = "running"
        self._emit("kernel.status", {"status": "busy", "run_id": run_id})
        self._emit("run.started", {"run_id": run_id, "mode": "all", "cells": [0, 1], "params": {}})
        self._emit("cell.started", {"run_id": run_id, "index": 1, "execution_count": 1})
        self._emit("output", {"run_id": run_id, "index": 1,
                              "output": {"output_type": "stream", "name": "stdout", "text": "hello\n"}})

    def _finish(self, status: str) -> Optional[str]:
        run_id = self.run_id
        if run_id is None:
            return None
        meta = self.runs[run_id]["metadata"]["darkpyonix"]
        meta["status"], meta["ended_at"] = status, protocol.now_iso()
        self._emit("cell.finished", {"run_id": run_id, "index": 1, "status": status, "duration": 0.01})
        self._emit("run.finished", {"run_id": run_id, "status": status, "duration": 0.01})
        self.status, self.run_id = "idle", None
        self._emit("kernel.status", {"status": "idle"})
        if self.queue:
            self._start(self.queue.pop(0))
        return run_id

    def _dispatch(self, method: str, params: Dict[str, Any], writer) -> Any:
        if method == "status":
            info = self.announce()
            for k in ("dkp", "op"):
                info.pop(k)
            info["kernel_version"] = info.pop("dkp_kernel_version")
            info.update(queue=list(self.queue), execution_count=1,
                        runs_dir=os.path.join(os.path.dirname(self.path), "__runs__", os.path.basename(self.path)))
            return info
        if method == "run":
            run_id = protocol.new_run_id()
            while run_id in self.runs:
                run_id = protocol.new_run_id()
            if self.status == "busy" and params.get("on_busy", "reject") == "reject":
                raise protocol.DKPError("busy", "a run is executing",
                                        {"current": self._summary(self.run_id), "queue_length": len(self.queue)})
            self.runs[run_id] = {"nbformat": 4, "nbformat_minor": 5, "cells": [], "metadata": {"darkpyonix": {
                "run_id": run_id, "kernel_id": self.kernel_id, "file": self.path, "mode": params.get("mode", "all"),
                "params": params.get("params", {}), "status": "queued", "started_at": protocol.now_iso(),
                "ended_at": None}}}
            self.order.insert(0, run_id)
            if self.status == "busy":
                self.queue.append(run_id)
                self._emit("run.queued", {"run_id": run_id, "position": len(self.queue)})
                return {"run_id": run_id, "state": "queued", "position": len(self.queue)}
            self._start(run_id)
            return {"run_id": run_id, "state": "running"}
        if method == "interrupt":
            run_id = self._finish("interrupted")
            return {"interrupted": True, "run_id": run_id} if run_id else {"interrupted": False}
        if method == "cancel":
            run_id = params.get("run_id")
            if run_id in self.queue:
                self.queue.remove(run_id)
                self.runs[run_id]["metadata"]["darkpyonix"]["status"] = "cancelled"
                return {"cancelled": True}
            return {"cancelled": False}
        if method == "restart":
            return {"restarted": True}
        if method == "shutdown":
            return {"shutting_down": True}
        if method == "namespace":
            return {"variables": [{"name": "x", "type": "int", "repr": "1", "shape": None, "dtype": None,
                                   "len": None}][:params.get("limit", 200)]}
        if method == "runs.list":
            return {"runs": [self._summary(r) for r in self.order[:params.get("limit", 20)]]}
        if method == "runs.get":
            ref = params.get("run_id")
            if ref == "current":
                ref = self.run_id
            elif ref == "latest":
                done = [r for r in self.order if self.runs[r]["metadata"]["darkpyonix"]["ended_at"]]
                ref = done[0] if done else None
            if ref not in self.runs:
                raise protocol.DKPError("not_found", "no such run")
            return self.runs[ref]
        if method == "subscribe":
            since = int(params.get("since") or 0)
            replay = [e for e in self.ring if e["seq"] > since]
            oldest = self.ring[0]["seq"] if self.ring else self.seq + 1
            if since + 1 < oldest:
                writer.write(protocol.encode({"op": "event", "seq": oldest - 1, "type": "replay_truncated",
                                              "time": protocol.now_iso(), "data": {"oldest_seq": oldest}}))
            for e in replay:
                writer.write(protocol.encode(e))
            self._subscribers.add(writer)
            return {"seq": self.seq, "replayed": len(replay)}
        if method == "unsubscribe":
            self._subscribers.discard(writer)
            return {}
        raise protocol.DKPError("unknown_method", method)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonce = protocol.new_nonce()
        writer.write(protocol.encode({"op": "hello", "kernel_id": self.kernel_id,
                                      "nonce": base64.b64encode(nonce).decode(),
                                      "kernel_version": protocol.KERNEL_VERSION}))
        try:
            auth = await asyncio.wait_for(_read(reader), protocol.HANDSHAKE_TIMEOUT)
            if not protocol.verify_mac(self.key, nonce, self.kernel_id, auth.get("mac", "")):
                writer.write(protocol.encode({"op": "error", "code": "auth_failed", "message": "bad mac"}))
                await writer.drain()
                writer.close()
                return
            self.connections += 1
            writer.write(protocol.encode({"op": "welcome", "session": "s_fake", "seq": self.seq}))
            while True:
                req = await _read(reader)
                if req.get("op") != "request":
                    continue
                method, params = req.get("method"), req.get("params") or {}
                self.calls.append((method, params))
                try:
                    result = self._dispatch(method, params, writer)
                    reply = {"op": "response", "id": req["id"], "ok": True, "result": result}
                except protocol.DKPError as exc:
                    reply = {"op": "response", "id": req["id"], "ok": False, "error": exc.to_dict()}
                # For subscribe, _dispatch has already written the replay: the response follows it.
                writer.write(protocol.encode(reply))
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.TimeoutError):
            pass
        finally:
            self._subscribers.discard(writer)
            writer.close()


def _standalone(path: str, out: str) -> None:
    import signal
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "kernel"))
    from darkpyonix import _home
    kernel = FakeKernel(path, _home.user_key()).start()
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *a: stop.set())
    with open(out + ".tmp", "w") as f:
        json.dump(kernel.announce(), f)
    os.replace(out + ".tmp", out)
    while not stop.wait(0.2):
        pass
    kernel.stop()


if __name__ == "__main__":
    _standalone(sys.argv[1], sys.argv[2])
