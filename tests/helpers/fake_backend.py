"""Manager fixtures for tests.

``ServedManager`` runs the Python (FastAPI) prototype in-thread with a ``FakeBackend``.
``ExternalManager`` starts any manager implementation from ``DARKPYONIX_MANAGER_CMD`` and
talks to it only over HTTP; it finds fake kernels through their FR-D2 registry entries.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shlex
import socket
import subprocess
import threading
import time
from typing import Any, Dict, List, Optional

import httpx
import uvicorn

from darkpyonix.kernel import protocol
from darkpyonix.manager.app import ManagerState, create_app
from darkpyonix.manager.auth import Auth
from darkpyonix.manager.kernels import KernelBackend, KernelDirectory

from .fake_kernel import FakeKernel


class FakeBackend(KernelBackend):
    """Discovery sees ``kernels``; ``launch`` starts a ``FakeKernel`` (unless ``launchable`` is off)."""

    def __init__(self, key: bytes, launchable: bool = True, wait_timeout: float = 2.0) -> None:
        self.key = key
        self.launchable = launchable
        self.wait_timeout = wait_timeout
        self.kernels = {}  # type: Dict[str, FakeKernel]
        self.launches = []  # type: List[Dict[str, Any]]
        self._lock = threading.Lock()

    def add(self, kernel: FakeKernel) -> FakeKernel:
        with self._lock:
            self.kernels[kernel.kernel_id] = kernel
        return kernel

    def close(self) -> None:
        for k in list(self.kernels.values()):
            k.stop()

    def user_key(self) -> bytes:
        return self.key

    def discover(self, kernel_id=None, timeout=protocol.QUERY_TIMEOUT):
        with self._lock:
            return [k.announce() for k in self.kernels.values() if kernel_id in (None, k.kernel_id)]

    def launch(self, path, python=None, cwd=None, env=None):
        self.launches.append({"path": path, "python": python, "cwd": cwd, "env": env})
        if not self.launchable:
            return 999999
        return self.add(FakeKernel(path, self.key).start()).pid

    def wait_for_announce(self, kernel_id, pid=None, timeout=10.0):
        deadline = time.monotonic() + min(timeout, self.wait_timeout)
        while time.monotonic() < deadline:
            found = self.discover(kernel_id)
            if found:
                return found[0]
            time.sleep(0.02)
        return None

    def build_document(self, path, kernel_id=None, viewer_outputs=True):
        source = open(path, encoding="utf-8").read()
        cell = {"index": 0, "type": "preamble", "title": None, "source": source,
                "source_sha256": hashlib.sha256(source.encode()).hexdigest(), "metadata": {}}
        if viewer_outputs:
            cell["outputs"] = [{"output_type": "stream", "name": "stdout", "text": "hello\n"}]
        return {"path": path, "kernel_id": kernel_id, "latest_run": None, "cells": [cell]}


class StaticBackend(KernelBackend):
    """Discovery reads announce bodies from JSON files (for manager subprocesses)."""

    def __init__(self, announce_files: List[str]) -> None:
        self.files = announce_files

    def discover(self, kernel_id=None, timeout=protocol.QUERY_TIMEOUT):
        out = []
        for f in self.files:
            with open(f) as fh:
                a = json.load(fh)
            if kernel_id in (None, a["kernel_id"]):
                out.append(a)
        return out


EXTERNAL_CMD = os.environ.get("DARKPYONIX_MANAGER_CMD")


class _HttpManager:
    url = ""
    token = ""

    def client(self, token: Optional[str] = "master", **kw) -> httpx.Client:
        headers = {}
        if token == "master":
            token = self.token
        if token:
            headers["Authorization"] = "Bearer " + token
        return httpx.Client(base_url=self.url, headers=headers, timeout=kw.pop("timeout", 10.0), **kw)


class ExternalManager(_HttpManager):
    """An ephemeral manager started from a shell command, e.g. the Rust build.

    It runs with this test's ``DARKPYONIX_HOME`` and ``DARKPYONIX_DISCOVERY=registry`` and is
    found through the ``managers/<pid>.json`` it must publish (FR-M3)."""

    def __init__(self, cmd: str) -> None:
        self.cmd = cmd
        self.proc = None  # type: Optional[subprocess.Popen]

    def __enter__(self) -> "ExternalManager":
        from darkpyonix import _home
        mdir = _home.managers_dir()
        before = set(os.listdir(mdir))
        env = dict(os.environ, DARKPYONIX_DISCOVERY="registry")
        self.proc = subprocess.Popen(shlex.split(self.cmd), env=env)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            new = [f for f in os.listdir(mdir) if f.endswith(".json") and f not in before]
            if new:
                with open(os.path.join(mdir, new[0])) as f:
                    record = json.load(f)
                self.url, self.token = record["url"], record["token"]
                return self
            time.sleep(0.05)
        self.proc.kill()
        raise RuntimeError("manager %r did not publish managers/<pid>.json" % self.cmd)

    def __exit__(self, *exc) -> None:
        self.proc.terminate()
        try:
            self.proc.wait(10)
        except subprocess.TimeoutExpired:
            self.proc.kill()


def make_manager(backend: KernelBackend, mode: str = "ephemeral"):
    """The manager under test: ``DARKPYONIX_MANAGER_CMD`` if set, else the Python prototype."""
    if EXTERNAL_CMD:
        if mode != "ephemeral":
            import pytest
            pytest.skip("external managers are tested in ephemeral mode only")
        return ExternalManager(EXTERNAL_CMD)
    return ServedManager(backend, mode=mode)


class ServedManager(_HttpManager):
    """The Python prototype served by uvicorn on 127.0.0.1:<random> in a background thread."""

    def __init__(self, backend: KernelBackend, mode: str = "ephemeral", idle_timeout: Optional[float] = None) -> None:
        self.directory = KernelDirectory(backend)
        self.auth = Auth()
        self.state = ManagerState(mode, idle_timeout)
        self.app = create_app(self.directory, self.auth, self.state)
        self.token = self.auth.master_token
        self._server = uvicorn.Server(uvicorn.Config(self.app, lifespan="off", log_level="warning",
                                                     access_log=False, timeout_graceful_shutdown=1))
        self._sock = socket.socket()
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(64)
        self.url = "http://127.0.0.1:%d" % self._sock.getsockname()[1]
        self._thread = None  # type: Optional[threading.Thread]

    async def _main(self) -> None:
        try:
            await self._server.serve(sockets=[self._sock])
        finally:
            await self.directory.aclose()

    def __enter__(self) -> "ServedManager":
        self._thread = threading.Thread(target=lambda: asyncio.run(self._main()), daemon=True)
        self._thread.start()
        deadline = time.monotonic() + 5
        while not self._server.started and time.monotonic() < deadline:
            time.sleep(0.01)
        return self

    def __exit__(self, *exc) -> None:
        self._server.should_exit = True
        self._thread.join(10)
        self._sock.close()

def read_sse(response: httpx.Response, count: int) -> List[Dict[str, Any]]:
    """Read ``count`` SSE messages (comments skipped) as ``{id, event, data}``."""
    out = []
    current = {}  # type: Dict[str, Any]
    for line in response.iter_lines():
        if line == "":
            if "event" in current or "data" in current:
                if "data" in current:
                    current["data"] = json.loads(current["data"])
                out.append(current)
                if len(out) >= count:
                    return out
            current = {}
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        current[field] = value[1:] if value.startswith(" ") else value
    return out
