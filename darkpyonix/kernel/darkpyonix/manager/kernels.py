"""The manager's view of running kernels (SPEC FR-M2, FR-M5, FR-D1, FR-D2).

``KernelBackend`` is the only place that touches the kernel package's discovery, launcher
and document modules, so tests can replace it with a fake. ``KernelDirectory`` adds a cached
discovery view, idempotent start and one shared ``KernelConnection`` per kernel.

SUPERSEDED PROTOTYPE: the manager is being rewritten in Rust. This Python module is kept as
a reference for the contract's behaviour; the language-neutral oracle is tests/ (fake DKP/1
kernel in tests/helpers, HTTP-level tests runnable via DARKPYONIX_MANAGER_CMD).
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Dict, List, Optional

from darkpyonix import _home
from darkpyonix.kernel import protocol
from darkpyonix.kernel.protocol import DKPError

from .aclient import KernelConnection

START_TIMEOUT = 10.0           # FR-M2
CACHE_TTL = protocol.ANNOUNCE_INTERVAL
NOTEBOOK_SUFFIXES = (".py", ".pynb")
KERNEL_FIELDS = ("kernel_id", "path", "pid", "status", "run_id", "queue", "execution_count",
                 "python", "started_at", "host", "kernel_version", "runs_dir")


class KernelBackend:
    """Adapter over ``darkpyonix.kernel.{discovery,launcher,document}``. All calls block."""

    def kernel_id_for(self, path: str) -> str:
        return protocol.kernel_id_for(path)

    def discover(self, kernel_id: Optional[str] = None, timeout: float = protocol.QUERY_TIMEOUT) -> List[Dict[str, Any]]:
        from darkpyonix.kernel import discovery
        return discovery.discover(kernel_id=kernel_id, timeout=timeout)

    def launch(self, path: str, python: Optional[str] = None, cwd: Optional[str] = None,
               env: Optional[Dict[str, str]] = None) -> int:
        from darkpyonix.kernel import launcher
        return launcher.launch(path, python=python, cwd=cwd, env=env)

    def wait_for_announce(self, kernel_id: str, pid: Optional[int] = None,
                          timeout: float = START_TIMEOUT) -> Optional[Dict[str, Any]]:
        from darkpyonix.kernel import launcher
        return launcher.wait_for_announce(kernel_id, pid=pid, timeout=timeout)

    def build_document(self, path: str, kernel_id: Optional[str] = None,
                       viewer_outputs: bool = True) -> Dict[str, Any]:
        from darkpyonix.kernel import document
        return document.build_document(path, kernel_id=kernel_id, viewer_outputs=viewer_outputs)

    def user_key(self) -> bytes:
        return _home.user_key()


def runs_dir_for(path: str) -> str:
    """``<file folder>/__runs__/<file name>`` (FR-R1)."""
    return os.path.join(os.path.dirname(path), "__runs__", os.path.basename(path))


def kernel_view(info: Dict[str, Any]) -> Dict[str, Any]:
    """Shape an announce body or KernelInfo as the OpenAPI ``Kernel`` schema."""
    out = {k: info[k] for k in KERNEL_FIELDS if k in info}
    if "kernel_version" not in out and "dkp_kernel_version" in info:
        out["kernel_version"] = info["dkp_kernel_version"]
    out.setdefault("run_id", None)
    if "path" in out:
        out.setdefault("runs_dir", runs_dir_for(out["path"]))
    return out


class StartResult:
    def __init__(self, kernel: Dict[str, Any], created: bool) -> None:
        self.kernel = kernel
        self.created = created


class KernelDirectory:
    """Discovery-backed kernel list plus a pool of shared kernel connections."""

    def __init__(self, backend: Optional[KernelBackend] = None, host: str = "127.0.0.1") -> None:
        self.backend = backend or KernelBackend()
        self.host = host
        self._cache = {}  # type: Dict[str, Dict[str, Any]]
        self._cache_at = 0.0
        self._conns = {}  # type: Dict[str, KernelConnection]
        self._conn_locks = {}  # type: Dict[str, asyncio.Lock]
        self._start_locks = {}  # type: Dict[str, asyncio.Lock]
        self._key = None  # type: Optional[bytes]

    # ------------------------------------------------------------ discovery

    async def _discover(self, kernel_id: Optional[str] = None) -> List[Dict[str, Any]]:
        found = await asyncio.to_thread(self.backend.discover, kernel_id)
        return [a for a in found if isinstance(a, dict) and a.get("kernel_id")]

    async def list(self, refresh: bool = False) -> List[Dict[str, Any]]:
        if refresh or not self._cache_at or time.monotonic() - self._cache_at > CACHE_TTL:
            found = await self._discover()
            self._cache = {a["kernel_id"]: a for a in found}
            self._cache_at = time.monotonic()
        return list(self._cache.values())

    async def find(self, kernel_id: str) -> Optional[Dict[str, Any]]:
        """Announce body of a running kernel, asking the network when the cache has none."""
        cached = self._cache.get(kernel_id)
        if cached is not None and time.monotonic() - self._cache_at <= CACHE_TTL:
            return cached
        found = [a for a in await self._discover(kernel_id) if a["kernel_id"] == kernel_id]
        if not found:
            self._cache.pop(kernel_id, None)
            return None
        self._cache[kernel_id] = found[0]
        return found[0]

    def forget(self, kernel_id: str) -> None:
        self._cache.pop(kernel_id, None)

    # ------------------------------------------------------------ start (FR-M2)

    async def ensure(self, path: str, python: Optional[str] = None, cwd: Optional[str] = None,
                     env: Optional[Dict[str, str]] = None) -> StartResult:
        """Return the running kernel for ``path`` or launch one. Raises ``DKPError``."""
        canonical = protocol.canonical_path(path)
        if not os.path.isfile(canonical):
            raise DKPError("bad_request", "no such file: %s" % path)
        if not canonical.endswith(NOTEBOOK_SUFFIXES):
            raise DKPError("bad_request", "not a .py or .pynb file: %s" % path)
        kernel_id = self.backend.kernel_id_for(canonical)
        lock = self._start_locks.setdefault(kernel_id, asyncio.Lock())
        async with lock:
            found = [a for a in await self._discover(kernel_id) if a["kernel_id"] == kernel_id]
            if found:
                self._cache[kernel_id] = found[0]
                return StartResult(kernel_view(found[0]), created=False)
            pid = await asyncio.to_thread(self.backend.launch, canonical, python,
                                          cwd or os.path.dirname(canonical), env)
            # Any announce for this id counts: if another manager won the race, our process
            # exits with EXIT_ALREADY_RUNNING (FR-K3) and theirs is the kernel for the file.
            announce = await asyncio.to_thread(self.backend.wait_for_announce, kernel_id, None,
                                               START_TIMEOUT)
            if not announce:
                raise DKPError("start_timeout", "kernel %s (pid %s) did not announce itself within %d s"
                               % (kernel_id, pid, START_TIMEOUT), {"kernel_id": kernel_id, "pid": pid})
            self._cache[kernel_id] = announce
            return StartResult(kernel_view(announce), created=True)

    # ------------------------------------------------------------ connections

    def _user_key(self) -> bytes:
        if self._key is None:
            self._key = self.backend.user_key()
        return self._key

    async def connection(self, kernel_id: str) -> KernelConnection:
        """The shared connection to ``kernel_id``. Raises ``DKPError`` (not_found, kernel_unreachable)."""
        conn = self._conns.get(kernel_id)
        if conn is not None and not conn.closed.is_set():
            return conn
        lock = self._conn_locks.setdefault(kernel_id, asyncio.Lock())
        async with lock:
            conn = self._conns.get(kernel_id)
            if conn is not None and not conn.closed.is_set():
                return conn
            announce = await self.find(kernel_id)
            if announce is None:
                raise DKPError("not_found", "no running kernel %s" % kernel_id)
            try:
                conn = await KernelConnection.connect(self.host, int(announce["port"]), kernel_id,
                                                      self._user_key())
            except DKPError:
                # The announce may be stale (hard restart, crash): ask once more.
                self.forget(kernel_id)
                announce = await self.find(kernel_id)
                if announce is None:
                    raise DKPError("not_found", "no running kernel %s" % kernel_id)
                conn = await KernelConnection.connect(self.host, int(announce["port"]), kernel_id,
                                                      self._user_key())
            self._conns[kernel_id] = conn
            return conn

    async def drop(self, kernel_id: str) -> None:
        conn = self._conns.pop(kernel_id, None)
        if conn is not None:
            await conn.close()

    async def aclose(self) -> None:
        """Close every connection. Kernels are never touched (FR-M3, INTENT D2)."""
        for kernel_id in list(self._conns):
            await self.drop(kernel_id)
