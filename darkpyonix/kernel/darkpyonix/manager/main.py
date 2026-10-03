"""Run a manager process (SPEC FR-M3, FR-M4).

Ephemeral: binds ``127.0.0.1`` on a random port, publishes ``managers/<pid>.json`` (0600)
with its URL and master token, and exits after ``idle_timeout`` seconds without requests or
open event streams. Dedicated: binds the configured host and port and never idles out.
Either way, exiting closes kernel connections and never touches the kernels (INTENT D2).

SUPERSEDED PROTOTYPE: the manager is being rewritten in Rust. This Python module is kept as
a reference for the contract's behaviour; the language-neutral oracle is tests/ (fake DKP/1
kernel in tests/helpers, HTTP-level tests runnable via DARKPYONIX_MANAGER_CMD).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import socket
import sys
import threading
from typing import Callable, Optional

import uvicorn

from darkpyonix import _home

from .app import VERSION, ManagerState, create_app
from .auth import Auth
from .kernels import KernelBackend, KernelDirectory

DEFAULT_IDLE_TIMEOUT = 120.0
DEFAULT_DEDICATED_PORT = 46881


def record_path(pid: Optional[int] = None) -> str:
    return os.path.join(_home.managers_dir(), "%d.json" % (pid or os.getpid()))


def _bind(host: str, port: int) -> socket.socket:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    if port:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(128)
    sock.setblocking(False)
    return sock


async def serve_manager(mode: str = "ephemeral", host: Optional[str] = None, port: Optional[int] = None,
                        idle_timeout: Optional[float] = DEFAULT_IDLE_TIMEOUT,
                        backend: Optional[KernelBackend] = None, token: Optional[str] = None,
                        on_ready: Optional[Callable[[dict], None]] = None) -> None:
    if mode not in ("ephemeral", "dedicated"):
        raise ValueError("mode must be 'ephemeral' or 'dedicated'")
    if mode == "ephemeral":
        host, port = "127.0.0.1", port or 0
    else:
        host, port, idle_timeout = host or "127.0.0.1", port or DEFAULT_DEDICATED_PORT, None
        token = token or os.environ.get("DARKPYONIX_MANAGER_TOKEN")
    state = ManagerState(mode, idle_timeout)
    directory = KernelDirectory(backend)
    auth = Auth(token)
    app = create_app(directory, auth, state)
    sock = _bind(host, port)
    bound = sock.getsockname()[1]
    reach = "127.0.0.1" if host in ("0.0.0.0", "::", "") else host
    url = "http://%s:%d" % ("[%s]" % reach if ":" in reach else reach, bound)
    server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="warning", access_log=False,
                                           timeout_graceful_shutdown=2))
    record = {"pid": state.pid, "url": url, "token": auth.master_token, "mode": mode,
              "started_at": state.started_at, "version": VERSION}
    path = record_path(state.pid)
    _home.write_private(path, json.dumps(record).encode("utf-8"))

    async def watchdog() -> None:
        tick = max(0.05, min(1.0, idle_timeout / 4.0))
        while not server.should_exit:
            await asyncio.sleep(tick)
            if state.idle_for() >= idle_timeout:
                server.should_exit = True

    dog = asyncio.ensure_future(watchdog()) if idle_timeout else None
    if on_ready is not None:
        on_ready(record)
    try:
        await server.serve(sockets=[sock])
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
        if dog is not None:
            dog.cancel()
        await directory.aclose()
        sock.close()


def run_manager(mode: str = "ephemeral", host: Optional[str] = None, port: Optional[int] = None,
                idle_timeout: Optional[float] = DEFAULT_IDLE_TIMEOUT, backend: Optional[KernelBackend] = None,
                token: Optional[str] = None, on_ready: Optional[Callable[[dict], None]] = None) -> int:
    """Serve until idle (ephemeral) or until SIGINT/SIGTERM. Returns the exit code."""
    if threading.current_thread() is threading.main_thread() and hasattr(signal, "SIGTERM"):
        # uvicorn re-raises the signal it caught after a graceful stop; a no-op handler lets
        # serve_manager's cleanup (registry removal) finish and the process exit with 0.
        signal.signal(signal.SIGTERM, lambda signum, frame: None)
    try:
        asyncio.run(serve_manager(mode, host, port, idle_timeout, backend, token, on_ready))
    except KeyboardInterrupt:
        pass
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="darkpyonix manager")
    parser.add_argument("--dedicated", action="store_true")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--idle-timeout", type=float, default=DEFAULT_IDLE_TIMEOUT)
    args = parser.parse_args(argv)

    def ready(record: dict) -> None:
        sys.stderr.write("darkpyonix manager (%s) at %s, pid %d\n" % (record["mode"], record["url"], record["pid"]))
        sys.stderr.flush()

    return run_manager("dedicated" if args.dedicated else "ephemeral", args.host, args.port,
                       args.idle_timeout, on_ready=ready)


if __name__ == "__main__":
    sys.exit(main())
