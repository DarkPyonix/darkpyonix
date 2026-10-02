"""INTEGRATION PLACEHOLDER for the kernel entry point (issue #9).

This minimal stand-in exists only so the process plumbing (lock, discovery, registry,
launcher) can be tested with real processes. It owns the file lock, announces itself as
``idle`` with a dummy port 0, and sleeps until SIGTERM/SIGINT. It runs no cells and has no
control channel. The integration leader replaces this module with the real kernel; keep the
``main(argv)`` signature.
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
from typing import Any, Dict, List, Optional

from darkpyonix import _home
from darkpyonix.kernel import lock, registry
from darkpyonix.kernel.discovery import DiscoveryResponder
from darkpyonix.kernel.protocol import (
    EXIT_ALREADY_RUNNING, KERNEL_VERSION, canonical_path, kernel_id_for, now_iso,
)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="darkpyonix-kernel")
    parser.add_argument("--file", required=True)
    args, _unknown = parser.parse_known_args(argv)

    path = canonical_path(args.file)
    kernel_id = kernel_id_for(path)
    file_lock = lock.FileLock(lock.lock_path(kernel_id))
    if not file_lock.acquire():
        existing = registry.read(kernel_id) or {"kernel_id": kernel_id}
        sys.stderr.write(json.dumps(existing, ensure_ascii=False) + "\n")
        sys.stderr.flush()
        return EXIT_ALREADY_RUNNING

    tag = _home.user_tag()
    state = {
        "kernel_id": kernel_id,
        "path": path,
        "pid": os.getpid(),
        "port": 0,
        "status": "idle",
        "run_id": None,
        "python": {
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "executable": sys.executable,
        },
        "dkp_kernel_version": KERNEL_VERSION,
        "started_at": now_iso(),
        "host": socket.gethostname(),
    }  # type: Dict[str, Any]

    stop = threading.Event()

    def on_signal(signum: int, frame: Any) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    responder = DiscoveryResponder(lambda: state, tag)
    try:
        responder.start()
        while not stop.is_set():
            stop.wait(0.5)
    finally:
        state["status"] = "stopping"
        responder.stop(bye=True)
        registry.remove(kernel_id)
        file_lock.release()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
