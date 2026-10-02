"""Helpers for tests that start real kernel processes (FR-K*, FR-D*)."""
from __future__ import annotations

import os
import signal
import subprocess
import time

from darkpyonix.kernel import launcher, registry


def notebook(scratch: str, name: str = "nb.py") -> str:
    path = os.path.join(scratch, name)
    with open(path, "w") as f:
        f.write("print('hello')\n")
    return path


def start(python: str, path: str, env_extra=None) -> subprocess.Popen:
    """Start a kernel for ``path`` in the foreground (a direct child we can reap)."""
    env = dict(os.environ)
    env.update(env_extra or {})
    return subprocess.Popen(
        launcher.bootstrap_command(python, path), cwd=os.path.dirname(path), env=env,
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )


def kill(pid: int, sig: int = signal.SIGKILL) -> None:
    try:
        os.kill(pid, sig)
    except OSError:
        pass


def reap(proc: subprocess.Popen, timeout: float = 5.0) -> None:
    if proc.poll() is None:
        kill(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout)
        except subprocess.TimeoutExpired:
            kill(proc.pid)
            proc.wait(timeout)
    for stream in (proc.stdout, proc.stderr):
        if stream:
            stream.close()


def wait_pid_gone(pid: int, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not registry.pid_alive(pid):
            return True
        time.sleep(0.02)
    return False
