"""Helpers for tests that start real kernel processes (FR-K*, FR-D*)."""
from __future__ import annotations

import os
import signal
import subprocess
import time

from darkpyonix import _launcher as launcher, _registry as registry


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


# ------------------------------------------------------------------ DKP/1 helpers

def write_notebook(scratch: str, name: str, text: str) -> str:
    """Write ``text`` (dedented) as a notebook file under ``scratch``."""
    import textwrap
    path = os.path.join(scratch, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(text).lstrip("\n"))
    return path


def connect(info):
    """A connected, subscribed ``KernelClient`` for an announce body."""
    from darkpyonix._client import KernelClient
    c = KernelClient(info["port"], info["kernel_id"], name="test", kind="cli")
    c.connect()
    c.subscribe()
    return c


def run_and_wait(c, params=None, timeout: float = 30.0):
    """Send ``run`` and collect events until it finishes. Returns (status, events)."""
    acc = c.request("run", params or {"mode": "all"})
    events = []
    deadline = time.time() + timeout
    while time.time() < deadline:
        ev = c.next_event(timeout=0.5)
        if ev is None:
            continue
        events.append(ev)
        if ev["type"] == "run.finished" and ev["data"]["run_id"] == acc["run_id"]:
            return ev["data"]["status"], events
    raise AssertionError("run %s did not finish" % acc["run_id"])


def stream_text(nb, name: str = "stdout") -> str:
    return "".join(o.get("text", "") for cell in nb["cells"] for o in cell.get("outputs", [])
                   if o.get("output_type") == "stream" and o.get("name") == name)
