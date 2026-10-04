"""Ephemeral manager lifecycle (SPEC FR-M3): registry entry, idle exit, kernels untouched."""
from __future__ import annotations

import json
import os
import signal
import stat
import subprocess
import sys
import time

import httpx

import darkpyonix
from conftest import KERNEL_ROOT, REPO

HELPERS = os.path.join(REPO, "tests", "helpers")


def _wait(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


MANAGER_SCRIPT = """
import sys
sys.path[:0] = [%(kernel)r, %(tests)r]
from helpers.fake_backend import StaticBackend
from darkpyonix.manager.main import run_manager
sys.exit(run_manager("ephemeral", idle_timeout=1.0, backend=StaticBackend([%(announce)r])))
"""


def test_fr_m3_ephemeral_manager_exits_when_idle_and_kernels_remain(dp_home, scratch):
    notebook = os.path.join(scratch, "train.py")
    with open(notebook, "w") as f:
        f.write("print('hello')\n")
    announce_file = os.path.join(scratch, "announce.json")
    env = dict(os.environ, DARKPYONIX_HOME=dp_home)
    kernel = subprocess.Popen([sys.executable, os.path.join(HELPERS, "fake_kernel.py"), notebook, announce_file],
                              env=env)
    manager = None
    try:
        assert _wait(lambda: os.path.exists(announce_file))
        announce = json.load(open(announce_file))
        script = MANAGER_SCRIPT % {"kernel": KERNEL_ROOT, "tests": os.path.join(REPO, "tests"),
                                   "announce": announce_file}
        manager = subprocess.Popen([sys.executable, "-c", script], env=env)
        record_file = os.path.join(dp_home, "managers", "%d.json" % manager.pid)
        assert _wait(lambda: os.path.exists(record_file))
        assert stat.S_IMODE(os.stat(record_file).st_mode) == 0o600
        record = json.load(open(record_file))
        assert record["pid"] == manager.pid and record["mode"] == "ephemeral" and record["version"] == darkpyonix.__version__
        assert record["url"].startswith("http://127.0.0.1:") and record["token"] and record["started_at"]

        with httpx.Client(base_url=record["url"], headers={"Authorization": "Bearer " + record["token"]},
                          timeout=5) as c:
            assert c.get("/api/kernels/%s" % announce["kernel_id"]).json()["pid"] == kernel.pid
            # An open event stream keeps the manager alive past its idle timeout.
            with c.stream("GET", "/api/kernels/%s/events" % announce["kernel_id"]) as s:
                assert s.status_code == 200
                time.sleep(2.0)
                assert manager.poll() is None
        assert manager.wait(timeout=10) == 0
        assert not os.path.exists(record_file)
        assert kernel.poll() is None  # the kernel outlives the manager (FR-M3, INTENT D2)
    finally:
        if manager is not None and manager.poll() is None:
            manager.kill()
        kernel.send_signal(signal.SIGTERM)
        kernel.wait(timeout=10)
