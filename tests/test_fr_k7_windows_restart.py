"""FR-K7 hard restart on Windows: the re-executed command line keeps its arguments whole.

MSVC ``_wexecv`` joins ``argv`` with spaces without quoting, so the multi-line ``-c`` bootstrap and a
notebook path with spaces must reach the new process intact. Runs only on Windows (CI Windows job).
"""
from __future__ import annotations

import os
import time

import pytest

from darkpyonix import _launcher as launcher
from darkpyonix._client import KernelClient
from darkpyonix._protocol import kernel_id_for

pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows _wexecv quoting")


def test_fr_k7_hard_restart_keeps_kernel_id_on_windows(python, dp_home, scratch):
    folder = os.path.join(scratch, "with space")
    os.makedirs(folder)
    path = os.path.join(folder, "re start.py")
    with open(path, "w") as f:
        f.write('# %% [code]\nbefore_restart = "a \\"quoted\\" value"\n')
    kid = kernel_id_for(path)
    pid = launcher.launch(path, python=python)
    old = launcher.wait_for_announce(kid, pid=pid, timeout=30)
    assert old is not None, "kernel did not announce"
    new = None
    try:
        c = KernelClient(old["control"], kid, name="test", kind="cli")
        c.connect()
        try:
            assert c.request("restart", {"hard": True}) == {"restarted": True}
        finally:
            c.close()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and new is None:
            info = launcher.wait_for_announce(kid, timeout=1.0)
            if info is not None and info["started_at"] > old["started_at"]:
                new = info
        assert new is not None, "kernel did not come back after a hard restart"
        assert new["kernel_id"] == kid and new["path"] == old["path"]
        assert new["python"] == old["python"]
    finally:
        for entry in (new, old):
            if entry is None:
                continue
            try:
                c = KernelClient(entry["control"], kid, name="test", kind="cli")
                c.connect()
                c.request("shutdown", {})
                c.close()
                break
            except Exception:
                continue
