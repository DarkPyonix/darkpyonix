"""The integrated kernel process over DKP/1: launch, run, log, interrupt, busy, survival.

These drive the real kernel main (kernel/darkpyonix/kernel/__main__.py) through the public
surfaces: launcher, discovery, KernelClient.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import textwrap
import time

import pytest

from darkpyonix.kernel import launcher
from darkpyonix.kernel.client import KernelClient
from darkpyonix.kernel.discovery import discover
from darkpyonix.kernel.protocol import DKPError, kernel_id_for
from darkpyonix.kernel.runs import runs_dir_for

NB = textwrap.dedent('''\
    import time

    # %% [code]
    x = 21
    print("hello", x)

    # %% [code]
    x * 2

    # %% [code]
    step = 0
    while LOOP:
        step += 1
        time.sleep(0.01)
    print("loop done", step)
    ''')


def _write(scratch, loop):
    path = os.path.join(scratch, "train.py")
    with open(path, "w") as f:
        f.write(NB.replace("LOOP", "True" if loop else "False"))
    return path


def _start(path, python):
    pid = launcher.launch(path, python=python)
    info = launcher.wait_for_announce(kernel_id_for(path), pid=pid, timeout=15.0)
    assert info is not None, "kernel did not announce"
    assert info["port"] > 0
    return pid, info


def _client(info):
    c = KernelClient(info["port"], info["kernel_id"], name="test", kind="cli")
    c.connect()
    return c


def _wait_finished(c, run_id, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        ev = c.next_event(timeout=0.5)
        if ev and ev["type"] == "run.finished" and ev["data"]["run_id"] == run_id:
            return ev["data"]["status"]
    raise AssertionError("run %s did not finish" % run_id)


def _stop(pid):
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    for _ in range(100):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    os.kill(pid, signal.SIGKILL)


def test_e2e_run_all_logs_and_outputs(scratch, dp_home, python):
    path = _write(scratch, loop=False)
    pid, info = _start(path, python)
    try:
        c = _client(info)
        c.subscribe()
        acc = c.request("run", {"mode": "all"})
        assert _wait_finished(c, acc["run_id"]) == "ok"
        nb = c.request("runs.get", {"run_id": "latest"})
        outs = [o for cell in nb["cells"] for o in cell["outputs"]]
        text = "".join(o.get("text", "") for o in outs if o["output_type"] == "stream")
        assert "hello 21" in text and "loop done 0" in text
        assert any(o["output_type"] == "execute_result" and o["data"]["text/plain"] == "42" for o in outs)
        logs = [f for f in os.listdir(runs_dir_for(path)) if f.endswith(".ipynb")]
        assert len(logs) == 1
        c.close()
    finally:
        _stop(pid)


def test_e2e_interrupt_keeps_state_and_busy_is_rejected(scratch, dp_home):
    path = _write(scratch, loop=True)
    pid, info = _start(path, sys.executable)
    try:
        c = _client(info)
        c.subscribe()
        acc = c.request("run", {"mode": "all"})
        time.sleep(0.5)
        with pytest.raises(DKPError) as e:
            c.request("run", {"mode": "all"})
        assert e.value.code == "busy"
        t0 = time.time()
        assert c.request("interrupt")["interrupted"] is True
        assert _wait_finished(c, acc["run_id"]) == "interrupted"
        assert time.time() - t0 < 1.5
        names = {v["name"] for v in c.request("namespace")["variables"]}
        assert {"x", "step"} <= names
        c.close()
    finally:
        _stop(pid)


def test_fr_k4_kernel_survives_manager_kill(scratch, dp_home):
    """A 'manager' process starts the kernel and a run, then is SIGKILLed; the run finishes."""
    path = os.path.join(scratch, "slow.py")
    with open(path, "w") as f:
        f.write("import time\n\n# %% [code]\nfor i in range(15):\n    time.sleep(0.1)\nprint('finished', i)\n")
    driver = textwrap.dedent('''
        import sys, time
        sys.path.insert(0, %r)
        from darkpyonix.kernel import launcher
        from darkpyonix.kernel.client import KernelClient
        from darkpyonix.kernel.protocol import kernel_id_for
        p = %r
        pid = launcher.launch(p)
        info = launcher.wait_for_announce(kernel_id_for(p), pid=pid, timeout=15)
        c = KernelClient(info["port"], info["kernel_id"]); c.connect()
        c.request("run", {"mode": "all"})
        print(pid, flush=True)
        time.sleep(60)
    ''') % (launcher.KERNEL_ROOT, path)
    mgr = subprocess.Popen([sys.executable, "-c", driver], stdout=subprocess.PIPE, text=True,
                           env=dict(os.environ))
    kpid = int(mgr.stdout.readline())
    try:
        mgr.kill()
        mgr.wait(5)
        deadline = time.time() + 15
        status = None
        while time.time() < deadline:
            infos = [k for k in discover(kernel_id_for(path)) if k["kernel_id"] == kernel_id_for(path)]
            assert infos, "kernel vanished with its manager"
            c = _client(infos[0])
            runs = c.request("runs.list", {"limit": 5})["runs"]
            c.close()
            if runs and runs[0]["status"] in ("ok", "error", "interrupted"):
                status = runs[0]["status"]
                break
            time.sleep(0.3)
        assert status == "ok"
    finally:
        _stop(kpid)
