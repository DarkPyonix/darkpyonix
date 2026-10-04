"""FR-K1, FR-K3, FR-K4, FR-K7, FR-K8: kernel process plumbing (launch, lock, detached
lifetime, hard restart, shutdown)."""
from __future__ import annotations

import json
import os
import signal
import stat
import subprocess
import sys
import time

import pytest

from darkpyonix import _home
from darkpyonix.kernel import discovery, launcher, lock, registry
from darkpyonix.kernel.protocol import EXIT_ALREADY_RUNNING, canonical_path, kernel_id_for
from darkpyonix.kernel.runs import RunStore

from kernel_procs import (
    connect, kill, notebook, reap, run_and_wait, start, stream_text, wait_pid_gone,
    write_notebook,
)

pytestmark = pytest.mark.skipif(os.name == "nt", reason="POSIX signals in these tests")


def _clean_env():
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    return env


def test_fr_k1_kernel_runs_from_uninstalled_interpreter(python, dp_home, scratch):
    env = _clean_env()
    # The interpreter under test does not have darkpyonix installed.
    bare = subprocess.run([python, "-c", "import darkpyonix"], cwd=scratch, env=env,
                          capture_output=True, text=True, timeout=30)
    assert bare.returncode != 0 and "darkpyonix" in bare.stderr

    # The bootstrap makes `import darkpyonix` work in the child (what user code will do).
    probe = launcher.bootstrap_command(python, notebook(scratch))
    probe[2] = launcher.BOOTSTRAP_PRELUDE + "import darkpyonix, sys; print(darkpyonix.__file__); sys.exit(0)"
    out = subprocess.run(probe, cwd=scratch, env=env, capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip().startswith(launcher.KERNEL_ROOT)

    path = notebook(scratch)
    proc = subprocess.Popen(launcher.bootstrap_command(python, path), cwd=scratch, env=env,
                            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        info = launcher.wait_for_announce(kernel_id_for(path), pid=proc.pid, timeout=15)
        assert info is not None, proc.stderr.read1(65536) if proc.poll() is not None else "no announce"
        assert info["kernel_id"] == kernel_id_for(path)
        assert info["path"] == canonical_path(path)
        assert info["pid"] == proc.pid
        assert info["status"] == "idle"
        # Compare the version: /usr/bin/python3 on macOS is a shim for another executable.
        version = subprocess.run([python, "-c", "import platform; print(platform.python_version())"],
                                 env=env, capture_output=True, text=True, timeout=30).stdout.strip()
        assert info["python"]["version"] == version
    finally:
        reap(proc)


def test_fr_k1_kernel_from_uninstalled_venv_runs_a_cell(python, dp_home, scratch, monkeypatch):
    """A venv interpreter without DarkPyonix starts a kernel whose cells import darkpyonix."""
    monkeypatch.delenv("PYTHONPATH", raising=False)
    venv = os.path.join(scratch, "venv")
    made = subprocess.run([python, "-m", "venv", "--without-pip", venv],
                          env=_clean_env(), capture_output=True, text=True, timeout=120)
    if made.returncode != 0 and "ensurepip" in made.stderr + made.stdout:
        pytest.skip("venv module unusable for %s" % python)
    assert made.returncode == 0, made.stderr
    vpy = os.path.join(venv, "bin", "python")
    bare = subprocess.run([vpy, "-c", "import darkpyonix"], cwd=scratch, env=_clean_env(),
                          capture_output=True, text=True, timeout=30)
    assert bare.returncode != 0 and "darkpyonix" in bare.stderr

    path = write_notebook(scratch, "venv_nb.py", """
        # %% [code]
        import sys
        import darkpyonix
        print(darkpyonix.__file__)
        print(sys.prefix)
        print(sys.prefix != sys.base_prefix)
        """)
    pid = launcher.launch(path, python=vpy)
    try:
        info = launcher.wait_for_announce(kernel_id_for(path), pid=pid, timeout=20)
        assert info is not None, "venv kernel did not announce"
        c = connect(info)
        try:
            status, _ = run_and_wait(c)
            nb = c.request("runs.get", {"run_id": "latest"})
        finally:
            c.close()
        assert status == "ok", nb
        module, prefix, in_venv = stream_text(nb).splitlines()
        assert module.startswith(launcher.KERNEL_ROOT)
        assert os.path.realpath(prefix) == os.path.realpath(venv)
        assert in_venv == "True"
    finally:
        kill(pid, signal.SIGTERM)
        if not wait_pid_gone(pid):
            kill(pid, signal.SIGKILL)
            wait_pid_gone(pid)


def test_fr_k3_second_kernel_for_same_file_is_refused(python, dp_home, scratch):
    path = notebook(scratch)
    kid = kernel_id_for(path)
    first = start(python, path)
    second = None
    try:
        info = launcher.wait_for_announce(kid, pid=first.pid, timeout=15)
        assert info is not None
        second = start(python, path)
        assert second.wait(15) == EXIT_ALREADY_RUNNING
        err = second.stderr.read().decode("utf-8", "replace")
        existing = json.loads(err.strip().splitlines()[-1])
        assert existing["kernel_id"] == kid and existing["pid"] == first.pid
        # The first kernel is unaffected.
        assert first.poll() is None
        again = launcher.wait_for_announce(kid, pid=first.pid, timeout=5)
        assert again is not None and again["pid"] == first.pid
    finally:
        if second is not None:
            reap(second)
        reap(first)


def test_fr_k3_lock_is_released_when_kernel_dies(python, dp_home, scratch):
    path = notebook(scratch)
    kid = kernel_id_for(path)
    first = start(python, path)
    second = None
    try:
        assert launcher.wait_for_announce(kid, pid=first.pid, timeout=15) is not None
        kill(first.pid, signal.SIGKILL)
        first.wait(5)
        second = start(python, path)
        info = launcher.wait_for_announce(kid, pid=second.pid, timeout=15)
        assert info is not None and info["pid"] == second.pid
        assert second.poll() is None
    finally:
        if second is not None:
            reap(second)
        reap(first)


def test_fr_k4_kernel_survives_launcher_exit(python, dp_home, scratch):
    path = notebook(scratch)
    kid = kernel_id_for(path)
    # A throwaway "manager" process launches the kernel and exits at once.
    code = ("import sys; sys.path.insert(0, %r); from darkpyonix.kernel import launcher; "
            "print(launcher.launch(%r, python=%r))" % (launcher.KERNEL_ROOT, path, python))
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    pid = int(out.stdout.strip())
    try:
        info = launcher.wait_for_announce(kid, pid=pid, timeout=15)
        assert info is not None and info["pid"] == pid
        assert registry.pid_alive(pid)
        # Detached: its own session, no controlling terminal shared with us.
        assert os.getsid(pid) == pid and os.getsid(pid) != os.getsid(0)
        log = os.path.join(_home.kernels_dir(), kid + ".log")
        assert os.path.exists(log)
        assert stat.S_IMODE(os.stat(log).st_mode) == 0o600
    finally:
        kill(pid, signal.SIGTERM)
        if not wait_pid_gone(pid):
            kill(pid, signal.SIGKILL)
            wait_pid_gone(pid)
    # A clean shutdown removes the registry entry (PROTOCOL §2.5).
    assert not os.path.exists(os.path.join(_home.kernels_dir(), kid + ".json"))


def _announce_from(kid, pid, newer_than, timeout=20.0):
    """Wait for ``kid``'s announce from ``pid`` whose ``started_at`` differs from ``newer_than``."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = launcher.wait_for_announce(kid, pid=pid, timeout=1.0)
        if info is not None and info["started_at"] != newer_than:
            return info
        time.sleep(0.05)
    return None


def test_fr_k7_hard_restart_keeps_kernel_id(python, dp_home, scratch):
    path = write_notebook(scratch, "restart.py", """
        # %% [code]
        before_restart = 1
        """)
    kid = kernel_id_for(path)
    pid = launcher.launch(path, python=python)
    try:
        old = launcher.wait_for_announce(kid, pid=pid, timeout=15)
        assert old is not None
        c = connect(old)
        try:
            assert run_and_wait(c)[0] == "ok"
            assert "before_restart" in {v["name"] for v in c.request("namespace")["variables"]}
            assert c.request("restart", {"hard": True}) == {"restarted": True}
        finally:
            c.close()
        # The process re-executes itself (same pid on POSIX, os.execv) and comes back.
        new = _announce_from(kid, pid, old["started_at"])
        assert new is not None, "kernel did not come back after a hard restart"
        assert new["kernel_id"] == kid
        assert new["pid"] != old["pid"] or new["started_at"] != old["started_at"]
        assert new["started_at"] > old["started_at"]
        assert new["python"] == old["python"] and new["path"] == old["path"]
        c = connect(new)
        try:
            names = {v["name"] for v in c.request("namespace")["variables"]}
            assert "before_restart" not in names
            assert c.request("status")["started_at"] == new["started_at"]
        finally:
            c.close()
    finally:
        kill(pid, signal.SIGTERM)
        if not wait_pid_gone(pid):
            kill(pid, signal.SIGKILL)
            wait_pid_gone(pid)


def test_fr_k8_shutdown_is_graceful(python, dp_home, scratch, monkeypatch):
    monkeypatch.delenv("DARKPYONIX_DISCOVERY", raising=False)   # bye travels by multicast
    path = write_notebook(scratch, "shutdown.py", """
        import time
        # %% [code]
        print("started", flush=True)
        while True:
            time.sleep(0.01)
        """)
    kid = kernel_id_for(path)
    seen = []
    listener = discovery.DiscoveryClient(_home.user_tag())
    listener.listen(seen.append)
    proc = start(python, path)
    try:
        info = launcher.wait_for_announce(kid, pid=proc.pid, timeout=15)
        assert info is not None
        assert os.path.exists(registry.entry_path(kid))
        c = connect(info)
        try:
            acc = c.request("run", {"mode": "all"})
            deadline = time.time() + 15
            while time.time() < deadline:
                ev = c.next_event(timeout=0.5)
                if ev and ev["type"] == "output" and "started" in ev["data"]["output"].get("text", ""):
                    break
            else:
                raise AssertionError("cell did not start")
            assert c.request("shutdown") == {"shutting_down": True}
        finally:
            c.close()
        assert proc.wait(15) == 0
        deadline = time.time() + 5
        while time.time() < deadline and not any(m.get("op") == "bye" for m in seen):
            time.sleep(0.02)
    finally:
        listener.close()
        reap(proc)
    assert any(m.get("op") == "bye" and m.get("kernel_id") == kid and m.get("pid") == proc.pid
               for m in seen)
    assert not os.path.exists(registry.entry_path(kid))
    # The OS lock is gone: a new holder gets it at once (the lock file itself may remain).
    probe = lock.FileLock(lock.lock_path(kid))
    assert probe.acquire()
    probe.release()
    store = RunStore(path, kid)
    meta = store.get(acc["run_id"])["metadata"]["darkpyonix"]
    assert meta["status"] == "interrupted"
    assert store.list(1)[0]["status"] == "interrupted"
