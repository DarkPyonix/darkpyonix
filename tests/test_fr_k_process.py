"""FR-K1, FR-K3, FR-K4: kernel process plumbing (launch, lock, detached lifetime)."""
from __future__ import annotations

import json
import os
import signal
import stat
import subprocess
import sys

import pytest

from darkpyonix import _home
from darkpyonix.kernel import launcher, registry
from darkpyonix.kernel.protocol import EXIT_ALREADY_RUNNING, canonical_path, kernel_id_for

from kernel_procs import kill, notebook, reap, start, wait_pid_gone

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
