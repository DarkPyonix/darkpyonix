"""``darkpyonix.run_command`` (SPEC FR-F6) and the package helpers ``uv`` / ``pip``.

Output is read by helper threads but written to ``sys.stdout`` / ``sys.stderr`` from the
calling thread, one line at a time, so a kernel that captures output per executing thread
sees it. ``KeyboardInterrupt`` is forwarded to the child's process group as SIGINT
(CTRL_BREAK_EVENT on Windows); after a grace period the group is terminated.
"""
from __future__ import annotations

import os
import queue
import shutil
import signal
import subprocess
import sys
import textwrap
import threading

INTERRUPT_GRACE = 5.0  # seconds the child gets to exit after a forwarded interrupt
_POSIX = os.name == "posix"


def _reader(pipe, which, q):
    try:
        for line in iter(pipe.readline, ""):
            q.put((which, line))
    except (OSError, ValueError):
        pass
    finally:
        q.put((which, None))


def _write(which, line):
    stream = sys.stdout if which == 1 else sys.stderr
    if stream is None:
        return
    stream.write(line)
    try:
        stream.flush()
    except Exception:
        pass


def _signal_group(proc, sig_name):
    if proc.poll() is not None:
        return
    try:
        if _POSIX:
            os.killpg(proc.pid, getattr(signal, sig_name))
        elif sig_name == "SIGINT":
            proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
        else:
            proc.kill()
    except (ProcessLookupError, PermissionError, OSError):
        pass


def _pump(q, open_pipes, timeout):
    """Write queued lines; returns the number of pipes still open."""
    try:
        which, line = q.get(timeout=timeout)
    except queue.Empty:
        return open_pipes
    if line is None:
        return open_pipes - 1
    _write(which, line)
    return open_pipes


def run_command(cmd, check=False, cwd=None, env=None, shell=True):
    """Run ``cmd`` in a subprocess, streaming its output line by line; return the exit code.

    ``env`` entries are added to (not substituted for) the current environment.
    ``check=True`` raises ``subprocess.CalledProcessError`` on a non-zero exit code.
    """
    if isinstance(cmd, str):
        cmd = textwrap.dedent(cmd).strip()
    full_env = None
    if env is not None:
        full_env = dict(os.environ)
        full_env.update({str(k): str(v) for k, v in env.items()})
    kwargs = {}
    if _POSIX:
        kwargs["start_new_session"] = True
    else:
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:
            pass
    proc = subprocess.Popen(
        cmd, shell=shell, cwd=cwd, env=full_env,
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True, errors="replace", bufsize=1, **kwargs
    )
    q = queue.Queue()
    threads = [
        threading.Thread(target=_reader, args=(proc.stdout, 1, q), daemon=True),
        threading.Thread(target=_reader, args=(proc.stderr, 2, q), daemon=True),
    ]
    for t in threads:
        t.start()
    open_pipes = 2
    try:
        while open_pipes:
            open_pipes = _pump(q, open_pipes, 0.1)
        code = proc.wait()
    except KeyboardInterrupt:
        _signal_group(proc, "SIGINT")
        _drain_until_exit(proc, q, open_pipes, INTERRUPT_GRACE)
        if proc.poll() is None:
            _signal_group(proc, "SIGTERM")
            _drain_until_exit(proc, q, 0, 1.0)
        if proc.poll() is None:
            _signal_group(proc, "SIGKILL")
            proc.wait()
        raise
    finally:
        for pipe in (proc.stdout, proc.stderr):
            try:
                pipe.close()
            except Exception:
                pass
    if check and code != 0:
        raise subprocess.CalledProcessError(code, cmd)
    return code


def _drain_until_exit(proc, q, open_pipes, grace):
    import time
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        if open_pipes:
            open_pipes = _pump(q, open_pipes, 0.05)
        elif proc.poll() is not None:
            break
        else:
            time.sleep(0.05)
    while True:  # whatever is already queued
        try:
            which, line = q.get_nowait()
        except queue.Empty:
            break
        if line is not None:
            _write(which, line)


def _installed():
    import importlib
    importlib.invalidate_caches()


class _Pip(object):
    """``darkpyonix.pip``: pip for the interpreter running this code."""

    def __init__(self, uv=False):
        self._uv = uv

    def _base(self):
        if self._uv:
            exe = shutil.which("uv")
            if exe:
                return [exe, "pip"], ["--python", sys.executable], True
        return [sys.executable, "-m", "pip"], [], False

    def install(self, *pkgs):
        if not pkgs:
            raise ValueError("install() needs at least one package")
        base, extra, _ = self._base()
        run_command(base + ["install"] + extra + list(pkgs), check=True, shell=False)
        _installed()

    def uninstall(self, *pkgs):
        if not pkgs:
            raise ValueError("uninstall() needs at least one package")
        base, extra, is_uv = self._base()
        yes = [] if is_uv else ["-y"]  # uv never prompts
        run_command(base + ["uninstall"] + extra + yes + list(pkgs), check=True, shell=False)
        _installed()

    def __repr__(self):
        return "<darkpyonix.%s for %s>" % ("uv.pip" if self._uv else "pip", sys.executable)


class _Uv(object):
    """``darkpyonix.uv``: uv (falling back to pip) for the interpreter running this code."""

    def __init__(self):
        self.pip = _Pip(uv=True)

    def add(self, *pkgs):
        self.pip.install(*pkgs)

    def remove(self, *pkgs):
        self.pip.uninstall(*pkgs)

    def __repr__(self):
        return "<darkpyonix.uv for %s>" % sys.executable


pip = _Pip()
uv = _Uv()
