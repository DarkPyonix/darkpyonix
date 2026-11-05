"""Start a kernel for a file in any interpreter, detached from the caller (FR-K1, FR-K4).

The interpreter does not need DarkPyonix installed: the bootstrap loads the ``darkpyonix``
package from this file's directory under that name (the equivalent of ``python -m darkpyonix``).
Standard library only; Python 3.8+.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, Sequence

from darkpyonix import _home
from darkpyonix._protocol import canonical_path, kernel_id_for

# The directory of the ``darkpyonix`` package (this file's directory). In a source checkout it
# is named ``kernel``, so it cannot be put on ``sys.path``; the bootstrap loads it by path.
PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))

BOOTSTRAP_PRELUDE = (
    "import sys, importlib.util as u; d = %r; "
    "s = u.spec_from_file_location('darkpyonix', d + '/__init__.py', submodule_search_locations=[d]); "
    "m = u.module_from_spec(s); sys.modules['darkpyonix'] = m; s.loader.exec_module(m); "
) % PACKAGE_DIR
BOOTSTRAP = BOOTSTRAP_PRELUDE + "from darkpyonix.__main__ import main; sys.exit(main(sys.argv[1:]))"

READY_STATUSES = ("idle", "busy")

# Kernels this process launched, so their exit can be reaped (a zombie still answers kill 0).
_children = {}  # type: Dict[int, subprocess.Popen]


def bootstrap_command(python: str, path: str, extra_args: Sequence[str] = ()) -> List[str]:
    """``[python, "-c", BOOTSTRAP, "--file", <canonical path>, *extra_args]``."""
    return [python, "-c", BOOTSTRAP, "--file", canonical_path(path)] + list(extra_args)


def log_path(kernel_id: str) -> str:
    return os.path.join(_home.kernels_dir(), kernel_id + ".log")


def launch(path: str, python: Optional[str] = None, cwd: Optional[str] = None,
           env: Optional[Dict[str, str]] = None) -> int:
    """Start a detached kernel for ``path`` and return its pid without waiting."""
    path = canonical_path(path)
    kernel_id = kernel_id_for(path)
    child_env = dict(os.environ)
    child_env["DARKPYONIX_HOME"] = _home.home()
    if env:
        child_env.update(env)
    log_fd = os.open(log_path(kernel_id), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        kwargs = {}  # type: Dict[str, Any]
        if os.name == "nt":
            kwargs["creationflags"] = (
                getattr(subprocess, "DETACHED_PROCESS", 0x00000008)
                | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200)
            )
        else:
            kwargs["start_new_session"] = True
        proc = subprocess.Popen(
            bootstrap_command(python or sys.executable, path),
            cwd=cwd or os.path.dirname(path),
            env=child_env,
            stdin=subprocess.DEVNULL,
            stdout=log_fd,
            stderr=log_fd,
            close_fds=True,
            **kwargs
        )
    finally:
        os.close(log_fd)
    _children[proc.pid] = proc
    return proc.pid


def _reap(pid: int) -> None:
    """If ``pid`` is a kernel we launched and it has exited, collect it."""
    proc = _children.get(pid)
    if proc is not None and proc.poll() is not None:
        _children.pop(pid, None)


def pid_alive(pid: int) -> bool:
    """Whether a process with ``pid`` exists on this machine (zombies count as dead)."""
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    if os.name == "nt":
        return _pid_alive_windows(pid)
    # Reap our own exited children (kernels this process launched) so they read as dead.
    _reap(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by someone else
    except OSError:
        return False
    return True


def _pid_alive_windows(pid: int) -> bool:
    import ctypes
    from ctypes import wintypes

    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    STILL_ACTIVE = 259
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        # ERROR_ACCESS_DENIED means the process exists but belongs to someone else.
        return ctypes.get_last_error() == 5
    try:
        code = wintypes.DWORD()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
            return True
        return code.value == STILL_ACTIVE
    finally:
        kernel32.CloseHandle(handle)


def wait_for_announce(kernel_id: str, pid: Optional[int] = None,
                      timeout: float = 10.0) -> Optional[Dict[str, Any]]:
    """Poll discovery until ``kernel_id`` reports ``idle`` or ``busy``; None on timeout.

    With ``pid``, only an entry from that process counts, and the wait ends early if it dies.
    """
    from darkpyonix import _discovery as discovery

    deadline = time.monotonic() + timeout
    while True:
        for entry in discovery.discover(kernel_id, timeout=0.1):
            if entry.get("kernel_id") != kernel_id or entry.get("status") not in READY_STATUSES:
                continue
            if pid is not None and entry.get("pid") != pid:
                continue
            return entry
        if pid is not None and not pid_alive(pid):
            return None
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.05)
