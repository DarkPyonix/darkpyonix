"""The discovery registry ``<home>/kernels/<kernel_id>.json`` (FR-D2, PROTOCOL §1, §2.4).

Each entry is the kernel's announce body without the nonce; it holds no secrets.
Standard library only; Python 3.8+.
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, List

from darkpyonix import _home

_SKIP_IN_FILE = ("nonce",)


def entry_path(kernel_id: str) -> str:
    return os.path.join(_home.kernels_dir(), kernel_id + ".json")


def write(info: Dict[str, Any]) -> None:
    """Atomically write the announce body ``info`` (0644)."""
    body = {k: v for k, v in info.items() if k not in _SKIP_IN_FILE}
    data = json.dumps(body, ensure_ascii=False, sort_keys=True).encode("utf-8")
    _home.write_public(entry_path(str(info["kernel_id"])), data)


def remove(kernel_id: str) -> None:
    try:
        os.unlink(entry_path(kernel_id))
    except FileNotFoundError:
        pass


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
    from darkpyonix.kernel import launcher
    launcher._reap(pid)
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


def read(kernel_id: str) -> Dict[str, Any]:
    """Return one entry or ``{}`` if missing or unreadable."""
    try:
        with open(entry_path(kernel_id), "rb") as f:
            data = json.loads(f.read().decode("utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def scan(prune: bool = True) -> List[Dict[str, Any]]:
    """Return live entries of this user's kernels; delete dead or foreign ones if ``prune``."""
    directory = _home.kernels_dir()
    tag = _home.user_tag()
    out = []
    for name in sorted(os.listdir(directory)):
        if not name.endswith(".json"):
            continue
        path = os.path.join(directory, name)
        try:
            with open(path, "rb") as f:
                entry = json.loads(f.read().decode("utf-8"))
        except FileNotFoundError:
            continue
        except (OSError, ValueError):
            entry = None
        ok = (
            isinstance(entry, dict)
            and entry.get("user_tag") == tag
            and entry.get("kernel_id") == name[:-len(".json")]
            and pid_alive(entry.get("pid", 0))
        )
        if ok:
            out.append(entry)
        elif prune:
            try:
                os.unlink(path)
            except OSError:
                pass
    return out
