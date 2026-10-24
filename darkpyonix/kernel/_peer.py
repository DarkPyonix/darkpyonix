"""Who is on the other end of a local socket (PROTOCOL §3.2, FR-A1).

The control channel lets only the same OS account through, judged by what the OS reports about the
peer process, not by a secret. Standard library only; Python 3.8+.

POSIX: ``SO_PEERCRED`` (Linux), ``LOCAL_PEERCRED`` (macOS), ``getpeereid`` through ``ctypes``
(other BSDs). Windows named pipes are not served yet (#55 follow-up); ``account`` has the SID path.
"""
from __future__ import annotations

import os
import socket
import struct
import sys

# 3.8 has no socket.SOL_LOCAL; level 0 is SOL_LOCAL on macOS. LOCAL_PEERCRED is 1 there.
_LOCAL_PEERCRED = getattr(socket, "LOCAL_PEERCRED", 1)
_XUCRED_SIZE = 76  # sizeof(struct xucred)


def peer_uid(sock: socket.socket) -> int:
    """The effective UID of the process that connected ``sock`` (an ``AF_UNIX`` socket)."""
    if sys.platform.startswith("linux"):
        raw = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("iII"))
        return struct.unpack("iII", raw)[1]  # struct ucred: pid, uid, gid
    if sys.platform == "darwin":
        raw = sock.getsockopt(0, _LOCAL_PEERCRED, _XUCRED_SIZE)
        return struct.unpack_from("II", raw)[1]  # struct xucred: cr_version, cr_uid
    import ctypes
    libc = ctypes.CDLL(None, use_errno=True)
    uid, gid = ctypes.c_uint(), ctypes.c_uint()
    if libc.getpeereid(sock.fileno(), ctypes.byref(uid), ctypes.byref(gid)) != 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))
    return uid.value


def same_account(sock: socket.socket) -> bool:
    """True only if the OS says the peer runs as this process's account. Any failure is False."""
    try:
        return peer_uid(sock) == os.geteuid()
    except (OSError, AttributeError, ValueError, struct.error):
        return False


def account() -> str:
    """``uid:<n>`` on POSIX, ``sid:<S-1-5-...>`` on Windows (PROTOCOL §2.2)."""
    if os.name != "nt":
        return "uid:%d" % os.getuid()
    return "sid:" + _windows_sid()


def _windows_sid() -> str:  # pragma: no cover - needs Windows
    import ctypes
    from ctypes import wintypes
    k32, adv = ctypes.WinDLL("kernel32", use_last_error=True), ctypes.WinDLL("advapi32", use_last_error=True)
    k32.GetCurrentProcess.restype = wintypes.HANDLE
    token = wintypes.HANDLE()
    TOKEN_QUERY = 0x0008
    if not adv.OpenProcessToken(k32.GetCurrentProcess(), TOKEN_QUERY, ctypes.byref(token)):
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        size = wintypes.DWORD()
        adv.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))  # 1 = TokenUser
        buf = ctypes.create_string_buffer(size.value)
        if not adv.GetTokenInformation(token, 1, buf, size, ctypes.byref(size)):
            raise ctypes.WinError(ctypes.get_last_error())
        sid_ptr = ctypes.c_void_p.from_buffer(buf).value  # TOKEN_USER starts with the SID pointer
        text = wintypes.LPWSTR()
        if not adv.ConvertSidToStringSidW(ctypes.c_void_p(sid_ptr), ctypes.byref(text)):
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            return text.value
        finally:
            k32.LocalFree(text)
    finally:
        k32.CloseHandle(token)
