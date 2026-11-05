"""The Windows named-pipe control channel (PROTOCOL §3.1-§3.2, FR-A1, #80).

The kernel serves ``\\\\.\\pipe\\darkpyonix-<user_tag>-<kernel_id>``. The pipe's DACL grants access to
this account's SID only and refuses remote clients. Whoever connects is checked again by asking the OS
which account the peer process runs as; the client does the same for the server process.

Everything that touches Win32 sits in ``_Win32`` and ``OverlappedPipe`` and is built lazily, so this
module imports on every platform. ``PipeSocket`` and ``serve_connection`` talk to a duck-typed stream
(``read(n, timeout)``, ``write(data)``, ``abort()``, ``close()``) and are tested without Windows.

Standard library only (``ctypes``); Python 3.8+.
"""
from __future__ import annotations

import socket
import threading
from typing import Any, Callable, Optional

PIPE_PREFIX = "\\\\.\\pipe\\"
BUFFER = 64 * 1024


def pipe_name(user_tag: str, kernel_id: str) -> str:
    """``\\\\.\\pipe\\darkpyonix-<user_tag>-<kernel_id>`` (PROTOCOL §3.1)."""
    return "%sdarkpyonix-%s-%s" % (PIPE_PREFIX, user_tag, kernel_id)


def is_pipe_name(control: str) -> bool:
    return control.lower().startswith(PIPE_PREFIX)


def dacl_sddl(sid: str) -> str:
    """Protected DACL with one entry: generic-all for ``sid`` and nobody else (FR-A1)."""
    return "D:P(A;;GA;;;%s)" % sid


# ---------------------------------------------------------------- stream -> socket-like adapter

class PipeSocket:
    """The few ``socket`` calls ``KernelClient`` and ``_protocol`` use, over a pipe stream."""

    def __init__(self, stream: Any) -> None:
        self._stream = stream
        self._timeout = None  # type: Optional[float]

    def settimeout(self, value: Optional[float]) -> None:
        self._timeout = value

    def gettimeout(self) -> Optional[float]:
        return self._timeout

    def recv(self, n: int) -> bytes:
        """Up to ``n`` bytes; ``b""`` at end of stream; ``socket.timeout`` when the timeout passes."""
        return self._stream.read(n, self._timeout)

    def sendall(self, data: bytes) -> None:
        self._stream.write(bytes(data))

    def shutdown(self, how: int = 0) -> None:
        self._stream.abort()

    def close(self) -> None:
        self._stream.close()


# ---------------------------------------------------------------- kernel side: pipe <-> socketpair

def serve_connection(stream: Any, deliver: Callable[[socket.socket], None],
                     allowed: Callable[[Any], bool]) -> None:
    """Check ``stream``'s peer, then bridge it to one end of a socket pair given to ``deliver``.

    ``ControlServer`` treats the other end like any accepted socket, so the framing, handshake and
    buffering code is the one POSIX uses. Runs on the pipe's own thread. A peer that fails ``allowed``
    gets no byte (FR-A1).
    """
    try:
        ok = allowed(stream)
    except Exception:
        ok = False
    if not ok:
        stream.close()
        return
    near, far = socket.socketpair()
    deliver(far)

    def to_socket() -> None:
        try:
            while True:
                data = stream.read(BUFFER, None)
                if not data:
                    break
                near.sendall(data)
        except (OSError, ValueError):
            pass
        try:
            near.shutdown(socket.SHUT_WR)
        except OSError:
            pass

    def to_pipe() -> None:
        graceful = True
        try:
            while True:
                data = near.recv(BUFFER)
                if not data:
                    break
                stream.write(data)
        except (OSError, ValueError):
            graceful = False
        try:
            if graceful:
                stream.flush()  # let the client read what was written before the pipe goes away
        except OSError:
            pass
        stream.close()
        try:
            near.close()
        except OSError:
            pass

    threading.Thread(target=to_socket, name="dkp-pipe-in", daemon=True).start()
    threading.Thread(target=to_pipe, name="dkp-pipe-out", daemon=True).start()


# ---------------------------------------------------------------- Win32 (needs Windows)

_ERROR_IO_PENDING = 997
_ERROR_PIPE_CONNECTED = 535
_ERROR_PIPE_BUSY = 231
_ERROR_BROKEN_PIPE = 109
_ERROR_NO_DATA = 232
_ERROR_PIPE_NOT_CONNECTED = 233
_ERROR_OPERATION_ABORTED = 995
_ERROR_ACCESS_DENIED = 5
_WAIT_TIMEOUT = 0x102
_WAIT_FAILED = 0xFFFFFFFF
_INFINITE = 0xFFFFFFFF
_EOF_ERRORS = (_ERROR_BROKEN_PIPE, _ERROR_NO_DATA, _ERROR_PIPE_NOT_CONNECTED)


class _Win32:  # pragma: no cover - needs Windows
    """Prototypes of the Win32 calls used here, declared with explicit types for 64-bit handles."""

    _instance = None  # type: Optional[_Win32]

    @classmethod
    def get(cls) -> "_Win32":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self) -> None:
        import ctypes
        from ctypes import wintypes
        self.ct, self.wt = ctypes, wintypes
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        adv = ctypes.WinDLL("advapi32", use_last_error=True)
        H, D, P, B = wintypes.HANDLE, wintypes.DWORD, ctypes.c_void_p, wintypes.BOOL

        class OVERLAPPED(ctypes.Structure):
            _fields_ = [("Internal", ctypes.c_size_t), ("InternalHigh", ctypes.c_size_t),
                        ("Offset", D), ("OffsetHigh", D), ("hEvent", H)]

        class SECURITY_ATTRIBUTES(ctypes.Structure):
            _fields_ = [("nLength", D), ("lpSecurityDescriptor", P), ("bInheritHandle", B)]

        self.OVERLAPPED, self.SECURITY_ATTRIBUTES = OVERLAPPED, SECURITY_ATTRIBUTES

        def proto(lib: Any, name: str, res: Any, *args: Any) -> Any:
            fn = getattr(lib, name)
            fn.restype, fn.argtypes = res, list(args)
            return fn

        pov = ctypes.POINTER(OVERLAPPED)
        self.CreateNamedPipeW = proto(k32, "CreateNamedPipeW", H, wintypes.LPCWSTR, D, D, D, D, D, D,
                                      ctypes.POINTER(SECURITY_ATTRIBUTES))
        self.CreateFileW = proto(k32, "CreateFileW", H, wintypes.LPCWSTR, D, D, P, D, D, H)
        self.ConnectNamedPipe = proto(k32, "ConnectNamedPipe", B, H, pov)
        self.DisconnectNamedPipe = proto(k32, "DisconnectNamedPipe", B, H)
        self.ReadFile = proto(k32, "ReadFile", B, H, P, D, ctypes.POINTER(D), pov)
        self.WriteFile = proto(k32, "WriteFile", B, H, P, D, ctypes.POINTER(D), pov)
        self.FlushFileBuffers = proto(k32, "FlushFileBuffers", B, H)
        self.CancelIoEx = proto(k32, "CancelIoEx", B, H, pov)
        self.GetOverlappedResult = proto(k32, "GetOverlappedResult", B, H, pov, ctypes.POINTER(D), B)
        self.CreateEventW = proto(k32, "CreateEventW", H, P, B, B, wintypes.LPCWSTR)
        self.WaitForSingleObject = proto(k32, "WaitForSingleObject", D, H, D)
        self.CloseHandle = proto(k32, "CloseHandle", B, H)
        self.LocalFree = proto(k32, "LocalFree", P, P)
        self.GetCurrentProcess = proto(k32, "GetCurrentProcess", H)
        self.OpenProcess = proto(k32, "OpenProcess", H, D, B, D)
        self.GetNamedPipeClientProcessId = proto(k32, "GetNamedPipeClientProcessId", B, H,
                                                 ctypes.POINTER(D))
        self.GetNamedPipeServerProcessId = proto(k32, "GetNamedPipeServerProcessId", B, H,
                                                 ctypes.POINTER(D))
        self.WaitNamedPipeW = proto(k32, "WaitNamedPipeW", B, wintypes.LPCWSTR, D)
        self.OpenProcessToken = proto(adv, "OpenProcessToken", B, H, D, ctypes.POINTER(H))
        self.GetTokenInformation = proto(adv, "GetTokenInformation", B, H, D, P, D, ctypes.POINTER(D))
        self.ConvertSidToStringSidW = proto(adv, "ConvertSidToStringSidW", B, P,
                                            ctypes.POINTER(wintypes.LPWSTR))
        self.ConvertStringSecurityDescriptorToSecurityDescriptorW = proto(
            adv, "ConvertStringSecurityDescriptorToSecurityDescriptorW", B, wintypes.LPCWSTR, D,
            ctypes.POINTER(P), P)
        self.invalid = ctypes.c_void_p(-1).value

    def last_error(self) -> int:
        return self.ct.get_last_error()

    def sid_of_process(self, pid: int) -> str:
        """The SID string of the account process ``pid`` runs as. Raises ``OSError``."""
        ct, wt = self.ct, self.wt
        PROCESS_QUERY_LIMITED_INFORMATION, TOKEN_QUERY, TOKEN_USER = 0x1000, 0x0008, 1
        proc = self.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not proc:
            raise ct.WinError(self.last_error())
        try:
            token = wt.HANDLE()
            if not self.OpenProcessToken(proc, TOKEN_QUERY, ct.byref(token)):
                raise ct.WinError(self.last_error())
            try:
                size = wt.DWORD()
                self.GetTokenInformation(token, TOKEN_USER, None, 0, ct.byref(size))
                buf = ct.create_string_buffer(size.value)
                if not self.GetTokenInformation(token, TOKEN_USER, buf, size, ct.byref(size)):
                    raise ct.WinError(self.last_error())
                sid_ptr = ct.c_void_p.from_buffer(buf).value  # TOKEN_USER starts with the SID pointer
                text = wt.LPWSTR()
                if not self.ConvertSidToStringSidW(ct.c_void_p(sid_ptr), ct.byref(text)):
                    raise ct.WinError(self.last_error())
                try:
                    return text.value
                finally:
                    self.LocalFree(ct.cast(text, ct.c_void_p))
            finally:
                self.CloseHandle(token)
        finally:
            self.CloseHandle(proc)


class OverlappedPipe:  # pragma: no cover - needs Windows
    """One end of a pipe opened for overlapped I/O, so a read never blocks a write on the handle."""

    def __init__(self, handle: int) -> None:
        self._w = _Win32.get()
        self.handle = handle
        self._lock = threading.Lock()
        self._busy = 0
        self._closing = False
        self._closed = False

    # -- lifetime: the handle is closed only once no thread is inside a call on it

    def _enter(self) -> None:
        with self._lock:
            if self._closing:
                raise OSError(_ERROR_BROKEN_PIPE, "pipe is closed")
            self._busy += 1

    def _leave(self) -> None:
        with self._lock:
            self._busy -= 1
            last = self._closing and self._busy == 0 and not self._closed
            if last:
                self._closed = True
        if last:
            self._w.CloseHandle(self.handle)

    def abort(self) -> None:
        """Cancel every pending call on this end; later calls fail."""
        with self._lock:
            self._closing = True
            idle = self._busy == 0 and not self._closed
            if idle:
                self._closed = True
            elif not self._closed:
                self._w.CancelIoEx(self.handle, None)
        if idle:
            self._w.CloseHandle(self.handle)

    close = abort

    # -- overlapped I/O

    def _wait(self, ov: Any, started: bool, timeout: Optional[float]) -> int:
        """Finish an overlapped call; returns the byte count. ``socket.timeout`` on timeout."""
        w = self._w
        n = w.wt.DWORD()
        if not started:
            err = w.last_error()
            if err != _ERROR_IO_PENDING:
                raise OSError(err, "pipe call failed (%d)" % err)
            with self._lock:
                if self._closing:
                    w.CancelIoEx(self.handle, w.ct.byref(ov))
            ms = _INFINITE if timeout is None else max(0, int(timeout * 1000))
            result = w.WaitForSingleObject(ov.hEvent, ms)
            if result == _WAIT_FAILED:
                err = w.last_error()
                w.CancelIoEx(self.handle, w.ct.byref(ov))
                w.GetOverlappedResult(self.handle, w.ct.byref(ov), w.ct.byref(n), True)
                raise OSError(err, "pipe wait failed (%d)" % err)
            if result == _WAIT_TIMEOUT:
                w.CancelIoEx(self.handle, w.ct.byref(ov))
                w.GetOverlappedResult(self.handle, w.ct.byref(ov), w.ct.byref(n), True)
                if n.value:
                    return n.value
                raise socket.timeout("timed out")
        if not w.GetOverlappedResult(self.handle, w.ct.byref(ov), w.ct.byref(n), True):
            err = w.last_error()
            raise OSError(err, "pipe call failed (%d)" % err)
        return n.value

    def _new_overlapped(self) -> Any:
        w = self._w
        ov = w.OVERLAPPED()
        ov.hEvent = w.CreateEventW(None, True, False, None)
        if not ov.hEvent:
            raise w.ct.WinError(w.last_error())
        return ov

    def read(self, n: int, timeout: Optional[float]) -> bytes:
        w = self._w
        self._enter()
        ov = None
        try:
            ov = self._new_overlapped()
            buf = w.ct.create_string_buffer(n)
            started = bool(w.ReadFile(self.handle, buf, n, None, w.ct.byref(ov)))
            try:
                got = self._wait(ov, started, timeout)
            except OSError as e:
                if isinstance(e, socket.timeout):
                    raise
                if e.errno in _EOF_ERRORS or (e.errno == _ERROR_OPERATION_ABORTED and self._closing):
                    return b""
                raise
            return buf.raw[:got]
        finally:
            if ov is not None:
                w.CloseHandle(ov.hEvent)
            self._leave()

    def write(self, data: bytes) -> None:
        w = self._w
        view = memoryview(data)
        while len(view):
            self._enter()
            ov = None
            try:
                ov = self._new_overlapped()
                chunk = bytes(view[:BUFFER])
                started = bool(w.WriteFile(self.handle, chunk, len(chunk), None, w.ct.byref(ov)))
                sent = self._wait(ov, started, None)
            finally:
                if ov is not None:
                    w.CloseHandle(ov.hEvent)
                self._leave()
            view = view[sent:]

    def flush(self) -> None:
        self._enter()
        try:
            self._w.FlushFileBuffers(self.handle)
        finally:
            self._leave()

    def peer_pid(self, server_end: bool) -> int:
        w = self._w
        pid = w.wt.DWORD()
        fn = w.GetNamedPipeClientProcessId if server_end else w.GetNamedPipeServerProcessId
        self._enter()
        try:
            if not fn(self.handle, w.ct.byref(pid)):
                raise w.ct.WinError(w.last_error())
        finally:
            self._leave()
        return pid.value

    def peer_sid(self, server_end: bool) -> str:
        return self._w.sid_of_process(self.peer_pid(server_end))


def own_sid() -> str:  # pragma: no cover - needs Windows
    w = _Win32.get()
    return w.sid_of_process(w.ct.windll.kernel32.GetCurrentProcessId())


def connect_client(name: str, timeout: float) -> OverlappedPipe:  # pragma: no cover - needs Windows
    """Open the kernel's pipe. The server may only identify us, never act as us (SQOS identification)."""
    import time
    w = _Win32.get()
    GENERIC_RW = 0xC0000000
    OPEN_EXISTING = 3
    FILE_FLAG_OVERLAPPED = 0x40000000
    SECURITY_SQOS_PRESENT, SECURITY_IDENTIFICATION = 0x00100000, 0x00010000
    flags = FILE_FLAG_OVERLAPPED | SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION
    deadline = time.monotonic() + timeout
    while True:
        h = w.CreateFileW(name, GENERIC_RW, 0, None, OPEN_EXISTING, flags, None)
        if h not in (None, w.invalid):
            return OverlappedPipe(h)
        err = w.last_error()
        if err != _ERROR_PIPE_BUSY or time.monotonic() >= deadline:
            raise OSError(err, "cannot open %s (%d)" % (name, err))
        w.WaitNamedPipeW(name, max(1, int((deadline - time.monotonic()) * 1000)))


class PipeListener:  # pragma: no cover - needs Windows
    """Accepts connections on the kernel's pipe, one thread, one fresh instance per connection."""

    def __init__(self, name: str, deliver: Callable[[socket.socket], None]) -> None:
        self.name = name
        self._deliver = deliver
        self._sid = own_sid()
        self._w = _Win32.get()
        self._stop = threading.Event()
        self._current = None  # type: Optional[OverlappedPipe]
        self._current_lock = threading.Lock()
        self._thread = None  # type: Optional[threading.Thread]
        self._first = self._new_instance(first=True)  # raises if another process owns the name

    def _new_instance(self, first: bool = False) -> OverlappedPipe:
        w, ct = self._w, self._w.ct
        PIPE_ACCESS_DUPLEX, FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED = 3, 0x00080000, 0x40000000
        PIPE_TYPE_BYTE, PIPE_READMODE_BYTE, PIPE_WAIT, PIPE_REJECT_REMOTE_CLIENTS = 0, 0, 0, 8
        PIPE_UNLIMITED_INSTANCES = 255
        sd = ct.c_void_p()
        if not w.ConvertStringSecurityDescriptorToSecurityDescriptorW(
                dacl_sddl(self._sid), 1, ct.byref(sd), None):
            raise ct.WinError(w.last_error())
        try:
            sa = w.SECURITY_ATTRIBUTES(ct.sizeof(w.SECURITY_ATTRIBUTES), sd, False)
            h = w.CreateNamedPipeW(
                self.name,
                PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED | (FILE_FLAG_FIRST_PIPE_INSTANCE if first else 0),
                PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
                PIPE_UNLIMITED_INSTANCES, BUFFER, BUFFER, 0, ct.byref(sa))
        finally:
            w.LocalFree(sd)
        if h in (None, w.invalid):
            raise ct.WinError(w.last_error())
        return OverlappedPipe(h)

    def start(self) -> None:
        self._current = self._first
        self._thread = threading.Thread(target=self._loop, name="dkp-pipe-accept", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        with self._current_lock:
            self._stop.set()
            if self._current is not None:
                self._current.abort()
        if self._thread is not None:
            self._thread.join(5)

    def _connect(self, pipe: OverlappedPipe) -> bool:
        w = self._w
        entered = False
        ov = None
        try:
            pipe._enter()
            entered = True
            ov = pipe._new_overlapped()
            if w.ConnectNamedPipe(pipe.handle, w.ct.byref(ov)):
                return True
            err = w.last_error()
            if err == _ERROR_PIPE_CONNECTED:
                return True
            if err != _ERROR_IO_PENDING:
                return False
            pipe._wait(ov, False, None)
            return True
        except OSError:
            return False
        finally:
            if ov is not None:
                w.CloseHandle(ov.hEvent)
            if entered:
                pipe._leave()

    def _loop(self) -> None:
        while not self._stop.is_set():
            with self._current_lock:
                pipe = self._current
            if pipe is None:
                return
            connected = self._connect(pipe)
            if not connected:
                pipe.close()
            try:
                next_pipe = self._new_instance() if not self._stop.is_set() else None
            except OSError:
                next_pipe = None
            with self._current_lock:
                if self._stop.is_set() and next_pipe is not None:
                    next_pipe.close()
                    next_pipe = None
                self._current = next_pipe
            if connected:
                serve_connection(pipe, self._deliver, self._same_account)
            if next_pipe is None:
                return

    def _same_account(self, pipe: OverlappedPipe) -> bool:
        try:
            return pipe.peer_sid(server_end=True) == self._sid
        except OSError:
            return False  # cannot tell (e.g. access denied): refuse (FR-A1)


def same_server_account(pipe: "OverlappedPipe") -> bool:  # pragma: no cover - needs Windows
    """Client side of FR-A1: the process serving ``pipe`` runs as this account."""
    try:
        return pipe.peer_sid(server_end=False) == own_sid()
    except OSError:
        return False
