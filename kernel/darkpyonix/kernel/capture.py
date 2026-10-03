"""Output capture for the executor (SPEC FR-X5, NFR-K3, INTENT D12).

Standard library only; Python 3.8+.

- ``OutputRouter`` owns one daemon thread. Producers (the main thread, user threads, fd
  readers) only append to a deque; the router thread turns items into nbformat 4 outputs on
  the current ``CellRecord``, emits ``output`` / ``output.clear`` events, and calls
  ``store.update(run)``. Consecutive stream text for the same cell and stream name is
  coalesced for up to ``STREAM_COALESCE_SECONDS``; when the router falls behind, everything
  that piled up is merged into one event, so producers never block.
- ``StreamCapture(...)`` builds the replacement for ``sys.stdout`` / ``sys.stderr``.
- ``FdCapture`` points file descriptors 1 and 2 at pipes so that ``os.write(1, ...)``, C
  extensions and child processes are captured too.
"""
from __future__ import annotations

import codecs
import collections
import io
import os
import sys
import threading
import time
import uuid
from typing import Any, Callable, Dict, Optional

from darkpyonix.kernel.protocol import STREAM_COALESCE_SECONDS

_POLL = 0.02        # router poll (and stream flush) interval while a cell is active
_IDLE_POLL = 0.1    # flush interval for stream text written between cells


class OutputRouter(object):
    """Routes outputs of the active cell to its record and to ``output`` events."""

    def __init__(self, emit: Callable[[str, Dict[str, Any]], None], store: Any = None,
                 coalesce: float = STREAM_COALESCE_SECONDS) -> None:
        self._emit = emit
        self._store = store
        # Text has already waited up to one poll interval in the stream buffers.
        self._coalesce = max(0.0, coalesce - _POLL)
        self._q = collections.deque()       # producers append, the router thread pops
        self._wake = threading.Event()
        self._thread = None                  # type: Optional[threading.Thread]
        # True between begin_cell and end_cell; read by StreamCapture on every write.
        self.active = False
        # Where stream text goes when no cell is active: name -> callable(text).
        self._passthrough = {}               # type: Dict[str, Callable[[str], None]]
        # Buffered writers (StreamCapture) the router thread flushes periodically.
        self._flushables = []                # type: list

    def add_flushable(self, buffer: Any) -> None:
        self._flushables.append(buffer)

    def remove_flushable(self, buffer: Any) -> None:
        try:
            self._flushables.remove(buffer)
        except ValueError:
            pass

    def flush_streams(self) -> None:
        """Push buffered stream text into the router queue (any thread)."""
        for buf in list(self._flushables):
            try:
                buf.flush()
            except Exception:
                pass

    # ------------------------------------------------------------ lifecycle

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name="darkpyonix-output", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        if self._thread is None:
            return
        self._push(("stop",))
        self._thread.join(timeout)
        self._thread = None

    def set_passthrough(self, name: str, write: Optional[Callable[[str], None]]) -> None:
        if write is None:
            self._passthrough.pop(name, None)
        else:
            self._passthrough[name] = write

    # ------------------------------------------------------------ producers (any thread)

    def _push(self, item: tuple) -> None:
        self._q.append(item)
        self._wake.set()

    def write_stream(self, name: str, text: str) -> None:
        if text:
            self._q.append(("s", name, text))
            if not self.active:
                self._wake.set()

    def write_display(self, bundle: Dict[str, Any], kind: str = "display_data",
                      execution_count: Optional[int] = None,
                      metadata: Optional[Dict[str, Any]] = None, record: bool = True) -> None:
        output = {"output_type": kind, "data": dict(bundle), "metadata": dict(metadata or {})}
        if kind == "execute_result":
            output["execution_count"] = execution_count
        self.flush_streams()        # keep print() text written before this output ahead of it
        self._push(("o", output, record))

    def write_output(self, output: Dict[str, Any], record: bool = True) -> None:
        """Any nbformat 4 output (used for ``error``)."""
        self.flush_streams()
        self._push(("o", dict(output), record))

    def clear(self, wait: bool = False) -> None:
        self.flush_streams()
        self._push(("c", bool(wait)))

    def begin_cell(self, run: Any, cell: Any, doc_cell_id: Optional[str] = None) -> None:
        """Route outputs to ``cell`` of ``run``; events name it by ``index`` and by the shared
        document's ``doc_cell_id`` (PROTOCOL §3.4 ``cell_id``, ``None`` when unknown)."""
        self._push(("cell", run, cell, doc_cell_id))
        self.active = True

    def end_cell(self, timeout: float = 5.0) -> None:
        self.flush_streams()
        self.active = False
        self._push(("cell", None, None, None))
        self.flush(timeout)

    def flush(self, timeout: float = 5.0) -> bool:
        """Block until everything queued so far is recorded and emitted."""
        if self._thread is None or not self._thread.is_alive():
            return False
        done = threading.Event()
        self._push(("b", done))
        return done.wait(timeout)

    # ------------------------------------------------------------ router thread

    def _loop(self) -> None:
        q = self._q
        run = cell = None
        cell_id = None              # type: Optional[str]  # the shared document's id of ``cell``
        clear_wait = False
        pend_name = None            # type: Optional[str]
        pend = []                   # type: list
        pend_since = 0.0
        dirty = False
        stopping = False

        def flush_pending():
            nonlocal pend_name, pend, dirty, clear_wait
            if pend_name is None:
                return
            text = "".join(pend)
            name = pend_name
            pend_name, pend = None, []
            if cell is None:
                write = self._passthrough.get(name)
                if write is not None:
                    try:
                        write(text)
                    except Exception:
                        pass
                return
            outputs = cell.outputs
            if clear_wait:
                del outputs[:]
                clear_wait = False
            last = outputs[-1] if outputs else None
            if last is not None and last.get("output_type") == "stream" and last.get("name") == name:
                last["text"] = last["text"] + text
            else:
                outputs.append({"output_type": "stream", "name": name, "text": text})
            dirty = True
            self._safe_emit("output", {
                "run_id": run.run_id, "index": cell.index, "cell_id": cell_id,
                "output": {"output_type": "stream", "name": name, "text": text},
            })

        while not stopping:
            if not q:
                if pend_name is not None:
                    timeout = max(0.0, pend_since + self._coalesce - time.monotonic())
                    timeout = min(timeout, _POLL) if self.active else timeout
                elif self.active:
                    timeout = _POLL
                elif self._flushables:
                    timeout = _IDLE_POLL
                else:
                    timeout = None
                self._wake.wait(timeout)
                self._wake.clear()
            if self._flushables:
                self.flush_streams()
            while q:
                item = q.popleft()
                tag = item[0]
                if tag == "s":
                    name = item[1]
                    if pend_name != name:
                        flush_pending()
                        pend_name = name
                        pend_since = time.monotonic()
                    pend.append(item[2])
                    continue
                flush_pending()
                if tag == "o":
                    output, record = item[1], item[2]
                    if cell is None:
                        continue
                    if record:
                        if clear_wait:
                            del cell.outputs[:]
                            clear_wait = False
                        cell.outputs.append(output)
                        dirty = True
                    self._safe_emit("output", {"run_id": run.run_id, "index": cell.index,
                                               "cell_id": cell_id, "output": output})
                elif tag == "c":
                    if cell is None:
                        continue
                    if item[1]:
                        clear_wait = True
                    else:
                        del cell.outputs[:]
                        clear_wait = False
                        dirty = True
                    self._safe_emit("output.clear", {"run_id": run.run_id, "index": cell.index,
                                                     "cell_id": cell_id, "wait": item[1]})
                elif tag == "cell":
                    if dirty and run is not None:
                        self._update(run)
                        dirty = False
                    run, cell, cell_id = item[1], item[2], item[3]
                    clear_wait = False
                elif tag == "b":
                    if dirty and run is not None:
                        self._update(run)
                        dirty = False
                    item[1].set()
                elif tag == "stop":
                    stopping = True
                    break
            if pend_name is not None and (stopping or
                                          time.monotonic() - pend_since >= self._coalesce):
                flush_pending()
            if dirty and run is not None:
                self._update(run)
                dirty = False

    def _safe_emit(self, type_: str, data: Dict[str, Any]) -> None:
        try:
            self._emit(type_, data)
        except Exception:
            pass

    def _update(self, run: Any) -> None:
        if self._store is None:
            return
        try:
            self._store.update(run)
        except Exception:
            pass


class _RouterSink(io.RawIOBase):
    """The raw end under ``StreamCapture``: whole buffers of bytes, decoded once."""

    # A plain class attribute instead of IOBase's property: CPython's text and buffered
    # layers check ``raw.closed`` on every write unless the raw is a FileIO, and the
    # property call alone costs a third of a ``print`` (NFR-K3). The kernel's streams are
    # never closed.
    closed = False

    def __init__(self, name: str, router: OutputRouter, fallback: Any, fd: Optional[int]) -> None:
        super().__init__()
        self._name = name
        self._router = router
        self._fallback = fallback
        self._fd = fd
        self._decoder = codecs.getincrementaldecoder("utf-8")("replace")

    def writable(self) -> bool:
        return True

    def readable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def isatty(self) -> bool:
        return False

    def close(self) -> None:
        # Library code sometimes closes sys.stdout; keep the kernel's stream usable.
        pass

    def fileno(self) -> int:
        # With FdCapture the real descriptor is a captured pipe, so handing it out (for
        # example to subprocess) keeps the output in the cell.
        if self._fd is None:
            raise io.UnsupportedOperation("fileno")
        return self._fd

    def write(self, b) -> int:
        data = bytes(b)
        text = self._decoder.decode(data)
        if text:
            if self._router.active:
                self._router.write_stream(self._name, text)
            elif self._fallback is not None:
                try:
                    self._fallback.write(text)
                    self._fallback.flush()
                except Exception:
                    pass
        return len(data)


def StreamCapture(name: str, router: OutputRouter, fallback: Any, fd: Optional[int] = None,
                  buffer_size: int = 65536) -> io.TextIOWrapper:
    """A replacement for ``sys.stdout`` / ``sys.stderr`` that feeds ``router`` (FR-X5, NFR-K3).

    The result is a plain C-level ``io.TextIOWrapper`` configured like Python's own
    ``sys.stdout`` on a pipe (a subclass would lose CPython's fast paths), over a
    ``BufferedWriter`` whose raw end hands decoded text to the ``OutputRouter``. ``print``
    therefore costs what it costs on a pipe. The router thread calls ``flush()`` every
    ``_POLL`` seconds while a cell runs (the same as another thread doing
    ``print(..., flush=True)``) and the executor flushes at the end of every cell, so output
    still streams live. Text that reaches the raw end while no cell is active goes to
    ``fallback`` (the original stream). ``fileno()`` is ``fd`` (1 or 2 under ``FdCapture``)
    or raises ``io.UnsupportedOperation``; ``isatty()`` is False.
    """
    sink = _RouterSink(name, router, fallback, fd)
    stream = io.TextIOWrapper(io.BufferedWriter(sink, buffer_size), encoding="utf-8",
                              errors="replace", newline="\n", line_buffering=False,
                              write_through=False)
    router.add_flushable(stream)
    return stream


class FdCapture(object):
    """Redirect file descriptors 1 and 2 to pipes read by daemon threads (FR-X5).

    POSIX and Windows both have ``os.pipe``/``os.dup2``. On Windows only the C runtime
    descriptors are redirected: ``os.write(1, ...)`` and extensions using the same CRT are
    captured, child processes that inherit the Win32 standard handles are not.
    """

    def __init__(self, router: OutputRouter) -> None:
        self._router = router
        self._saved = {}        # type: Dict[int, int]
        self._readers = []      # type: list
        self._marker = b"\x00DPSYNC-" + uuid.uuid4().hex.encode("ascii") + b"\x00"
        self._cond = threading.Condition()
        self._seen = {1: 0, 2: 0}
        self._sent = {1: 0, 2: 0}
        self.started = False
        self._libc_fflush = None

    def start(self) -> None:
        if self.started:
            return
        for stream in (sys.__stdout__, sys.__stderr__, sys.stdout, sys.stderr):
            try:
                stream.flush()
            except Exception:
                pass
        for fd, name in ((1, "stdout"), (2, "stderr")):
            saved = os.dup(fd)
            r, w = os.pipe()
            os.dup2(w, fd)
            os.close(w)
            self._saved[fd] = saved
            self._router.set_passthrough(name, self._make_passthrough(saved))
            t = threading.Thread(target=self._reader, args=(r, fd, name),
                                 name="darkpyonix-fd%d" % fd, daemon=True)
            t.start()
            self._readers.append(t)
        try:
            import ctypes
            libc = ctypes.CDLL(None) if os.name != "nt" else None
            if libc is not None:
                self._libc_fflush = libc.fflush
        except Exception:
            self._libc_fflush = None
        self.started = True

    def original_fd(self, fd: int) -> int:
        return self._saved.get(fd, fd)

    def original_text(self, fd: int, encoding: str = "utf-8") -> Any:
        """A line-buffered text stream on the saved (real) descriptor."""
        raw = io.FileIO(self.original_fd(fd), "w", closefd=False)
        return io.TextIOWrapper(raw, encoding=encoding, errors="replace", line_buffering=True,
                                write_through=True)

    @staticmethod
    def _make_passthrough(fd: int) -> Callable[[str], None]:
        def write(text: str) -> None:
            data = text.encode("utf-8", "replace")
            while data:
                n = os.write(fd, data)
                data = data[n:]
        return write

    def _reader(self, rfd: int, fd: int, name: str) -> None:
        marker = self._marker
        mlen = len(marker)
        decoder = codecs.getincrementaldecoder("utf-8")("replace")
        router = self._router
        buf = b""
        while True:
            try:
                data = os.read(rfd, 65536)
            except InterruptedError:
                continue
            except OSError:
                break
            if not data:
                break
            buf += data
            hits = 0
            while True:
                i = buf.find(marker)
                if i < 0:
                    break
                if i:
                    router.write_stream(name, decoder.decode(buf[:i]))
                buf = buf[i + mlen:]
                hits += 1
            keep = 0
            if b"\x00" in buf[-mlen:]:
                for k in range(min(mlen - 1, len(buf)), 0, -1):
                    if marker.startswith(buf[-k:]):
                        keep = k
                        break
            out, buf = (buf[:len(buf) - keep], buf[len(buf) - keep:]) if keep else (buf, b"")
            if out:
                router.write_stream(name, decoder.decode(out))
            if hits:
                with self._cond:
                    self._seen[fd] += hits
                    self._cond.notify_all()
        tail = decoder.decode(buf, final=True)
        if tail:
            router.write_stream(name, tail)
        try:
            os.close(rfd)
        except OSError:
            pass

    def sync(self, timeout: float = 2.0) -> bool:
        """Wait until everything written to fds 1 and 2 so far has reached the router."""
        if not self.started:
            return True
        if self._libc_fflush is not None:
            try:
                self._libc_fflush(None)
            except Exception:
                pass
        for stream in (sys.__stdout__, sys.__stderr__):
            try:
                stream.flush()
            except Exception:
                pass
        targets = {}
        for fd in (1, 2):
            try:
                os.write(fd, self._marker)
            except OSError:
                continue
            with self._cond:
                self._sent[fd] += 1
                targets[fd] = self._sent[fd]
        deadline = time.monotonic() + timeout
        with self._cond:
            for fd, want in targets.items():
                while self._seen[fd] < want:
                    left = deadline - time.monotonic()
                    if left <= 0:
                        return False
                    self._cond.wait(left)
        return True

    def stop(self) -> None:
        if not self.started:
            return
        self.sync(1.0)
        for fd, saved in self._saved.items():
            try:
                os.dup2(saved, fd)
            except OSError:
                pass
        for t in self._readers:
            t.join(0.5)
        for fd, name in ((1, "stdout"), (2, "stderr")):
            self._router.set_passthrough(name, None)
        for saved in self._saved.values():
            try:
                os.close(saved)
            except OSError:
                pass
        self._saved.clear()
        self._readers = []
        self.started = False
