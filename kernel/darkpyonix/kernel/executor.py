"""The executor: runs notebook cells on the main thread (SPEC FR-K5..K8, FR-X1..X5, FR-F5).

Standard library only; Python 3.8+.

Threading model (INTENT D12):

- ``run_forever()`` runs on the main thread and executes one run at a time from a FIFO queue.
- ``submit``, ``cancel``, ``interrupt``, ``restart``, ``namespace``, ``status`` and
  ``shutdown`` are called from control threads.
- Outputs travel through an ``OutputRouter`` thread; the main thread only enqueues.

Interrupts are delivered as SIGINT to the main thread (``signal.pthread_kill`` on POSIX, so a
blocking ``time.sleep`` wakes up; ``_thread.interrupt_main`` elsewhere). The executor's SIGINT
handler raises ``KeyboardInterrupt`` only while user code runs; an interrupt that arrives
during bookkeeping is remembered for the run and stops it at the next cell boundary.
"""
from __future__ import annotations

import __future__ as _future
import _thread
import ast
import builtins
import collections
import linecache
import os
import reprlib
import signal
import sys
import threading
import time
import types
from typing import Any, Callable, Dict, List, Optional, Tuple

from darkpyonix import format as fmt
from darkpyonix.kernel import hostctx
from darkpyonix.kernel.capture import FdCapture, OutputRouter, StreamCapture
from darkpyonix.kernel.display import format_bundle
from darkpyonix.kernel.model import CellRecord, Run, RunRequest
from darkpyonix.kernel.protocol import DKPError, new_run_id, now_iso

_KERNEL_DIR = os.path.dirname(os.path.abspath(__file__))
_FUTURE_MASK = 0
for _name in _future.all_feature_names:
    _FUTURE_MASK |= getattr(_future, _name).compiler_flag

# Names the kernel puts into the namespace; never listed by ``namespace``.
INJECTED_NAMES = frozenset(("__runs__", "darkpyonix", "__builtins__", "__name__", "__file__",
                            "__doc__", "__package__", "__spec__", "__loader__"))

_REPR = reprlib.Repr()
_REPR.maxstring = 200
_REPR.maxother = 200
REPR_LIMIT = 200


def _type_name(obj: Any) -> str:
    t = type(obj)
    module = getattr(t, "__module__", None)
    qual = getattr(t, "__qualname__", None) or t.__name__
    if module in (None, "builtins", "__main__"):
        return qual
    return "%s.%s" % (module, qual)


class Executor(object):
    """Executes runs of one notebook file in one namespace."""

    def __init__(self, path: str, kernel_id: str, emit: Callable[[str, Dict[str, Any]], None],
                 store: Any, python_info: Dict[str, Any], host: str,
                 capture_fds: bool = True) -> None:
        self.path = os.path.abspath(path)
        self.kernel_id = kernel_id
        self.store = store
        self.python_info = python_info
        self.host = host
        self.hard_restart_requested = False
        self._emit_lock = threading.Lock()
        self._emit_fn = emit
        self._capture_fds = capture_fds

        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self._pending = collections.deque()     # RunRequest, FIFO (FR-X3)
        self._current_req = None                # type: Optional[RunRequest]
        self._current = None                    # type: Optional[Run]
        self._stopping = False
        self._reset_requested = False
        self._interrupt_target = None           # type: Optional[str]
        self._in_user = False                   # True only while user code runs

        self.execution_count = 0
        self._preamble_done = False
        self._future_flags = 0
        self._ns = self._new_namespace()
        self._main_module = None                # type: Optional[types.ModuleType]
        self._saved_main = None                 # type: Optional[types.ModuleType]
        self._exit_requested = False

        self.router = OutputRouter(self.emit, store)
        self.fdcapture = None                   # type: Optional[FdCapture]
        self._saved_streams = None              # type: Optional[Tuple[Any, Any]]
        self._captures = []                     # type: List[Any]
        self._saved_sigint = None
        # The real stderr while capture is installed (for kernel diagnostics).
        self.original_stderr = sys.stderr

        script_dir = os.path.dirname(self.path)
        if script_dir not in sys.path:
            sys.path.insert(0, script_dir)

    # ================================================================ events

    def emit(self, type_: str, data: Dict[str, Any]) -> None:
        with self._emit_lock:
            try:
                self._emit_fn(type_, data)
            except Exception:
                pass

    # ================================================================ control API (any thread)

    def submit(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """PROTOCOL §3.3 ``run``."""
        req = self._parse_request(params)
        with self._lock:
            if self._stopping:
                raise DKPError("shutting_down", "kernel is shutting down")
            busy = self._current_req is not None or bool(self._pending)
            if busy and req.on_busy == "reject":
                raise DKPError("busy", "a run is in progress", data={
                    "current": self._current_summary(),
                    "queue_length": len(self._pending),
                })
            if busy:
                self._pending.append(req)
                position = len(self._pending)
            else:
                self._current_req = req     # picked up by run_forever right away
            self._cond.notify_all()
        if busy:
            self.emit("run.queued", {"run_id": req.run_id, "position": position})
            return {"run_id": req.run_id, "state": "queued", "position": position}
        return {"run_id": req.run_id, "state": "running"}

    def _parse_request(self, params: Any) -> RunRequest:
        if params is None:
            params = {}
        if not isinstance(params, dict):
            raise DKPError("bad_request", "params must be an object")
        mode = params.get("mode", "all")
        if mode not in ("all", "cells"):
            raise DKPError("bad_request", "mode must be 'all' or 'cells'")
        cells = params.get("cells")
        if mode == "cells":
            if not isinstance(cells, list) or not cells or not all(
                    isinstance(c, int) and not isinstance(c, bool) and c >= 0 for c in cells):
                raise DKPError("bad_request", "mode 'cells' needs a non-empty list of cell indexes")
        else:
            cells = None
        source = params.get("source")
        if source is not None and not isinstance(source, str):
            raise DKPError("bad_request", "source must be a string")
        run_params = params.get("params")
        if run_params is None:
            run_params = {}
        if not isinstance(run_params, dict) or not all(isinstance(k, str) for k in run_params):
            raise DKPError("bad_request", "params.params must be an object")
        on_busy = params.get("on_busy", "reject")
        if on_busy not in ("reject", "queue"):
            raise DKPError("bad_request", "on_busy must be 'reject' or 'queue'")
        if cells:
            self._check_cell_indexes(cells, source)
        return RunRequest(new_run_id(), mode=mode, cells=cells, source=source,
                          params=run_params, on_busy=on_busy)

    def _check_cell_indexes(self, cells: List[int], source: Optional[str]) -> None:
        try:
            doc = self._load_document(source)[0]
        except Exception:
            return      # unreadable now; the run itself reports the failure
        bad = [c for c in cells if c >= len(doc.cells)]
        if bad:
            raise DKPError("bad_request", "no such cell: %s (the file has %d cells)"
                           % (", ".join(str(c) for c in bad), len(doc.cells)))

    def _current_summary(self) -> Optional[Dict[str, Any]]:
        if self._current is not None:
            return self._current.summary()
        req = self._current_req
        if req is None:
            return None
        return {"run_id": req.run_id, "status": "running", "mode": req.mode,
                "cells": list(req.cells),
                "params": dict(req.params), "started_at": None, "ended_at": None}

    def cancel(self, run_id: str) -> Dict[str, Any]:
        """Cancel a queued run (a running run is stopped with ``interrupt``)."""
        with self._lock:
            for req in list(self._pending):
                if req.run_id == run_id:
                    self._pending.remove(req)
                    break
            else:
                return {"cancelled": False}
        self.emit("run.finished", {"run_id": run_id, "status": "cancelled", "duration": 0.0})
        return {"cancelled": True}

    def interrupt(self) -> Dict[str, Any]:
        """Raise ``KeyboardInterrupt`` in the running cell (FR-X4). The queue is kept."""
        with self._lock:
            req = self._current_req
            if req is None:
                return {"interrupted": False}
            self._interrupt_locked(req.run_id)
            return {"interrupted": True, "run_id": req.run_id}

    def _interrupt_locked(self, run_id: str) -> None:
        self._interrupt_target = run_id
        if self._in_user:
            self._send_sigint()

    @staticmethod
    def _send_sigint() -> None:
        if hasattr(signal, "pthread_kill"):
            try:
                signal.pthread_kill(threading.main_thread().ident, signal.SIGINT)
                return
            except (OSError, ValueError):
                pass
        _thread.interrupt_main()

    def restart(self, hard: bool = False) -> Dict[str, Any]:
        """Soft: fresh namespace, execution count 0 (FR-K7). Hard: stop the loop so that the
        kernel main can re-exec the process; ``hard_restart_requested`` is then True."""
        if hard:
            self.hard_restart_requested = True
            self.shutdown()
            return {"restarted": True}
        with self._lock:
            self._reset_requested = True
            if self._current_req is not None:
                # Applied by the main thread once the interrupted run has finished.
                self._interrupt_locked(self._current_req.run_id)
            else:
                self._apply_reset_locked()
            self._cond.notify_all()
        return {"restarted": True}

    def shutdown(self) -> Dict[str, Any]:
        """Interrupt the running cell, let its run finish as interrupted, end ``run_forever``."""
        with self._lock:
            self._stopping = True
            if self._current_req is not None:
                self._interrupt_locked(self._current_req.run_id)
            self._cond.notify_all()
        return {"shutting_down": True}

    def status(self) -> Dict[str, Any]:
        with self._lock:
            if self._stopping:
                state = "stopping"
            elif self._current_req is not None or self._pending:
                state = "busy"
            else:
                state = "idle"
            return {
                "status": state,
                "run_id": self._current_req.run_id if self._current_req is not None else None,
                "queue": [r.run_id for r in self._pending],
                "execution_count": self.execution_count,
            }

    def namespace(self, limit: int = 200) -> Dict[str, Any]:
        """PROTOCOL §3.6. While busy only ``name`` and ``type``: no user code runs here."""
        with self._lock:
            busy = self._current_req is not None
            ns = self._ns
        items = None
        for _ in range(5):
            try:
                items = list(ns.copy().items())
                break
            except RuntimeError:
                continue
        variables = []  # type: List[Dict[str, Any]]
        for name, value in items or ():
            if len(variables) >= limit:
                break
            if not isinstance(name, str) or name.startswith("_") or name in INJECTED_NAMES:
                continue
            if isinstance(value, types.ModuleType):
                continue
            var = {"name": name, "type": _type_name(value), "repr": None,
                   "shape": None, "dtype": None, "len": None}
            if not busy:
                self._describe(value, var)
            variables.append(var)
        return {"variables": variables}

    @staticmethod
    def _describe(value: Any, var: Dict[str, Any]) -> None:
        try:
            text = _REPR.repr(value)
        except Exception as exc:
            text = "<repr failed: %s>" % type(exc).__name__
        if len(text) > REPR_LIMIT:
            text = text[:REPR_LIMIT - 3] + "..."
        var["repr"] = text
        if isinstance(value, type):
            return
        try:
            shape = getattr(value, "shape", None)
            if shape is not None and not callable(shape):
                var["shape"] = [int(x) for x in shape]
        except Exception:
            pass
        try:
            dtype = getattr(value, "dtype", None)
            if dtype is not None and not callable(dtype):
                var["dtype"] = str(dtype)
        except Exception:
            pass
        try:
            if hasattr(type(value), "__len__"):
                var["len"] = int(len(value))
        except Exception:
            pass

    # ================================================================ namespace

    def _new_namespace(self) -> Dict[str, Any]:
        ns = {
            "__name__": "__main__", "__file__": self.path, "__doc__": None,
            "__package__": None, "__spec__": None, "__loader__": None,
            "__builtins__": builtins,
        }  # type: Dict[str, Any]
        try:
            ns["__runs__"] = self.store.magic()
        except Exception:
            ns["__runs__"] = None
        return ns

    def _apply_reset_locked(self) -> None:
        if not self._reset_requested:
            return
        self._reset_requested = False
        self._ns = self._new_namespace()
        self.execution_count = 0
        self._preamble_done = False
        self._future_flags = 0
        if self._main_module is not None:
            self._bind_main_module()

    def _bind_main_module(self) -> None:
        mod = types.ModuleType("__main__")
        mod.__dict__.clear()
        mod.__dict__.update(self._ns)
        self._ns = mod.__dict__
        self._main_module = mod
        sys.modules["__main__"] = mod

    # ================================================================ main loop

    def run_forever(self) -> None:
        """Execute runs until ``shutdown()``. Must be called on the main thread."""
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError("Executor.run_forever must run on the main thread")
        self._install()
        try:
            self.emit("kernel.status", {"status": "idle"})
            while True:
                with self._cond:
                    while (self._current_req is None and not self._pending
                           and not self._stopping):
                        self._apply_reset_locked()
                        self._cond.wait()
                    if self._stopping:
                        break
                    self._apply_reset_locked()
                    if self._current_req is None:
                        self._current_req = self._pending.popleft()
                    req = self._current_req
                    self._interrupt_target = None
                try:
                    self._execute(req)
                except KeyboardInterrupt:
                    pass
                except Exception as exc:
                    self._log("executor error in run %s: %r" % (req.run_id, exc))
                finally:
                    with self._lock:
                        self._current_req = None
                        self._current = None
                        self._apply_reset_locked()
                        idle = not self._pending and not self._stopping
                    if idle:
                        self.emit("kernel.status", {"status": "idle"})
        finally:
            with self._lock:
                dropped = list(self._pending)
                if self._current_req is not None:     # submitted but never started
                    dropped.insert(0, self._current_req)
                    self._current_req = None
                self._pending.clear()
            for req in dropped:
                self.emit("run.finished", {"run_id": req.run_id, "status": "cancelled",
                                           "duration": 0.0})
            self.emit("kernel.status", {"status": "stopping"})
            self._uninstall()

    def _install(self) -> None:
        self._saved_main = sys.modules.get("__main__")
        self._main_module = types.ModuleType("__main__")
        self._bind_main_module()
        self.router.start()
        out_fallback, err_fallback = sys.stdout, sys.stderr
        out_fd = err_fd = None
        if self._capture_fds:
            try:
                fdc = FdCapture(self.router)
                fdc.start()
                self.fdcapture = fdc
                out_fallback = fdc.original_text(1, getattr(sys.stdout, "encoding", None) or "utf-8")
                err_fallback = fdc.original_text(2, getattr(sys.stderr, "encoding", None) or "utf-8")
                out_fd, err_fd = 1, 2
            except OSError:
                self.fdcapture = None
        self.original_stderr = err_fallback
        self._saved_streams = (sys.stdout, sys.stderr)
        self._captures = [StreamCapture("stdout", self.router, out_fallback, out_fd),
                          StreamCapture("stderr", self.router, err_fallback, err_fd)]
        sys.stdout, sys.stderr = self._captures
        try:
            self._saved_sigint = signal.signal(signal.SIGINT, self._on_sigint)
        except (ValueError, OSError):
            self._saved_sigint = None
        hostctx._install(self._display_hook)

    def _uninstall(self) -> None:
        hostctx._uninstall()
        if self._saved_sigint is not None:
            try:
                signal.signal(signal.SIGINT, self._saved_sigint)
            except (ValueError, OSError):
                pass
            self._saved_sigint = None
        if self._saved_streams is not None:
            for stream in self._captures:
                self.router.remove_flushable(stream)
                try:
                    # detach() flushes; the buffer is then closed exactly once when collected.
                    stream.detach()
                except Exception:
                    pass
            self._captures = []
            sys.stdout, sys.stderr = self._saved_streams
            self._saved_streams = None
        if self.fdcapture is not None:
            self.fdcapture.stop()
            self.fdcapture = None
        self.original_stderr = sys.stderr
        self.router.stop()
        if self._saved_main is not None:
            sys.modules["__main__"] = self._saved_main

    def _on_sigint(self, signum, frame) -> None:
        if self._in_user:
            raise KeyboardInterrupt
        # Outside user code: the run is already marked by interrupt(); never raise into
        # the executor's own bookkeeping.

    def _display_hook(self, data: Dict[str, Any], metadata: Dict[str, Any], silent: bool) -> None:
        self.router.write_display(data, "display_data", metadata=metadata, record=not silent)

    def _log(self, text: str) -> None:
        try:
            self.original_stderr.write("darkpyonix: %s\n" % text)
            self.original_stderr.flush()
        except Exception:
            pass

    # ================================================================ one run

    def _read_source(self, source: Optional[str]) -> str:
        if source is not None:
            return source
        with open(self.path, "rb") as f:
            raw = f.read()
        return raw.decode("utf-8-sig")

    def _load_document(self, source: Optional[str]):
        text = self._read_source(source)
        return fmt.parse(text, self.path), text

    def _execute(self, req: RunRequest) -> None:
        t0 = time.monotonic()
        doc = text = None
        load_error = None
        try:
            doc, text = self._load_document(req.source)
        except Exception as exc:
            load_error = exc
        run = Run(req, self.kernel_id, self.path, doc.file_sha256 if doc else "",
                  self.python_info, self.host)
        run.status = "running"
        run.started_at = now_iso()
        hostctx._set_params(req.params)
        if doc is not None:
            if req.mode == "all":
                indexes = list(range(len(doc.cells)))
            else:
                indexes = list(req.cells)
                if not self._preamble_done and 0 not in indexes:
                    indexes.insert(0, 0)
            if doc.cells and not doc.cells[0].source.strip():
                # An empty preamble is not a cell anyone wants to see in the log.
                indexes = [i for i in indexes if i != 0]
                self._preamble_done = True
        else:
            indexes = []
        with self._lock:
            self._current = run
        self.emit("kernel.status", {"status": "busy", "run_id": run.run_id})
        self._store_call("begin", run)
        self.emit("run.started", {"run_id": run.run_id, "mode": run.mode, "cells": indexes,
                                  "params": dict(run.params)})
        status = "ok"
        if load_error is not None:
            status = "error"
            self._log("cannot load %s: %r" % (self.path, load_error))
        else:
            linecache.cache[self.path] = (len(text), None, text.splitlines(True), self.path)
            lines = self._cell_lines(doc, text)
            for idx in indexes:
                if self._interrupt_target == run.run_id:
                    status = "interrupted"
                    break
                if idx >= len(doc.cells):
                    status = "error"
                    self._log("run %s: no cell %d" % (run.run_id, idx))
                    break
                cstatus = self._run_cell(run, doc.cells[idx], lines.get(idx))
                if cstatus != "ok":
                    status = cstatus
                    break
                if self._exit_requested:
                    break
        self._exit_requested = False
        run.status = status
        run.ended_at = now_iso()
        self.router.flush()
        self._store_call("finish", run)
        self.emit("run.finished", {"run_id": run.run_id, "status": status,
                                   "duration": round(time.monotonic() - t0, 6)})

    def _store_call(self, method: str, run: Run) -> None:
        try:
            getattr(self.store, method)(run)
        except Exception as exc:
            self._log("run store %s failed: %r" % (method, exc))

    @staticmethod
    def _cell_lines(doc: Any, text: str) -> Dict[int, int]:
        """0-based line offset of each cell's source within ``text`` (for tracebacks)."""
        out = {}
        pos = 0
        for cell in doc.cells:
            src = cell.source
            if not src:
                continue
            i = text.find(src, pos)
            if i < 0:
                continue
            out[cell.index] = text.count("\n", 0, i)
            pos = i + len(src)
        return out

    def _run_cell(self, run: Run, cell: Any, line_offset: Optional[int]) -> str:
        rec = CellRecord(cell.index, cell.type, cell.source, cell.source_sha256,
                         title=cell.title, cell_id=cell.id, metadata=cell.metadata)
        self.execution_count += 1
        count = self.execution_count
        rec.execution_count = count
        rec.started_at = now_iso()
        run.cells.append(rec)
        if cell.index == 0:
            self._preamble_done = True
        self.emit("cell.started", {"run_id": run.run_id, "index": cell.index,
                                   "execution_count": count})
        self.router.begin_cell(run, rec)
        t0 = time.monotonic()
        status, error = self._exec_cell(run, cell, line_offset, count)
        if error is not None:
            self.router.write_output(error)
        if self.fdcapture is not None:
            self.fdcapture.sync()
        self.router.end_cell()
        rec.status = status
        rec.ended_at = now_iso()
        self.emit("cell.finished", {"run_id": run.run_id, "index": cell.index,
                                    "status": status, "duration": round(time.monotonic() - t0, 6)})
        self._store_call("update", run)
        return status

    def _compile(self, source: str, filename: str, line_offset: int):
        tree = compile(source, filename, "exec", ast.PyCF_ONLY_AST | self._future_flags, True)
        if line_offset:
            ast.increment_lineno(tree, line_offset)
        expr = None
        if tree.body and isinstance(tree.body[-1], ast.Expr):
            last = tree.body.pop()
            expr = ast.Expression(body=last.value)
        body_code = compile(tree, filename, "exec", self._future_flags, True)
        self._future_flags |= body_code.co_flags & _FUTURE_MASK
        expr_code = None
        if expr is not None:
            expr_code = compile(expr, filename, "eval", self._future_flags, True)
        return body_code, expr_code

    def _exec_cell(self, run: Run, cell: Any, line_offset: Optional[int],
                   count: int) -> Tuple[str, Optional[Dict[str, Any]]]:
        if line_offset is None:
            filename, line_offset = "<cell %d>" % cell.index, 0
        else:
            filename = self.path
        try:
            body_code, expr_code = self._compile(cell.source, filename, line_offset)
        except (SyntaxError, ValueError, OverflowError) as exc:
            return "error", self._error_output(type(exc), exc, None)
        ns = self._ns
        try:
            try:
                self._in_user = True
                if self._interrupt_target == run.run_id:
                    raise KeyboardInterrupt
                exec(body_code, ns)
                if expr_code is not None:
                    value = eval(expr_code, ns)
                    if value is not None:
                        ns["_"] = value
                        builtins._ = value  # type: ignore[attr-defined]
                        data, metadata = format_bundle(value)
                        self.router.write_display(data, "execute_result", count, metadata)
            finally:
                self._in_user = False
        except KeyboardInterrupt:
            return "interrupted", None
        except SystemExit as exc:
            self._exit_requested = True
            if exc.code is None or exc.code == 0:
                return "ok", None
            return "error", self._error_output(*sys.exc_info())
        except BaseException:
            return "error", self._error_output(*sys.exc_info())
        return "ok", None

    def _error_output(self, etype, value, tb) -> Dict[str, Any]:
        """nbformat ``error`` output with the executor's own frames removed."""
        import traceback
        while tb is not None and os.path.dirname(
                os.path.abspath(tb.tb_frame.f_code.co_filename)) == _KERNEL_DIR:
            tb = tb.tb_next
        try:
            value.__traceback__ = tb
        except Exception:
            pass
        sys.last_type, sys.last_value, sys.last_traceback = etype, value, tb
        try:
            lines = traceback.format_exception(etype, value, tb)
        except Exception:
            lines = ["%s\n" % etype.__name__]
        try:
            evalue = str(value)
        except Exception:
            evalue = "<exception str() failed>"
        return {"output_type": "error", "ename": etype.__name__, "evalue": evalue,
                "traceback": lines}
