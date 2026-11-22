"""Concurrent execution for ``[parallel]``..``[concurrent]`` cells (SPEC FR-X8, FR-X10, FR-X11).

Standard library only; Python 3.8+. Awaitables and coroutine functions run on a dedicated
event loop on the calling (main) thread; regular callables run in a ``ThreadPoolExecutor``
with a copied ``contextvars.Context``. Inside a DarkPyonix kernel, an optional hook on
``darkpyonix._hostctx`` supplies per-cell ``ContextVar`` routing and completion callbacks.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import inspect
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

from darkpyonix import _hostctx


class ParallelError(Exception):
    """Raised when one or more items passed to ``run_parallel`` fail (SPEC FR-X10).

    ``errors`` holds ``(index, exception)`` tuples ordered by argument position.
    """

    def __init__(self, errors: Sequence[Tuple[int, BaseException]]) -> None:
        self.errors = list(errors)  # type: List[Tuple[int, BaseException]]
        if not self.errors:
            message = "parallel execution failed"
        else:
            parts = []
            for idx, exc in self.errors:
                detail = str(exc)
                if detail:
                    parts.append("[%d] %s: %s" % (idx, type(exc).__name__, detail))
                else:
                    parts.append("[%d] %s" % (idx, type(exc).__name__))
            noun = "item" if len(self.errors) == 1 else "items"
            message = "%d parallel %s failed: %s" % (len(self.errors), noun, ", ".join(parts))
        super().__init__(message)


class ParallelHook(object):
    """Optional kernel hook interface for per-cell routing and completion (SPEC FR-X9..X11).

    When running inside a kernel, ``darkpyonix._hostctx.get_parallel_hook()`` may return an
    object implementing these methods. Outside a kernel no hook is active.
    """

    def cell_of(self, item: Any) -> Optional[int]:
        """Return the member cell index associated with ``item``, or ``None``."""
        return None

    def enter_cell(self, cell_index: int) -> Any:
        """Bind the current ``contextvars.Context`` to ``cell_index``."""
        return None

    def item_finished(self, cell_index: int, exc_info: Optional[Tuple[Any, Any, Any]]) -> None:
        """Notify the kernel on the main thread that one item for ``cell_index`` finished."""
        return None


def _get_hook() -> Any:
    getter = getattr(_hostctx, "get_parallel_hook", None)
    if getter is None:
        return None
    return getter()


def _close_coroutines(items: Sequence[Any]) -> None:
    for item in items:
        if inspect.iscoroutine(item):
            item.close()


def _is_coroutine_callable(item: Any) -> bool:
    if inspect.iscoroutinefunction(item):
        return True
    if callable(item) and not inspect.isclass(item):
        call = getattr(item, "__call__", None)
        if call is not None and inspect.iscoroutinefunction(call):
            return True
    return False


def _interrupt_thread_ids(tids: Sequence[int]) -> bool:
    """Best-effort async ``KeyboardInterrupt`` injection via ``PyThreadState_SetAsyncExc``.

    Returns ``True`` if ``ctypes`` is available, ``False`` otherwise (after writing a
    warning to ``sys.stderr``).
    """
    try:
        import ctypes
        set_async_exc = ctypes.pythonapi.PyThreadState_SetAsyncExc
    except (ImportError, AttributeError, OSError):
        sys.stderr.write(
            "darkpyonix: ctypes is unavailable; cannot interrupt running worker threads\n"
        )
        return False

    for tid in tids:
        try:
            res = set_async_exc(ctypes.c_ulong(tid), ctypes.py_object(KeyboardInterrupt))
            if res > 1:
                set_async_exc(ctypes.c_ulong(tid), ctypes.c_void_p(0))
        except (OSError, ValueError):
            pass
    return True


def _stop_threads(active_tids: Set[int], thread_lock: threading.Lock,
                  stop_requested: threading.Event, signaled: Set[int],
                  grace: float = 1.0) -> None:
    """Signal running worker threads with ``KeyboardInterrupt`` and wait up to ``grace`` s."""
    stop_requested.set()
    deadline = time.monotonic() + grace
    ctypes_unavailable = False
    while True:
        with thread_lock:
            tids = list(active_tids)
        if not tids:
            return
        if not ctypes_unavailable:
            new_tids = [t for t in tids if t not in signaled]
            if new_tids:
                if not _interrupt_thread_ids(new_tids):
                    ctypes_unavailable = True
                else:
                    signaled.update(new_tids)
        if time.monotonic() >= deadline:
            return
        time.sleep(0.01)


def run_parallel(*items: Any, fail_fast: bool = False,
                 max_threads: Optional[int] = None) -> List[Any]:
    """Run awaitables and callables concurrently and return results in argument order.

    Awaitables and uncalled coroutine functions run on a fresh event loop on the calling
    thread (SPEC FR-K5, FR-X8). Other callables run in a ``ThreadPoolExecutor`` with up to
    ``max_threads`` workers (defaulting to the number of callable items). Any other value
    raises ``TypeError`` before any item starts.
    """
    if max_threads is not None:
        if isinstance(max_threads, bool) or not isinstance(max_threads, int) or max_threads <= 0:
            _close_coroutines(items)
            raise ValueError(
                "max_threads must be a positive integer or None, got %r" % (max_threads,)
            )

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        _close_coroutines(items)
        raise RuntimeError(
            "darkpyonix.run_parallel() cannot run inside a running event loop; "
            "use 'await asyncio.gather(...)' there instead"
        )

    kinds = []  # type: List[str]
    for idx, item in enumerate(items):
        if inspect.isawaitable(item):
            kinds.append("awaitable")
        elif _is_coroutine_callable(item):
            kinds.append("corofunc")
        elif callable(item):
            kinds.append("callable")
        else:
            _close_coroutines(items)
            raise TypeError(
                "run_parallel() item at index %d must be awaitable or callable, got %s"
                % (idx, type(item).__name__)
            )

    if not items:
        return []

    hook = _get_hook()
    cell_indices = [
        hook.cell_of(item) if hook is not None else None for item in items
    ]  # type: List[Optional[int]]

    num_callables = sum(1 for k in kinds if k == "callable")
    if num_callables > 0:
        workers = num_callables if max_threads is None else min(max_threads, num_callables)
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=workers
        )  # type: Optional[concurrent.futures.ThreadPoolExecutor]
    else:
        executor = None

    n = len(items)
    results = [None] * n  # type: List[Any]
    errors = {}  # type: Dict[int, BaseException]
    started = [False] * n
    finished_hooks = [False] * n

    thread_lock = threading.Lock()
    active_tids = set()  # type: Set[int]
    signaled_tids = set()  # type: Set[int]
    stop_requested = threading.Event()
    fail_fast_triggered = False
    interrupted = False

    def _notify_finished(idx: int, exc_info: Optional[Tuple[Any, Any, Any]]) -> None:
        if finished_hooks[idx]:
            return
        finished_hooks[idx] = True
        cell_index = cell_indices[idx]
        if hook is not None and cell_index is not None:
            hook.item_finished(cell_index, exc_info)

    def _run_callable(ctx: contextvars.Context, fn: Callable[[], Any]) -> Any:
        if stop_requested.is_set():
            raise KeyboardInterrupt()
        tid = threading.get_ident()
        with thread_lock:
            if stop_requested.is_set():
                raise KeyboardInterrupt()
            active_tids.add(tid)
        try:
            return ctx.run(fn)
        finally:
            with thread_lock:
                active_tids.discard(tid)

    loop = asyncio.new_event_loop()
    tasks = []  # type: List[asyncio.Task]

    def _trigger_fail_fast(failed_idx: int) -> None:
        nonlocal fail_fast_triggered
        if fail_fast_triggered:
            return
        fail_fast_triggered = True
        stop_requested.set()
        for j, task in enumerate(tasks):
            if j != failed_idx and not task.done():
                task.cancel()
        with thread_lock:
            tids = [t for t in active_tids if t not in signaled_tids]
        if tids and _interrupt_thread_ids(tids):
            signaled_tids.update(tids)

    async def _run_one(idx: int, kind: str, item: Any, ctx: contextvars.Context) -> None:
        try:
            if kind == "awaitable":
                started[idx] = True
                val = await item
            elif kind == "corofunc":
                started[idx] = True
                coro = item()
                val = await coro
            else:
                started[idx] = True
                val = await loop.run_in_executor(executor, _run_callable, ctx, item)
        except asyncio.CancelledError:
            _notify_finished(idx, sys.exc_info())
            raise
        except KeyboardInterrupt:
            if stop_requested.is_set():
                _notify_finished(idx, sys.exc_info())
                return
            _notify_finished(idx, sys.exc_info())
            raise
        except BaseException as exc:
            errors[idx] = exc
            _notify_finished(idx, sys.exc_info())
            if fail_fast:
                _trigger_fail_fast(idx)
        else:
            results[idx] = val
            _notify_finished(idx, None)

    async def _run_all() -> None:
        for idx, (kind, item) in enumerate(zip(kinds, items)):
            ctx = contextvars.copy_context()
            cell_index = cell_indices[idx]
            if hook is not None and cell_index is not None:
                ctx.run(hook.enter_cell, cell_index)
            task = ctx.run(loop.create_task, _run_one(idx, kind, item, ctx))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)

    try:
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_run_all())
        except KeyboardInterrupt:
            interrupted = True
            kb_exc_info = sys.exc_info()
            stop_requested.set()

            all_pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
            for task in all_pending:
                task.cancel()
            with thread_lock:
                tids = list(active_tids)
            if tids:
                _interrupt_thread_ids(tids)

            if all_pending:
                try:
                    loop.run_until_complete(asyncio.gather(*all_pending, return_exceptions=True))
                except KeyboardInterrupt:
                    pass

            _stop_threads(active_tids, thread_lock, stop_requested, signaled_tids, grace=1.0)
            for idx in range(n):
                _notify_finished(idx, kb_exc_info)
            raise

        if fail_fast_triggered:
            _stop_threads(active_tids, thread_lock, stop_requested, signaled_tids, grace=1.0)
            cancel_exc = asyncio.CancelledError()
            cancel_info = (asyncio.CancelledError, cancel_exc, None)
            for idx in range(n):
                _notify_finished(idx, cancel_info)
    finally:
        for idx, item in enumerate(items):
            if not started[idx] and inspect.iscoroutine(item):
                item.close()
        if executor is not None:
            executor.shutdown(wait=not (interrupted or fail_fast_triggered))
        try:
            remaining = [t for t in asyncio.all_tasks(loop) if not t.done()]
            for task in remaining:
                task.cancel()
            if remaining:
                loop.run_until_complete(asyncio.gather(*remaining, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
        except BaseException:
            pass
        finally:
            asyncio.set_event_loop(None)
            loop.close()

    if errors:
        ordered_errors = [(idx, errors[idx]) for idx in sorted(errors)]
        raise ParallelError(ordered_errors)

    return results
