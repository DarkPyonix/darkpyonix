"""FR-X8, FR-X10 (plain-Python), and FR-X11 (plain-Python): ``darkpyonix.run_parallel``."""
from __future__ import annotations

import asyncio
import contextvars
import gc
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
import warnings

import pytest

from conftest import SRC_ROOT
from darkpyonix import ParallelError, run_parallel
from darkpyonix import _hostctx as hostctx
from darkpyonix._parallel import ParallelHook


def _env(**extra):
    env = dict(os.environ)
    env["PYTHONPATH"] = SRC_ROOT
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env.update(extra)
    return env


def _write(scratch, name, text):
    path = os.path.join(scratch, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(text).lstrip("\n"))
    return path


def _run(python, path, *args, **kw):
    return subprocess.run(
        [python, path] + list(args),
        capture_output=True,
        text=True,
        env=_env(),
        cwd=os.path.dirname(path),
        timeout=60,
        **kw,
    )


# --------------------------------------------------------------------------- FR-X8


def test_fr_x8_run_parallel_awaits_concurrently_on_main_thread():
    async def job(label):
        await asyncio.sleep(1.0)
        return (label, threading.current_thread() is threading.main_thread())

    t0 = time.monotonic()
    result = run_parallel(job("a"), job("b"), job("c"))
    elapsed = time.monotonic() - t0

    assert result == [("a", True), ("b", True), ("c", True)]
    assert elapsed < 1.5, "expected concurrent execution under 1.5s, took %.2fs" % elapsed

    # Uncalled coroutine function (T10) runs on the main-thread event loop, not in a thread.
    async def corofunc():
        await asyncio.sleep(0.05)
        return ("corofunc", threading.current_thread() is threading.main_thread())

    assert run_parallel(corofunc) == [("corofunc", True)]
    assert run_parallel() == []


def test_fr_x8_callables_run_in_threads():
    def slow_work(label):
        def _fn():
            time.sleep(1.0)
            return (label, threading.current_thread() is threading.main_thread())
        return _fn

    t0 = time.monotonic()
    result = run_parallel(slow_work("x"), slow_work("y"), slow_work("z"))
    elapsed = time.monotonic() - t0

    assert result == [("x", False), ("y", False), ("z", False)]
    assert elapsed < 1.5, "expected threaded execution under 1.5s, took %.2fs" % elapsed

    # Mixed awaitables and callables preserve exact argument order.
    async def async_item():
        await asyncio.sleep(0.05)
        return ("async", threading.current_thread() is threading.main_thread())

    def sync_item():
        time.sleep(0.05)
        return ("sync", threading.current_thread() is threading.main_thread())

    assert run_parallel(async_item(), sync_item, async_item) == [
        ("async", True),
        ("sync", False),
        ("async", True),
    ]

    # max_threads=1 serializes callables.
    t1 = time.monotonic()
    res_serial = run_parallel(
        lambda: (time.sleep(0.2), 1)[1],
        lambda: (time.sleep(0.2), 2)[1],
        max_threads=1,
    )
    assert res_serial == [1, 2]
    assert time.monotonic() - t1 >= 0.35

    # Invalid max_threads raises ValueError and closes passed coroutines without warning.
    for bad in (0, -1, True, False, 1.5, "2"):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with pytest.raises(ValueError):
                run_parallel(async_item(), max_threads=bad)
            gc.collect()
        unawaited = [w for w in caught if "was never awaited" in str(w.message)]
        assert unawaited == []


def test_fr_x8_invalid_item_raises_type_error_before_running():
    ran = []

    async def should_not_run_async():
        ran.append("async")
        return 1

    def should_not_run_sync():
        ran.append("sync")
        return 2

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(TypeError):
            run_parallel(should_not_run_async(), should_not_run_sync, 42)
        gc.collect()

    assert ran == []
    unawaited = [w for w in caught if "was never awaited" in str(w.message)]
    assert unawaited == []

    # Calling inside an already running event loop raises RuntimeError and closes coroutines.
    async def nested():
        return run_parallel(should_not_run_async())

    with warnings.catch_warnings(record=True) as caught_nested:
        warnings.simplefilter("always")
        with pytest.raises(RuntimeError, match="running event loop"):
            asyncio.run(nested())
        gc.collect()

    assert ran == []
    unawaited_nested = [w for w in caught_nested if "was never awaited" in str(w.message)]
    assert unawaited_nested == []


def test_fr_x8_same_under_plain_python(python, scratch):
    path = _write(scratch, "parallel_plain.py", '''
        import asyncio
        import time
        import darkpyonix

        # %% [parallel]
        __co_routines__ = []

        # %% [code]
        async def load(split):
            await asyncio.sleep(0.05)
            return "async:" + split

        __co_routines__.append(load("train"))

        # %% [code]
        def sync_load():
            time.sleep(0.05)
            return "sync:val"

        __co_routines__.append(sync_load)

        # %% [concorrunt]
        if __name__ == "__main__":
            print(darkpyonix.run_parallel(*__co_routines__))
    ''')
    out = _run(python, path)
    assert out.returncode == 0, out.stderr
    assert out.stdout == "['async:train', 'sync:val']\n"


# --------------------------------------------------------------------------- FR-X9 hook contract


def test_fr_x9_parallel_hook_contract(monkeypatch):
    current_cell = contextvars.ContextVar("test_parallel_cell", default=None)
    finished = []

    class RecordingHook(ParallelHook):
        def __init__(self, mapping):
            self._mapping = mapping

        def cell_of(self, item):
            return self._mapping.get(id(item))

        def enter_cell(self, cell_index):
            return current_cell.set(cell_index)

        def item_finished(self, cell_index, exc_info):
            finished.append((
                cell_index,
                exc_info[0].__name__ if exc_info is not None else None,
                threading.current_thread() is threading.main_thread(),
            ))

    async def coro_cell():
        await asyncio.sleep(0.02)
        return current_cell.get()

    def thread_cell():
        time.sleep(0.02)
        return current_cell.get()

    async def unassociated():
        await asyncio.sleep(0.01)
        return current_cell.get()

    c1 = coro_cell()
    u1 = unassociated()
    hook = RecordingHook({id(c1): 10, id(thread_cell): 20})
    monkeypatch.setattr(hostctx, "get_parallel_hook", lambda: hook, raising=False)

    res = run_parallel(c1, thread_cell, u1)
    assert res == [10, 20, None]
    assert current_cell.get() is None
    assert sorted(finished) == [(10, None, True), (20, None, True)]


# --------------------------------------------------------------------------- FR-X10


def test_fr_x10_failure_waits_for_siblings_then_raises():
    sibling_coro_done = False
    sibling_thread_done = False

    async def failing_coro():
        await asyncio.sleep(0.05)
        raise ValueError("coro boom")

    async def slow_coro():
        nonlocal sibling_coro_done
        await asyncio.sleep(0.25)
        sibling_coro_done = True
        return "coro_ok"

    def slow_thread():
        nonlocal sibling_thread_done
        time.sleep(0.25)
        sibling_thread_done = True
        return "thread_ok"

    def failing_thread():
        time.sleep(0.1)
        raise KeyError("missing")

    with pytest.raises(ParallelError) as info:
        run_parallel(failing_coro(), slow_coro(), slow_thread, failing_thread)

    assert sibling_coro_done is True
    assert sibling_thread_done is True
    errs = info.value.errors
    assert len(errs) == 2
    assert errs[0][0] == 0 and isinstance(errs[0][1], ValueError)
    assert errs[1][0] == 3 and isinstance(errs[1][1], KeyError)
    msg = str(info.value)
    assert "[0] ValueError" in msg
    assert "[3] KeyError" in msg


def test_fr_x10_fail_fast_cancels_siblings():
    coro_cancelled = False
    thread_interrupted = False
    thread_started = threading.Event()

    async def fast_fail():
        while not thread_started.is_set():
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.03)
        raise RuntimeError("fast boom")

    async def long_coro():
        nonlocal coro_cancelled
        try:
            for _ in range(500):
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            coro_cancelled = True
            raise

    def long_thread():
        nonlocal thread_interrupted
        thread_started.set()
        n = 0
        try:
            while True:
                n += 1
        except KeyboardInterrupt:
            thread_interrupted = True
            raise

    t0 = time.monotonic()
    with pytest.raises(ParallelError) as info:
        run_parallel(fast_fail(), long_coro(), long_thread, fail_fast=True)
    elapsed = time.monotonic() - t0

    assert elapsed < 1.5, "fail_fast took too long: %.2fs" % elapsed
    assert len(info.value.errors) == 1
    assert info.value.errors[0][0] == 0
    assert isinstance(info.value.errors[0][1], RuntimeError)
    assert coro_cancelled is True
    assert thread_interrupted is True


# --------------------------------------------------------------------------- FR-X11


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX SIGINT delivery test")
def test_fr_x11_plain_python_interrupt(python, scratch):
    path = _write(scratch, "parallel_interrupt.py", '''
        import asyncio
        import threading
        import darkpyonix

        coro_ready = [False, False]
        coro_cancelled = [False, False]
        thread_ready = threading.Event()
        thread_steps = [0]
        thread_interrupted = [False]

        async def spin_coro(idx):
            coro_ready[idx] = True
            try:
                while True:
                    await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                coro_cancelled[idx] = True
                raise

        async def announce_ready():
            while not (all(coro_ready) and thread_ready.is_set() and thread_steps[0] > 0):
                await asyncio.sleep(0.01)
            print("ready", flush=True)
            while True:
                await asyncio.sleep(0.05)

        def spin_thread():
            thread_ready.set()
            try:
                while True:
                    thread_steps[0] += 1
            except KeyboardInterrupt:
                thread_interrupted[0] = True
                raise

        try:
            darkpyonix.run_parallel(spin_coro(0), spin_coro(1), announce_ready(), spin_thread)
        except KeyboardInterrupt:
            print(
                "interrupted",
                coro_cancelled[0],
                coro_cancelled[1],
                thread_interrupted[0],
                thread_steps[0] > 0,
                flush=True,
            )
    ''')
    proc = subprocess.Popen(
        [python, path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=_env(),
        cwd=scratch,
    )
    try:
        line = proc.stdout.readline()
        assert line == "ready\n", "unexpected startup output: %r (stderr=%r)" % (
            line, proc.stderr.read(),
        )
        t0 = time.monotonic()
        os.kill(proc.pid, signal.SIGINT)
        rest = proc.stdout.read()
        code = proc.wait(timeout=5)
        elapsed = time.monotonic() - t0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)

    assert code == 0, proc.stderr.read()
    assert elapsed < 1.5, "SIGINT teardown took %.2fs" % elapsed
    assert rest == "interrupted True True True True\n"
