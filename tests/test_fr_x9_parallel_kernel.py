"""FR-F7, FR-X7, FR-X9, FR-X10, FR-X11: parallel group execution in the kernel executor."""
from __future__ import annotations

import json
import os
import subprocess
import textwrap

import pytest

from conftest import SRC_ROOT

DRIVER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "helpers", "executor_driver.py")


def drive(python, scratch, source, steps, name="nb.py", timeout=60, **plan):
    path = os.path.join(scratch, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(source).lstrip("\n"))
    plan.update({"path": path, "src_root": SRC_ROOT, "steps": steps})
    plan_path = os.path.join(scratch, "plan.json")
    out_path = os.path.join(scratch, "out.json")
    with open(plan_path, "w", encoding="utf-8") as f:
        json.dump(plan, f)
    proc = subprocess.run([python, DRIVER, plan_path, out_path], capture_output=True,
                          text=True, timeout=timeout, cwd=scratch)
    assert proc.returncode == 0, proc.stderr
    with open(out_path, encoding="utf-8") as f:
        out = json.load(f)
    out["stdout"], out["stderr"], out["path"] = proc.stdout, proc.stderr, path
    return out


def run_all(**extra):
    params = {"mode": "all"}
    params.update(extra)
    return [{"op": "submit", "params": params}, {"op": "wait_idle"}]


def run_cells(cells, **extra):
    params = {"mode": "cells", "cells": cells}
    params.update(extra)
    return [{"op": "submit", "params": params}, {"op": "wait_idle"}]


def cell_record(run, index):
    for c in run["cells"]:
        if c["index"] == index:
            return c
    raise AssertionError("cell %d did not run: %r" % (index, [c["index"] for c in run["cells"]]))


def cell_stream(run, index, name="stdout"):
    rec = cell_record(run, index)
    return "".join(o["text"] for o in rec["outputs"]
                   if o["output_type"] == "stream" and o["name"] == name)


def cell_errors(run, index):
    rec = cell_record(run, index)
    return [o for o in rec["outputs"] if o["output_type"] == "error"]


def events(out, type_):
    return [e for e in out["events"] if e["type"] == type_]


# ---------------------------------------------------------------- FR-F7


def test_fr_f7_layout_does_not_change_execution(python, scratch):
    source = """
        # %% [code]
        # @layout: horizontal
        # @width: 1fr
        order = []
        order.append("a")
        val = 10

        # %% [code]
        # @width: 2fr
        order.append("b")
        val += 5

        # %% [code]
        # @layout: grid
        order.append("c")
        val *= 2

        # %% [code]
        # @layout: vertical
        order.append("d")
        print(",".join(order), val)
    """
    out = drive(python, scratch, source, run_all())
    run = out["runs"][0]
    assert run["status"] == "ok", run
    assert [c["index"] for c in run["cells"]] == [1, 2, 3, 4]
    assert cell_stream(run, 4) == "a,b,c,d 30\n"

    # Plain python produces the exact same output.
    env = dict(os.environ, PYTHONPATH=SRC_ROOT, PYTHONDONTWRITEBYTECODE="1")
    plain = subprocess.run([python, out["path"]], capture_output=True, text=True, env=env, timeout=30)
    assert plain.returncode == 0, plain.stderr
    assert plain.stdout == "a,b,c,d 30\n"


# ---------------------------------------------------------------- FR-X7


@pytest.mark.parametrize("closing", ["concorrunt", "concurrent"])
def test_fr_x7_running_one_member_runs_the_group(python, scratch, closing):
    source = """
        import asyncio
        import darkpyonix

        # %% [code]
        prefix = "item"

        # %% [parallel]
        __co_routines__ = []

        # %% [code]
        async def work_a():
            await asyncio.sleep(0.02)
            return prefix + ":A"

        __co_routines__.append(work_a())

        # %% [code]
        def work_b():
            return prefix + ":B"

        __co_routines__.append(work_b)

        # %% [%s]
        results = darkpyonix.run_parallel(*__co_routines__)
        print(results)

        # %% [code]
        print("after-group")
    """.replace("%s", closing)

    # Run cell 1 first so preamble and setup are done, then request only member cell 3.
    out = drive(python, scratch, source, run_cells([1]) + run_cells([3]))
    assert len(out["runs"]) == 2
    group_run = out["runs"][1]
    assert group_run["status"] == "ok", group_run

    started = events(out, "run.started")[1]["data"]
    assert started["cells"] == [2, 3, 4, 5]
    assert [c["index"] for c in group_run["cells"]] == [2, 3, 4, 5]
    assert all(c["status"] == "ok" for c in group_run["cells"])
    assert cell_stream(group_run, 5) == "['item:A', 'item:B']\n"


def test_fr_x7_unterminated_group_runs_sequentially(python, scratch):
    source = """
        # %% [parallel]
        items = []

        # %% [code]
        items.append("one")
        print("cell2", items)

        # %% [code]
        items.append("two")
        print("cell3", items)
    """
    out = drive(python, scratch, source, run_all())
    run = out["runs"][0]
    assert run["status"] == "ok", run
    assert [c["index"] for c in run["cells"]] == [1, 2, 3]
    assert "darkpyonix: [parallel] at cell 1 has no [concurrent]; running its cells one by one" in (
        cell_stream(run, 1, "stderr")
    )
    assert cell_stream(run, 2) == "cell2 ['one']\n"
    assert cell_stream(run, 3) == "cell3 ['one', 'two']\n"
    for ev in events(out, "cell.started"):
        assert "group" not in ev["data"]


# ---------------------------------------------------------------- FR-X9


def test_fr_x9_member_output_goes_to_its_cell(python, scratch):
    source = """
        import asyncio
        import time
        import darkpyonix

        # %% [parallel]
        __co_routines__ = []

        # %% [code]
        async def task_a():
            print("A-start")
            await asyncio.sleep(0.05)
            darkpyonix.display("display-from-A")
            print("A-end")
            return "A"

        __co_routines__.append(task_a())

        # %% [code]
        def task_b():
            print("B-start")
            time.sleep(0.05)
            print("B-end")
            return "B"

        __co_routines__.append(task_b)

        # %% [concurrent]
        async def unassociated():
            await asyncio.sleep(0.02)
            print("extra-line")
            return "X"

        res = darkpyonix.run_parallel(*__co_routines__, unassociated())
        print("done", res)
    """
    out = drive(python, scratch, source, run_all())
    run = out["runs"][0]
    assert run["status"] == "ok", run

    out_a = cell_stream(run, 2)
    out_b = cell_stream(run, 3)
    out_c = cell_stream(run, 4)

    assert out_a == "A-start\nA-end\n"
    assert out_b == "B-start\nB-end\n"
    assert "extra-line\n" in out_c
    assert "done ['A', 'B', 'X']\n" in out_c
    assert "A-start" not in out_b and "A-start" not in out_c
    assert "B-start" not in out_a and "B-start" not in out_c
    assert "extra-line" not in out_a and "extra-line" not in out_b

    disp_a = [o for o in cell_record(run, 2)["outputs"] if o["output_type"] == "display_data"]
    assert any("display-from-A" in str(o["data"]) for o in disp_a)

    started_by_idx = {e["data"]["index"]: e["data"] for e in events(out, "cell.started")}
    for idx in (1, 2, 3, 4):
        assert started_by_idx[idx]["group"] == {"head": 1, "end": 4}


def test_fr_x9_members_finish_independently(python, scratch):
    source = """
        import asyncio
        import darkpyonix

        # %% [parallel]
        __co_routines__ = []

        # %% [code]
        helper_val = 42

        # %% [code]
        async def fast():
            await asyncio.sleep(0.05)
            return ("fast", helper_val)

        __co_routines__.append(fast())

        # %% [code]
        async def slow():
            await asyncio.sleep(0.4)
            return ("slow", helper_val)

        __co_routines__.append(slow())

        # %% [concurrent]
        out = darkpyonix.run_parallel(*__co_routines__)
        print(out)
    """
    out = drive(python, scratch, source, run_all())
    run = out["runs"][0]
    assert run["status"] == "ok", run
    assert cell_stream(run, 5) == "[('fast', 42), ('slow', 42)]\n"

    cell_events = [
        (e["type"], e["data"]["index"], e["t"])
        for e in out["events"]
        if e["type"] in ("cell.started", "cell.finished")
    ]
    pos = {(typ, idx): i for i, (typ, idx, _t) in enumerate(cell_events)}
    time_of = {(typ, idx): t for typ, idx, t in cell_events}

    # Member with no appended items (cell 2) finishes before cell 3 starts.
    assert pos[("cell.finished", 2)] < pos[("cell.started", 3)]

    # Cells 3 (fast) and 4 (slow) are both running simultaneously: both start before either finishes.
    assert pos[("cell.started", 3)] < pos[("cell.finished", 3)]
    assert pos[("cell.started", 4)] < pos[("cell.finished", 3)]
    assert pos[("cell.started", 5)] < pos[("cell.finished", 3)]

    # Cell 3 finishes noticeably before cell 4 finishes, and before [concurrent] (cell 5) finishes.
    assert pos[("cell.finished", 3)] < pos[("cell.finished", 4)]
    assert pos[("cell.finished", 4)] < pos[("cell.finished", 5)]
    assert time_of[("cell.finished", 4)] - time_of[("cell.finished", 3)] >= 0.2


# ---------------------------------------------------------------- FR-X10


def test_fr_x10_kernel_failure_routes_error_to_member_and_raises_parallel_error(python, scratch):
    source = """
        import asyncio
        import darkpyonix

        # %% [parallel]
        __co_routines__ = []

        # %% [code]
        async def boom():
            await asyncio.sleep(0.05)
            raise ValueError("boom A")

        __co_routines__.append(boom())

        # %% [code]
        async def survivor():
            await asyncio.sleep(0.2)
            print("B completed")
            return "ok_b"

        __co_routines__.append(survivor())

        # %% [concurrent]
        darkpyonix.run_parallel(*__co_routines__)

        # %% [code]
        print("should-not-run")
    """
    out = drive(python, scratch, source, run_all())
    run = out["runs"][0]
    assert run["status"] == "error", run
    assert [c["index"] for c in run["cells"]] == [0, 1, 2, 3, 4]  # cell 5 skipped

    # Member A gets the ValueError output and status error.
    assert cell_record(run, 2)["status"] == "error"
    errs_a = cell_errors(run, 2)
    assert len(errs_a) == 1
    assert errs_a[0]["ename"] == "ValueError"
    assert errs_a[0]["evalue"] == "boom A"

    # Member B ran to completion.
    assert cell_record(run, 3)["status"] == "ok"
    assert cell_stream(run, 3) == "B completed\n"

    # [concurrent] gets ParallelError.
    assert cell_record(run, 4)["status"] == "error"
    errs_c = cell_errors(run, 4)
    assert len(errs_c) == 1
    assert errs_c[0]["ename"] == "ParallelError"
    assert "ValueError" in errs_c[0]["evalue"]

    # fail_fast=True marks cancelled sibling as interrupted, not error.
    source_ff = """
        import asyncio
        import darkpyonix

        # %% [parallel]
        __co_routines__ = []

        # %% [code]
        async def fast_boom():
            await asyncio.sleep(0.05)
            raise RuntimeError("fast fail")

        __co_routines__.append(fast_boom())

        # %% [code]
        async def slow_sibling():
            for _ in range(300):
                await asyncio.sleep(0.01)
            print("never")

        __co_routines__.append(slow_sibling())

        # %% [concurrent]
        darkpyonix.run_parallel(*__co_routines__, fail_fast=True)
    """
    out_ff = drive(python, scratch, source_ff, run_all(), name="nb_ff.py")
    run_ff = out_ff["runs"][0]
    assert run_ff["status"] == "error", run_ff
    assert cell_record(run_ff, 2)["status"] == "error"
    assert cell_errors(run_ff, 2)[0]["ename"] == "RuntimeError"
    assert cell_record(run_ff, 3)["status"] == "interrupted"
    assert cell_errors(run_ff, 3) == []
    assert cell_stream(run_ff, 3) == ""
    assert cell_record(run_ff, 4)["status"] == "error"
    assert cell_errors(run_ff, 4)[0]["ename"] == "ParallelError"


# ---------------------------------------------------------------- FR-X11


def test_fr_x11_interrupt_cancels_tasks_and_threads(python, scratch):
    source = """
        import asyncio
        import threading
        import darkpyonix

        # %% [code]
        kept = "alive"
        coro_cancelled = [False, False]
        thread_ready = threading.Event()
        thread_steps = [0]
        thread_interrupted = [False]

        # %% [parallel]
        __co_routines__ = []

        # %% [code]
        async def loop_coro_0():
            try:
                while True:
                    await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                coro_cancelled[0] = True
                raise

        __co_routines__.append(loop_coro_0())

        # %% [code]
        async def loop_coro_1():
            try:
                while True:
                    await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                coro_cancelled[1] = True
                raise

        __co_routines__.append(loop_coro_1())

        # %% [code]
        def spin_thread():
            thread_ready.set()
            try:
                while True:
                    thread_steps[0] += 1
            except KeyboardInterrupt:
                thread_interrupted[0] = True
                raise

        __co_routines__.append(spin_thread)

        # %% [concurrent]
        darkpyonix.run_parallel(*__co_routines__)

        # %% [code]
        print(kept, coro_cancelled, thread_interrupted, thread_steps[0] > 0)
    """
    steps = [
        {"op": "submit", "params": {"mode": "all"}},
        # Cells 0 (preamble), 1 (setup), 2 ([parallel]), 3, 4, 5 (members), 6 ([concurrent]) = 7 starts
        {"op": "wait_event", "type": "cell.started", "count": 7},
        {"op": "sleep", "seconds": 0.25},
        {"op": "interrupt"},
        {"op": "wait_idle"},
    ] + run_cells([7])

    out = drive(python, scratch, source, steps)
    assert len(out["runs"]) == 2

    interrupted_run = out["runs"][0]
    assert interrupted_run["status"] == "interrupted", interrupted_run
    for idx in (3, 4, 5, 6):
        assert cell_record(interrupted_run, idx)["status"] == "interrupted"

    # Interrupt completed promptly (within 1.5s of issuing interrupt).
    interrupt_t = out["results"][3]["t"]
    run_finished_t = events(out, "run.finished")[0]["t"]
    assert run_finished_t - interrupt_t < 1.5, (
        "interrupt took %.2fs" % (run_finished_t - interrupt_t)
    )

    # Namespace survived and all three parallel items stopped cleanly.
    check_run = out["runs"][1]
    assert check_run["status"] == "ok", check_run
    assert cell_stream(check_run, 7) == "alive [True, True] [True] True\n"
