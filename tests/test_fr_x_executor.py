"""FR-K5..K7, FR-X1..X5, FR-F5, NFR-K3: the executor, driven on the main thread of a child.

Interrupts (SIGINT to the main thread) and fd-level capture (dup2 onto fds 1 and 2) must not
hijack pytest's own process, so every test runs tests/helpers/executor_driver.py under the
interpreter being tested and reads back the events, outputs and control results it dumps.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
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
    with open(plan_path, "w") as f:
        json.dump(plan, f)
    proc = subprocess.run([python, DRIVER, plan_path, out_path], capture_output=True,
                          text=True, timeout=timeout, cwd=scratch)
    assert proc.returncode == 0, proc.stderr
    with open(out_path) as f:
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


def stream_text(run, name="stdout"):
    return "".join(o["text"] for c in run["cells"] for o in c["outputs"]
                   if o["output_type"] == "stream" and o["name"] == name)


def outputs_of(run, index):
    for c in run["cells"]:
        if c["index"] == index:
            return c["outputs"]
    raise AssertionError("cell %d did not run" % index)


def events(out, type_):
    return [e for e in out["events"] if e["type"] == type_]


# ---------------------------------------------------------------- FR-K5

def test_fr_k5_cells_run_on_main_thread(python, scratch):
    out = drive(python, scratch, """
        # %%
        import signal, threading
        print(threading.current_thread() is threading.main_thread())
        signal.signal(signal.SIGTERM, signal.getsignal(signal.SIGTERM))
        print("signal ok")
        """, run_all())
    run = out["runs"][0]
    assert run["status"] == "ok", run
    assert stream_text(run) == "True\nsignal ok\n"


# ---------------------------------------------------------------- FR-K6

def test_fr_k6_namespace_lists_user_variables(python, scratch):
    out = drive(python, scratch, """
        import os
        # %%
        numbers = [1, 2, 3]
        label = "x" * 500
        _hidden = 1
        class Thing:
            pass
        thing = Thing()
        """, run_all() + [{"op": "namespace"}])
    variables = {v["name"]: v for v in out["results"][-1]["result"]["variables"]}
    assert set(variables) == {"numbers", "label", "Thing", "thing"}
    assert variables["numbers"] == {"name": "numbers", "type": "list", "repr": "[1, 2, 3]",
                                    "shape": None, "dtype": None, "len": 3}
    assert len(variables["label"]["repr"]) <= 200
    assert variables["thing"]["type"] == "Thing"


def test_fr_k6_namespace_does_not_call_repr_while_busy(python, scratch):
    marker = os.path.join(scratch, "repr-called")
    out = drive(python, scratch, """
        # %%
        class Spy:
            def __repr__(self):
                open(MARKER, "w").close()
                return "Spy()"
            @property
            def shape(self):
                open(MARKER, "w").close()
                return (1,)
            def __len__(self):
                open(MARKER, "w").close()
                return 1
        spy = Spy()
        # %%
        import time
        time.sleep(1.0)
        """.replace("MARKER", repr(marker)), [
            {"op": "submit", "params": {"mode": "all"}},
            {"op": "wait_event", "type": "cell.started", "count": 2},
            {"op": "sleep", "seconds": 0.2},
            {"op": "namespace"},
            {"op": "wait_idle"},
        ])
    variables = out["results"][3]["result"]["variables"]
    assert variables == [
        {"name": "Spy", "type": "type", "repr": None, "shape": None, "dtype": None, "len": None},
        {"name": "spy", "type": "Spy", "repr": None, "shape": None, "dtype": None, "len": None},
    ]
    assert not os.path.exists(marker)


# ---------------------------------------------------------------- FR-K7

def test_fr_k7_soft_restart_clears_namespace(python, scratch):
    out = drive(python, scratch, """
        # %%
        before = 42
        # %%
        print("before" in globals())
        """, run_all() + [{"op": "restart"}, {"op": "status"}, {"op": "namespace"}]
        + run_cells([2]))
    assert out["results"][2]["result"] == {"restarted": True}
    assert out["results"][3]["result"]["execution_count"] == 0
    assert out["results"][4]["result"]["variables"] == []
    after = out["runs"][1]
    assert stream_text(after) == "False\n"
    assert after["cells"][0]["execution_count"] == 1


def test_fr_k7_hard_restart_stops_loop_and_sets_flag(python, scratch):
    out = drive(python, scratch, """
        # %%
        x = 1
        """, [{"op": "restart", "hard": True}])
    assert out["hard_restart_requested"] is True


# ---------------------------------------------------------------- FR-K8 (executor side)

def test_fr_k8_shutdown_interrupts_running_cell_and_finishes_run(python, scratch):
    out = drive(python, scratch, """
        # %%
        import time
        time.sleep(30)
        # %%
        print("never")
        """, [
            {"op": "submit", "params": {"mode": "all"}},
            {"op": "submit", "params": {"mode": "all", "on_busy": "queue"}},
            {"op": "wait_event", "type": "cell.started"},
        ])     # the driver calls shutdown() after the last step
    assert [r["status"] for r in out["runs"]] == ["interrupted"]
    finished = {e["data"]["run_id"]: e["data"]["status"] for e in events(out, "run.finished")}
    assert finished == {out["results"][0]["result"]["run_id"]: "interrupted",
                        out["results"][1]["result"]["run_id"]: "cancelled"}
    assert events(out, "kernel.status")[-1]["data"]["status"] == "stopping"


# ---------------------------------------------------------------- FR-X1 / FR-F5

PLAIN = """
    import sys
    total = 0
    # %% sums [code]
    for i in range(5):
        total += i
        print("i", i)
    # %%
    def f(n):
        return n * 2
    print(f(total), __name__)
    print("to stderr", file=sys.stderr)
    # %%
    print("tail", end="")
    print()
    sys.stdout.write("no newline")
    """


def test_fr_x1_run_all_matches_plain_python(python, scratch):
    out = drive(python, scratch, PLAIN, run_all())
    plain = subprocess.run([python, out["path"]], capture_output=True, text=True, timeout=30)
    run = out["runs"][0]
    assert run["status"] == "ok"
    assert stream_text(run) == plain.stdout
    assert stream_text(run, "stderr") == plain.stderr


def test_fr_x1_run_cells_keeps_namespace(python, scratch):
    out = drive(python, scratch, """
        print("preamble")
        # %%
        a = 10
        # %%
        b = a + 1
        print(b)
        # %%
        print("never")
        """, run_cells([1]) + run_cells([2]))
    first, second = out["runs"]
    assert [c["index"] for c in first["cells"]] == [0, 1]
    assert [c["index"] for c in second["cells"]] == [2]          # preamble only once
    assert stream_text(second) == "11\n"
    assert [c["execution_count"] for c in first["cells"] + second["cells"]] == [1, 2, 3]


def test_fr_x1_source_overrides_file_and_file_is_main(python, scratch):
    out = drive(python, scratch, "# %%\nprint('disk')\n",
                run_all(source="# %%\nprint('buffer', __name__, __file__.endswith('nb.py'))\n"))
    assert stream_text(out["runs"][0]) == "buffer __main__ True\n"


# ---------------------------------------------------------------- FR-X2

def test_fr_x2_last_expression_is_execute_result(python, scratch):
    out = drive(python, scratch, """
        # %%
        x = [i**2 for i in range(5)]; x
        # %%
        None
        # %%
        print(_)
        """, run_all())
    run = out["runs"][0]
    result = outputs_of(run, 1)
    assert result == [{"output_type": "execute_result", "execution_count": 1,
                       "data": {"text/plain": "[0, 1, 4, 9, 16]"}, "metadata": {}}]
    assert outputs_of(run, 2) == []
    assert stream_text(run) == "[0, 1, 4, 9, 16]\n"


def test_fr_x2_error_stops_run_and_hides_kernel_frames(python, scratch):
    out = drive(python, scratch, """
        # %%
        def inner():
            raise ValueError("bad value")
        # %%
        print("before")
        inner()
        # %%
        print("skipped")
        """, run_all())
    run = out["runs"][0]
    assert run["status"] == "error"
    assert [c["index"] for c in run["cells"]] == [1, 2]
    assert run["cells"][1]["status"] == "error"
    error = outputs_of(run, 2)[-1]
    assert error["output_type"] == "error"
    assert (error["ename"], error["evalue"]) == ("ValueError", "bad value")
    tb = "".join(error["traceback"])
    assert SRC_ROOT not in tb and "executor.py" not in tb
    # Frames point at the user's file and its real line numbers.
    assert 'File "%s", line 6' % out["path"] in tb
    assert 'File "%s", line 3, in inner' % out["path"] in tb
    finished = events(out, "run.finished")[0]["data"]
    assert finished["status"] == "error"


def test_fr_x2_syntax_error_is_an_error_output(python, scratch):
    out = drive(python, scratch, "# %%\nx = (\n", run_all())
    run = out["runs"][0]
    assert run["status"] == "error"
    assert outputs_of(run, 1)[0]["ename"] == "SyntaxError"


# ---------------------------------------------------------------- FR-X3

SLOW = """
    # %%
    import time
    time.sleep(0.6)
    print("slow done")
    """


def test_fr_x3_second_run_is_rejected_when_busy(python, scratch):
    out = drive(python, scratch, SLOW, [
        {"op": "submit", "params": {"mode": "all"}},
        {"op": "submit", "params": {"mode": "all"}},
        {"op": "wait_idle"},
    ])
    first, second = out["results"][0], out["results"][1]
    assert first["result"]["state"] == "running"
    assert second["error"]["code"] == "busy"
    assert second["error"]["data"]["current"]["run_id"] == first["result"]["run_id"]
    assert second["error"]["data"]["queue_length"] == 0
    assert len(out["runs"]) == 1


def test_fr_x3_queued_run_waits_its_turn(python, scratch):
    out = drive(python, scratch, SLOW, [
        {"op": "submit", "params": {"mode": "all"}},
        {"op": "wait_event", "type": "run.started"},
        {"op": "submit", "params": {"mode": "all", "on_busy": "queue", "params": {"n": 2}}},
        {"op": "submit", "params": {"mode": "all", "on_busy": "queue"}},
        {"op": "status"},
        {"op": "cancel", "run_id": "$3"},
        {"op": "wait_idle"},
    ])
    first, second, third = (out["results"][i]["result"] for i in (0, 2, 3))
    assert second == {"run_id": second["run_id"], "state": "queued", "position": 1}
    assert third["position"] == 2
    assert out["results"][4]["result"]["queue"] == [second["run_id"], third["run_id"]]
    assert out["results"][5]["result"] == {"cancelled": True}
    assert [r["run_id"] for r in out["runs"]] == [first["run_id"], second["run_id"]]
    assert out["runs"][1]["params"] == {"n": 2}
    order = [(e["type"], e["data"]["run_id"]) for e in out["events"]
             if e["type"] in ("run.started", "run.finished")]
    assert order == [("run.started", first["run_id"]), ("run.finished", third["run_id"]),
                     ("run.finished", first["run_id"]), ("run.started", second["run_id"]),
                     ("run.finished", second["run_id"])]


def test_fr_x3_bad_request_is_rejected(python, scratch):
    out = drive(python, scratch, "# %%\nx = 1\n", [
        {"op": "submit", "params": {"mode": "cells", "cells": []}},
        {"op": "submit", "params": {"mode": "nope"}},
        {"op": "submit", "params": {"mode": "cells", "cells": [7]}},
    ])
    assert [r["error"]["code"] for r in out["results"]] == ["bad_request"] * 3


# ---------------------------------------------------------------- FR-X4

def test_fr_x4_interrupt_stops_cell_and_keeps_state(python, scratch):
    out = drive(python, scratch, """
        # %%
        step = 0
        while True:
            step += 1
        # %%
        print(step > 0)
        # %%
        import time
        time.sleep(30)
        """, [
            {"op": "submit", "params": {"mode": "cells", "cells": [1]}},
            {"op": "wait_event", "type": "cell.started"},
            {"op": "sleep", "seconds": 0.2},
            {"op": "interrupt"},
            {"op": "wait_idle"},
        ] + run_cells([2]) + [
            {"op": "submit", "params": {"mode": "cells", "cells": [3]}},
            {"op": "wait_event", "type": "cell.started", "count": 3},
            {"op": "sleep", "seconds": 0.2},
            {"op": "interrupt"},
            {"op": "wait_idle"},
            {"op": "interrupt"},
        ])
    loop_run, check_run, sleep_run = out["runs"]
    assert out["results"][3]["result"]["interrupted"] is True
    assert loop_run["status"] == "interrupted"
    assert loop_run["cells"][0]["status"] == "interrupted"
    finished = [e for e in events(out, "run.finished")]
    assert finished[0]["t"] - out["results"][3]["t"] < 1.0
    assert check_run["status"] == "ok" and stream_text(check_run) == "True\n"
    # A blocking sleep is interrupted too.
    assert sleep_run["status"] == "interrupted"
    assert finished[2]["t"] - out["results"][10]["t"] < 1.0
    # Nothing to interrupt when idle.
    assert out["results"][-1]["result"] == {"interrupted": False}


# ---------------------------------------------------------------- FR-X5

@pytest.mark.skipif(os.name == "nt", reason="echo is a shell builtin on Windows")
def test_fr_x5_fd_level_output_is_captured(python, scratch):
    out = drive(python, scratch, """
        # %%
        import os, subprocess
        os.write(1, b"x\\n")
        subprocess.run(["echo", "y"])
        os.write(2, b"z\\n")
        """, run_all())
    run = out["runs"][0]
    assert stream_text(run) == "x\ny\n"
    assert stream_text(run, "stderr") == "z\n"
    assert "x\n" not in out["stdout"]


def test_fr_x5_repr_protocol_becomes_mime_bundle(python, scratch):
    out = drive(python, scratch, """
        # %%
        class Rich:
            def _repr_html_(self):
                return "<b>rich</b>"
            def _repr_png_(self):
                return b"\\x89PNG"
            def __repr__(self):
                return "Rich()"
        from darkpyonix._display import display
        display(Rich())
        Rich()
        """, run_all())
    outs = outputs_of(out["runs"][0], 1)
    assert [o["output_type"] for o in outs] == ["display_data", "execute_result"]
    for o in outs:
        assert o["data"] == {"text/html": "<b>rich</b>", "image/png": "iVBORw==",
                             "text/plain": "Rich()"}


def test_fr_x5_streams_are_coalesced_into_one_output(python, scratch):
    out = drive(python, scratch, """
        # %%
        for i in range(1000):
            print(i)
        """, run_all())
    outs = outputs_of(out["runs"][0], 1)
    assert len(outs) == 1
    assert outs[0]["text"] == "".join("%d\n" % i for i in range(1000))
    assert len(events(out, "output")) < 50


def test_fr_x5_hostctx_exposes_params_and_display(python, scratch):
    out = drive(python, scratch, """
        # %%
        from darkpyonix import _hostctx as hostctx
        print(hostctx.in_kernel(), hostctx.current_params())
        hostctx.emit_display({"text/markdown": "# shown"})
        ok = hostctx.emit_display({"data": {"text/markdown": "live only"}, "metadata": {}}, silent=True)
        """, run_all(params={"lr": 0.1}))
    outs = outputs_of(out["runs"][0], 1)
    assert outs == [
        {"output_type": "stream", "name": "stdout", "text": "True {'lr': 0.1}\n"},
        {"output_type": "display_data", "data": {"text/markdown": "# shown"}, "metadata": {}},
    ]
    live = [e["data"]["output"] for e in events(out, "output")]
    assert {"output_type": "display_data", "data": {"text/markdown": "live only"},
            "metadata": {}} in live


# ---------------------------------------------------------------- NFR-K3

PRINT_LOOP = """
t, c = time.perf_counter(), time.process_time()
for i in range(100000):
    print(i)
sys.stdout.flush()
R.append((time.perf_counter() - t, time.process_time() - c))
"""
REPEAT = 5


def _measure_print_overhead(python, scratch):
    script = "import sys, time\nR = []\n" + PRINT_LOOP * REPEAT + "sys.stderr.write(repr(R))\n"
    p = subprocess.run([python, "-c", script], capture_output=True, text=True, timeout=120)
    plain = eval(p.stderr)
    source = "import sys, time\nR = []\n# %%\n" + "# %%\n".join([PRINT_LOOP] * REPEAT) + \
        "# %%\nprint(R)\n"
    out = drive(python, scratch, source, run_all(), timeout=120)
    text = stream_text(out["runs"][0])
    assert text.count("\n") == 100000 * REPEAT + 1
    kernel = eval(text.splitlines()[-1])
    k_wall, k_cpu = min(k[0] for k in kernel), min(k[1] for k in kernel)
    p_wall, p_cpu = min(q[0] for q in plain), min(q[1] for q in plain)
    sys.stderr.write("NFR-K3 %s: wall %.4fs vs %.4fs (x%.2f), cpu %.4fs vs %.4fs (x%.2f)\n" % (
        python, k_wall, p_wall, k_wall / p_wall, k_cpu, p_cpu, k_cpu / p_cpu))
    return k_cpu / p_cpu


@pytest.mark.skipif(os.environ.get("DARKPYONIX_SKIP_PERF") == "1", reason="perf disabled")
def test_nfr_k3_print_overhead(python, scratch):
    """100k prints in a cell vs the same loop under plain python with stdout on a pipe.

    Best of five on both sides, wall clock and process CPU time (all threads: the main
    thread, the output router and the fd readers). The CPU ratio is asserted because, unlike
    wall clock, it does not swing with the machine's load; both are printed for the SPEC's
    measurement record. A loaded machine still adds noise, so the best of three attempts
    counts.
    """
    ratios = []
    for _ in range(3):
        ratios.append(_measure_print_overhead(python, scratch))
        if ratios[-1] <= 1.5:
            break
    assert min(ratios) <= 1.5
