"""The shared document and run attribution through a real kernel process (SPEC §10a FR-S1..S8).

These launch the kernel main (kernel/darkpyonix/kernel/__main__.py) and drive it with several
``KernelClient`` connections, the way managers do: ``doc.*``/``presence.*`` methods, the
event stream, ``run`` with ``cell_ids`` and ``client``, ``interrupt``, ``runs.get``/``runs.wait``.
"""
from __future__ import annotations

import os
import signal
import sys
import textwrap
import time

import pytest

from darkpyonix.kernel import launcher
from darkpyonix.kernel.client import KernelClient
from darkpyonix.kernel.protocol import DKPError, kernel_id_for

from test_fr_s_collab import Replica, external_write, read_file, state_of

NB = textwrap.dedent('''\
    import time

    # %% [code]
    x = 1
    print("x is", x)

    # %% [code]
    y = x * 10
    print("y is", y)

    # %% [code]
    step = 0
    while LOOP:
        step += 1
        time.sleep(0.01)
    ''')


def who(cid, permission="editor"):
    return {"client_id": cid, "user": "user-" + cid, "nickname": "dev-" + cid,
            "permission": permission}


def by(cid):
    return {"client_id": cid, "user": "user-" + cid, "nickname": "dev-" + cid}


A, B = who("A"), who("B")


@pytest.fixture
def kernel(scratch, dp_home):
    """Start a kernel for a notebook; yields (path, connect). Every process is stopped."""
    state = {"pid": None, "clients": []}

    def boot(loop=False):
        path = os.path.join(scratch, "shared.py")
        with open(path, "wb") as f:
            f.write(NB.replace("LOOP", "True" if loop else "False").encode("utf-8"))
        pid = launcher.launch(path, python=sys.executable)
        state["pid"] = pid
        info = launcher.wait_for_announce(kernel_id_for(path), pid=pid, timeout=15.0)
        assert info is not None and info["port"] > 0, "kernel did not announce"

        def connect():
            c = KernelClient(info["port"], info["kernel_id"], name="test", kind="cli")
            c.connect()
            state["clients"].append(c)
            return c
        return path, connect

    yield boot
    for c in state["clients"]:
        try:
            c.close()
        except Exception:
            pass
    pid = state["pid"]
    if pid is not None:
        try:
            os.kill(pid, signal.SIGTERM)
            for _ in range(100):
                os.kill(pid, 0)
                time.sleep(0.05)
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def drain(conn, until, timeout=10.0):
    """Events from ``conn`` up to and including the first that satisfies ``until``."""
    out = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        ev = conn.next_event(timeout=max(0.01, deadline - time.monotonic()))
        if ev is None:
            break
        out.append(ev)
        if until(ev):
            return out
    raise AssertionError("event not seen; got %r" % ([e["type"] for e in out],))


def finished(run_id):
    return lambda e: e["type"] == "run.finished" and e["data"]["run_id"] == run_id


def expect_error(code, fn, *args):
    with pytest.raises(DKPError) as info:
        fn(*args)
    assert info.value.code == code, (info.value.code, info.value.message)
    return info.value


def stream_text(nb):
    return "".join(o.get("text", "") for cell in nb["cells"] for o in cell["outputs"]
                   if o["output_type"] == "stream")


def test_fr_s1_s2_s3_s5_s8_two_clients_edit_converge_save_and_run(kernel):
    path, connect = kernel()
    original = read_file(path)
    a, b = connect(), connect()

    # FR-S1: snapshot, then the event stream from the snapshot's seq.
    snap_a = a.request("doc.snapshot", {}, timeout=5)
    a.subscribe(since=snap_a["seq"])
    snap_b = b.request("doc.snapshot", {}, timeout=5)
    b.subscribe(since=snap_b["seq"])
    ra, rb = Replica(snap_a), Replica(snap_b)
    assert state_of(snap_a) == state_of(snap_b)
    cells = snap_a["cells"]
    assert [c["index"] for c in cells] == [0, 1, 2, 3]
    assert all(c["cell_id"].startswith("c_") and c["version"] == 1 for c in cells)
    c1 = cells[1]
    assert "@id" not in original and c1["cell_id"] not in original  # ids are never written

    # FR-S8: a viewer cannot edit or lock.
    viewer = who("V", "viewer3")
    expect_error("forbidden", b.request, "doc.lock", {"client": viewer, "cell_id": c1["cell_id"]})

    # FR-S3: B locks; A can neither lock nor edit that cell.
    b.request("doc.lock", {"client": B, "cell_id": c1["cell_id"]}, timeout=5)
    err = expect_error("locked", a.request, "doc.lock", {"client": A, "cell_id": c1["cell_id"]})
    assert err.data["locked_by"] == "B"
    expect_error("locked", a.request, "doc.cell.update",
                 {"client": A, "cell_id": c1["cell_id"], "base_version": 1, "source": "no"})

    # FR-S2: B edits with base_version; a stale base is a conflict and changes nothing.
    new_source = c1["source"].replace("x = 1", "x = 5")
    assert new_source != c1["source"]
    upd = b.request("doc.cell.update", {"client": B, "cell_id": c1["cell_id"], "base_version": 1,
                                        "source": new_source}, timeout=5)["cell"]
    assert upd["version"] == 2 and upd["source"] == new_source
    err = expect_error("conflict", b.request, "doc.cell.update",
                       {"client": B, "cell_id": c1["cell_id"], "base_version": 1, "source": "z"})
    assert err.data["cell"]["source"] == new_source
    b.request("doc.unlock", {"client": B, "cell_id": c1["cell_id"]}, timeout=5)

    # FR-S6: a run by cell_id right away reads the edited source (pending edits are flushed).
    acc = a.request("run", {"cell_ids": [c1["cell_id"]], "client": A}, timeout=5)
    evs = drain(a, finished(acc["run_id"]))
    assert evs[-1]["data"]["status"] == "ok"
    for ev in evs:
        ra.apply(ev)
    started = [e["data"] for e in evs if e["type"] == "cell.started"]
    assert [d["cell_id"] for d in started] == [cells[0]["cell_id"], c1["cell_id"]]
    log = a.request("runs.get", {"run_id": acc["run_id"]}, timeout=5)
    assert "x is 5" in stream_text(log)
    assert log["metadata"]["darkpyonix"]["cells"] == [1]

    # FR-S1: the other client reaches the same state from events alone.
    for ev in drain(b, lambda e: e["type"] == "doc.unlock"):
        rb.apply(ev)
    assert ra.state() == rb.state() == state_of(a.request("doc.snapshot", {}, timeout=5))
    assert ra.state()["cells"][1][1:3] == (new_source, 2)

    # FR-S5: the edit is on disk; every other byte is unchanged.
    assert read_file(path) == original.replace(c1["source"], new_source, 1)

    # FR-S5: an external edit reaches every client as doc.reloaded within a second.
    time.sleep(0.6)  # let a poll see the kernel's own save first
    text = read_file(path)
    external_write(path, text.replace("y = x * 10", "y = x * 100"))
    t0 = time.monotonic()
    for conn, rep in ((a, ra), (b, rb)):
        evs = drain(conn, lambda e: e["type"] == "doc.reloaded", timeout=3)
        for ev in evs:
            rep.apply(ev)
    assert time.monotonic() - t0 < 1.5
    snap = a.request("doc.snapshot", {}, timeout=5)
    assert ra.state() == rb.state() == state_of(snap)
    c2 = snap["cells"][2]
    assert c2["cell_id"] == cells[2]["cell_id"] and "x * 100" in c2["source"] and c2["version"] == 2

    # And a run sees it.
    acc = b.request("run", {"cell_ids": [c1["cell_id"], c2["cell_id"]], "client": B}, timeout=5)
    drain(b, finished(acc["run_id"]))
    assert "y is 500" in stream_text(b.request("runs.get", {"run_id": acc["run_id"]}, timeout=5))

    # Unknown cell ids are refused before anything runs.
    expect_error("not_found", a.request, "run", {"cell_ids": ["c_nope"], "client": A})
    expect_error("bad_request", a.request, "run", {"cell_ids": [c1["cell_id"]], "cells": [1]})


def test_fr_s4_presence_and_leave_release_locks_through_kernel(kernel):
    path, connect = kernel()
    a, b = connect(), connect()
    snap = a.request("doc.snapshot", {}, timeout=5)
    a.subscribe(since=snap["seq"])
    c1 = snap["cells"][1]["cell_id"]

    b.request("presence.update", {"client": who("B", "viewer1"), "focused_cell_id": c1,
                                  "cursor": {"cell_id": c1, "line": 0, "column": 3}}, timeout=5)
    ev = drain(a, lambda e: e["type"] == "presence.update")[-1]["data"]
    assert ev["client_id"] == "B" and ev["focused_cell_id"] == c1
    assert ev["cursor"] == {"cell_id": c1, "line": 0, "column": 3}
    assert [p["client_id"] for p in a.request("doc.snapshot", {}, timeout=5)["presence"]] == ["B"]

    b.request("presence.update", {"client": B}, timeout=5)  # now an editor
    b.request("doc.lock", {"client": B, "cell_id": c1}, timeout=5)
    b.request("presence.leave", {"client": B}, timeout=5)
    evs = drain(a, lambda e: e["type"] == "presence.leave")
    unlock = [e["data"] for e in evs if e["type"] == "doc.unlock"]
    assert unlock and unlock[0]["reason"] == "disconnected" and unlock[0]["cell_id"] == c1
    snap = a.request("doc.snapshot", {}, timeout=5)
    assert snap["presence"] == [] and "lock" not in snap["cells"][1]


def test_fr_s6_runs_are_attributed(kernel):
    path, connect = kernel(loop=True)
    a, b = connect(), connect()
    snap = a.request("doc.snapshot", {}, timeout=5)
    a.subscribe(since=snap["seq"])
    c1 = snap["cells"][1]["cell_id"]

    first = a.request("run", {"mode": "all", "client": A}, timeout=5)
    time.sleep(0.5)
    queued = b.request("run", {"cell_ids": [c1], "client": B, "on_busy": "queue"}, timeout=5)
    assert queued["state"] == "queued"
    assert b.request("interrupt", {"client": B}, timeout=5)["interrupted"] is True
    evs = drain(a, finished(first["run_id"]))
    evs += drain(a, finished(queued["run_id"]))

    def of(type_, run_id):
        return [e["data"] for e in evs if e["type"] == type_ and e["data"]["run_id"] == run_id]

    for t in ("run.started", "cell.started", "cell.finished", "run.finished"):
        assert of(t, first["run_id"]), t
        assert all(d["started_by"] == by("A") for d in of(t, first["run_id"])), t
        assert all(d["started_by"] == by("B") for d in of(t, queued["run_id"])), t
    assert of("run.queued", queued["run_id"])[0]["started_by"] == by("B")
    fin = of("run.finished", first["run_id"])[0]
    assert fin["status"] == "interrupted" and fin["interrupted_by"] == by("B")
    fin = of("run.finished", queued["run_id"])[0]
    assert fin["status"] == "ok" and "interrupted_by" not in fin

    meta = a.request("runs.get", {"run_id": first["run_id"]}, timeout=5)["metadata"]["darkpyonix"]
    assert meta["started_by"] == by("A") and meta["interrupted_by"] == by("B")
    meta = a.request("runs.get", {"run_id": queued["run_id"]}, timeout=5)["metadata"]["darkpyonix"]
    assert meta["started_by"] == by("B") and meta["interrupted_by"] is None
    summaries = dict((s["run_id"], s) for s in a.request("runs.list", {}, timeout=5)["runs"])
    assert summaries[first["run_id"]]["started_by"] == by("A")
    assert summaries[first["run_id"]]["interrupted_by"] == by("B")
    assert summaries[queued["run_id"]]["started_by"] == by("B")

    # Without a client (an older manager) the fields are null.
    anon = a.request("run", {"cell_ids": [c1]}, timeout=5)
    evs = drain(a, finished(anon["run_id"]))
    assert evs[-1]["data"]["started_by"] is None
    meta = a.request("runs.get", {"run_id": anon["run_id"]}, timeout=5)["metadata"]["darkpyonix"]
    assert meta["started_by"] is None and meta["interrupted_by"] is None


def test_fr_s7_wait_returns_on_finish_or_timeout(kernel):
    path, connect = kernel(loop=True)
    a, b = connect(), connect()
    run = a.request("run", {"mode": "all", "client": A}, timeout=5)

    t0 = time.monotonic()
    res = b.request("runs.wait", {"run_id": run["run_id"], "timeout": 1}, timeout=10)
    assert 0.9 <= time.monotonic() - t0 < 3
    assert res["status"] == "running" and res["run_id"] == run["run_id"]

    def interrupt_later():
        time.sleep(0.5)
        a.request("interrupt", {"client": A}, timeout=5)

    import threading
    th = threading.Thread(target=interrupt_later)
    th.start()
    t0 = time.monotonic()
    res = b.request("runs.wait", {"run_id": "current", "timeout": 30}, timeout=40)
    th.join()
    assert time.monotonic() - t0 < 5
    assert res["status"] == "interrupted" and res["run_id"] == run["run_id"]
    assert res["run"]["started_by"] == by("A") and res["run"]["interrupted_by"] == by("A")

    # A finished run returns at once; unknown runs are not_found.
    t0 = time.monotonic()
    assert b.request("runs.wait", {"run_id": "latest", "timeout": 30}, timeout=40)["status"] \
        == "interrupted"
    assert time.monotonic() - t0 < 1
    expect_error("not_found", b.request, "runs.wait", {"run_id": "r_nope", "timeout": 1})
