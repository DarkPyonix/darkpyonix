"""Shared document, locks and presence in the kernel (SPEC §10a FR-S1..S5, FR-S8; PROTOCOL §4).

``DocumentState`` is driven directly (with a fake clock where time matters) and through a real
``ControlServer`` + ``KernelClient`` pair to prove the methods and events travel over DKP.
"""
from __future__ import annotations

import copy
import os
import time

import pytest

from darkpyonix import format as dpformat
from darkpyonix.kernel import protocol as p
from darkpyonix.kernel.client import KernelClient
from darkpyonix.kernel.collab import DocumentState
from darkpyonix.kernel.events import EventLog
from darkpyonix.kernel.server import ControlServer

KID = "k_0123456789abcdef0123"
KEY = b"k" * 32

NOTEBOOK = (
    '"""Doc."""\n'
    "import darkpyonix\n"
    "\n"
    "\n"
    "# %% Load [code]\n"
    "# @width: 1fr\n"
    "x = 1\n"
    "\n"
    "# %% [markdown]\n"
    'darkpyonix.markdown("""\n'
    "# Title\n"
    '""")\n'
    "\n"
    "# %%\n"
    "# @id: keep-me\n"
    "y = x + 1\r\n"
    "print(y)"
)


def client(cid, permission="editor", user=None, nickname=None):
    return {"client_id": cid, "user": user or ("user-" + cid), "nickname": nickname or ("dev-" + cid),
            "permission": permission}


A = client("A")
B = client("B")


class FakeClock(object):
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


class Recorder(object):
    """An ``emit`` that feeds a real EventLog and remembers the events."""

    def __init__(self):
        self.log = EventLog()
        self.events = []

    def __call__(self, type_, data):
        self.events.append(self.log.append(type_, data))

    def types(self):
        return [e["type"] for e in self.events]

    def of(self, type_):
        return [e["data"] for e in self.events if e["type"] == type_]

    def clear(self):
        self.events = []


def write_file(path, text):
    with open(path, "wb") as f:
        f.write(text.encode("utf-8"))


def external_write(path, text):
    """Write like another editor would, and make sure (mtime, size) moves."""
    before = os.stat(path)
    write_file(path, text)
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns + 2 * 10 ** 9))


def read_file(path):
    with open(path, "rb") as f:
        return f.read().decode("utf-8")


@pytest.fixture
def nb(scratch):
    path = os.path.join(scratch, "nb.py")
    write_file(path, NOTEBOOK)
    return path


def make(nb, clock=None, **kw):
    rec = Recorder()
    ds = DocumentState(nb, rec, clock=clock or FakeClock(), seq_provider=lambda: rec.log.seq, **kw)
    return ds, rec


def ids(ds):
    return [c["cell_id"] for c in ds.snapshot({})["cells"]]


def by_id(snap, cid):
    for c in snap["cells"]:
        if c["cell_id"] == cid:
            return c
    raise KeyError(cid)


def expect_error(code, fn, *args):
    with pytest.raises(p.DKPError) as info:
        fn(*args)
    assert info.value.code == code, info.value.to_dict()
    return info.value


# ------------------------------------------------------------------ a client's replica


class Replica(object):
    """What a client builds from ``doc.snapshot`` plus events (PROTOCOL §4)."""

    def __init__(self, snap):
        self.doc_version = snap["doc_version"]
        self.seq = snap["seq"]
        self.cells = [dict(c) for c in snap["cells"]]
        self.presence = dict((x["client_id"], x) for x in snap["presence"])

    def _pos(self, cid):
        return [c["cell_id"] for c in self.cells].index(cid)

    def apply(self, ev):
        if ev["seq"] is not None and ev["seq"] <= self.seq:
            return
        self.seq = ev["seq"]
        t, d = ev["type"], ev["data"]
        if t == "doc.cell.created":
            self.cells.insert(d["cell"]["index"], dict(d["cell"]))
        elif t == "doc.cell.updated":
            self.cells[self._pos(d["cell"]["cell_id"])] = dict(d["cell"])
        elif t == "doc.cell.deleted":
            del self.cells[self._pos(d["cell_id"])]
        elif t == "doc.cell.moved":
            del self.cells[self._pos(d["cell"]["cell_id"])]
            self.cells.insert(d["cell"]["index"], dict(d["cell"]))
        elif t == "doc.reloaded":
            self.cells = [dict(c) for c in d["cells"]]
        elif t == "doc.lock":
            self.cells[self._pos(d["cell_id"])]["lock"] = d["lock"]
        elif t == "doc.unlock":
            if d["cell_id"] in [c["cell_id"] for c in self.cells]:
                self.cells[self._pos(d["cell_id"])].pop("lock", None)
        elif t == "presence.update":
            self.presence[d["client_id"]] = d
        elif t == "presence.leave":
            self.presence.pop(d["client_id"], None)
        if "doc_version" in d:
            self.doc_version = d["doc_version"]

    def state(self):
        return {"doc_version": self.doc_version,
                "cells": [(c["cell_id"], c["source"], c["version"], c["type"],
                           (c.get("lock") or {}).get("locked_by")) for c in self.cells]}


def state_of(snap):
    return {"doc_version": snap["doc_version"],
            "cells": [(c["cell_id"], c["source"], c["version"], c["type"],
                       (c.get("lock") or {}).get("locked_by")) for c in snap["cells"]]}


# ------------------------------------------------------------------ FR-S1


def test_fr_s1_cell_ids_are_stable_and_never_written(nb):
    ds, rec = make(nb)
    snap = ds.snapshot({})
    cells = snap["cells"]
    assert snap["doc_version"] == 0 and snap["presence"] == []
    assert [c["index"] for c in cells] == [0, 1, 2, 3]
    assert [c["type"] for c in cells] == ["preamble", "code", "markdown", "code"]
    assert cells[3]["cell_id"] == "keep-me"
    assert all(c["cell_id"].startswith("c_") for c in cells[:3])
    assert all(c["version"] == 1 for c in cells)
    assert cells[1]["title"] == "Load" and cells[1]["metadata"] == {"width": "1fr"}
    # An edit elsewhere keeps the ids and does not write generated ids to the file.
    before = ids(ds)
    ds.cell_update({"client": A, "cell_id": before[1], "base_version": 1, "source": "x = 2\n\n"})
    ds.flush()
    assert ids(ds) == before
    text = read_file(nb)
    assert "c_" not in text and text.count("# @id") == 1


def test_fr_s1_snapshot_plus_events_converge(nb):
    ds, rec = make(nb)
    ds.presence_update({"client": B, "focused_cell_id": ids(ds)[1]})
    # Client B joins later than A: both take a snapshot at different moments.
    replica_a = Replica(ds.snapshot({}))
    c1, c2, c3 = ids(ds)[1:]
    ds.cell_create({"client": A, "after": c1, "type": "code", "source": "z = 3\n"})
    replica_b = Replica(ds.snapshot({}))
    new_id = ids(ds)[2]
    ds.lock({"client": A, "cell_id": c3})
    ds.cell_update({"client": A, "cell_id": c3, "base_version": 1, "source": "y = 10\n"})
    ds.unlock({"client": A, "cell_id": c3, "source": "y = 11\n", "base_version": 2})
    ds.cell_move({"client": B, "cell_id": c2, "to_index": 1})
    ds.cell_update({"client": B, "cell_id": new_id, "base_version": 1, "type": "shell",
                    "metadata": {"collapsed": True}})
    ds.cell_delete({"client": A, "cell_id": c1, "base_version": 1})
    ds.cell_create({"client": B, "type": "markdown", "source": "darkpyonix.markdown('end')\n"})

    for ev in rec.events:  # every subscriber sees the whole log; replicas skip what they had
        replica_a.apply(ev)
        replica_b.apply(ev)
    final = ds.snapshot({})
    assert final["seq"] == rec.log.seq
    assert replica_a.state() == state_of(final)
    assert replica_b.state() == state_of(final)
    assert [c["cell_id"] for c in final["cells"]][1:3] == [c2, new_id]
    for ev in rec.events:
        if ev["type"].startswith("doc.cell.") or ev["type"] in ("doc.lock", "doc.unlock"):
            assert set(ev["data"]["by"]) == {"client_id", "user", "nickname"}


# ------------------------------------------------------------------ FR-S2


def test_fr_s2_edit_ops_and_version_conflict(nb):
    ds, rec = make(nb)
    pre, c1, c2, c3 = ids(ds)

    # create: after, before, end
    after = ds.cell_create({"client": A, "after": c1, "type": "code", "source": "a = 1\n"})["cell"]
    before = ds.cell_create({"client": A, "before": c1, "source": "b = 1\n"})["cell"]
    end = ds.cell_create({"client": A, "type": "markdown", "source": "m\n",
                          "metadata": {"collapsed": True}})["cell"]
    order = ids(ds)
    assert order == [pre, before["cell_id"], c1, after["cell_id"], c2, c3, end["cell_id"]]
    assert before["type"] == "code" and end["metadata"] == {"collapsed": True}
    assert rec.of("doc.cell.created")[0]["by"] == {"client_id": "A", "user": "user-A",
                                                  "nickname": "dev-A"}
    assert [d["doc_version"] for d in rec.of("doc.cell.created")] == [1, 2, 3]
    expect_error("bad_request", ds.cell_create, {"client": A, "before": pre, "source": ""})
    expect_error("not_found", ds.cell_create, {"client": A, "after": "nope", "source": ""})

    # update: right version
    cell = ds.cell_update({"client": B, "cell_id": c1, "base_version": 1,
                           "source": "x = 42\n\n"})["cell"]
    assert cell["version"] == 2 and cell["source"] == "x = 42\n\n"
    assert cell["source_sha256"] == dpformat.source_sha256("x = 42\n")
    assert rec.of("doc.cell.updated")[-1]["cell"]["version"] == 2

    # update: stale version → conflict, nothing changes
    dv = ds.snapshot({})["doc_version"]
    n_events = len(rec.events)
    err = expect_error("conflict", ds.cell_update,
                       {"client": A, "cell_id": c1, "base_version": 1, "source": "lost\n"})
    assert err.data["cell"]["version"] == 2 and err.data["cell"]["source"] == "x = 42\n\n"
    assert ds.snapshot({})["doc_version"] == dv and len(rec.events) == n_events
    expect_error("conflict", ds.cell_delete, {"client": A, "cell_id": c1, "base_version": 1})
    expect_error("bad_request", ds.cell_update, {"client": A, "cell_id": c1, "source": "x"})

    # locked by another client → locked for update and delete
    ds.lock({"client": A, "cell_id": c2})
    err = expect_error("locked", ds.cell_update,
                       {"client": B, "cell_id": c2, "base_version": 1, "source": "no\n"})
    assert err.data["locked_by"] == "A"
    expect_error("locked", ds.cell_delete, {"client": B, "cell_id": c2, "base_version": 1})
    assert by_id(ds.snapshot({}), c2)["source"].startswith("darkpyonix.markdown")

    # move and delete
    moved = ds.cell_move({"client": B, "cell_id": c3, "to_index": 1})["cell"]
    assert moved["index"] == 1 and ids(ds)[1] == c3
    assert rec.of("doc.cell.moved")[-1]["cell"]["cell_id"] == c3
    expect_error("bad_request", ds.cell_move, {"client": B, "cell_id": c3, "to_index": 0})
    expect_error("bad_request", ds.cell_move, {"client": B, "cell_id": pre, "to_index": 2})
    assert ds.cell_delete({"client": B, "cell_id": after["cell_id"], "base_version": 1}) == \
        {"deleted": True}
    assert rec.of("doc.cell.deleted")[-1]["cell_id"] == after["cell_id"]
    assert after["cell_id"] not in ids(ds)
    expect_error("bad_request", ds.cell_delete, {"client": A, "cell_id": pre, "base_version": 1})
    expect_error("not_found", ds.cell_update,
                 {"client": A, "cell_id": after["cell_id"], "base_version": 1, "source": ""})
    # the indices in the snapshot follow the order
    assert [c["index"] for c in ds.snapshot({})["cells"]] == list(range(len(ids(ds))))


# ------------------------------------------------------------------ FR-S3


def test_fr_s3_lock_exclusive_idle_release_and_disconnect(nb):
    clock = FakeClock()
    ds, rec = make(nb, clock=clock)
    _, c1, c2, c3 = ids(ds)

    lock = ds.lock({"client": A, "cell_id": c1})["lock"]
    assert lock["locked_by"] == "A" and lock["user"] == "user-A" and lock["cell_id"] == c1
    assert {"locked_at", "last_activity", "expires_at"} <= set(lock)
    assert rec.of("doc.lock")[-1]["lock"]["locked_by"] == "A"
    err = expect_error("locked", ds.lock, {"client": B, "cell_id": c1})
    assert err.data["locked_by"] == "A"
    assert ds.lock({"client": A, "cell_id": c1})["lock"]["locked_by"] == "A"  # renew
    assert by_id(ds.snapshot({}), c1)["lock"]["locked_by"] == "A"

    # Activity by the holder renews the lock; 3 minutes of silence releases it.
    clock.advance(170)
    ds.cell_update({"client": A, "cell_id": c1, "base_version": 1, "source": "x = 5\n"})
    clock.advance(170)
    ds.tick()
    assert "lock" in by_id(ds.snapshot({}), c1)
    clock.advance(11)
    ds.tick()
    assert "lock" not in by_id(ds.snapshot({}), c1)
    unlock = rec.of("doc.unlock")[-1]
    assert unlock["cell_id"] == c1 and unlock["reason"] == "idle" and unlock["by"]["client_id"] == "A"
    assert ds.lock({"client": B, "cell_id": c1})["lock"]["locked_by"] == "B"

    # Explicit release with the final source (2025 cell_unlocked_with_code).
    err = expect_error("locked", ds.unlock, {"client": A, "cell_id": c1})
    expect_error("conflict", ds.unlock,
                 {"client": B, "cell_id": c1, "source": "x = 6\n", "base_version": 1})
    cell = ds.unlock({"client": B, "cell_id": c1, "source": "x = 6\n", "base_version": 2})["cell"]
    assert cell["source"] == "x = 6\n" and cell["version"] == 3 and "lock" not in cell
    assert rec.types()[-2:] == ["doc.cell.updated", "doc.unlock"]
    assert rec.of("doc.unlock")[-1]["reason"] == "released"

    # Disconnect: explicit presence.leave releases every lock of that client.
    ds.presence_update({"client": A})
    ds.lock({"client": A, "cell_id": c2})
    ds.lock({"client": A, "cell_id": c3})
    ds.presence_leave({"client": A})
    reasons = [(d["cell_id"], d["reason"]) for d in rec.of("doc.unlock")[-2:]]
    assert sorted(reasons) == sorted([(c2, "disconnected"), (c3, "disconnected")])
    assert rec.types()[-1] == "presence.leave"
    assert all("lock" not in c for c in ds.snapshot({})["cells"])

    # Disconnect: the presence grace expires (the event stream is gone for 30 s).
    ds.presence_update({"client": B})
    ds.lock({"client": B, "cell_id": c2})
    clock.advance(29)
    ds.tick()
    assert "lock" in by_id(ds.snapshot({}), c2)
    clock.advance(1.5)
    ds.tick()
    assert rec.of("doc.unlock")[-1] == dict(rec.of("doc.unlock")[-1], cell_id=c2,
                                            reason="disconnected")
    assert rec.of("presence.leave")[-1]["client_id"] == "B"
    assert ds.snapshot({})["presence"] == []


# ------------------------------------------------------------------ FR-S4


def test_fr_s4_presence_focus_cursor_and_leave(nb):
    clock = FakeClock()
    ds, rec = make(nb, clock=clock)
    _, c1, c2, _ = ids(ds)
    viewer = client("V", permission="viewer1", nickname="phone")
    viewer["avatar"] = "https://example.invalid/a.png"

    ds.presence_update({"client": viewer})  # join
    joined = rec.of("presence.update")[-1]
    assert joined["client_id"] == "V" and joined["nickname"] == "phone"
    assert joined["avatar"] == "https://example.invalid/a.png"

    ds.presence_update({"client": viewer, "focused_cell_id": c1})
    focus = rec.of("presence.update")[-1]
    assert focus["focused_cell_id"] == c1 and focus["focused_at"]
    snap = ds.snapshot({})["presence"]
    assert len(snap) == 1 and snap[0]["permission"] == "viewer1" and snap[0]["last_seen"]
    expect_error("not_found", ds.presence_update, {"client": viewer, "focused_cell_id": "nope"})
    expect_error("bad_request", ds.presence_update,
                 {"client": viewer, "cursor": {"cell_id": c1, "line": -1, "column": 0}})

    # Heartbeat: no change, no event.
    rec.clear()
    clock.advance(1)
    ds.presence_update({"client": viewer})
    assert rec.events == []

    # Cursor: the first change goes out, changes within 50 ms are coalesced to the latest.
    clock.advance(1)
    for col in range(5):
        ds.presence_update({"client": viewer,
                            "cursor": {"cell_id": c1, "line": 0, "column": col}})
        clock.advance(0.01)
    assert [d["cursor"]["column"] for d in rec.of("presence.update")] == [0]
    clock.advance(0.001)
    ds.tick()  # 50 ms after the first emit
    assert [d["cursor"]["column"] for d in rec.of("presence.update")] == [0, 4]
    ds.tick()
    assert len(rec.of("presence.update")) == 2

    # One second of typing at 100 updates/s stays within 20 events/s (+1 trailing flush).
    rec.clear()
    clock.advance(1)
    start = clock()
    for i in range(100):
        ds.presence_update({"client": viewer, "cursor": {
            "cell_id": c2, "line": 1, "column": i, "selection": [[1, 0], [1, i]]}})
        clock.advance(0.01)
        ds.tick()
    clock.advance(0.06)
    ds.tick()
    emitted = rec.of("presence.update")
    assert len(emitted) <= 21, len(emitted)
    assert emitted[-1]["cursor"] == {"cell_id": c2, "line": 1, "column": 99,
                                     "selection": [[1, 0], [1, 99]]}
    assert clock() - start < 1.1

    # Blur goes out at once.
    rec.clear()
    ds.presence_update({"client": viewer, "focused_cell_id": None})
    assert rec.of("presence.update")[-1]["focused_cell_id"] is None

    # Leave.
    ds.presence_leave({"client": viewer})
    left = rec.of("presence.leave")[-1]
    assert left["client_id"] == "V" and left["user"] == "user-V"
    assert ds.snapshot({})["presence"] == []


# ------------------------------------------------------------------ FR-S5


def test_fr_s5_client_edit_is_saved_byte_exact(nb):
    clock = FakeClock()
    ds, rec = make(nb, clock=clock, save_debounce=0.3)
    original = read_file(nb)
    parsed = dpformat.parse(original)
    _, c1, c2, c3 = ids(ds)

    ds.cell_update({"client": A, "cell_id": c2, "base_version": 1,
                    "source": 'darkpyonix.markdown("""\n# Changed\n""")\n\n'})
    ds.tick()
    assert read_file(nb) == original  # still inside the debounce window
    clock.advance(0.31)
    ds.tick()
    expected = "".join(
        c.header + (c.source if c.index != 2 else 'darkpyonix.markdown("""\n# Changed\n""")\n\n')
        for c in parsed.cells)
    assert read_file(nb) == expected
    assert "\r\n" in read_file(nb)  # untouched CRLF of the last cell survives

    # Our own save is not an external change.
    clock.advance(1)
    ds.tick()
    assert "doc.reloaded" not in rec.types()

    # A new cell gets a proper header; a type/metadata change regenerates only that header.
    ds.cell_create({"client": A, "after": c3, "type": "shell", "source": "ls\n"})
    ds.cell_update({"client": A, "cell_id": c1, "base_version": 1,
                    "metadata": {"width": "2fr", "collapsed": True}})
    ds.flush()
    text = read_file(nb)
    assert text == (parsed.cells[0].header + parsed.cells[0].source
                    + "# %% Load [code]\n# @width: 2fr\n# @collapsed: true\n" + parsed.cells[1].source
                    + '# %% [markdown]\ndarkpyonix.markdown("""\n# Changed\n""")\n\n'
                    + parsed.cells[3].header + parsed.cells[3].source + "\n"
                    + "# %% [shell]\nls\n")
    reparsed = dpformat.parse(text)
    assert [c.type for c in reparsed.cells] == ["preamble", "code", "markdown", "code", "shell"]
    assert reparsed.cells[3].id == "keep-me"
    # no temporary files left behind
    assert sorted(os.listdir(os.path.dirname(nb))) == ["nb.py"]
    clock.advance(1)
    ds.tick()
    assert "doc.reloaded" not in rec.types()


def test_fr_s5_external_edit_reloads_and_locked_cell_conflicts(nb):
    clock = FakeClock()
    ds, rec = make(nb, clock=clock)
    pre, c1, c2, c3 = ids(ds)
    ds.lock({"client": A, "cell_id": c2})
    ds.cell_update({"client": A, "cell_id": c2, "base_version": 1,
                    "source": "darkpyonix.markdown('local')\n\n"})
    ds.flush()
    replica = Replica(ds.snapshot({}))
    rec.clear()

    # Outside: the last cell (with @id) moves to the top, c1 changes, c2 (locked) changes,
    # and a cell is added.
    disk = (
        '"""Doc."""\nimport darkpyonix\n\n\n'
        "# %%\n# @id: keep-me\ny = x + 1\r\nprint(y)\n\n"
        "# %% Load [code]\n# @width: 1fr\nx = 100\n\n"
        "# %% [markdown]\ndarkpyonix.markdown('disk')\n\n"
        "# %% [code]\nnew = True\n"
    )
    external_write(nb, disk)
    clock.advance(0.6)
    ds.tick()

    assert rec.types()[:2] == ["doc.reloaded", "doc.conflict"]
    reloaded = rec.of("doc.reloaded")[0]
    assert reloaded["cause"] == "external"
    cells = reloaded["cells"]
    assert [c["cell_id"] for c in cells][:4] == [pre, c3, c1, c2]
    assert cells[4]["cell_id"] not in (pre, c1, c2, c3) and cells[4]["source"] == "new = True\n"
    assert cells[2]["source"] == "x = 100\n\n" and cells[2]["version"] == 2
    assert cells[1]["version"] == 1  # moved, unchanged
    locked = cells[3]
    assert locked["source"] == "darkpyonix.markdown('local')\n\n" and locked["version"] == 2
    assert locked["conflict"] == {"disk_source": "darkpyonix.markdown('disk')\n\n"}
    assert locked["lock"]["locked_by"] == "A"
    conflict = rec.of("doc.conflict")[0]
    assert conflict == {"cell_id": c2,
                        "local": {"source": "darkpyonix.markdown('local')\n\n", "version": 2,
                                  "by": {"client_id": "A", "user": "user-A", "nickname": "dev-A"}},
                        "disk": {"source": "darkpyonix.markdown('disk')\n\n"}}
    for ev in rec.events:
        replica.apply(ev)
    assert replica.state() == state_of(ds.snapshot({}))
    assert by_id(ds.snapshot({}), c2)["conflict"]

    # An unrelated edit is saved without overwriting the disk version of the conflicted cell.
    ds.cell_update({"client": B, "cell_id": c1, "base_version": 2, "source": "x = 101\n\n"})
    ds.flush()
    text = read_file(nb)
    assert "darkpyonix.markdown('disk')" in text and "x = 101" in text
    assert "local" not in text

    # The holder updates: the conflict clears and the holder's content is saved.
    rec.clear()
    cell = ds.cell_update({"client": A, "cell_id": c2, "base_version": 2,
                           "source": "darkpyonix.markdown('merged')\n\n"})["cell"]
    assert "conflict" not in cell and cell["version"] == 3
    ds.flush()
    assert "darkpyonix.markdown('merged')" in read_file(nb)
    assert "conflict" not in by_id(ds.snapshot({}), c2)
    clock.advance(1)
    ds.tick()
    assert "doc.reloaded" not in rec.types()

    # Unlock without a source also resolves a conflict, keeping the holder's content.
    current = read_file(nb)
    external_write(nb, current.replace("merged", "disk2"))
    clock.advance(1)
    ds.tick()
    assert by_id(ds.snapshot({}), c2)["conflict"] == {
        "disk_source": "darkpyonix.markdown('disk2')\n\n"}
    ds.unlock({"client": A, "cell_id": c2})
    assert "conflict" not in by_id(ds.snapshot({}), c2)
    ds.flush()
    assert "merged" in read_file(nb) and "disk2" not in read_file(nb)

    # A plain external edit of an unlocked cell just reloads.
    rec.clear()
    external_write(nb, read_file(nb).replace("x = 101", "x = 102"))
    clock.advance(1)
    ds.tick()
    assert rec.types() == ["doc.reloaded"]
    assert by_id(ds.snapshot({}), c1)["source"] == "x = 102\n\n"


# ------------------------------------------------------------------ FR-S8


@pytest.mark.parametrize("permission", ["viewer1", "viewer2", "viewer3"])
def test_fr_s8_edit_and_lock_need_editor(nb, permission):
    ds, rec = make(nb)
    _, c1, _, _ = ids(ds)
    viewer = client("V", permission=permission)
    for method, params in [
        (ds.cell_create, {"source": "a\n"}),
        (ds.cell_update, {"cell_id": c1, "base_version": 1, "source": "a\n"}),
        (ds.cell_delete, {"cell_id": c1, "base_version": 1}),
        (ds.cell_move, {"cell_id": c1, "to_index": 2}),
        (ds.lock, {"cell_id": c1}),
        (ds.unlock, {"cell_id": c1, "source": "a\n"}),
    ]:
        expect_error("forbidden", method, dict(params, client=viewer))
    assert rec.events == []
    # but presence is open from viewer1
    assert ds.presence_update({"client": viewer, "focused_cell_id": c1}) == {}
    assert ds.presence_leave({"client": viewer}) == {}


@pytest.mark.parametrize("permission", ["editor", "admin"])
def test_fr_s8_editor_and_admin_edit_and_lock(nb, permission):
    ds, _ = make(nb)
    _, c1, _, _ = ids(ds)
    who = client("E", permission=permission)
    ds.lock({"client": who, "cell_id": c1})
    assert ds.cell_update({"client": who, "cell_id": c1, "base_version": 1,
                           "source": "x = 9\n"})["cell"]["version"] == 2
    ds.unlock({"client": who, "cell_id": c1})
    assert ds.cell_create({"client": who, "source": "a\n"})["cell"]["version"] == 1


def test_fr_s8_client_is_required_and_checked(nb):
    ds, _ = make(nb)
    _, c1, _, _ = ids(ds)
    expect_error("bad_request", ds.lock, {"cell_id": c1})
    expect_error("bad_request", ds.presence_update, {"client": {"permission": "admin"}})
    expect_error("forbidden", ds.lock, {"client": client("X", permission="root"), "cell_id": c1})
    assert {"conflict", "locked", "forbidden"} <= set(p.ERROR_CODES)


# ------------------------------------------------------------------ over DKP


@pytest.fixture
def served(nb):
    events = EventLog()
    ds = DocumentState(nb, events.append, seq_provider=lambda: events.seq,
                       save_debounce=0.3, poll_interval=0.1)
    srv = ControlServer(KID, KEY, events, ds.handlers())
    ds.start()
    srv.start()
    conns = []

    def connect():
        c = KernelClient(srv.port, KID, key=KEY)
        c.connect()
        conns.append(c)
        return c

    yield ds, connect
    for c in conns:
        c.close()
    srv.stop()
    ds.stop()


def _drain(conn, until, timeout=3.0):
    """Events from ``conn`` until one satisfies ``until`` (inclusive)."""
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


def test_fr_s1_methods_and_events_travel_over_dkp(served, nb):
    ds, connect = served
    a, b = connect(), connect()
    assert set(ds.handlers()) == {
        "doc.snapshot", "doc.cell.create", "doc.cell.update", "doc.cell.delete", "doc.cell.move",
        "doc.lock", "doc.unlock", "presence.update", "presence.leave"}

    snap_a = a.request("doc.snapshot", {}, timeout=5)
    a.subscribe(since=snap_a["seq"])
    replica = Replica(snap_a)
    c1 = snap_a["cells"][1]["cell_id"]

    b.request("presence.update", {"client": B, "focused_cell_id": c1}, timeout=5)
    b.request("doc.lock", {"client": B, "cell_id": c1}, timeout=5)
    err = None
    try:
        a.request("doc.lock", {"client": A, "cell_id": c1}, timeout=5)
    except p.DKPError as e:
        err = e
    assert err is not None and err.code == "locked" and err.data["locked_by"] == "B"
    try:
        a.request("doc.cell.create", {"client": client("V", "viewer3"), "source": ""}, timeout=5)
        raise AssertionError("expected forbidden")
    except p.DKPError as e:
        assert e.code == "forbidden"
    b.request("doc.unlock", {"client": B, "cell_id": c1, "source": "x = 7\n\n",
                             "base_version": 1}, timeout=5)
    made = b.request("doc.cell.create", {"client": B, "after": c1, "type": "code",
                                         "source": "w = 1\n"}, timeout=5)["cell"]

    for ev in _drain(a, lambda e: e["type"] == "doc.cell.created"):
        replica.apply(ev)
    assert replica.state() == state_of(a.request("doc.snapshot", {}, timeout=5))
    assert replica.presence["B"]["focused_cell_id"] == c1

    # FR-S5 with the real background thread: saved after the debounce, external edits seen
    # within a second.
    deadline = time.monotonic() + 2
    while "w = 1" not in read_file(nb) and time.monotonic() < deadline:
        time.sleep(0.02)
    assert "x = 7\n\n" in read_file(nb) and "w = 1\n" in read_file(nb)
    time.sleep(0.3)  # let a poll see our own save
    text = read_file(nb)
    external_write(nb, text.replace("w = 1", "w = 2"))
    t0 = time.monotonic()
    evs = _drain(a, lambda e: e["type"] == "doc.reloaded", timeout=2)
    assert time.monotonic() - t0 < 1.0
    assert [e["type"] for e in evs].count("doc.reloaded") == 1
    reloaded = evs[-1]["data"]["cells"]
    assert [c["source"] for c in reloaded if c["cell_id"] == made["cell_id"]] == ["w = 2\n"]
