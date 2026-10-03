"""SPEC §4 run logs: FR-R1 … FR-R5 (RunStore, __runs__, document mapping)."""
from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import sys
import threading
import time

import pytest

from conftest import KERNEL_ROOT
from darkpyonix import format as dpformat
from darkpyonix.kernel import document, runs
from darkpyonix.kernel.model import CellRecord, Run, RunRequest
from darkpyonix.kernel.protocol import DKPError, kernel_id_for, new_run_id, now_iso


def sha(text):
    return hashlib.sha256(text.replace("\r\n", "\n").encode("utf-8")).hexdigest()


PY = {"version": "3.11.4", "executable": sys.executable, "implementation": "cpython"}


def make_run(path, run_id=None, mode="all", cells=None, params=None):
    req = RunRequest(run_id or new_run_id(), mode=mode, cells=cells, params=params)
    run = Run(req, kernel_id_for(path), os.path.abspath(path), "f" * 64, PY, "testhost")
    run.status = "running"
    run.started_at = now_iso()
    return run


def _source_sha(source):
    try:
        from darkpyonix.format._parser import source_sha256
    except ImportError:
        return sha(source)
    return source_sha256(source)


def add_cell(run, index, source, outputs=(), type="code", cell_id=None, status="ok", count=None):
    # Records carry the parser's hash (FORMAT §2.4), as the executor will record it.
    rec = CellRecord(index, type, source, _source_sha(source), title="cell %d" % index, cell_id=cell_id)
    rec.started_at = now_iso()
    rec.outputs.extend(outputs)
    rec.execution_count = count if count is not None else index
    rec.status = status
    rec.ended_at = now_iso()
    run.cells.append(rec)
    return rec


def finish(store, run, status="ok"):
    run.status = status
    run.ended_at = now_iso()
    store.finish(run)


def stream(text, name="stdout"):
    return {"output_type": "stream", "name": name, "text": text}


def notebook_file(scratch, name="train.py"):
    path = os.path.join(scratch, name)
    with open(path, "w") as f:
        f.write("import os\n")
    return path


def test_runs_dir_is_beside_the_file(scratch):
    path = os.path.join(scratch, "exp", "train.py")
    assert runs.runs_dir_for(path) == os.path.join(scratch, "exp", "__runs__", "train.py") + os.sep


# ---------------------------------------------------------------- FR-R1

def test_fr_r1_run_log_is_valid_nbformat(scratch):
    path = notebook_file(scratch)
    store = runs.RunStore(path, kernel_id_for(path))
    run = make_run(path, params={"lr": 0.1})
    store.begin(run)
    add_cell(run, 0, "import os\n")
    add_cell(run, 1, "print('hi')\n1 + 1", [
        stream("hi\n"),
        {"output_type": "execute_result", "execution_count": 1, "metadata": {},
         "data": {"text/plain": "2"}},
    ], cell_id="c-3f2a")
    add_cell(run, 2, "darkpyonix.markdown('# T')", [
        {"output_type": "display_data", "metadata": {}, "data": {"text/markdown": "# T"}}],
        type="markdown")
    add_cell(run, 3, "1/0", [{"output_type": "error", "ename": "ZeroDivisionError",
                              "evalue": "division by zero", "traceback": ["..."]}],
             status="error")
    finish(store, run, "error")

    log = os.path.join(store.dir, run.run_id + ".ipynb")
    with open(log, encoding="utf-8") as f:
        nb = json.load(f)
    assert nb["nbformat"] == 4 and nb["nbformat_minor"] == 5
    meta = nb["metadata"]["darkpyonix"]
    for key in ("run_id", "kernel_id", "file", "file_sha256", "mode", "params", "status",
                "started_at", "ended_at", "python", "host"):
        assert key in meta, key
    assert meta["run_id"] == run.run_id and meta["status"] == "error"
    assert meta["params"] == {"lr": 0.1}
    assert nb["metadata"]["language_info"]["version"] == "3.11.4"
    assert [c["cell_type"] for c in nb["cells"]] == ["code"] * 4
    for c in nb["cells"]:
        dp = c["metadata"]["darkpyonix"]
        for key in ("index", "type", "title", "source_sha256", "status", "started_at", "ended_at"):
            assert key in dp, key
    assert nb["cells"][1]["metadata"]["darkpyonix"]["id"] == "c-3f2a"
    assert nb["cells"][2]["metadata"]["darkpyonix"]["type"] == "markdown"
    assert nb["cells"][1]["outputs"][0]["text"] == "hi\n"
    assert runs.to_notebook(run) == nb

    nbformat = pytest.importorskip("nbformat")
    nbformat.validate(nb)
    nbformat.validate(nbformat.from_dict(nb))


# ---------------------------------------------------------------- FR-R2

_CHILD = r"""
import os, sys, time
sys.path.insert(0, %(root)r)
from darkpyonix.kernel import runs
from darkpyonix.kernel.model import CellRecord, Run, RunRequest
from darkpyonix.kernel.protocol import kernel_id_for, new_run_id, now_iso
path = %(path)r
store = runs.RunStore(path, kernel_id_for(path))
run = Run(RunRequest(new_run_id()), kernel_id_for(path), path, "0" * 64,
          {"version": sys.version.split()[0]}, "h")
run.status = "running"
run.started_at = now_iso()
rec = CellRecord(1, "code", "loop()", "0" * 64)
rec.started_at = now_iso()
run.cells.append(rec)
store.begin(run)
print(run.run_id, flush=True)
while True:
    rec.outputs.append({"output_type": "stream", "name": "stdout", "text": "t=%%.3f\n" %% time.time()})
    store.update(run)
    time.sleep(0.01)
"""


def test_fr_r2_log_survives_kernel_kill(scratch, python):
    path = notebook_file(scratch)
    child = subprocess.Popen([python, "-c", _CHILD % {"root": KERNEL_ROOT, "path": path}],
                             stdout=subprocess.PIPE, text=True)
    try:
        run_id = child.stdout.readline().strip()
        time.sleep(2.6)
        killed_at = time.time()
        os.kill(child.pid, signal.SIGKILL)
        child.wait(10)
    finally:
        if child.poll() is None:
            child.kill()
        child.stdout.close()

    store = runs.RunStore(path, kernel_id_for(path))
    with open(os.path.join(store.dir, run_id + ".ipynb"), encoding="utf-8") as f:
        nb = json.load(f)
    assert nb["metadata"]["darkpyonix"]["status"] == "running"
    texts = [o["text"] for o in nb["cells"][0]["outputs"]]
    last = float(texts[-1].strip().split("=")[1])
    assert last >= killed_at - 1.3, (killed_at - last)

    assert store.recover_crashed() == [run_id]
    assert store.get(run_id)["metadata"]["darkpyonix"]["status"] == "crashed"
    assert store.list()[0]["status"] == "crashed"
    assert store.get("latest")["metadata"]["darkpyonix"]["run_id"] == run_id
    assert not [n for n in os.listdir(store.dir) if n.endswith(".tmp")]
    assert store.recover_crashed() == []


def test_fr_r2_recover_leaves_this_processes_current_run_alone(scratch):
    path = notebook_file(scratch)
    store = runs.RunStore(path, kernel_id_for(path))
    run = make_run(path)
    store.begin(run)
    assert store.recover_crashed() == []
    assert store.get(run.run_id)["metadata"]["darkpyonix"]["status"] == "running"


def test_fr_r2_update_is_throttled(scratch):
    path = notebook_file(scratch)
    store = runs.RunStore(path, kernel_id_for(path))
    run = make_run(path)
    rec = add_cell(run, 1, "loop()")
    store.begin(run)
    log = os.path.join(store.dir, run.run_id + ".ipynb")

    seen = []
    stop = threading.Event()

    def watch():
        last = None
        while not stop.is_set():
            try:
                st = os.stat(log)
                sig = (st.st_ino, st.st_mtime_ns, st.st_size)
            except OSError:
                sig = None
            if sig != last:
                seen.append(time.monotonic())
                last = sig
            time.sleep(0.002)

    t = threading.Thread(target=watch)
    t.start()
    start = time.monotonic()
    n = 0
    while time.monotonic() - start < 3.0:
        rec.outputs.append(stream("line %d\n" % n))
        store.update(run)
        n += 1
        time.sleep(0.001)
    time.sleep(1.2)
    stop.set()
    t.join()
    writes = len(seen) - 1          # the first sample is the file written by begin()
    assert n > 500
    assert 2 <= writes <= 5, writes  # ~3 s of updates → ~3 rewrites, never one per update
    gaps = [b - a for a, b in zip(seen[1:], seen[2:])]
    assert all(g > 0.9 for g in gaps), gaps
    with open(log, encoding="utf-8") as f:
        assert len(json.load(f)["cells"][0]["outputs"]) == n   # the last update was written


# ---------------------------------------------------------------- FR-R3

def test_fr_r3_runs_magic_exposes_logs_as_json(scratch):
    path = notebook_file(scratch)
    store = runs.RunStore(path, kernel_id_for(path))
    magic = store.magic()
    assert magic.current is None and magic.latest is None and magic.list() == []
    assert magic.dir == store.dir

    first = make_run(path)
    store.begin(first)
    add_cell(first, 1, "print(1)", [stream("1\n")])
    finish(store, first)

    second = make_run(path)
    while second.run_id == first.run_id:
        second = make_run(path)
    store.begin(second)
    add_cell(second, 1, "print(2)", [stream("2\n")])
    store.update(second)

    assert magic.latest["metadata"]["darkpyonix"]["run_id"] == first.run_id
    assert magic.current["metadata"]["darkpyonix"]["run_id"] == second.run_id
    assert magic.current["cells"][0]["outputs"][0]["text"] == "2\n"
    assert magic[-1]["metadata"]["darkpyonix"]["run_id"] == first.run_id
    assert magic[first.run_id]["cells"][0]["outputs"] == [stream("1\n")]
    assert [s["run_id"] for s in magic.list()] == [second.run_id, first.run_id]
    assert magic.list(limit=1)[0]["path"] == os.path.join(store.dir, second.run_id + ".ipynb")
    for value in (magic.current, magic.latest, magic[-1], magic[first.run_id], magic.list(), magic.dir):
        json.dumps(value)

    got = magic.latest
    got["metadata"]["darkpyonix"]["status"] = "tampered"
    got["cells"].clear()
    cur = magic.current
    cur["cells"].clear()
    assert magic.latest["metadata"]["darkpyonix"]["status"] == "ok"
    assert len(magic.current["cells"]) == 1 and len(second.cells) == 1

    with pytest.raises(KeyError):
        magic["20000101-000000-0000"]
    with pytest.raises(KeyError):
        magic["../../etc/passwd"]
    with pytest.raises(IndexError):
        magic[-5]
    assert "__runs__" in repr(magic) and first.run_id in repr(magic)

    with pytest.raises(DKPError) as e:
        store.get("nope")
    assert e.value.code == "not_found"
    finish(store, second)
    assert magic.current is None
    assert magic.latest["metadata"]["darkpyonix"]["run_id"] == second.run_id


R3_NB = """\
import sys

# %% greet
# @id: "c-greet"
x = 21
print("hello", x)
print("oops", file=sys.stderr)
print("again")

# %% answer
x * 2

# %% check
import json
r = __runs__.latest
c = __runs__.current
out = {
    "is_dict": isinstance(r, dict) and isinstance(r.cells[1], dict),
    "latest_id": r.run_id, "status": r.status, "params": r.params,
    "positions": [cell.index for cell in r.cells],
    "text": r.cells[1].text, "stderr": r.cells[1].stderr,
    "preamble_text": r.cells[0].text, "result": r.cells[2].result,
    "no_result": r.cells[1].result, "stream_name": r.cells[1].outputs[0].name,
    "by_title": r.cell("greet").index, "by_index": r.cell(2).title,
    "by_id": r.cell("c-greet").title,
    "path": r.path, "file_equals_notebook": json.load(open(r.path)) == r.notebook,
    "notebook_is_plain": type(r.notebook) is dict and type(r.notebook["cells"][0]) is dict,
    "dumps_roundtrip": json.loads(json.dumps(r)) == r.notebook,
    "current_id": c.run_id, "current_path": c.path,
    "by_id_id": __runs__[r.run_id].run_id, "last_id": __runs__[-1].run_id,
    "list_ids": [s.run_id for s in __runs__.list()], "list_path": __runs__.list()[1].path,
}
try:
    r.cell("nope")
    out["missing_cell"] = "no error"
except KeyError:
    out["missing_cell"] = "KeyError"
try:
    r.no_such_field
    out["missing_attr"] = "no error"
except AttributeError:
    out["missing_attr"] = "AttributeError"
print(json.dumps(out))
"""


def test_fr_r3_runs_magic_attribute_access_in_kernel_cell(scratch, dp_home, python):
    from darkpyonix.kernel import launcher
    from darkpyonix.kernel.client import KernelClient

    path = os.path.join(scratch, "train.py")
    with open(path, "w") as f:
        f.write(R3_NB)
    pid = launcher.launch(path, python=python)
    try:
        info = launcher.wait_for_announce(kernel_id_for(path), pid=pid, timeout=15.0)
        assert info is not None, "kernel did not announce"
        c = KernelClient(info["port"], info["kernel_id"], name="test", kind="cli")
        c.connect()
        c.subscribe()

        def run(cells):
            acc = c.request("run", {"mode": "cells", "cells": cells})
            deadline = time.time() + 20
            while time.time() < deadline:
                ev = c.next_event(timeout=0.5)
                if ev and ev["type"] == "run.finished" and ev["data"]["run_id"] == acc["run_id"]:
                    if ev["data"]["status"] != "ok":
                        log = c.request("runs.get", {"run_id": acc["run_id"]})
                        raise AssertionError([o for cell in log["cells"] for o in cell["outputs"]])
                    return acc["run_id"]
            raise AssertionError("run %s did not finish" % acc["run_id"])

        first = run([1, 2])
        second = run([3])
        nb = c.request("runs.get", {"run_id": second})
        outs = [o for cell in nb["cells"] for o in cell["outputs"]]
        text = "".join(o["text"] for o in outs if o["output_type"] == "stream" and o["name"] == "stdout")
        assert text and not [o for o in outs if o["output_type"] == "error"], nb
        got = json.loads(text)
        c.close()
    finally:
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass

    log_dir = runs.runs_dir_for(path)
    assert got == {
        "is_dict": True,
        "latest_id": first, "status": "ok", "params": {},
        "positions": [0, 1, 2],
        "text": "hello 21\nagain\n", "stderr": "oops\n",
        "preamble_text": "", "result": "42",
        "no_result": None, "stream_name": "stdout",
        "by_title": 1, "by_index": "answer", "by_id": "greet",
        "path": os.path.join(log_dir, first + ".ipynb"), "file_equals_notebook": True,
        "notebook_is_plain": True, "dumps_roundtrip": True,
        "current_id": second, "current_path": os.path.join(log_dir, second + ".ipynb"),
        "by_id_id": first, "last_id": first,
        "list_ids": [second, first], "list_path": os.path.join(log_dir, first + ".ipynb"),
        "missing_cell": "KeyError", "missing_attr": "AttributeError",
    }


def test_fr_r1_index_lists_runs_newest_first_and_is_rebuilt_when_corrupt(scratch):
    path = notebook_file(scratch)
    store = runs.RunStore(path, kernel_id_for(path))
    ids = []
    for i in range(3):
        run = make_run(path, run_id="20261003-14223%d-a1f%d" % (i, i))
        run.started_at = "2026-10-03T14:22:3%d.000Z" % i
        store.begin(run)
        finish(store, run)
        ids.append(run.run_id)
    index = os.path.join(store.dir, "index.json")
    with open(index, encoding="utf-8") as f:
        data = json.load(f)
    assert [r["run_id"] for r in data["runs"]] == ids[::-1]
    for r in data["runs"]:
        for key in ("run_id", "status", "started_at", "ended_at", "mode"):
            assert key in r

    with open(index, "w") as f:
        f.write("{not json")
    fresh = runs.RunStore(path, kernel_id_for(path))
    listed = fresh.list()
    assert [r["run_id"] for r in listed] == ids[::-1]
    assert all(r["status"] == "ok" and os.path.isfile(r["path"]) for r in listed)
    with open(index, encoding="utf-8") as f:
        assert [r["run_id"] for r in json.load(f)["runs"]] == ids[::-1]

    os.remove(index)
    os.remove(os.path.join(store.dir, ids[0] + ".ipynb"))
    assert [r["run_id"] for r in runs.RunStore(path, "k").list()] == [ids[2], ids[1]]
    assert [r["run_id"] for r in runs.RunStore(path, "k").list(limit=1)] == [ids[2]]


def test_d8_run_store_never_writes_a_gitignore(scratch):
    path = notebook_file(scratch)
    store = runs.RunStore(path, kernel_id_for(path))
    run = make_run(path)
    store.begin(run)
    add_cell(run, 1, "x", [stream("y\n")])
    store.update(run)
    finish(store, run)
    store.recover_crashed()
    store.list()
    found = []
    for root, _dirs, files in os.walk(scratch):
        found += [os.path.join(root, f) for f in files if f == ".gitignore"]
    assert found == []


# ---------------------------------------------------------------- FR-R4

SOURCE = """import darkpyonix


# %% [code]
x = 1
print(x)

# %% [code]
# @id: "keep"
y = 2

# %% [code]
z = 3
"""


def _load(path, monkeypatch):
    """The real parser once #8 lands; until then a hand-built document of SOURCE's shape."""
    try:
        return dpformat.load(path)
    except NotImplementedError:
        pass

    def fake_load(p):
        text = open(p, encoding="utf-8").read()
        parts = text.split("# %% [code]\n")
        cells = [dpformat.Cell(0, "preamble", parts[0], source_sha256=sha(parts[0]))]
        for i, body in enumerate(parts[1:], 1):
            meta, cid = {}, None
            if body.startswith("# @id:"):
                line, body = body.split("\n", 1)
                cid = json.loads(line.split(":", 1)[1])
                meta = {"id": cid}
            cells.append(dpformat.Cell(i, "code", body, raw_type="code", metadata=meta,
                                       source_sha256=sha(body), id=cid))
        return dpformat.NotebookDocument(p, cells, sha(text))

    monkeypatch.setattr(dpformat, "load", fake_load)
    return fake_load(path)


def test_fr_r4_document_maps_latest_outputs_and_marks_stale(scratch, monkeypatch):
    path = os.path.join(scratch, "train.py")
    with open(path, "w") as f:
        f.write(SOURCE)
    doc = _load(path, monkeypatch)
    store = runs.RunStore(path, kernel_id_for(path))

    old = make_run(path, run_id="20261003-100000-0001")
    old.started_at = "2026-10-03T10:00:00.000Z"
    store.begin(old)
    for c in doc.cells:
        add_cell(old, c.index, c.source, [stream("old %d\n" % c.index)], cell_id=c.id)
    finish(store, old)

    new = make_run(path, run_id="20261003-110000-0002", mode="cells", cells=[1, 2])
    new.started_at = "2026-10-03T11:00:00.000Z"
    store.begin(new)
    for c in doc.cells[1:3]:
        add_cell(new, c.index, c.source, [stream("new %d\n" % c.index)], cell_id=c.id,
                 count=10 + c.index)
    finish(store, new)

    # Edit cell 1 and insert a cell before "keep": ids and hashes still find their records.
    with open(path, "w") as f:
        f.write(SOURCE.replace("x = 1\n", "x = 100\n").replace(
            "# %% [code]\n# @id", "# %% [code]\nw = 0\n\n# %% [code]\n# @id"))

    got = document.build_document(path)
    assert got["path"] == os.path.abspath(path)
    assert got["kernel_id"] == kernel_id_for(path)
    assert got["latest_run"]["run_id"] == new.run_id
    assert got["file_sha256"] == dpformat.load(path).file_sha256
    by_src = dict((c["source"].strip(), c) for c in got["cells"])

    edited = by_src["x = 100\nprint(x)"]
    assert edited["stale"] is True and edited["run_id"] == new.run_id
    assert edited["outputs"] == [stream("new 1\n")] and edited["execution_count"] == 11
    assert edited["status"] == "ok"

    keep = by_src["y = 2"]          # moved from index 2 to 3, matched by id
    assert keep["stale"] is False and keep["outputs"] == [stream("new 2\n")]
    assert keep["index"] == 3 and keep["run_id"] == new.run_id

    z = by_src["z = 3"]             # moved from index 3 to 4, matched by hash in the older run
    assert z["stale"] is False and z["outputs"] == [stream("old 3\n")]
    assert z["run_id"] == old.run_id

    assert by_src["import darkpyonix"]["outputs"] == [stream("old 0\n")]
    inserted = by_src["w = 0"]      # index 2 now; the old index-2 record carries id "keep"
    assert inserted["outputs"] == [] and inserted["run_id"] is None
    assert inserted["stale"] is False and inserted["status"] is None
    assert sum(1 for c in got["cells"] if c["stale"]) == 1
    for c in got["cells"]:
        for key in ("index", "type", "source", "source_sha256", "metadata"):
            assert key in c
    json.dumps(got)

    viewer = document.build_document(path, kernel_id="k_x", viewer_outputs=False)
    assert viewer["kernel_id"] == "k_x"
    assert all("outputs" not in c for c in viewer["cells"])


def test_fr_r4_document_without_runs(scratch, monkeypatch):
    path = os.path.join(scratch, "fresh.py")
    with open(path, "w") as f:
        f.write(SOURCE)
    _load(path, monkeypatch)
    got = document.build_document(path)
    assert got["latest_run"] is None
    assert all(c["outputs"] == [] and c["stale"] is False for c in got["cells"])
    assert not os.path.exists(os.path.join(scratch, "__runs__"))


# ---------------------------------------------------------------- FR-R5

def test_fr_r5_oversized_stream_spills_to_sidecar(scratch, monkeypatch):
    monkeypatch.setenv("DARKPYONIX_RUN_OUTPUT_LIMIT", "1000")
    path = notebook_file(scratch)
    store = runs.RunStore(path, kernel_id_for(path))
    run = make_run(path)
    rec = add_cell(run, 2, "spam()")
    store.begin(run)
    chunks = []
    for i in range(60):
        text = "line %03d é—%s\n" % (i, "x" * 80)
        chunks.append(text)
        rec.outputs.append(stream(text, "stderr" if i % 7 == 0 else "stdout"))
        if i == 5:
            rec.outputs.append({"output_type": "display_data", "metadata": {},
                                "data": {"text/plain": "img"}})
        if i in (5, 20, 40):
            store.update(run)
            store.flush()
    finish(store, run)

    with open(os.path.join(store.dir, run.run_id + ".ipynb"), encoding="utf-8") as f:
        nb = json.load(f)
    outs = nb["cells"][0]["outputs"]
    notices = [o for o in outs if o["output_type"] == "stream" and "[darkpyonix]" in o["text"]]
    assert len(notices) == 1
    assert run.run_id + ".cell2.log" in notices[0]["text"]
    kept = "".join(o["text"] for o in outs if o["output_type"] == "stream" and o not in notices)
    assert len(kept.encode("utf-8")) <= 1000
    assert any(o["output_type"] == "display_data" for o in outs)
    with open(os.path.join(store.dir, run.run_id + ".cell2.log"), encoding="utf-8") as f:
        rest = f.read()
    assert kept + rest == "".join(chunks)
    assert len(rec.outputs) == 61   # the in-memory run is untouched
    pytest.importorskip("nbformat").validate(nb)


def test_fr_r5_twenty_mib_cell_log_stays_under_seventeen_mib(scratch):
    path = notebook_file(scratch)
    store = runs.RunStore(path, kernel_id_for(path))
    run = make_run(path)
    rec = add_cell(run, 1, "big()")
    store.begin(run)
    line = "0123456789abcdef" * 64 + "\n"            # 1025 bytes
    for _ in range(20):
        rec.outputs.append(stream(line * 1024))      # ~1 MiB per output, 20 MiB total
    store.update(run)
    finish(store, run)
    log = os.path.join(store.dir, run.run_id + ".ipynb")
    side = os.path.join(store.dir, run.run_id + ".cell1.log")
    assert os.path.getsize(log) < 17 * 1024 * 1024
    with open(log, encoding="utf-8") as f:
        outs = json.load(f)["cells"][0]["outputs"]
    kept = "".join(o["text"] for o in outs[:-1])
    with open(side, encoding="utf-8") as f:
        assert kept + f.read() == line * 1024 * 20
