"""WebSocket history and execution counts follow the FR-R4 cell matching (SPEC FR-S9, K2)."""
from __future__ import annotations

import json
import time

import pytest

from darkpyonix import _wsync as ws

from test_fr_s9_ws import connect, who, kernel  # noqa: F401


def snap(*cells):
    return [{"cell_id": cid, "index": i, "source_sha256": sha} for i, (cid, sha) in enumerate(cells)]


def log(*recs):
    return {"cells": [{"execution_count": n, "outputs": [],
                       "metadata": {"darkpyonix": {"index": i, "id": rid, "source_sha256": sha,
                                                   "ended_at": "t"}}}
                      for n, (i, rid, sha) in enumerate(recs, 1)]}


def test_fr_s9_match_inserted_cell_above_keeps_ids():
    # Run: index 0 pre, 1 "a", 2 "b". Then a cell "n" is inserted above "a".
    run = log((0, None, "hp"), (1, "a", "ha"), (2, "b", "hb"))
    doc = snap(("p", "hp"), ("n", "hn"), ("a", "ha"), ("b", "hb"))
    got = [(c and c["cell_id"], stale) for c, _r, stale in ws.match_run_cells(doc, run)]
    assert got == [("p", False), ("a", False), ("b", False)]


def test_fr_s9_match_by_hash_when_ids_are_absent():
    run = log((0, None, "hp"), (1, None, "ha"), (2, None, "hb"))
    doc = snap(("p", "hp"), ("n", "hn"), ("a", "ha"), ("b", "hb"))
    got = [(c and c["cell_id"], stale) for c, _r, stale in ws.match_run_cells(doc, run)]
    assert got == [("p", False), ("a", False), ("b", False)]


def test_fr_s9_match_edited_cell_falls_back_to_index_and_is_stale():
    run = log((0, None, "hp"), (1, None, "ha"))
    doc = snap(("p", "hp"), ("a", "ha-edited"))
    got = [(c and c["cell_id"], stale) for c, _r, stale in ws.match_run_cells(doc, run)]
    assert got == [("p", False), ("a", True)]


def test_fr_s9_match_stale_index_match_lands_on_the_cell_now_there():
    run = log((0, None, "hp"), (1, None, "ha"))
    doc = snap(("p", "hp"), ("n", "hn"), ("a", "ha-edited"))
    got = [(c and c["cell_id"], stale) for c, _r, stale in ws.match_run_cells(doc, run)]
    assert got[0] == ("p", False)
    # Hash fails everywhere, so the record falls to index 1, now the inserted "n", flagged stale;
    # the moved and edited "a" gets nothing.
    assert got[1] == ("n", True)


def test_fr_s9_match_no_cross_matching_of_ids():
    # Record "a" (deleted from the file) must not land on cell "b", which has its own record.
    run = log((1, "a", "ha"), (2, "b", "hb"))
    doc = snap(("p", "hp"), ("b", "hb-edited"))
    out = ws.match_run_cells(doc, run)
    assert out[0][0] is None and out[0][2] is True
    assert out[1][0]["cell_id"] == "b" and out[1][2] is True


def test_fr_s9_match_record_names_another_cell_is_not_index_matched():
    run = log((1, "a", "ha"))
    doc = snap(("p", "hp"), ("b", "hb"), ("a", "ha-edited"))
    out = ws.match_run_cells(doc, run)
    assert out[0][0]["cell_id"] == "a"  # by id, at its new position
    assert out[0][2] is True


def test_fr_s9_history_survives_a_cell_inserted_after_the_run(kernel):
    info, client = kernel
    run_id = client.request("run", {"mode": "all"})["run_id"]
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        runs = client.request("runs.list", {"limit": 1})["runs"]
        if runs and runs[0]["status"] == "ok":
            break
        time.sleep(0.1)
    before = [c for c in client.request("doc.snapshot", {})["cells"]]
    hello = [c for c in before if "hello" in c["source"]][0]["cell_id"]
    y = [c for c in before if "y = x + 1" in c["source"]][0]["cell_id"]
    client.request("doc.cell.create", {"client": who("dev_zzzzzzzz"), "before": hello,
                                       "source": "inserted = 1\n"})
    a = connect(info)
    a.send({"type": "request_history"})
    hist = a.until("history_data")["history"]
    owners = {}
    for e in hist:
        owners[e["cell_id"]] = e
    assert hello in owners and y in owners
    assert "hello from the kernel" in json.dumps(owners[hello]["outputs"])
    assert all(e["stale"] is False for e in hist)
    a.send({"type": "request_code"})
    code = a.until("code_data")["cells"]
    ids = [c["cell_id"] for c in code]
    new_cell = [c for c in code if "inserted" in c["source"]][0]
    assert new_cell["execution_count"] is None
    assert [c for c in code if c["cell_id"] == hello][0]["execution_count"] == \
        owners[hello]["execution_count"]
    assert len(ids) == len(set(ids))
    a.close()

