"""A notebook file with its latest outputs mapped onto its cells (SPEC FR-R4).

Standard library only; Python 3.8+. Used by the manager's ``/document`` endpoints; reads
``__runs__/`` directly, so it works whether or not the file's kernel is running.

Which outputs a cell gets: run logs are taken newest first by ``started_at`` (an in-progress
or crashed run counts; it is the newest thing that happened to the file). Within one log,
records are matched to cells by ``id``, then ``source_sha256``, then ``index``, each record
used at most once. A cell the newest run did not execute (e.g. a ``mode: cells`` run of
other cells) takes its outputs from the newest older run that did, so ``run_id`` can differ
between cells. At most ``MAX_RUNS_SCANNED`` logs are read. A matched record whose
``source_sha256`` differs from the cell's is marked ``stale: true``.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from darkpyonix import format as dpformat
from darkpyonix import _runs as _runs
from darkpyonix._protocol import kernel_id_for

MAX_RUNS_SCANNED = 20


def _records(nb: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = []
    for cell in nb.get("cells") or []:
        if not isinstance(cell, dict):
            continue
        dp = (cell.get("metadata") or {}).get("darkpyonix") or {}
        if not isinstance(dp, dict) or not isinstance(dp.get("index"), int):
            continue
        out.append({
            "index": dp["index"], "id": dp.get("id"), "source_sha256": dp.get("source_sha256"),
            "status": dp.get("status"), "execution_count": cell.get("execution_count"),
            "outputs": cell.get("outputs") or [],
        })
    return out


def _map_run(cells: List[Any], pending: List[int],
             records: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """Match records to the cells at positions ``pending``: id → source_sha256 → index.

    The index fallback skips a record whose ``id`` names another cell of the file: that
    record belongs to the cell with the id, wherever it has moved.
    """
    doc_ids = set(c.id for c in cells if c.id)
    found = {}   # type: Dict[int, Dict[str, Any]]
    used = set()  # type: set

    def take(pos: int, pred) -> None:
        for i, rec in enumerate(records):
            if i not in used and pred(rec):
                used.add(i)
                found[pos] = rec
                return

    for pos in pending:
        cid = cells[pos].id
        if cid:
            take(pos, lambda r, cid=cid: r["id"] == cid)
    for pos in pending:
        if pos not in found:
            sha = cells[pos].source_sha256
            take(pos, lambda r, sha=sha: r["source_sha256"] == sha)
    for pos in pending:
        if pos not in found:
            idx = cells[pos].index
            take(pos, lambda r, idx=idx: r["index"] == idx
                 and not (r["id"] and r["id"] in doc_ids and r["id"] != cells[pos].id))
    return found


def build_document(path: str, kernel_id: Optional[str] = None,
                   viewer_outputs: bool = True) -> Dict[str, Any]:
    """The OpenAPI ``Document`` for ``path``. ``viewer_outputs=False`` omits outputs (viewer1)."""
    full = os.path.abspath(path)
    doc = dpformat.load(full)
    store = _runs.RunStore(full, kernel_id or kernel_id_for(full))
    summaries = store.list(limit=MAX_RUNS_SCANNED)

    cells = list(doc.cells)
    mapped = {}   # type: Dict[int, Dict[str, Any]]
    pending = list(range(len(cells)))
    for summary in summaries:
        if not pending:
            break
        try:
            nb = store.get(summary["run_id"])
        except Exception:
            continue
        found = _map_run(cells, pending, _records(nb))
        for pos, rec in found.items():
            rec["run_id"] = summary["run_id"]
            mapped[pos] = rec
        pending = [p for p in pending if p not in found]

    out_cells = []
    for pos, cell in enumerate(cells):
        rec = mapped.get(pos)
        item = {
            "index": cell.index, "type": cell.type, "title": cell.title,
            "source": cell.source, "source_sha256": cell.source_sha256,
            "metadata": dict(cell.metadata),
            "outputs": list(rec["outputs"]) if rec else [],
            "execution_count": rec["execution_count"] if rec else None,
            "status": rec["status"] if rec else None,
            "stale": bool(rec) and rec["source_sha256"] != cell.source_sha256,
            "run_id": rec["run_id"] if rec else None,
        }  # type: Dict[str, Any]
        if not viewer_outputs:
            del item["outputs"]
        out_cells.append(item)

    return {
        "path": full,
        "kernel_id": store.kernel_id,
        "file_sha256": doc.file_sha256,
        "latest_run": summaries[0] if summaries else None,
        "cells": out_cells,
    }
