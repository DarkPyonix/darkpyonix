"""Run and cell records shared by the executor, the run store and the control channel.

Standard library only; Python 3.8+. A ``Run`` is one run request (SPEC FR-X1); its notebook
form is the run log of FR-R1.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

RUN_STATUSES = ("queued", "running", "ok", "error", "interrupted", "cancelled", "crashed")
CELL_STATUSES = ("ok", "error", "interrupted")


class RunRequest(object):
    __slots__ = ("run_id", "mode", "cells", "source", "params", "on_busy")

    def __init__(self, run_id: str, mode: str = "all", cells: Optional[List[int]] = None,
                 source: Optional[str] = None, params: Optional[Dict[str, Any]] = None,
                 on_busy: str = "reject") -> None:
        if mode not in ("all", "cells"):
            raise ValueError("mode must be 'all' or 'cells'")
        if on_busy not in ("reject", "queue"):
            raise ValueError("on_busy must be 'reject' or 'queue'")
        if mode == "cells" and not cells:
            raise ValueError("mode 'cells' needs a non-empty 'cells' list")
        self.run_id = run_id
        self.mode = mode
        self.cells = list(cells or [])
        self.source = source
        self.params = dict(params or {})
        self.on_busy = on_busy


class CellRecord(object):
    __slots__ = ("index", "type", "title", "source", "source_sha256", "cell_id", "metadata",
                 "status", "execution_count", "outputs", "started_at", "ended_at")

    def __init__(self, index: int, type: str, source: str, source_sha256: str,
                 title: Optional[str] = None, cell_id: Optional[str] = None,
                 metadata: Optional[Dict[str, Any]] = None) -> None:
        self.index = index
        self.type = type
        self.title = title
        self.source = source
        self.source_sha256 = source_sha256
        self.cell_id = cell_id
        self.metadata = dict(metadata or {})
        self.status = None              # type: Optional[str]
        self.execution_count = None     # type: Optional[int]
        self.outputs = []               # type: List[Dict[str, Any]]  nbformat 4 outputs
        self.started_at = None          # type: Optional[str]
        self.ended_at = None            # type: Optional[str]


class Run(object):
    __slots__ = ("run_id", "kernel_id", "file", "file_sha256", "mode", "cell_indexes", "params",
                 "status", "started_at", "ended_at", "python", "host", "cells")

    def __init__(self, request: RunRequest, kernel_id: str, file: str, file_sha256: str,
                 python: Dict[str, Any], host: str) -> None:
        self.run_id = request.run_id
        self.kernel_id = kernel_id
        self.file = file
        self.file_sha256 = file_sha256
        self.mode = request.mode
        self.cell_indexes = list(request.cells)
        self.params = dict(request.params)
        self.status = "queued"
        self.started_at = None          # type: Optional[str]
        self.ended_at = None            # type: Optional[str]
        self.python = python
        self.host = host
        self.cells = []                 # type: List[CellRecord]

    def summary(self) -> Dict[str, Any]:
        """The ``RunSummary`` of manager.openapi.yaml (``path`` is added by the run store)."""
        return {
            "run_id": self.run_id, "status": self.status, "mode": self.mode,
            "cells": list(self.cell_indexes), "params": dict(self.params),
            "started_at": self.started_at, "ended_at": self.ended_at,
        }
