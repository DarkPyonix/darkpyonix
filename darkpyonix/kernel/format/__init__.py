"""Parser for the DarkPyonix notebook format (docs/FORMAT.md §2, SPEC FR-F1).

Standard library only; Python 3.8+. Shared by the kernel, the manager and the runtime API.

The data model below is the contract other modules build against. Parsing details (how
blank lines between cells are attributed, how ``source_sha256`` is computed) are documented
in ``_parser``.
"""
from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional

from ._parser import BOM, format_header, parse_cells, source_sha256

# Canonical cell types (FORMAT §3). Unknown types are preserved as written.
PREAMBLE = "preamble"
CODE = "code"
KNOWN_TYPES = (
    "preamble", "code", "markdown", "argparse", "binding", "shell",
    "parallel", "concurrent", "cinterop", "cppinterop", "rustinterop",
    "sql", "toml", "yaml", "json",
)
TYPE_ALIASES = {"concorrunt": "concurrent"}
# Provisional default spelling when serializing a new cell with raw_type=None (SPEC FR-X7).
DEFAULT_RAW_TYPES = {"concurrent": "concorrunt"}


class Cell(object):
    """One cell. ``index`` 0 is the preamble (text before the first marker)."""

    __slots__ = ("index", "type", "raw_type", "title", "metadata", "source",
                 "header", "source_sha256", "id", "row", "width", "group")

    def __init__(self, index: int, type: str, source: str, *, raw_type: Optional[str] = None,
                 title: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None,
                 header: str = "", source_sha256: str = "", id: Optional[str] = None,
                 row: Optional[int] = None, width: Optional[str] = None,
                 group: Optional[Dict[str, int]] = None) -> None:
        self.index = index
        self.type = type                    # canonical type (aliases resolved)
        self.raw_type = raw_type            # as written between [ ], or None
        self.title = title
        self.metadata = metadata if metadata is not None else {}
        self.source = source                # body without marker and metadata lines
        self.header = header                # exact marker + metadata lines, for byte-exact serialize
        self.source_sha256 = source_sha256  # SHA-256 of source with \r\n → \n
        self.id = id                        # metadata "id" if present
        self.row = row                      # computed horizontal row index, or None when vertical
        self.width = width                  # computed CSS grid track width, or None when vertical
        self.group = group                  # computed {"head": int, "end": int}, or None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index, "type": self.type, "title": self.title,
            "metadata": dict(self.metadata), "source": self.source,
            "source_sha256": self.source_sha256, "id": self.id,
            "row": self.row, "width": self.width,
            "group": dict(self.group) if self.group is not None else None,
        }


class NotebookDocument(object):
    """A parsed notebook file. ``cells[0]`` is always the preamble (possibly empty)."""

    __slots__ = ("path", "cells", "file_sha256", "bom")

    def __init__(self, path: Optional[str], cells: List[Cell], file_sha256: str, *,
                 bom: bool = False) -> None:
        self.path = path
        self.cells = cells
        self.file_sha256 = file_sha256      # SHA-256 of the whole file (UTF-8 bytes)
        self.bom = bom                      # file started with U+FEFF; kept out of the preamble

    @property
    def preamble(self) -> Cell:
        return self.cells[0]

    def cell(self, index: int) -> Cell:
        return self.cells[index]


def _cell_width(metadata: Dict[str, Any]) -> str:
    raw = metadata.get("width")
    if raw is None or raw == "":
        return "1fr"
    return raw if isinstance(raw, str) else str(raw)


def compute_layout(cells: List[Cell]) -> None:
    """Compute ``group``, ``row`` and ``width`` on every cell in place (SPEC FR-X7, FR-F7).

    These fields are derived from the cell sequence and never written back to source files.
    """
    for cell in cells:
        cell.group = None
    open_head = None  # type: Optional[int]
    for i, cell in enumerate(cells):
        if i == 0 or cell.type == PREAMBLE:
            continue
        if cell.type == "parallel":
            open_head = i
        elif cell.type == "concurrent":
            if open_head is not None:
                head_idx = cells[open_head].index
                end_idx = cell.index
                for k in range(open_head, i + 1):
                    cells[k].group = {"head": head_idx, "end": end_idx}
                open_head = None

    current_row = None  # type: Optional[int]
    next_row = 0
    for i, cell in enumerate(cells):
        if i == 0 or cell.type == PREAMBLE:
            cell.row = None
            cell.width = None
            current_row = None
            continue

        grp = cell.group
        if grp is not None and (cell.index == grp["head"] or cell.index == grp["end"]):
            current_row = None
            cell.row = None
            cell.width = None
            continue

        if "layout" in cell.metadata:
            raw_layout = cell.metadata["layout"]
            if raw_layout in ("horizontal", "grid"):
                current_row = next_row
                next_row += 1
                cell.row = current_row
                cell.width = _cell_width(cell.metadata)
            else:
                current_row = None
                cell.row = None
                cell.width = None
        elif grp is not None and cell.index == grp["head"] + 1:
            current_row = next_row
            next_row += 1
            cell.row = current_row
            cell.width = _cell_width(cell.metadata)
        elif current_row is not None:
            cell.row = current_row
            cell.width = _cell_width(cell.metadata)
        else:
            cell.row = None
            cell.width = None


def parse(text: str, path: Optional[str] = None) -> NotebookDocument:
    """Parse notebook source text. Never raises on unknown types or metadata."""
    bom, chunks = parse_cells(text)
    cells = []  # type: List[Cell]
    for index, (match, items, header, source) in enumerate(chunks):
        if match is None:
            cells.append(Cell(0, PREAMBLE, source, header=header,
                              source_sha256=source_sha256(source)))
            continue
        raw_type = match.group("type")
        if raw_type is None:
            type_ = CODE
        else:
            lowered = raw_type.lower()
            type_ = TYPE_ALIASES.get(lowered, lowered)
        metadata = dict(items)
        cid = metadata.get("id")
        cells.append(Cell(
            index, type_, source, raw_type=raw_type, title=match.group("title") or None,
            metadata=metadata, header=header, source_sha256=source_sha256(source),
            id=None if cid is None else (cid if isinstance(cid, str) else str(cid)),
        ))
    compute_layout(cells)
    file_sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return NotebookDocument(path, cells, file_sha, bom=bom)


def load(path: str) -> NotebookDocument:
    """Read ``path`` as UTF-8 (BOM tolerated) and parse it.

    The file is decoded without newline translation so that ``serialize`` reproduces its bytes.
    """
    with open(path, "rb") as f:
        data = f.read()
    return parse(data.decode("utf-8"), path)


def serialize(doc: NotebookDocument) -> str:
    """Inverse of ``parse``: byte-identical for an unmodified document.

    A cell without a recorded ``header`` (one built by a client) gets a marker and metadata
    lines generated from its title, type and metadata.
    """
    parts = []  # type: List[str]
    last = ""   # last non-empty part, to tell whether the text so far ends a line
    for cell in doc.cells:
        header = cell.header
        if not header and cell.index != 0 and cell.type != PREAMBLE:
            written_type = (
                cell.raw_type if cell.raw_type is not None
                else DEFAULT_RAW_TYPES.get(cell.type, cell.type)
            )
            header = format_header(cell.title, written_type, cell.metadata)
        if header and last and not last.endswith("\n"):
            parts.append("\n")
        for part in (header, cell.source):
            if part:
                parts.append(part)
                last = part
    return (BOM if doc.bom else "") + "".join(parts)
