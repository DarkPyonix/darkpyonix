"""Parser for the DarkPyonix notebook format (docs/FORMAT.md §2, SPEC FR-F1).

Standard library only; Python 3.8+. Shared by the kernel, the manager and the runtime API.

The data model below is the contract other modules build against; ``parse`` /
``serialize`` are implemented by issue #8.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# Canonical cell types (FORMAT §3). Unknown types are preserved as written.
PREAMBLE = "preamble"
CODE = "code"
KNOWN_TYPES = (
    "preamble", "code", "markdown", "argparse", "binding", "shell",
    "parallel", "concurrent", "cinterop", "cppinterop", "rustinterop",
    "sql", "toml", "yaml", "json",
)
TYPE_ALIASES = {"concorrunt": "concurrent"}


class Cell(object):
    """One cell. ``index`` 0 is the preamble (text before the first marker)."""

    __slots__ = ("index", "type", "raw_type", "title", "metadata", "source",
                 "header", "source_sha256", "id")

    def __init__(self, index: int, type: str, source: str, *, raw_type: Optional[str] = None,
                 title: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None,
                 header: str = "", source_sha256: str = "", id: Optional[str] = None) -> None:
        self.index = index
        self.type = type                    # canonical type (aliases resolved)
        self.raw_type = raw_type            # as written between [ ], or None
        self.title = title
        self.metadata = metadata if metadata is not None else {}
        self.source = source                # body without marker and metadata lines
        self.header = header                # exact marker + metadata lines, for byte-exact serialize
        self.source_sha256 = source_sha256  # SHA-256 of source with \r\n → \n
        self.id = id                        # metadata "id" if present

    def to_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index, "type": self.type, "title": self.title,
            "metadata": dict(self.metadata), "source": self.source,
            "source_sha256": self.source_sha256, "id": self.id,
        }


class NotebookDocument(object):
    """A parsed notebook file. ``cells[0]`` is always the preamble (possibly empty)."""

    __slots__ = ("path", "cells", "file_sha256")

    def __init__(self, path: Optional[str], cells: List[Cell], file_sha256: str) -> None:
        self.path = path
        self.cells = cells
        self.file_sha256 = file_sha256

    @property
    def preamble(self) -> Cell:
        return self.cells[0]

    def cell(self, index: int) -> Cell:
        return self.cells[index]


def parse(text: str, path: Optional[str] = None) -> NotebookDocument:
    """Parse notebook source text. Never raises on unknown types or metadata."""
    raise NotImplementedError("issue #8")


def load(path: str) -> NotebookDocument:
    """Read ``path`` as UTF-8 (BOM tolerated) and parse it."""
    raise NotImplementedError("issue #8")


def serialize(doc: NotebookDocument) -> str:
    """Inverse of ``parse``: byte-identical for an unmodified document."""
    raise NotImplementedError("issue #8")
