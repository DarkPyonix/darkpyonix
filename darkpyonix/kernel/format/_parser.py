"""Line-based parser and serializer for the notebook format (docs/FORMAT.md §2, SPEC FR-F1).

Standard library only; Python 3.8+.

Layout of a parsed file: the text is cut at every marker line. Each cell owns

* ``header``: its marker line plus the metadata lines right after it, line endings included;
* ``source``: every following line up to (not including) the next marker, line endings
  included. Blank lines between cells therefore belong to the preceding cell's source.

``serialize`` concatenates ``header + source`` for every cell, so an unmodified document is
reproduced byte for byte (CRLF, missing trailing newline and a leading BOM included).

Like Jupytext, the parser is line based: a marker line inside a triple-quoted string still
opens a new cell.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, List, Optional, Tuple

# FORMAT §2.2, verbatim. Applied to a line without its line ending.
MARKER_RE = re.compile(
    r"^# %%(?:[ \t]+(?P<title>[^\[\n]*?))?"
    r"(?:[ \t]*\[(?P<type>[A-Za-z_][A-Za-z0-9_-]*)\])?[ \t]*$"
)
# FORMAT §2.3: ``# @key: value``.
METADATA_RE = re.compile(r"^# @(?P<key>[A-Za-z_][A-Za-z0-9_.-]*):(?:[ \t]*(?P<value>.*?))?[ \t]*$")

BOM = "﻿"


def _split_lines(text: str) -> List[str]:
    """Split on ``\\n`` only, keeping line endings (unlike str.splitlines, which also cuts on
    form feeds and Unicode separators that may legitimately occur inside Python source)."""
    lines = text.split("\n")
    out = [line + "\n" for line in lines[:-1]]
    if lines[-1]:
        out.append(lines[-1])
    return out


def _content(line: str) -> str:
    if line.endswith("\r\n"):
        return line[:-2]
    if line.endswith("\n"):
        return line[:-1]
    return line


def _metadata_value(raw: str) -> Any:
    try:
        return json.loads(raw)
    except ValueError:
        return raw


def source_sha256(source: str) -> str:
    """SHA-256 of a cell body (FORMAT §2.4).

    ``\\r\\n`` is normalised to ``\\n`` and trailing blank (empty or whitespace-only) lines are
    dropped, together with the final line ending. Blank lines before the next marker are
    layout, not code: adding or removing them, or the newline at end of file, must not make a
    cell's run records stale. Every other byte of the body is significant.
    """
    lines = source.replace("\r\n", "\n").split("\n")
    while lines and not lines[-1].strip():
        lines.pop()
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def parse_cells(text: str) -> Tuple[bool, List[Tuple[Any, List[Tuple[str, Any]], str, str]]]:
    """Return ``(bom, chunks)``; each chunk is ``(marker_match, metadata_items, header, source)``.

    The first chunk is the preamble (``marker_match`` None, header empty).
    """
    bom = text.startswith(BOM)
    if bom:
        text = text[len(BOM):]
    chunks = []  # type: List[Tuple[Any, List[Tuple[str, Any]], str, str]]
    match = None  # type: Any
    items = []  # type: List[Tuple[str, Any]]
    header = []  # type: List[str]
    body = []  # type: List[str]
    in_metadata = False
    for line in _split_lines(text):
        content = _content(line)
        m = MARKER_RE.match(content)
        if m is not None:
            chunks.append((match, items, "".join(header), "".join(body)))
            match, items, header, body = m, [], [line], []
            in_metadata = True
            continue
        if in_metadata:
            md = METADATA_RE.match(content)
            if md is not None:
                items.append((md.group("key"), _metadata_value(md.group("value") or "")))
                header.append(line)
                continue
            in_metadata = False
        body.append(line)
    chunks.append((match, items, "".join(header), "".join(body)))
    return bom, chunks


def format_header(title: Optional[str], type_: Optional[str], metadata: Dict[str, Any]) -> str:
    """Build a marker + metadata block for a cell that has no recorded header."""
    marker = "# %%"
    if title:
        marker += " " + title
    if type_:
        marker += " [" + type_ + "]"
    lines = [marker]
    for key, value in metadata.items():
        text = value if isinstance(value, str) and _metadata_value(value) == value else json.dumps(value)
        lines.append("# @%s: %s" % (key, text))
    return "\n".join(lines) + "\n"
