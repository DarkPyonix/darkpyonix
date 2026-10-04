"""FR-F1: the cell parser (docs/FORMAT.md §2–§3)."""
from __future__ import annotations

import hashlib
import os
from collections import Counter

import pytest

from conftest import REPO
from darkpyonix import format as fmt

REFERENCE = os.path.join(REPO, "docs", "examples", "darkpyonix_format.py")


def _sha(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def test_fr_f1_reference_file_parses():
    doc = fmt.load(REFERENCE)
    with open(REFERENCE, "rb") as f:
        raw = f.read()

    assert doc.path == REFERENCE
    assert doc.file_sha256 == hashlib.sha256(raw).hexdigest()

    pre = doc.preamble
    assert pre.index == 0 and pre.type == fmt.PREAMBLE
    assert pre.header == "" and pre.title is None and pre.raw_type is None
    assert pre.source.startswith('"""Starboard Notebook: Python support"""\nimport darkpyonix\n')

    cells = doc.cells[1:]
    assert len(cells) == 22
    assert [c.index for c in doc.cells] == list(range(23))
    assert Counter(c.type for c in cells) == Counter({
        "code": 11, "markdown": 2, "binding": 2, "argparse": 1, "shell": 1, "parallel": 1,
        "concurrent": 1, "cinterop": 1, "cppinterop": 1, "rustinterop": 1,
    })

    # `concorrunt` is an alias of `concurrent`; the spelling as written is kept.
    conc = [c for c in cells if c.type == "concurrent"]
    assert len(conc) == 1 and conc[0].raw_type == "concorrunt"

    # `# @width: 1fr` sits on the two code cells after [parallel] (trailing spaces on the marker).
    widths = [c for c in cells if c.metadata]
    assert len(widths) == 2
    for c in widths:
        assert c.type == "code" and c.metadata == {"width": "1fr"}
        assert "# @width" not in c.source
        assert c.header.startswith("# %% [code]") and c.header.endswith("# @width: 1fr\n")
    par = [i for i, c in enumerate(doc.cells) if c.type == "parallel"][0]
    assert doc.cells[par + 1].metadata == {"width": "1fr"}
    assert doc.cells[par + 2].metadata == {"width": "1fr"}

    for c in cells:
        assert c.title is None and c.id is None
        assert c.header.startswith("# %%")
        assert "# %%" not in c.source
        assert c.source_sha256 and len(c.source_sha256) == 64

    assert cells[0].type == "code" and cells[0].source.startswith("import torch\n")
    assert cells[1].type == "argparse"


def test_fr_f1_parse_serialize_roundtrip(scratch):
    with open(REFERENCE, "rb") as f:
        raw = f.read()
    assert fmt.serialize(fmt.load(REFERENCE)).encode("utf-8") == raw
    assert fmt.serialize(fmt.parse(raw.decode("utf-8"))) == raw.decode("utf-8")

    cases = [
        "",
        "x = 1",
        "x = 1\n",
        "import os\nprint(os.name)\n",                         # no markers at all
        "# %% [code]\nx = 1\n",                                 # empty preamble
        "# %%\nx = 1",                                          # no trailing newline, no type
        "import a\r\n\r\n# %% [code]\r\nx = 1\r\n\r\n# %% [markdown]\r\nm()\r\n",  # CRLF
        "# %% Load data [code]\n# @id: \"c-3f2a\"\n# @collapsed: true\nx = 1\n",
        "# %% just a title\nx = 1\n\n\n# %% [mystery-type]\ny = 2\n",
        "# %% [code]\n# @n: 3\n# @l: [1, 2]\n# @o: {\"a\": null}\n# @s: hello world\nz\n",
        "# %% [code]\n# %% [code]\n",                             # empty cells
        "# %%   [code]   \t\n  x\n",
        "﻿# %% [code]\nx = 1\n",                            # BOM
        "﻿pre = 1\r\n# %% [code]\r\nx = 1",
    ]
    for text in cases:
        doc = fmt.parse(text)
        assert fmt.serialize(doc) == text, repr(text)
        assert doc.file_sha256 == _sha(text)

    # Byte-exact through load as well (CRLF and BOM survive the file round trip).
    path = os.path.join(scratch, "nb.pynb")
    for text in cases:
        with open(path, "wb") as f:
            f.write(text.encode("utf-8"))
        assert fmt.serialize(fmt.load(path)).encode("utf-8") == text.encode("utf-8")


def test_fr_f1_marker_grammar():
    text = (
        "# %% Load data [code]\n"
        "# %% Title only\n"
        "# %%\n"
        "# %% [Markdown]\n"
        "# %% [concorrunt]\n"
        "# %% [mystery-type]   \n"
        "  # %% [code]\n"        # indented: not a marker
        "# %%% [code]\n"         # not a marker
        "# %%[code]\n"           # the regex allows no space before [type]
        "#%% [code]\n"           # not a marker
    )
    doc = fmt.parse(text)
    cells = doc.cells[1:]
    assert [(c.title, c.type, c.raw_type) for c in cells] == [
        ("Load data", "code", "code"),
        ("Title only", "code", None),
        (None, "code", None),
        (None, "markdown", "Markdown"),
        (None, "concurrent", "concorrunt"),
        (None, "mystery-type", "mystery-type"),
        (None, "code", "code"),
    ]
    assert cells[5].source == "  # %% [code]\n# %%% [code]\n"
    assert cells[6].source == "#%% [code]\n"


def test_fr_f1_metadata_lines():
    text = (
        "# %% [code]\n"
        "# @id: \"c-3f2a\"\n"
        "# @width: 1fr\n"
        "# @collapsed: true\n"
        "# @n: 3\n"
        "# @data: {\"a\": [1, null]}\n"
        "# @empty:\n"
        "x = 1\n"
        "# @after: 1\n"
    )
    c = fmt.parse(text).cells[1]
    assert c.metadata == {
        "id": "c-3f2a", "width": "1fr", "collapsed": True, "n": 3,
        "data": {"a": [1, None]}, "empty": "",
    }
    assert c.id == "c-3f2a"
    assert c.source == "x = 1\n# @after: 1\n"
    assert c.header == text[: text.index("x = 1")]

    # Metadata stops at the first non-metadata line, even a blank one.
    c = fmt.parse("# %% [code]\n\n# @width: 1fr\n").cells[1]
    assert c.metadata == {} and c.source == "\n# @width: 1fr\n"

    # A non-string id is stringified so ``Cell.id`` stays Optional[str].
    assert fmt.parse("# %%\n# @id: 7\n").cells[1].id == "7"


def test_fr_f1_source_sha256_ignores_line_endings_and_trailing_blank_lines():
    a = fmt.parse("# %% [code]\nx = 1\ny = 2\n\n\n# %% [code]\nz\n").cells[1]
    b = fmt.parse("# %% [code]\r\nx = 1\r\ny = 2\r\n# %% [code]\r\nz\r\n").cells[1]
    c = fmt.parse("# %% [code]\nx = 1\ny = 2").cells[1]
    d = fmt.parse("# %% [code]\nx = 1\n\ny = 2\n").cells[1]
    assert a.source == "x = 1\ny = 2\n\n\n"
    assert a.source_sha256 == b.source_sha256 == c.source_sha256 == _sha("x = 1\ny = 2")
    assert d.source_sha256 != a.source_sha256
    # Metadata does not change the source hash.
    e = fmt.parse("# %% Title [code]\n# @width: 1fr\nx = 1\ny = 2\n").cells[1]
    assert e.source_sha256 == a.source_sha256
    assert fmt.parse("").preamble.source_sha256 == _sha("")


def test_fr_f1_markers_inside_strings_are_still_markers():
    # Line-based like Jupytext: a marker line inside a triple-quoted string splits the cell.
    text = 's = """\n# %% [code]\n"""\n'
    doc = fmt.parse(text)
    assert len(doc.cells) == 2
    assert doc.preamble.source == 's = """\n'
    assert doc.cells[1].source == '"""\n'
    assert fmt.serialize(doc) == text


def test_fr_f1_bom_is_not_part_of_the_preamble():
    doc = fmt.parse("﻿# %% [code]\nx\n")
    assert doc.preamble.source == "" and len(doc.cells) == 2
    doc = fmt.parse("﻿import os\n")
    assert doc.preamble.source == "import os\n"
    compile(doc.preamble.source, "<preamble>", "exec")


def test_fr_f1_serialize_new_cell_without_header():
    doc = fmt.parse("# %% [code]\nx = 1")
    doc.cells.append(fmt.Cell(2, "markdown", "m()\n", title="Notes", metadata={"width": "1fr"}))
    assert fmt.serialize(doc) == "# %% [code]\nx = 1\n# %% Notes [markdown]\n# @width: 1fr\nm()\n"
    assert [c.type for c in fmt.parse(fmt.serialize(doc)).cells] == ["preamble", "code", "markdown"]


def test_fr_f1_to_dict_is_json_ready():
    import json

    doc = fmt.load(REFERENCE)
    json.dumps([c.to_dict() for c in doc.cells])


@pytest.mark.parametrize("text", ["# %% [code]\n", "x\n# %% [code]\ny\n"])
def test_fr_f1_preamble_always_present(text):
    assert fmt.parse(text).cells[0].type == fmt.PREAMBLE
