"""FR-F7 layout metadata and FR-X7 parallel group bounds (parser, document, collab)."""
from __future__ import annotations

import json
import os

from conftest import REPO
from darkpyonix import _collab as collab
from darkpyonix import _document as document
from darkpyonix import format as fmt

REFERENCE = os.path.join(REPO, "docs", "examples", "darkpyonix_format.py")

GRID_2X2 = (
    "# %% 학습 손실 [code]\n"
    "# @layout: horizontal\n"
    "plot(loss)\n"
    "\n"
    "# %% 검증 손실 [code]\n"
    "plot(val_loss)\n"
    "\n"
    "# %% 학습률 [code]\n"
    "# @layout: horizontal\n"
    "plot(lr)\n"
    "\n"
    "# %% GPU 메모리 [code]\n"
    "# @width: 2fr\n"
    "plot(mem)\n"
    "\n"
    "# %% [code]\n"
    "# @layout: vertical\n"
    "summary()\n"
)


def test_fr_f7_layout_rows_from_switches(scratch):
    doc = fmt.parse(GRID_2X2)
    pre = doc.preamble
    assert pre.row is None and pre.width is None and pre.group is None

    cells = doc.cells[1:]
    assert len(cells) == 5
    assert [c.row for c in cells] == [0, 0, 1, 1, None]
    assert [c.width for c in cells] == ["1fr", "1fr", "1fr", "2fr", None]
    assert [c.group for c in cells] == [None, None, None, None, None]

    # Row tracks by row number match SPEC FR-F7 acceptance example.
    row0 = [c.width for c in cells if c.row == 0]
    row1 = [c.width for c in cells if c.row == 1]
    assert row0 == ["1fr", "1fr"]
    assert row1 == ["1fr", "2fr"]

    # Public surface: Cell.to_dict() includes row, width, group and is JSON-serializable.
    dicts = [c.to_dict() for c in doc.cells]
    json.dumps(dicts)
    assert [(d["row"], d["width"], d["group"]) for d in dicts] == [
        (None, None, None),
        (0, "1fr", None),
        (0, "1fr", None),
        (1, "1fr", None),
        (1, "2fr", None),
        (None, None, None),
    ]

    # Computed fields are never serialized back into the file.
    serialized = fmt.serialize(doc)
    assert serialized == GRID_2X2
    assert "# @row:" not in serialized
    assert "# @group:" not in serialized

    # layout: grid is preserved in metadata and serialized verbatim, computed as horizontal.
    # Unknown layout values are preserved and treated as vertical.
    # Non-string width is stringified when horizontal and None when vertical.
    variants = (
        "# %% [code]\n"
        "# @layout: grid\n"
        "# @width: 320px\n"
        "a = 1\n"
        "\n"
        "# %% [code]\n"
        "# @width: 200\n"
        "b = 2\n"
        "\n"
        "# %% [code]\n"
        "# @layout: masonry\n"
        "# @width: 3fr\n"
        "c = 3\n"
        "\n"
        "# %% [code]\n"
        "# @layout: 42\n"
        "d = 4\n"
    )
    vdoc = fmt.parse(variants)
    vcells = vdoc.cells[1:]
    assert [c.row for c in vcells] == [0, 0, None, None]
    assert [c.width for c in vcells] == ["320px", "200", None, None]
    assert vcells[0].metadata["layout"] == "grid"
    assert vcells[2].metadata["layout"] == "masonry"
    assert vcells[3].metadata["layout"] == 42
    assert fmt.serialize(vdoc) == variants

    # FR-R4 document response and FR-S1 collab snapshot expose row, width, group.
    nb_path = os.path.join(scratch, "grid.pynb")
    with open(nb_path, "wb") as f:
        f.write(GRID_2X2.encode("utf-8"))

    built = document.build_document(nb_path)
    assert [(c["row"], c["width"], c["group"]) for c in built["cells"]] == [
        (None, None, None),
        (0, "1fr", None),
        (0, "1fr", None),
        (1, "1fr", None),
        (1, "2fr", None),
        (None, None, None),
    ]

    state = collab.DocumentState(nb_path, emit=lambda _t, _d: None)
    snap = state.snapshot()
    assert [(c["row"], c["width"], c["group"]) for c in snap["cells"]] == [
        (None, None, None),
        (0, "1fr", None),
        (0, "1fr", None),
        (1, "1fr", None),
        (1, "2fr", None),
        (None, None, None),
    ]

    # Collab updates recompute layout metadata in memory.
    client = {"client_id": "c1", "capabilities": ["read", "edit"]}
    target = snap["cells"][3]
    state.cell_update({
        "client": client,
        "cell_id": target["cell_id"],
        "base_version": target["version"],
        "metadata": {"layout": "vertical"},
    })
    updated_snap = state.snapshot()
    assert [c["row"] for c in updated_snap["cells"][1:]] == [0, 0, None, None, None]
    assert [c["width"] for c in updated_snap["cells"][1:]] == ["1fr", "1fr", None, None, None]


def test_fr_x7_parallel_group_bounds(scratch):
    # Reference file: [parallel]..[concorrunt] yields 1 group with 2 members and an implicit row.
    ref_doc = fmt.load(REFERENCE)
    par_indices = [i for i, c in enumerate(ref_doc.cells) if c.type == "parallel"]
    conc_indices = [i for i, c in enumerate(ref_doc.cells) if c.type == "concurrent"]
    assert len(par_indices) == 1 and len(conc_indices) == 1
    head, end = par_indices[0], conc_indices[0]
    assert end - head == 3  # 2 member cells between head and end

    expected_group = {"head": head, "end": end}
    for i, cell in enumerate(ref_doc.cells):
        if head <= i <= end:
            assert cell.group == expected_group
        else:
            assert cell.group is None

    assert ref_doc.cells[head].row is None and ref_doc.cells[head].width is None
    assert ref_doc.cells[head + 1].row == 0 and ref_doc.cells[head + 1].width == "1fr"
    assert ref_doc.cells[head + 2].row == 0 and ref_doc.cells[head + 2].width == "1fr"
    assert ref_doc.cells[end].row is None and ref_doc.cells[end].width is None
    # Cell after [concorrunt] starts vertical.
    assert ref_doc.cells[end + 1].row is None and ref_doc.cells[end + 1].width is None
    assert ref_doc.cells[end + 1].group is None

    # Both [concorrunt] and [concurrent] spellings produce the same canonical type, group and layout.
    template = (
        "# %% [code]\n"
        "# @layout: horizontal\n"
        "before = 1\n"
        "\n"
        "# %% [parallel]\n"
        "__co_routines__ = []\n"
        "\n"
        "# %% [code]\n"
        "__co_routines__.append(a())\n"
        "\n"
        "# %% [code]\n"
        "# @width: 2fr\n"
        "__co_routines__.append(b())\n"
        "\n"
        "# %% [%s]\n"
        "darkpyonix.run_parallel(*__co_routines__)\n"
        "\n"
        "# %% [code]\n"
        "after = 2\n"
    )
    for spelling in ("concorrunt", "concurrent"):
        text = template.replace("%s", spelling)
        doc = fmt.parse(text)
        assert fmt.serialize(doc) == text
        cells = doc.cells[1:]
        assert cells[4].type == "concurrent"
        assert cells[4].raw_type == spelling
        # Preceding horizontal cell is row 0; [parallel] closes it and is vertical;
        # members form row 1; [concurrent] closes row 1; trailing cell is vertical.
        assert [c.row for c in cells] == [0, None, 1, 1, None, None]
        assert [c.width for c in cells] == ["1fr", None, "1fr", "2fr", None, None]
        assert [c.group for c in cells] == [
            None,
            {"head": 2, "end": 5},
            {"head": 2, "end": 5},
            {"head": 2, "end": 5},
            {"head": 2, "end": 5},
            None,
        ]
        # to_dict exposes group without mutating internal state.
        d = cells[1].to_dict()
        assert d["group"] == {"head": 2, "end": 5}
        d["group"]["head"] = 999
        assert cells[1].group == {"head": 2, "end": 5}

    # New-cell serialization uses [concorrunt] when raw_type is None, while preserving explicit raw_type.
    new_doc = fmt.NotebookDocument(
        None,
        [
            fmt.Cell(0, fmt.PREAMBLE, ""),
            fmt.Cell(1, "parallel", "__co_routines__ = []\n"),
            fmt.Cell(2, "concurrent", "darkpyonix.run_parallel(*__co_routines__)\n"),
            fmt.Cell(3, "concurrent", "darkpyonix.run_parallel()\n", raw_type="concurrent"),
        ],
        "",
    )
    out = fmt.serialize(new_doc)
    assert "# %% [concorrunt]\n" in out
    assert "# %% [concurrent]\n" in out
    reparsed = fmt.parse(out)
    assert reparsed.cells[2].type == "concurrent" and reparsed.cells[2].raw_type == "concorrunt"
    assert reparsed.cells[3].type == "concurrent" and reparsed.cells[3].raw_type == "concurrent"

    # Explicit @layout inside a parallel group overrides the implicit horizontal row.
    vertical_members = (
        "# %% [parallel]\n"
        "__co_routines__ = []\n"
        "# %% [code]\n"
        "# @layout: vertical\n"
        "__co_routines__.append(a())\n"
        "# %% [code]\n"
        "__co_routines__.append(b())\n"
        "# %% [concurrent]\n"
        "darkpyonix.run_parallel(*__co_routines__)\n"
    )
    vm_doc = fmt.parse(vertical_members)
    assert [c.group for c in vm_doc.cells[1:]] == [{"head": 1, "end": 4}] * 4
    assert [c.row for c in vm_doc.cells[1:]] == [None, None, None, None]
    assert [c.width for c in vm_doc.cells[1:]] == [None, None, None, None]

    # Unterminated [parallel] before another [parallel]: earlier group is unterminated (no implicit row).
    nested_text = (
        "# %% [parallel]\n"
        "a = 1\n"
        "# %% [code]\n"
        "b = 2\n"
        "# %% [parallel]\n"
        "__co_routines__ = []\n"
        "# %% [code]\n"
        "__co_routines__.append(c())\n"
        "# %% [concurrent]\n"
        "darkpyonix.run_parallel(*__co_routines__)\n"
    )
    ndoc = fmt.parse(nested_text)
    ncells = ndoc.cells[1:]
    assert [c.group for c in ncells] == [
        None,
        None,
        {"head": 3, "end": 5},
        {"head": 3, "end": 5},
        {"head": 3, "end": 5},
    ]
    assert [c.row for c in ncells] == [None, None, None, 0, None]
    assert [c.width for c in ncells] == [None, None, None, "1fr", None]

    # Unterminated [parallel] at EOF gets group=None and no implicit row.
    eof_doc = fmt.parse("# %% [parallel]\na = 1\n# %% [code]\nb = 2\n")
    assert [c.group for c in eof_doc.cells[1:]] == [None, None]
    assert [c.row for c in eof_doc.cells[1:]] == [None, None]
    assert [c.width for c in eof_doc.cells[1:]] == [None, None]

    # Orphan [concurrent] without preceding [parallel] has group=None.
    orphan_doc = fmt.parse("# %% [code]\na = 1\n# %% [concurrent]\nb = 2\n")
    assert [c.group for c in orphan_doc.cells[1:]] == [None, None]
    assert [c.row for c in orphan_doc.cells[1:]] == [None, None]

    # Empty [parallel] immediately followed by [concurrent] is a valid 0-member group (T8).
    empty_doc = fmt.parse("# %% [parallel]\n# %% [concurrent]\n")
    assert [c.group for c in empty_doc.cells[1:]] == [{"head": 1, "end": 2}, {"head": 1, "end": 2}]
    assert [c.row for c in empty_doc.cells[1:]] == [None, None]
    assert [c.width for c in empty_doc.cells[1:]] == [None, None]

    # DocumentState and build_document expose group/row/width for parallel notebooks.
    par_path = os.path.join(scratch, "parallel.pynb")
    with open(par_path, "wb") as f:
        f.write(template.replace("%s", "concurrent").encode("utf-8"))
    built = document.build_document(par_path)
    assert [c["group"] for c in built["cells"][1:]] == [
        None,
        {"head": 2, "end": 5},
        {"head": 2, "end": 5},
        {"head": 2, "end": 5},
        {"head": 2, "end": 5},
        None,
    ]
    state = collab.DocumentState(par_path, emit=lambda _t, _d: None)
    snap = state.snapshot()
    assert [c["group"] for c in snap["cells"][1:]] == [
        None,
        {"head": 2, "end": 5},
        {"head": 2, "end": 5},
        {"head": 2, "end": 5},
        {"head": 2, "end": 5},
        None,
    ]
    assert [c["row"] for c in snap["cells"][1:]] == [0, None, 1, 1, None, None]
