"""FR-X6: matplotlib figures become image/png display_data, without the kernel importing matplotlib.

The figure tests run on every interpreter that has matplotlib (the one running pytest usually
does when the suite is started with ``--with matplotlib``); without one they skip.
"""
from __future__ import annotations

import base64
import os
import subprocess
import sys

import pytest

from conftest import PYTHONS
from test_fr_x_executor import drive, events, outputs_of, run_all, stream_text

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def _has_matplotlib(python):
    try:
        return subprocess.run([python, "-c", "import matplotlib"], capture_output=True,
                              timeout=60).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _matplotlib_pythons():
    seen, out = set(), []
    for p in list(PYTHONS) + [sys.executable]:
        real = os.path.realpath(p)
        if real in seen:
            continue
        seen.add(real)
        if _has_matplotlib(p):
            out.append(p)
    return out


MPL_PYTHONS = _matplotlib_pythons()


@pytest.fixture(params=MPL_PYTHONS or [None],
                ids=lambda p: os.path.basename(p) if p else "no-matplotlib")
def mpl_python(request):
    if request.param is None:
        pytest.skip("no interpreter with matplotlib on this machine")
    return request.param


def pngs(outputs):
    out = []
    for o in outputs:
        if o["output_type"] == "display_data" and "image/png" in o["data"]:
            out.append(o)
    return out


def assert_png(output):
    assert base64.b64decode(output["data"]["image/png"]).startswith(PNG_MAGIC)
    assert output["data"]["text/plain"].startswith("<Figure")


def test_fr_x6_matplotlib_show_emits_png(mpl_python, scratch):
    out = drive(mpl_python, scratch, """
        # %%
        import matplotlib.pyplot as plt
        plt.plot([1, 2])
        plt.show()
        print("after")
        """, run_all())
    run = out["runs"][0]
    assert run["status"] == "ok", run
    outs = outputs_of(run, 1)
    shown = pngs(outs)
    assert len(shown) == 1
    assert_png(shown[0])
    assert outs.index(shown[0]) < [o.get("text") for o in outs].index("after\n")
    live = [e["data"]["output"] for e in events(out, "output")]
    assert any(o["output_type"] == "display_data" and "image/png" in o["data"] for o in live)


def test_fr_x6_figure_left_at_cell_end_is_shown_once(mpl_python, scratch):
    out = drive(mpl_python, scratch, """
        # %%
        import matplotlib.pyplot as plt
        plt.plot([1, 2])
        # %%
        print("next")
        # %%
        plt.figure()
        plt.plot([3, 1])
        raise ValueError("late")
        """, run_all())
    run = out["runs"][0]
    first = pngs(outputs_of(run, 1))
    assert len(first) == 1
    assert_png(first[0])
    assert pngs(outputs_of(run, 2)) == []
    assert stream_text(run) == "next\n"
    failed = outputs_of(run, 3)
    assert [o["output_type"] for o in failed] == ["display_data", "error"]
    assert_png(failed[0])


def test_fr_x6_kernel_does_not_import_matplotlib(python, scratch):
    out = drive(python, scratch, """
        # %%
        import sys
        print(sorted(m for m in sys.modules if m.split(".")[0] == "matplotlib"))
        print(os.environ.get("MPLBACKEND"))
        """.replace("import sys", "import os, sys"), run_all())
    assert stream_text(out["runs"][0]) == "[]\n%s\n" % os.environ.get("MPLBACKEND")


def test_fr_x6_user_chosen_backend_is_kept(mpl_python, scratch):
    out = drive(mpl_python, scratch, """
        # %%
        import matplotlib
        matplotlib.use("agg")
        import matplotlib.pyplot as plt
        plt.plot([1, 2])
        print(matplotlib.get_backend().lower())
        """, run_all())
    run = out["runs"][0]
    assert run["status"] == "ok", run
    assert stream_text(run) == "agg\n"
    assert pngs(outputs_of(run, 1)) == []
