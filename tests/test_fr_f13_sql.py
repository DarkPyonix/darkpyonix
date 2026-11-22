"""FR-F13: darkpyonix.sql and SQLResult across every interpreter (NFR-K1)."""
from __future__ import annotations

import json
import os
import subprocess
import textwrap

from conftest import SRC_ROOT

DRIVER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "helpers", "executor_driver.py")


def _env(**extra):
    env = dict(os.environ)
    env["PYTHONPATH"] = SRC_ROOT
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env.update(extra)
    return env


def _write(scratch, name, text):
    path = os.path.join(scratch, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(text).lstrip("\n"))
    return path


def _run(python, path):
    return subprocess.run(
        [python, path],
        capture_output=True,
        text=True,
        env=_env(),
        cwd=os.path.dirname(path),
        timeout=60,
    )


def test_fr_f13_sql_default_sqlite_and_params(python, scratch):
    path = _write(scratch, "sql_check.py", '''
        import sqlite3
        import sys
        import types
        import darkpyonix

        # Default connection is a per-process sqlite memory DB shared across calls.
        created = darkpyonix.sql("CREATE TABLE items (id INTEGER, name TEXT)")
        assert isinstance(created, darkpyonix.SQLResult)
        assert isinstance(created, list)
        assert created.columns == ()
        assert list(created) == []

        ins1 = darkpyonix.sql("INSERT INTO items VALUES (?, ?)", params=(1, "alpha"))
        assert ins1.columns == ()
        assert ins1.rowcount == 1
        darkpyonix.sql("INSERT INTO items VALUES (?, ?)", params=[2, "beta"])
        darkpyonix.sql(
            "INSERT INTO items VALUES (:id, :name)",
            params={"id": 3, "name": "gamma"},
        )

        # Default target binds "_" in caller globals.
        res = darkpyonix.sql(
            "SELECT id, name FROM items WHERE id >= ? ORDER BY id",
            params=(2,),
        )
        assert isinstance(res, darkpyonix.SQLResult)
        assert res.columns == ("id", "name")
        assert list(res) == [(2, "beta"), (3, "gamma")]
        assert _ is res

        # Explicit target binds that name; target=None does not bind.
        res2 = darkpyonix.sql("SELECT name FROM items WHERE id = ?", target="one", params=(1,))
        assert one is res2
        assert list(one) == [("alpha",)]
        assert _ is res

        res3 = darkpyonix.sql("SELECT COUNT(*) AS n FROM items", target=None)
        assert list(res3) == [(3,)]
        assert res3.columns == ("n",)
        assert _ is res

        # Invalid target raises ValueError.
        for bad in ("bad name", "1bad", 123):
            try:
                darkpyonix.sql("SELECT 1", target=bad)
            except ValueError:
                pass
            else:
                raise AssertionError("expected ValueError for target=%r" % (bad,))

        # Custom con= uses that DB-API 2.0 connection instead of the default DB.
        other = sqlite3.connect(":memory:")
        other.execute("CREATE TABLE other_t (v TEXT)")
        other.execute("INSERT INTO other_t VALUES ('isolated')")
        other_res = darkpyonix.sql("SELECT v FROM other_t", con=other, target=None)
        assert other_res.columns == ("v",)
        assert list(other_res) == [("isolated",)]

        # darkpyonix.sql.connection overrides the default connection when con= is omitted.
        darkpyonix.sql.connection = other
        try:
            via_attr = darkpyonix.sql("SELECT v FROM other_t", target=None)
            assert list(via_attr) == [("isolated",)]
        finally:
            darkpyonix.sql.connection = None

        # Default DB is still intact after resetting .connection.
        still_there = darkpyonix.sql("SELECT COUNT(*) FROM items", target=None)
        assert list(still_there) == [(3,)]
        other.close()

        # to_pandas() reads sys.modules only and never imports pandas.
        assert "pandas" not in sys.modules
        try:
            res.to_pandas()
        except darkpyonix.InteropUnavailable as exc:
            assert isinstance(exc, ImportError)
        else:
            raise AssertionError("expected InteropUnavailable when pandas is not imported")
        assert "pandas" not in sys.modules

        # When pandas is already in sys.modules, to_pandas() delegates to pd.DataFrame.
        fake_pd = types.ModuleType("pandas")
        fake_pd.DataFrame = lambda data, columns=None: ("DF", list(data), tuple(columns))
        sys.modules["pandas"] = fake_pd
        try:
            df = res.to_pandas()
            assert df == ("DF", [(2, "beta"), (3, "gamma")], ("id", "name")), df
        finally:
            del sys.modules["pandas"]
    ''')
    out = _run(python, path)
    assert out.returncode == 0, out.stderr


def test_fr_f13_sql_result_renders_html(python, scratch):
    # 1. Direct _repr_html_ checks: escaping, 200-row cap, non-row statement summary.
    path = _write(scratch, "sql_html.py", '''
        import darkpyonix

        # Escaping in column names and cell values.
        darkpyonix.sql('CREATE TABLE xss ("a<b>c" TEXT)')
        darkpyonix.sql('INSERT INTO xss VALUES (?)', params=("<script>alert(1)</script> & ok",))
        res = darkpyonix.sql('SELECT "a<b>c" FROM xss', target=None)
        rendered = res._repr_html_()
        assert "<script>" not in rendered, rendered
        assert "&lt;script&gt;alert(1)&lt;/script&gt; &amp; ok" in rendered, rendered
        assert "a&lt;b&gt;c" in rendered, rendered

        # 200-row cap with "N more rows" footer.
        darkpyonix.sql("CREATE TABLE big (n INTEGER)")
        for i in range(250):
            darkpyonix.sql("INSERT INTO big VALUES (?)", params=(i,), target=None)
        big = darkpyonix.sql("SELECT n FROM big ORDER BY n", target=None)
        assert len(big) == 250
        big_html = big._repr_html_()
        assert "50 more rows" in big_html, big_html
        tbody_start = big_html.index("<tbody>")
        tbody_end = big_html.index("</tbody>")
        assert big_html[tbody_start:tbody_end].count("<tr>") == 200

        # Non-row statement shows rowcount summary.
        ins = darkpyonix.sql("INSERT INTO big VALUES (?)", params=(999,), target=None)
        ins_html = ins._repr_html_()
        assert "1" in ins_html and "<table>" not in ins_html, ins_html
    ''')
    out = _run(python, path)
    assert out.returncode == 0, out.stderr

    # 2. Kernel cell execution: default DB persists across cells and last expression emits text/html.
    nb_path = _write(scratch, "nb_sql.py", '''
        import darkpyonix
        # %%
        darkpyonix.sql("CREATE TABLE t (id INTEGER, label TEXT)")
        darkpyonix.sql("INSERT INTO t VALUES (?, ?)", params=(1, "a < b"))
        # %%
        darkpyonix.sql("SELECT id, label FROM t")
    ''')
    plan_path = os.path.join(scratch, "plan.json")
    out_path = os.path.join(scratch, "out.json")
    with open(plan_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "path": nb_path,
                "src_root": SRC_ROOT,
                "steps": [
                    {"op": "submit", "params": {"mode": "all"}},
                    {"op": "wait_idle"},
                ],
            },
            f,
        )
    proc = subprocess.run(
        [python, DRIVER, plan_path, out_path],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=scratch,
    )
    assert proc.returncode == 0, proc.stderr
    with open(out_path, encoding="utf-8") as f:
        result = json.load(f)
    run = result["runs"][0]
    assert run["status"] == "ok", run
    cell2_outputs = [c["outputs"] for c in run["cells"] if c["index"] == 2][0]
    exec_results = [o for o in cell2_outputs if o["output_type"] == "execute_result"]
    assert len(exec_results) == 1, cell2_outputs
    html_out = exec_results[0]["data"]["text/html"]
    assert "<th>id</th>" in html_out and "<th>label</th>" in html_out, html_out
    assert "a &lt; b" in html_out, html_out
