"""FR-F12: darkpyonix.json, toml, and yaml across every interpreter (NFR-K1)."""
from __future__ import annotations

import os
import subprocess
import textwrap

from conftest import SRC_ROOT


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


def test_fr_f12_toml_on_all_interpreters(python, scratch):
    path = _write(scratch, "toml_check.py", '''
        import datetime
        import json as std_json
        import darkpyonix

        doc = """
        title = "DarkPyonix"
        count = 42
        ratio = 3.5
        enabled = true
        tags = ["kernel", "runtime"]
        released = 2026-11-22

        [section]
        nested = { a = 1, b = "two" }
        """
        parsed = darkpyonix.toml(doc)
        assert parsed == {
            "title": "DarkPyonix",
            "count": 42,
            "ratio": 3.5,
            "enabled": True,
            "tags": ["kernel", "runtime"],
            "released": datetime.date(2026, 11, 22),
            "section": {"nested": {"a": 1, "b": "two"}},
        }, parsed

        # JSON helper returns identical structure for JSON text.
        jval = darkpyonix.json('{"k": [1, true, null, "x"]}')
        assert jval == {"k": [1, True, None, "x"]}, jval

        # Underlying parse errors propagate.
        try:
            darkpyonix.toml("= invalid =")
        except ValueError:
            pass
        else:
            raise AssertionError("expected TOML decode error")

        try:
            darkpyonix.json("{bad json")
        except std_json.JSONDecodeError:
            pass
        else:
            raise AssertionError("expected JSONDecodeError")
    ''')
    out = _run(python, path)
    assert out.returncode == 0, out.stderr


def test_fr_f12_target_binds_in_plain_python(python, scratch):
    path = _write(scratch, "target_bind.py", '''
        import darkpyonix

        # Default target is "_" even in plain python where "_" is not set automatically.
        ret = darkpyonix.json('{"a": 1}')
        assert ret == {"a": 1}
        assert _ == {"a": 1}

        # Explicit target binds that name without overwriting "_".
        ret2 = darkpyonix.toml('host = "localhost"\\nport = 8080\\n', target="cfg")
        assert ret2 == {"host": "localhost", "port": 8080}
        assert cfg == {"host": "localhost", "port": 8080}
        assert _ == {"a": 1}

        # target=None returns the value without binding anything.
        ret3 = darkpyonix.json('[10, 20]', target=None)
        assert ret3 == [10, 20]
        assert _ == {"a": 1}

        # Invalid targets raise ValueError.
        for bad in ("not valid", "123bad", "", 42):
            try:
                darkpyonix.json("1", target=bad)
            except ValueError:
                pass
            else:
                raise AssertionError("expected ValueError for target=%r" % (bad,))
            try:
                darkpyonix.toml("a = 1", target=bad)
            except ValueError:
                pass
            else:
                raise AssertionError("expected ValueError for toml target=%r" % (bad,))
    ''')
    out = _run(python, path)
    assert out.returncode == 0, out.stderr


def test_fr_f12_yaml_requires_pyyaml(python, scratch):
    path = _write(scratch, "yaml_check.py", '''
        import importlib.util
        import sys
        import darkpyonix

        # Importing darkpyonix and using json/toml never imports yaml.
        darkpyonix.json("1", target=None)
        darkpyonix.toml("a = 1", target=None)
        assert "yaml" not in sys.modules

        has_yaml = importlib.util.find_spec("yaml") is not None
        if not has_yaml:
            try:
                darkpyonix.yaml("a: 1\\n")
            except darkpyonix.InteropUnavailable as exc:
                assert isinstance(exc, ImportError)
                msg = str(exc).lower()
                assert "pyyaml" in msg and "install" in msg, msg
            else:
                raise AssertionError("expected InteropUnavailable when PyYAML is absent")
            assert "yaml" not in sys.modules
        else:
            parsed = darkpyonix.yaml("a: 1\\nb:\\n  - two\\n  - three\\n", target="y")
            assert parsed == {"a": 1, "b": ["two", "three"]}, parsed
            assert y == parsed
            # safe_load must reject arbitrary Python object construction tags.
            try:
                darkpyonix.yaml("!!python/object/apply:os.system ['echo nope']", target=None)
            except Exception:
                pass
            else:
                raise AssertionError("yaml() must use safe_load")
    ''')
    out = _run(python, path)
    assert out.returncode == 0, out.stderr
