"""FR-C3: wheel layout of the PyPI distribution, built with a fake binary.

No Rust is compiled here. The platform smoke tests (fresh venv, ``darkpyonix --version``,
manager, kernel) run in .github/workflows/publish-pypi.yml on every platform.
"""
from __future__ import annotations

import os
import subprocess
import sys
import zipfile

import pytest

from conftest import REPO

pytest.importorskip("hatchling")


def build_wheel(out, env_extra):
    env = {k: v for k, v in os.environ.items() if not k.startswith("DARKPYONIX_")}
    env.update(env_extra)
    proc = subprocess.run(
        [sys.executable, "-m", "hatchling", "build", "-t", "wheel", "-d", out],
        cwd=REPO, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        universal_newlines=True,
    )
    return proc


def only_wheel(out):
    names = [n for n in os.listdir(out) if n.endswith(".whl")]
    assert len(names) == 1, names
    return os.path.join(out, names[0])


def fake_binary(scratch, name="darkpyonix"):
    path = os.path.join(scratch, name)
    with open(path, "wb") as f:
        f.write(b"\x7fELF not a real binary")
    os.chmod(path, 0o644)  # CI artifacts lose the executable bit; the hook restores it
    return path


def test_fr_c3_fallback_wheel_is_pure_with_an_unsupported_script(scratch):
    out = os.path.join(scratch, "out")
    proc = build_wheel(out, {})
    assert proc.returncode == 0, proc.stdout
    whl = only_wheel(out)
    assert whl.endswith("-py3-none-any.whl")
    with zipfile.ZipFile(whl) as z:
        names = z.namelist()
        script, = [n for n in names if "/scripts/" in n]
        assert script.endswith(".data/scripts/darkpyonix")
        body = z.read(script).decode()
        assert body.startswith("#!python")
        assert "are not available" in body
        assert "darkpyonix/__main__.py" in names and "darkpyonix/_server.py" in names
        assert not [n for n in names if n.startswith("packaging/")]


def test_fr_c3_platform_wheel_carries_the_binary_as_the_script(scratch):
    out = os.path.join(scratch, "out")
    binary = fake_binary(scratch)
    proc = build_wheel(out, {"DARKPYONIX_BINARY": binary,
                             "DARKPYONIX_WHEEL_TAG": "manylinux_2_28_aarch64"})
    assert proc.returncode == 0, proc.stdout
    whl = only_wheel(out)
    assert whl.endswith("-py3-none-manylinux_2_28_aarch64.whl")
    with zipfile.ZipFile(whl) as z:
        names = z.namelist()
        info, = [i for i in z.infolist() if "/scripts/" in i.filename]
        assert info.filename.endswith(".data/scripts/darkpyonix")
        assert z.read(info) == b"\x7fELF not a real binary"
        assert (info.external_attr >> 16) & 0o111, "script must be executable"
        wheel_meta = z.read([n for n in names if n.endswith(".dist-info/WHEEL")][0]).decode()
        assert "Root-Is-Purelib: false" in wheel_meta
        assert "Tag: py3-none-manylinux_2_28_aarch64" in wheel_meta
        # the runtime API and the kernel ride along in the same wheel
        assert "darkpyonix/__init__.py" in names and "darkpyonix/_markdown.py" in names


def test_fr_c3_windows_binary_keeps_its_exe_suffix(scratch):
    out = os.path.join(scratch, "out")
    binary = fake_binary(scratch, "darkpyonix.exe")
    proc = build_wheel(out, {"DARKPYONIX_BINARY": binary, "DARKPYONIX_WHEEL_TAG": "win_amd64"})
    assert proc.returncode == 0, proc.stdout
    whl = only_wheel(out)
    assert whl.endswith("-py3-none-win_amd64.whl")
    with zipfile.ZipFile(whl) as z:
        assert any(n.endswith(".data/scripts/darkpyonix.exe") for n in z.namelist())


@pytest.mark.parametrize("env", [
    lambda b: {"DARKPYONIX_BINARY": b},
    lambda b: {"DARKPYONIX_WHEEL_TAG": "win_amd64"},
    lambda b: {"DARKPYONIX_BINARY": b, "DARKPYONIX_WHEEL_TAG": "linux_x86_64"},
    lambda b: {"DARKPYONIX_BINARY": b, "DARKPYONIX_WHEEL_TAG": "any"},
])
def test_fr_c3_half_set_or_unpublishable_tags_are_refused(scratch, env):
    out = os.path.join(scratch, "out")
    proc = build_wheel(out, env(fake_binary(scratch)))
    assert proc.returncode != 0
    assert not os.path.isdir(out) or not [n for n in os.listdir(out) if n.endswith(".whl")]
