"""Shared fixtures. All runtime state lives under .scratch/ (AGENTS.md "Where files go")."""
from __future__ import annotations

import glob
import os
import shutil
import subprocess
import sys
import uuid

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# The package lives in darkpyonix/kernel/ but is imported as ``darkpyonix`` (the build maps the
# folder to that name). Tests need a directory that *contains* a ``darkpyonix`` entry, both for
# this process and for the bare interpreters they spawn (PYTHONPATH), so a symlink
# ``.scratch/src/darkpyonix -> darkpyonix/kernel`` provides it (a copy where symlinks are not
# allowed). SRC_ROOT is that directory; PACKAGE_DIR the real folder.
PACKAGE_DIR = os.path.join(REPO, "darkpyonix", "kernel")
SRC_ROOT = os.path.join(REPO, ".scratch", "src")
SCRATCH = os.path.join(REPO, ".scratch", "tests")


def _link_package():
    link = os.path.join(SRC_ROOT, "darkpyonix")
    if os.path.realpath(link) == os.path.realpath(PACKAGE_DIR):
        return
    os.makedirs(SRC_ROOT, exist_ok=True)
    if os.path.lexists(link):
        if os.path.islink(link):
            os.unlink(link)
        else:
            shutil.rmtree(link)
    try:
        os.symlink(PACKAGE_DIR, link, target_is_directory=True)
    except (OSError, NotImplementedError):
        shutil.copytree(PACKAGE_DIR, link, ignore=shutil.ignore_patterns("__pycache__"))


_link_package()
if SRC_ROOT not in sys.path:
    sys.path.insert(0, SRC_ROOT)


def _probe(executable: str):
    try:
        out = subprocess.run(
            [executable, "-c", "import sys;print('%d.%d.%d' % sys.version_info[:3]);print(sys.executable)"],
            capture_output=True, text=True, timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode != 0:
        return None
    version, real = out.stdout.split()
    major, minor = (int(x) for x in version.split(".")[:2])
    if (major, minor) < (3, 8):
        return None
    return version, os.path.realpath(real)


def find_pythons():
    """Every distinct CPython >= 3.8 on this machine (NFR-K1).

    ``DARKPYONIX_TEST_PYTHONS`` (os.pathsep-separated) overrides discovery.
    """
    env = os.environ.get("DARKPYONIX_TEST_PYTHONS")
    candidates = env.split(os.pathsep) if env else []
    if not env:
        for d in os.environ.get("PATH", "").split(os.pathsep):
            candidates += glob.glob(os.path.join(d, "python3.[0-9]*"))
            candidates.append(os.path.join(d, "python3"))
        candidates += ["/usr/bin/python3", sys.executable]
        candidates += glob.glob("/Library/Frameworks/Python.framework/Versions/3.*/bin/python3")
        candidates += glob.glob("/opt/homebrew/bin/python3.[0-9]*")
    seen = {}
    for c in candidates:
        if not c or c.endswith("-config") or not os.path.isfile(c) or not os.access(c, os.X_OK):
            continue
        info = _probe(c)
        if info is None:
            continue
        version, real = info
        key = ".".join(version.split(".")[:2])
        seen.setdefault(key, c)
    return [seen[k] for k in sorted(seen, key=lambda v: tuple(int(x) for x in v.split(".")))]


PYTHONS = find_pythons()


@pytest.fixture
def scratch():
    """A fresh directory under .scratch/tests/, removed after the test."""
    path = os.path.join(SCRATCH, uuid.uuid4().hex[:12])
    os.makedirs(path)
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def dp_home(scratch, monkeypatch):
    """Point DARKPYONIX_HOME at a private directory for this test (and its children)."""
    home = os.path.join(scratch, "home")
    os.makedirs(home, mode=0o700)
    monkeypatch.setenv("DARKPYONIX_HOME", home)
    return home


@pytest.fixture(params=PYTHONS or [sys.executable], ids=lambda p: os.path.basename(p))
def python(request):
    """Parametrises a test over every available interpreter (NFR-K1)."""
    return request.param
