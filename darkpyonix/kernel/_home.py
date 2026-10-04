"""The DarkPyonix runtime home (``DARKPYONIX_HOME``, default ``~/.darkpyonix``).

PROTOCOL §1 lists what lives here. Standard library only; Python 3.8+.
"""
from __future__ import annotations

import hashlib
import os
import secrets
import shutil
import sys
from typing import Optional

USER_KEY_BYTES = 32


def home() -> str:
    """Return the runtime home, honouring ``DARKPYONIX_HOME``."""
    path = os.environ.get("DARKPYONIX_HOME") or os.path.join(os.path.expanduser("~"), ".darkpyonix")
    return os.path.abspath(path)


def subdir(name: str) -> str:
    """Return ``<home>/<name>``, creating it (and the home) with mode 0700 if missing."""
    path = os.path.join(home(), name)
    os.makedirs(path, mode=0o700, exist_ok=True)
    return path


def source_root() -> str:
    """Return a directory that contains this package under the name ``darkpyonix``.

    In a source checkout the package folder is named ``kernel``, so no directory on ``sys.path``
    can import it by name (the launcher loads it by path). Child interpreters that user code
    starts (``multiprocessing`` spawn, ``subprocess``) need such a directory. It is materialised
    under ``<home>/src/<id>/darkpyonix`` as a symlink to the package folder, or as a copy where
    symlinks are not allowed; ``<id>`` keys it to the checkout, so checkouts do not clash. When
    the folder is already named ``darkpyonix`` (installed, or extracted by the Rust launcher) its
    parent is returned and nothing is created.
    """
    package = os.path.dirname(os.path.abspath(__file__))
    if os.path.basename(package) == "darkpyonix":
        return os.path.dirname(package)
    root = os.path.join(subdir("src"), hashlib.sha256(package.encode("utf-8")).hexdigest()[:12])
    link = os.path.join(root, "darkpyonix")
    if os.path.realpath(link) == os.path.realpath(package):
        return root
    os.makedirs(root, mode=0o700, exist_ok=True)
    try:
        if os.path.lexists(link):
            if os.path.islink(link):
                os.unlink(link)
            else:
                shutil.rmtree(link)
        os.symlink(package, link, target_is_directory=True)
    except FileExistsError:
        pass  # another kernel of the same checkout won the race
    except (OSError, NotImplementedError):
        staging = link + ".%d.tmp" % os.getpid()
        shutil.copytree(package, staging, ignore=shutil.ignore_patterns("__pycache__"))
        try:
            os.replace(staging, link)
        except OSError:
            shutil.rmtree(staging, ignore_errors=True)
    return root


def expose_package() -> None:
    """Make ``import darkpyonix`` work in child interpreters of this process (FR-K1).

    ``multiprocessing`` spawn children inherit ``sys.path``; other children inherit
    ``PYTHONPATH``. Both get :func:`source_root` appended, after everything the user set.
    """
    root = source_root()
    if root not in sys.path:
        sys.path.append(root)
    parts = [x for x in os.environ.get("PYTHONPATH", "").split(os.pathsep) if x]
    if root not in parts:
        os.environ["PYTHONPATH"] = os.pathsep.join(parts + [root])


def kernels_dir() -> str:
    return subdir("kernels")


def locks_dir() -> str:
    return subdir("locks")


def managers_dir() -> str:
    return subdir("managers")


def write_private(path: str, data: bytes) -> None:
    """Atomically write ``data`` to ``path`` with mode 0600."""
    tmp = "%s.%s.tmp" % (path, secrets.token_hex(4))
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)


def write_public(path: str, data: bytes) -> None:
    """Atomically write ``data`` to ``path`` with mode 0644 (no secrets)."""
    tmp = "%s.%s.tmp" % (path, secrets.token_hex(4))
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        os.write(fd, data)
    finally:
        os.close(fd)
    os.replace(tmp, path)


def user_key() -> bytes:
    """Return the per-user key, creating it atomically on first use (PROTOCOL §1, FR-A1)."""
    path = os.path.join(home(), "user.key")
    try:
        with open(path, "rb") as f:
            key = f.read()
        if len(key) == USER_KEY_BYTES:
            return key
    except FileNotFoundError:
        pass
    os.makedirs(home(), mode=0o700, exist_ok=True)
    candidate = secrets.token_bytes(USER_KEY_BYTES)
    tmp = "%s.%s.tmp" % (path, secrets.token_hex(4))
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, candidate)
        os.fsync(fd)
    finally:
        os.close(fd)
    try:
        # link() fails if another process won the race; then use theirs.
        os.link(tmp, path)
    except FileExistsError:
        pass
    finally:
        os.unlink(tmp)
    with open(path, "rb") as f:
        return f.read()


def user_tag(key: Optional[bytes] = None) -> str:
    """``hex(SHA-256(user.key))[:16]`` (PROTOCOL §2.2)."""
    return hashlib.sha256(key if key is not None else user_key()).hexdigest()[:16]
