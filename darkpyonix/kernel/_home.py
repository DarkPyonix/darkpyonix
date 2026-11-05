"""The DarkPyonix runtime home (``DARKPYONIX_HOME``, default ``~/.darkpyonix``).

PROTOCOL §1 lists what lives here. Standard library only; Python 3.8+.
"""
from __future__ import annotations

import hashlib
import os
import secrets
import shutil
import stat
import sys
import tempfile


def home() -> str:
    """Return the runtime home, honouring ``DARKPYONIX_HOME``."""
    path = os.environ.get("DARKPYONIX_HOME") or os.path.join(os.path.expanduser("~"), ".darkpyonix")
    return os.path.abspath(path)


def subdir(name: str) -> str:
    """Return ``<home>/<name>``, creating it (and the home) with mode 0700 if missing."""
    path = os.path.join(home(), name)
    os.makedirs(path, mode=0o700, exist_ok=True)
    return path


def _package_dir() -> str:
    """The real folder of this package (symlinks resolved, so a link named ``darkpyonix`` to
    ``kernel`` still counts as a source checkout)."""
    return os.path.realpath(os.path.dirname(os.path.abspath(__file__)))


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
    package = _package_dir()
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
    ``PYTHONPATH``. Both get :func:`source_root` appended, after everything the user set. A no-op
    when the folder is already named ``darkpyonix``.
    """
    if os.path.basename(_package_dir()) == "darkpyonix":
        return  # installed or extracted: already importable; never leak its parent to children
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


def sockets_dir() -> str:
    return subdir("sockets")


# sockaddr_un.sun_path holds 104 bytes on macOS and the BSDs and 108 on Linux (terminator included).
_MAX_SOCKET_PATH = 100


def control_path(kernel_id: str) -> str:
    """Where the kernel's control socket lives (PROTOCOL §3.1): ``<home>/sockets/<kernel_id>.sock``.

    A runtime home with a long path would overflow ``sun_path``. Then the socket goes to a private
    directory ``<tmp>/darkpyonix-<uid>/`` instead; the announce's ``control`` names the real path, so no
    reader needs to know which one was used. POSIX only.
    """
    path = os.path.join(sockets_dir(), kernel_id + ".sock")
    if len(os.fsencode(path)) <= _MAX_SOCKET_PATH:
        return path
    base = os.path.join(tempfile.gettempdir(), "darkpyonix-%d" % os.geteuid())
    os.makedirs(base, mode=0o700, exist_ok=True)
    st = os.lstat(base)
    if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.geteuid() or st.st_mode & 0o077:
        raise OSError("%s is not a private directory of this account" % base)
    return os.path.join(base, kernel_id + ".sock")


def account() -> str:
    """The OS account that PROTOCOL §2.2 hashes: ``uid:<n>`` (POSIX) or ``sid:<S-1-5-...>``."""
    from . import _peer
    return _peer.account()


def user_tag() -> str:
    """``hex(SHA-256(account))[:16]`` (PROTOCOL §2.2). Derived from the OS account, no key file."""
    return hashlib.sha256(account().encode("utf-8")).hexdigest()[:16]
