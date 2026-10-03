"""NFR-K2: the kernel, the runtime API and the format parser import the standard library only."""
from __future__ import annotations

import ast
import os
import sys

from conftest import KERNEL_ROOT

PKG = os.path.join(KERNEL_ROOT, "darkpyonix")
# Code that must stay importable by a bare interpreter. The manager is excluded.
GUARDED = [PKG]
EXCLUDED = [os.path.join(PKG, "manager")]
# FR-X6: the matplotlib backend is loaded by matplotlib itself, never by kernel code.
MPLBACKEND = os.path.join(PKG, "kernel", "mplbackend.py")

# sys.stdlib_module_names exists from 3.10; this fallback covers what kernel code may use on 3.8/3.9.
_FALLBACK = set("""
__future__ _thread abc argparse array ast asyncio atexit base64 binascii bisect builtins
codecs collections contextlib copy csv ctypes dataclasses datetime decimal difflib dis enum errno
faulthandler fcntl fnmatch functools gc getpass glob gzip hashlib heapq hmac html http importlib
inspect io ipaddress itertools json keyword linecache locale logging marshal math mimetypes msvcrt
multiprocessing numbers operator os pathlib pickle platform posixpath pprint queue random re
reprlib secrets select selectors shlex shutil signal site socket socketserver sqlite3 stat string
struct subprocess sys sysconfig tempfile textwrap threading time timeit token tokenize traceback
types typing unicodedata urllib uuid warnings weakref winreg zipfile zlib
""".split())
STDLIB = set(getattr(sys, "stdlib_module_names", ())) | _FALLBACK


def _python_files():
    for root, dirs, files in os.walk(PKG):
        if any(root == e or root.startswith(e + os.sep) for e in EXCLUDED):
            continue
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for name in files:
            if name.endswith(".py"):
                yield os.path.join(root, name)


def _top_level_imports(path):
    tree = ast.parse(open(path, encoding="utf-8").read(), path)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield node.lineno, alias.name.split(".")[0]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            yield node.lineno, node.module.split(".")[0]


def test_nfr_k2_kernel_imports_stdlib_only():
    offenders = []
    for path in _python_files():
        for lineno, mod in _top_level_imports(path):
            if mod == "darkpyonix" or mod in STDLIB:
                continue
            if path == MPLBACKEND and mod == "matplotlib":
                continue
            offenders.append("%s:%d imports %s" % (os.path.relpath(path, KERNEL_ROOT), lineno, mod))
    assert not offenders, "non-stdlib imports in kernel/runtime code:\n" + "\n".join(offenders)


def test_nfr_k2_no_kernel_code_imports_the_matplotlib_backend():
    assert os.path.isfile(MPLBACKEND)
    offenders = []
    for path in _python_files():
        tree = ast.parse(open(path, encoding="utf-8").read(), path)
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module] + [node.module + "." + a.name for a in node.names]
            if any(n.endswith("mplbackend") for n in names):
                offenders.append("%s:%d" % (os.path.relpath(path, KERNEL_ROOT), node.lineno))
    assert not offenders, "kernel code imports mplbackend: " + ", ".join(offenders)
