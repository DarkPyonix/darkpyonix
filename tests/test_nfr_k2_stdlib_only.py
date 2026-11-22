"""NFR-K2: the kernel, the runtime API and the format parser import the standard library only."""
from __future__ import annotations

import ast
import os
import sys

from conftest import PACKAGE_DIR

PKG = PACKAGE_DIR
# Code that must stay importable by a bare interpreter.
GUARDED = [PKG]
EXCLUDED = []
# FR-X6: the matplotlib backend is loaded by matplotlib itself, never by kernel code.
MPLBACKEND = os.path.join(PKG, "_mplbackend.py")

# sys.stdlib_module_names exists from 3.10; this fallback covers what kernel code may use on 3.8/3.9.
_FALLBACK = set("""
__future__ _thread abc argparse array ast asyncio atexit base64 binascii bisect builtins calendar
codecs collections concurrent contextlib contextvars copy csv ctypes dataclasses datetime decimal difflib dis enum errno
faulthandler fcntl fnmatch functools gc getpass glob gzip hashlib heapq hmac html http importlib
inspect io ipaddress itertools json keyword linecache locale logging marshal math mimetypes msvcrt
multiprocessing numbers operator os pathlib pickle platform posixpath pprint queue random re
reprlib secrets select selectors shlex shutil signal site socket socketserver sqlite3 stat string
struct subprocess sys sysconfig tempfile textwrap threading time timeit token tokenize tomllib traceback
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
            offenders.append("%s:%d imports %s" % (os.path.relpath(path, PACKAGE_DIR), lineno, mod))
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
                offenders.append("%s:%d" % (os.path.relpath(path, PACKAGE_DIR), node.lineno))
    assert not offenders, "kernel code imports mplbackend: " + ", ".join(offenders)


# NFR-K2 amendment (INTENT D14, D25): non-stdlib dynamic imports are limited to these modules
# inside function bodies of the designated files.
DYNAMIC_ALLOWLIST = {
    "yaml": "_data.py",
    "cppyy": "_interop.py",
}


def _is_dynamic_import_call(node):
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name) and func.id in ("__import__", "import_module"):
        return True
    if isinstance(func, ast.Attribute) and func.attr in ("__import__", "import_module"):
        return True
    return False


def _is_init_getattr_exemption(rel_path, fn_stack, node):
    if rel_path != "__init__.py" or fn_stack != ["__getattr__"]:
        return False
    if not node.args:
        return False
    arg0 = node.args[0]
    return (
        isinstance(arg0, ast.BinOp)
        and isinstance(arg0.op, ast.Add)
        and isinstance(arg0.left, ast.Constant)
        and arg0.left.value == "darkpyonix."
    )


def _dynamic_import_arg(node):
    if node.args:
        return node.args[0]
    for kw in node.keywords:
        if kw.arg == "name":
            return kw.value
    return None


def test_nfr_k2_dynamic_imports_are_allowlisted():
    offenders = []
    for path in _python_files():
        rel = os.path.relpath(path, PACKAGE_DIR).replace(os.sep, "/")
        tree = ast.parse(open(path, encoding="utf-8").read(), path)

        def walk(node, fn_stack):
            if _is_dynamic_import_call(node):
                if not _is_init_getattr_exemption(rel, fn_stack, node):
                    arg = _dynamic_import_arg(node)
                    if not (isinstance(arg, ast.Constant) and isinstance(arg.value, str)):
                        offenders.append(
                            "%s:%d dynamic import argument is not a string constant"
                            % (rel, node.lineno)
                        )
                    else:
                        top = arg.value.split(".")[0]
                        if top == "darkpyonix" or top in STDLIB:
                            pass
                        elif top in DYNAMIC_ALLOWLIST:
                            allowed_file = DYNAMIC_ALLOWLIST[top]
                            if rel != allowed_file or not fn_stack:
                                offenders.append(
                                    "%s:%d dynamic import %r only allowed inside a function in %s"
                                    % (rel, node.lineno, top, allowed_file)
                                )
                        else:
                            offenders.append(
                                "%s:%d dynamic import %r is not allowlisted"
                                % (rel, node.lineno, arg.value)
                            )
            next_stack = fn_stack
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                next_stack = fn_stack + [node.name]
            for child in ast.iter_child_nodes(node):
                walk(child, next_stack)

        walk(tree, [])

    assert not offenders, "disallowed dynamic imports in kernel/runtime code:\n" + "\n".join(offenders)
