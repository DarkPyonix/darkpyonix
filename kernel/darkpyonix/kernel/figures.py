"""matplotlib figures as ``image/png`` display_data (SPEC FR-X6).

Standard library only; Python 3.8+. The kernel never imports matplotlib. ``install()`` puts a
meta path finder in front of the import system that watches for the first import of
``matplotlib.pyplot`` by user code. At that moment ``matplotlib`` itself is already loaded, so
the finder can pick the kernel's backend with ``matplotlib.use()`` before pyplot reads
``rcParams["backend"]``, but only when nothing else has chosen a backend yet. The finder never
finds anything itself; the regular finders load pyplot as usual.

No environment variable is set, so child processes see the user's matplotlib configuration.
"""
from __future__ import annotations

import sys
from typing import Any, Callable, Optional

BACKEND_MODULE = "darkpyonix.kernel.mplbackend"
BACKEND = "module://" + BACKEND_MODULE


def _backend_unset(mpl: Any) -> bool:
    """True when neither MPLBACKEND, matplotlibrc nor ``matplotlib.use()`` chose a backend."""
    sentinel = getattr(getattr(mpl, "rcsetup", None), "_auto_backend_sentinel", None)
    rc = getattr(mpl, "rcParams", None)
    if sentinel is None or rc is None:
        return False
    try:
        current = dict.__getitem__(rc, "backend")   # without resolving the sentinel
    except Exception:
        return False
    return current is sentinel


class _PyplotHook(object):
    """A ``sys.meta_path`` finder that chooses the kernel backend as pyplot is imported."""

    def find_spec(self, fullname: str, path: Any = None, target: Any = None) -> None:
        if fullname == "matplotlib.pyplot":
            mpl = sys.modules.get("matplotlib")
            if mpl is not None and _backend_unset(mpl):
                try:
                    mpl.use(BACKEND)
                except Exception:
                    pass
        return None

    # Python < 3.4 protocol, still consulted by some import hooks.
    def find_module(self, fullname: str, path: Any = None) -> None:
        return None


_hook = None  # type: Optional[_PyplotHook]


def install() -> None:
    global _hook
    if _hook is None:
        _hook = _PyplotHook()
        sys.meta_path.insert(0, _hook)


def uninstall() -> None:
    global _hook
    if _hook is not None:
        try:
            sys.meta_path.remove(_hook)
        except ValueError:
            pass
        _hook = None


def flush(log: Callable[[str], None]) -> None:
    """Show and close the figures still open at the end of a cell, if pyplot uses our backend."""
    backend = sys.modules.get(BACKEND_MODULE)
    if backend is None:
        return
    try:
        backend.flush_figures()
    except Exception as exc:
        log("cannot show matplotlib figures: %r" % (exc,))
