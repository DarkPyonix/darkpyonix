"""The bridge between a running kernel and the runtime API (``darkpyonix.*``).

Standard library only; Python 3.8+. The executor sets these module globals while it runs;
the runtime API reads them. Outside a kernel every function is a safe no-op, so notebook
files keep behaving like plain Python (SPEC FR-F5).
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Optional

_in_kernel = False
_params = {}            # type: Dict[str, Any]
_display = None         # type: Optional[Callable[[Dict[str, Any], Dict[str, Any], bool], None]]


def in_kernel() -> bool:
    """True while code runs inside a DarkPyonix kernel."""
    return _in_kernel


def current_params() -> Dict[str, Any]:
    """The ``params`` of the run being executed (a copy); ``{}`` outside a kernel."""
    return dict(_params)


def emit_display(bundle: Dict[str, Any], silent: bool = False) -> bool:
    """Emit a ``display_data`` output for the cell that is running.

    ``bundle`` is either a MIME bundle (``{"text/plain": ..., "text/html": ...}``) or
    ``{"data": <MIME bundle>, "metadata": {...}}``. ``silent=True`` shows the output to live
    subscribers but keeps it out of the run log (FORMAT §3.2). Returns False outside a kernel
    (nothing is printed).
    """
    hook = _display
    if not _in_kernel or hook is None:
        return False
    if isinstance(bundle.get("data"), dict):
        data = bundle["data"]
        metadata = bundle.get("metadata") or {}
    else:
        data = bundle
        metadata = {}
    hook(dict(data), dict(metadata), bool(silent))
    return True


# ---------------------------------------------------------------- executor side

def _install(display: Callable[[Dict[str, Any], Dict[str, Any], bool], None]) -> None:
    global _in_kernel, _display
    _display = display
    _in_kernel = True


def _uninstall() -> None:
    global _in_kernel, _display, _params
    _in_kernel = False
    _display = None
    _params = {}


def _set_params(params: Optional[Dict[str, Any]]) -> None:
    global _params
    _params = dict(params or {})
