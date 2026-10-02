"""Kernel host context for the runtime API (contract stub).

Contract stub -- the executor branch provides the real one; keep the interface:

- ``in_kernel() -> bool``: True while a kernel is executing user code in this process.
- ``current_params() -> dict``: the ``params`` of the run request being executed.
- ``emit_display(bundle, silent=False) -> None``: emit a ``display_data`` output whose
  ``bundle`` is an nbformat MIME dict such as ``{"text/markdown": "..."}``. ``silent=True``
  shows the output to attached clients without keeping it in the run record (FR-F2).

The kernel calls ``set_active(params, emit)`` before running cells and ``clear()`` after.
Standard library only; importing this module must stay free of side effects.
"""
from __future__ import annotations

_active = False
_params = {}  # type: dict
_emit = None


def set_active(params=None, emit=None):
    """Mark this process as hosting a run with ``params``; ``emit(bundle, silent)`` sends outputs."""
    global _active, _params, _emit
    _active = True
    _params = dict(params or {})
    _emit = emit


def clear():
    """Leave the hosted state; the runtime API behaves as under plain ``python file.py``."""
    global _active, _params, _emit
    _active = False
    _params = {}
    _emit = None


def in_kernel():
    return _active


def current_params():
    return dict(_params) if _active else {}


def emit_display(bundle, silent=False):
    if _active and _emit is not None:
        _emit(dict(bundle), bool(silent))
