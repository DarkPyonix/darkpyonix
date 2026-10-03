"""Defensive access to the kernel host context (``darkpyonix.kernel.hostctx``).

When the module is missing or reports ``in_kernel() == False`` the runtime API behaves as it
does under plain ``python file.py`` (SPEC FR-F5).
"""
from __future__ import annotations

import sys


def hostctx():
    """The host context module, or None when it cannot be imported."""
    mod = sys.modules.get("darkpyonix.kernel.hostctx")
    if mod is not None:
        return mod
    try:
        from darkpyonix.kernel import hostctx as mod  # tiny, stdlib only
    except ImportError:
        return None
    return mod


def active():
    """The host context module when a kernel is executing code here, else None."""
    mod = hostctx()
    if mod is None:
        return None
    try:
        return mod if mod.in_kernel() else None
    except Exception:
        return None


def current_params():
    mod = active()
    if mod is None:
        return {}
    return dict(mod.current_params() or {})
