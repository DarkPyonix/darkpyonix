"""Shared exception types for optional runtime and interop features (SPEC FR-F8..F13)."""
from __future__ import annotations


class InteropUnavailable(ImportError):
    """Raised when an optional runtime dependency or interop toolchain is missing."""
