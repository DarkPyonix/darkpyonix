"""Rich display: IPython-style ``_repr_*_`` methods to nbformat MIME bundles (SPEC FR-X5).

Standard library only; Python 3.8+.
"""
from __future__ import annotations

import base64
import inspect
import json
from typing import Any, Dict, Tuple

from darkpyonix import _hostctx as hostctx

TEXT_PLAIN_LIMIT = 100000

# (method, mime type, kind) in IPython's order.
_REPR_METHODS = (
    ("_repr_html_", "text/html", "text"),
    ("_repr_markdown_", "text/markdown", "text"),
    ("_repr_svg_", "image/svg+xml", "text"),
    ("_repr_png_", "image/png", "binary"),
    ("_repr_jpeg_", "image/jpeg", "binary"),
    ("_repr_latex_", "text/latex", "text"),
    ("_repr_json_", "application/json", "json"),
)


def _split(result: Any) -> Tuple[Any, Any]:
    """``_repr_*_`` may return ``value`` or ``(value, metadata)``."""
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[1], dict):
        return result
    return result, None


def _normalize(kind: str, value: Any) -> Any:
    """The JSON value for one MIME type, or None when ``value`` is unusable."""
    if value is None:
        return None
    if kind == "text":
        return value if isinstance(value, str) else None
    if kind == "binary":
        if isinstance(value, (bytes, bytearray)):
            return base64.b64encode(bytes(value)).decode("ascii")
        return value if isinstance(value, str) else None
    # json
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return None
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return None
    return value


def _plain(obj: Any) -> str:
    text = repr(obj)
    if len(text) > TEXT_PLAIN_LIMIT:
        text = text[:TEXT_PLAIN_LIMIT] + "..."
    return text


def format_bundle(obj: Any) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """``(data, metadata)`` for ``obj``. ``text/plain`` is always present.

    A failing ``_repr_*_`` method is skipped; a failing ``__repr__`` propagates, as it would
    in plain Python.
    """
    data = {}       # type: Dict[str, Any]
    metadata = {}   # type: Dict[str, Any]
    if not inspect.isclass(obj):
        method = _lookup(obj, "_repr_mimebundle_")
        if method is not None:
            try:
                result = method(include=None, exclude=None)
            except Exception:
                result = None
            bdata, bmeta = _split(result)
            if isinstance(bdata, dict):
                for mime, value in bdata.items():
                    if isinstance(mime, str) and value is not None:
                        if isinstance(value, (bytes, bytearray)):
                            value = base64.b64encode(bytes(value)).decode("ascii")
                        data[mime] = value
            if isinstance(bmeta, dict):
                metadata.update(bmeta)
        for name, mime, kind in _REPR_METHODS:
            if mime in data:
                continue
            method = _lookup(obj, name)
            if method is None:
                continue
            try:
                value, meta = _split(method())
            except Exception:
                continue
            value = _normalize(kind, value)
            if value is None:
                continue
            data[mime] = value
            if meta:
                metadata[mime] = meta
    if not isinstance(data.get("text/plain"), str):
        data["text/plain"] = _plain(obj)
    return data, metadata


def _lookup(obj: Any, name: str):
    """A bound ``_repr_*_`` method defined on the type (not a ``__getattr__`` fabrication)."""
    try:
        attr = inspect.getattr_static(obj, name)
    except AttributeError:
        return None
    if attr is None:
        return None
    try:
        method = getattr(obj, name)
    except Exception:
        return None
    return method if callable(method) else None


def display(*objs: Any) -> None:
    """Show each object as a ``display_data`` output; ``print(repr(obj))`` outside a kernel."""
    for obj in objs:
        if hostctx.in_kernel():
            data, metadata = format_bundle(obj)
            hostctx.emit_display({"data": data, "metadata": metadata})
        else:
            print(repr(obj))
