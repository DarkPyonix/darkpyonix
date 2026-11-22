"""``darkpyonix.json``, ``toml`` and ``yaml`` data format helpers (SPEC FR-F12)."""
from __future__ import annotations

import importlib
import json as _json
import sys

from darkpyonix._errors import InteropUnavailable

if sys.version_info >= (3, 11):
    import tomllib as _toml
else:
    from darkpyonix._vendor import tomli as _toml


def _validate_target(target):
    if target is not None and not (isinstance(target, str) and target.isidentifier()):
        raise ValueError(
            "target must be a valid Python identifier or None, got %r" % (target,)
        )


def json(text, target="_"):
    """Parse JSON ``text``, optionally binding the result to ``target`` in caller globals."""
    _validate_target(target)
    value = _json.loads(text)
    if target is not None:
        sys._getframe(1).f_globals[target] = value
    return value


def toml(text, target="_"):
    """Parse TOML ``text``, optionally binding the result to ``target`` in caller globals."""
    _validate_target(target)
    value = _toml.loads(text)
    if target is not None:
        sys._getframe(1).f_globals[target] = value
    return value


def yaml(text, target="_"):
    """Parse YAML ``text`` with PyYAML ``safe_load`` and optionally bind ``target``."""
    _validate_target(target)
    try:
        yaml_mod = importlib.import_module("yaml")
    except ImportError as exc:
        raise InteropUnavailable(
            "darkpyonix.yaml() requires PyYAML; install it with 'uv pip install pyyaml'"
        ) from exc
    value = yaml_mod.safe_load(text)
    if target is not None:
        sys._getframe(1).f_globals[target] = value
    return value
