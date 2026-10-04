"""``darkpyonix.params`` (SPEC FR-F3, FORMAT §3.3).

A value is resolved in this order: the run request's ``params`` (in a kernel) -> the command
line ``--name value`` / ``--name=value`` -> ``default``. With ``choices`` an ``int`` default is
an index into ``choices``. Invalid values raise ``ValueError`` naming the parameter.
"""
from __future__ import annotations

import math
import sys

from darkpyonix import _host

_MISSING = object()
_TRUE = {"true", "1", "yes", "y", "on"}
_FALSE = {"false", "0", "no", "n", "off"}


def _type_name(tp):
    return getattr(tp, "__name__", repr(tp))


def _parse_bool(name, value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    text = str(value).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    raise ValueError("parameter %r: %r is not a boolean (true/false/1/0)" % (name, value))


def _convert(name, value, tp):
    if tp is None or value is None:
        return value
    if tp is bool:
        return _parse_bool(name, value)
    if isinstance(value, tp) and not (isinstance(value, bool) and tp in (int, float)):
        return value
    if isinstance(value, bool) and tp in (int, float):
        raise ValueError("parameter %r: expected %s, got a boolean" % (name, _type_name(tp)))
    try:
        if tp is int and isinstance(value, str):
            text = value.strip()
            try:
                return int(text, 0)
            except ValueError:
                as_float = float(text)
                if as_float.is_integer():
                    return int(as_float)
                raise
        if tp is int and isinstance(value, float):
            if not value.is_integer():
                raise ValueError(value)
            return int(value)
        return tp(value)
    except (TypeError, ValueError):
        raise ValueError("parameter %r: cannot convert %r to %s" % (name, value, _type_name(tp))) from None


def _argv_value(name, argv, tp):
    """The value of ``--name`` in ``argv`` (last one wins), or _MISSING. Never consumes argv."""
    flag = "--" + name
    found = _MISSING
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--":
            break
        if arg.startswith(flag + "="):
            found = arg[len(flag) + 1:]
        elif arg == flag:
            nxt = argv[i + 1] if i + 1 < len(argv) else None
            if nxt is None or (nxt.startswith("--") and len(nxt) > 2):
                if tp is bool:
                    found = True
                else:
                    raise ValueError("parameter %r: %s needs a value" % (name, flag))
            else:
                found = nxt
                i += 1
        i += 1
    return found


def _close(a, b):
    return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-12)


def _in_choices(value, choices):
    for c in choices:
        if isinstance(value, bool) != isinstance(c, bool):
            continue
        if value == c:
            return True
        if isinstance(value, float) and isinstance(c, (int, float)) and _close(value, c):
            return True
    return False


class Params(object):
    """The ``darkpyonix.params`` object: declare parameters and read their values."""

    def __init__(self):
        self._specs = {}  # name -> spec dict, in declaration order
        self._values = {}

    def get(self, name, default=None, choices=None, range=None, type=None, help=None):  # noqa: A002
        """Declare parameter ``name`` and return its resolved value."""
        if not isinstance(name, str) or not name:
            raise ValueError("parameter name must be a non-empty string, got %r" % (name,))
        choices = list(choices) if choices is not None else None
        if choices is not None and not choices:
            raise ValueError("parameter %r: choices must not be empty" % name)
        if range is not None:
            range = tuple(range)
            if len(range) not in (2, 3):
                raise ValueError("parameter %r: range must be (min, max) or (min, max, step)" % name)

        # default: an index into choices when it is an int (not a bool).
        default_value = default
        if choices is not None and isinstance(default, int) and not isinstance(default, bool):
            if not -len(choices) <= default < len(choices):
                raise ValueError("parameter %r: default index %d is outside choices (%d items)"
                                 % (name, default, len(choices)))
            default_value = choices[default]

        tp = type
        if tp is None:
            sample = choices[0] if choices is not None else default_value
            if sample is None and range is not None:
                sample = range[0]
            if sample is not None:
                tp = sample.__class__
                if tp is int and range is not None and any(isinstance(x, float) for x in range):
                    tp = float

        value = _MISSING
        source = "default"
        run_params = _host.current_params()
        if name in run_params:
            value, source = run_params[name], "run"
        else:
            cli = _argv_value(name, sys.argv[1:], tp)
            if cli is not _MISSING:
                value, source = cli, "argv"
        if value is _MISSING:
            value = default_value

        value = _convert(name, value, tp)
        self._validate(name, value, choices, range, source)

        spec = {
            "name": name,
            "default": default_value,
            "default_index": choices.index(default_value) if choices is not None and default_value in choices else None,
            "choices": choices,
            "range": list(range) if range is not None else None,
            "type": _type_name(tp) if tp is not None else None,
            "help": help,
            "value": value,
            "source": source,
        }
        self._specs.pop(name, None)
        self._specs[name] = spec
        self._values[name] = value
        return value

    @staticmethod
    def _validate(name, value, choices, range, source):
        if choices is not None and not _in_choices(value, choices):
            raise ValueError("parameter %r: %r (from %s) is not one of %r" % (name, value, source, choices))
        if range is not None and value is not None:
            lo, hi = range[0], range[1]
            try:
                ok = lo <= value <= hi
            except TypeError:
                raise ValueError("parameter %r: %r is not comparable with range %r" % (name, value, range)) from None
            if not ok:
                raise ValueError("parameter %r: %r (from %s) is outside range [%r, %r]" % (name, value, source, lo, hi))
            if len(range) == 3 and range[2]:
                steps = (value - lo) / range[2]
                if not _close(steps, round(steps)):
                    raise ValueError("parameter %r: %r is not on the step %r from %r" % (name, value, range[2], lo))

    def all(self):
        """Resolved values of every parameter declared so far, in declaration order."""
        return dict(self._values)

    def spec(self):
        """Descriptions of declared parameters (for clients' parameter forms)."""
        return [dict(s) for s in self._specs.values()]

    def __repr__(self):
        return "<darkpyonix.params %r>" % (self._values,)


params = Params()
