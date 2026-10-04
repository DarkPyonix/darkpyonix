"""``@darkpyonix.binding`` (SPEC FR-F4, FORMAT §3.4, issue #6).

The decorator re-evaluates the decorated ``def`` / ``class`` from its source in a snapshot of
the defining module's globals taken at definition time. Imports and earlier bindings are in
the snapshot; ``[code]`` variables assigned later are not, so referencing them raises
``NameError``. Only ``binding`` (and decorators written above it, which the original statement
applies afterwards anyway) is stripped; decorators below it are re-applied.

Works the same in a kernel and under plain ``python file.py`` (SPEC FR-F5). Python 3.8
compatible: no ``ast.unparse``; decorator lines are blanked in the original source instead, so
line numbers in tracebacks still point at the file.
"""
from __future__ import annotations

import __future__
import ast
import builtins
import linecache
import sys
import textwrap
import warnings


_FUTURE_FLAGS = 0
for _feature in __future__.all_feature_names:
    _FUTURE_FLAGS |= getattr(__future__, _feature).compiler_flag


class _Unresolved(Exception):
    pass


def _resolve(expr, frame):
    """Resolve a dotted-name decorator expression without evaluating anything else."""
    if isinstance(expr, ast.Name):
        for scope in (frame.f_locals, frame.f_globals):
            if expr.id in scope:
                return scope[expr.id]
        if hasattr(builtins, expr.id):
            return getattr(builtins, expr.id)
        raise _Unresolved(expr.id)
    if isinstance(expr, ast.Attribute):
        base = _resolve(expr.value, frame)
        try:
            return getattr(base, expr.attr)
        except Exception:
            raise _Unresolved(expr.attr) from None
    raise _Unresolved(type(expr).__name__)


def _first_line(node):
    return min([node.lineno] + [d.lineno for d in node.decorator_list])


def _find_node(tree, name, lineno):
    """The def/class named ``name`` whose span (decorators included) contains ``lineno``."""
    kinds = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
    best = None
    for node in ast.walk(tree):
        if isinstance(node, kinds) and node.name == name and node.decorator_list:
            end = getattr(node, "end_lineno", None) or node.lineno
            if _first_line(node) <= lineno <= end:
                # Innermost match wins (a nested definition with the same name).
                if best is None or _first_line(node) >= _first_line(best):
                    best = node
    return best


def _source_for(obj, frame):
    """(filename, all lines, node) for the decorated definition, or None."""
    filename = frame.f_code.co_filename
    lines = linecache.getlines(filename, frame.f_globals)
    if not lines:
        return None
    try:
        tree = ast.parse("".join(lines), filename)
    except (SyntaxError, ValueError):
        return None
    node = _find_node(tree, obj.__name__, frame.f_lineno)
    if node is None:
        code = getattr(obj, "__code__", None)
        if code is not None and code.co_filename == filename:
            node = _find_node(tree, obj.__name__, code.co_firstlineno)
    if node is None:
        return None
    return filename, lines, node


def binding(obj):
    """Re-evaluate ``obj`` (a def or class) so it cannot see later ``[code]`` variables."""
    name = getattr(obj, "__name__", None)
    frame = sys._getframe(1)
    try:
        found = _source_for(obj, frame) if name else None
        if found is None:
            raise _Unresolved("source of %r is not available" % (name or obj,))
        filename, lines, node = found

        cut = -1
        for i, dec in enumerate(node.decorator_list):
            try:
                if _resolve(dec, frame) is binding:
                    cut = i
            except _Unresolved:
                continue
        if cut < 0:
            raise _Unresolved("could not find the binding decorator on %r" % name)

        first = _first_line(node)
        end = node.end_lineno
        segment = list(lines[first - 1:end])
        # Blank the binding decorator and those above it; they are applied by the original
        # statement. Decorators below it are kept and re-applied in the snapshot.
        for dec in node.decorator_list[:cut + 1]:
            # The '@' sits on the decorator's first line (or the line before a parenthesised one).
            start = dec.lineno
            while start > first and not segment[start - first].lstrip().startswith("@"):
                start -= 1
            for ln in range(start, dec.end_lineno + 1):
                segment[ln - first] = "\n"
        src = textwrap.dedent("".join(segment))
        src = "\n" * (first - 1) + src
        # Same __future__ features as the defining code (e.g. ``annotations``).
        flags = frame.f_code.co_flags & _FUTURE_FLAGS
        code = compile(src, filename, "exec", flags, dont_inherit=True)
    except (_Unresolved, SyntaxError, OSError, TypeError, ValueError) as exc:
        warnings.warn("darkpyonix.binding: returning %r unchanged (%s)" % (name or obj, exc), stacklevel=2)
        return obj
    finally:
        del frame

    snapshot = dict(sys._getframe(1).f_globals)
    exec(code, snapshot)
    return snapshot[name]
