"""``darkpyonix.display`` and the reserved interop calls (FORMAT §4)."""
from __future__ import annotations

from darkpyonix import _host


def display(*objs):
    """Show objects as ``display_data`` in a kernel; ``print(repr(obj))`` under plain python."""
    host = _host.active()
    if host is None:
        for obj in objs:
            print(repr(obj))
        return None
    try:
        from darkpyonix._display import display as kernel_display
    except ImportError:
        kernel_display = None
    if kernel_display is not None:
        kernel_display(*objs)
        return None
    for obj in objs:
        host.emit_display({"text/plain": repr(obj)})
    return None


def _reserved(api):
    def call(src, *args, **kwargs):
        raise NotImplementedError("darkpyonix.%s is reserved: issue #5" % api)
    call.__name__ = api
    call.__qualname__ = api
    call.__doc__ = "Reserved for compiled interop (issue #5); raises NotImplementedError in v1."
    return call


run_cinterop = _reserved("run_cinterop")
run_cppinterop = _reserved("run_cppinterop")
run_rustinterop = _reserved("run_rustinterop")
