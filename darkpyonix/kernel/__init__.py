"""DarkPyonix runtime API for notebook files (SPEC FR-F2..F6, FORMAT §4).

Standard library only (INTENT §2). Importing this package must stay cheap and must never
import any kernel-process module (``_server``, ``_executor``, ...): notebook code and the kernel
import it from interpreters where nothing but the standard library is available. The public names are loaded lazily
from private modules on first access (PEP 562).

Inside a kernel the calls talk to ``darkpyonix._hostctx``; anywhere else they behave
as under plain ``python file.py`` (SPEC FR-F5).
"""

__version__ = "0.2.0"

_EXPORTS = {
    "markdown": "_markdown",
    "params": "_params",
    "binding": "_binding",
    "run_command": "_command",
    "uv": "_command",
    "pip": "_command",
    "display": "_misc",
    "run_parallel": "_misc",
    "run_cinterop": "_misc",
    "run_cppinterop": "_misc",
    "run_rustinterop": "_misc",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name):
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError("module 'darkpyonix' has no attribute %r" % (name,))
    import importlib

    value = getattr(importlib.import_module("darkpyonix." + module), name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(_EXPORTS))
