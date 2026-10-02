"""DarkPyonix runtime API for notebook files.

Standard library only (INTENT §2). Importing this package must stay cheap and must never
import ``darkpyonix.manager``: notebook code and the kernel import it from interpreters
where nothing but the standard library is available.

The public runtime functions (``markdown``, ``params``, ``binding``, ``run_command``,
``display``, ...) are added by SPEC FR-F2..F6 (issue #17).
"""

__version__ = "0.1.0"
