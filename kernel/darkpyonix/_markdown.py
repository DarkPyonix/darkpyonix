"""``darkpyonix.markdown`` (SPEC FR-F2, FORMAT §3.2)."""
from __future__ import annotations

import textwrap
import warnings

from darkpyonix import _host

_warned = set()


def markdown(text, silent=False, **unknown):
    """Render ``text`` as Markdown in a kernel; do nothing under plain ``python file.py``.

    ``silent=True`` shows the output without keeping it in the run record. Unknown keyword
    arguments (such as the misspelt ``slient=``) are ignored with a warning, never an error.
    """
    for key in sorted(unknown):
        if key not in _warned:
            _warned.add(key)
            hint = " (did you mean 'silent'?)" if key != "silent" and sorted(key) == sorted("silent") else ""
            warnings.warn("darkpyonix.markdown() ignores unknown keyword argument %r%s" % (key, hint),
                          stacklevel=2)
    host = _host.active()
    if host is None:
        return None
    body = textwrap.dedent(str(text)).strip("\n")
    host.emit_display({"text/markdown": body}, bool(silent))
    return None
