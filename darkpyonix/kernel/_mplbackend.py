"""The kernel's matplotlib backend: figures become ``image/png`` display_data (SPEC FR-X6).

matplotlib loads this module itself, as ``module://darkpyonix._mplbackend``, when user
code imports pyplot inside a kernel (see ``figures``). Kernel code never imports it, which is
why it is the one kernel module allowed to import matplotlib (SPEC NFR-K2).

Drawing is done by Agg. ``plt.show()`` and the end of every cell display each open figure as
a PNG and close it, so a figure is shown once.
"""
from __future__ import annotations

import base64
import io

from matplotlib._pylab_helpers import Gcf
from matplotlib.backends.backend_agg import FigureCanvasAgg

from darkpyonix import _hostctx as hostctx

FigureCanvas = FigureCanvasAgg
try:  # classic module-level API, kept by matplotlib for backends that export it
    from matplotlib.backends.backend_agg import new_figure_manager, new_figure_manager_given_figure  # noqa: F401
except ImportError:  # pragma: no cover - pyplot derives them from FigureCanvas
    pass


def draw_if_interactive() -> None:
    """Interactive mode draws nothing here; figures are shown by show() or at cell end."""


def _bundle(figure):
    buf = io.BytesIO()
    figure.canvas.print_figure(buf, format="png", bbox_inches="tight")
    return {"data": {"image/png": base64.b64encode(buf.getvalue()).decode("ascii"),
                     "text/plain": repr(figure)},
            "metadata": {}}


def flush_figures() -> None:
    """Display every open figure as image/png and close it."""
    managers = Gcf.get_all_fig_managers()
    try:
        for manager in managers:
            figure = manager.canvas.figure
            if figure.axes or figure.images or figure.texts or figure.patches or figure.lines:
                hostctx.emit_display(_bundle(figure))
    finally:
        for manager in managers:
            Gcf.destroy(manager)


def show(*args, **kwargs) -> None:
    flush_figures()
