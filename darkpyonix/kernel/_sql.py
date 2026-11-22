"""``darkpyonix.sql`` and ``SQLResult`` (SPEC FR-F13)."""
from __future__ import annotations

import html
import sqlite3
import sys

from darkpyonix._errors import InteropUnavailable

_HTML_MAX_ROWS = 200


def _validate_target(target):
    if target is not None and not (isinstance(target, str) and target.isidentifier()):
        raise ValueError(
            "target must be a valid Python identifier or None, got %r" % (target,)
        )


class SQLResult(list):
    """List of result row tuples with column names, rowcount, and HTML display."""

    def __init__(self, rows=(), columns=(), rowcount=-1):
        super().__init__(rows)
        self.columns = tuple(columns)
        self.rowcount = rowcount

    def _repr_html_(self):
        if not self.columns:
            return "<p>OK (rowcount: %d)</p>" % (self.rowcount,)
        head = "".join("<th>%s</th>" % html.escape(str(col)) for col in self.columns)
        shown = self[:_HTML_MAX_ROWS]
        body_rows = []
        for row in shown:
            cells = "".join("<td>%s</td>" % html.escape(str(val)) for val in row)
            body_rows.append("<tr>%s</tr>" % cells)
        parts = [
            "<table>",
            "<thead><tr>%s</tr></thead>" % head,
            "<tbody>%s</tbody>" % "".join(body_rows),
        ]
        remaining = len(self) - len(shown)
        if remaining > 0:
            parts.append(
                '<tfoot><tr><td colspan="%d">%d more rows</td></tr></tfoot>'
                % (len(self.columns), remaining)
            )
        parts.append("</table>")
        return "".join(parts)

    def to_pandas(self):
        """Return a pandas DataFrame if ``pandas`` is already imported in this process."""
        pd = sys.modules.get("pandas")
        if pd is None:
            raise InteropUnavailable(
                "pandas is not imported; import pandas before calling SQLResult.to_pandas()"
            )
        return pd.DataFrame(self, columns=self.columns)


class _SQLRunner:
    """Callable ``darkpyonix.sql`` entry point backed by a DB-API 2.0 connection."""

    def __init__(self):
        self.connection = None
        self._default_connection = None

    def __call__(self, query, target="_", params=None, con=None):
        _validate_target(target)
        use_default = False
        conn = con if con is not None else self.connection
        if conn is None:
            if self._default_connection is None:
                self._default_connection = sqlite3.connect(":memory:")
            conn = self._default_connection
            use_default = True

        cursor = conn.cursor()
        try:
            if params is None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
            if cursor.description is not None:
                columns = tuple(col[0] for col in cursor.description)
                rows = [tuple(row) for row in cursor.fetchall()]
            else:
                columns = ()
                rows = []
            rowcount = cursor.rowcount
        finally:
            cursor.close()

        if use_default:
            conn.commit()

        result = SQLResult(rows, columns=columns, rowcount=rowcount)
        if target is not None:
            sys._getframe(1).f_globals[target] = result
        return result


sql = _SQLRunner()
