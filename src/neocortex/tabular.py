"""Shared fixed-width table rendering helpers."""

from __future__ import annotations

from collections.abc import Sequence


def render_table(columns: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    """Render one fixed-width table for terminal and chat output."""

    text_rows = [
        tuple("" if value is None else str(value) for value in row) for row in rows
    ]
    widths = []
    for index, column in enumerate(columns):
        column_width = len(column)
        if text_rows:
            column_width = max(column_width, *(len(row[index]) for row in text_rows))
        widths.append(column_width)

    header = " ".join(
        f"{column:<{widths[index]}}" for index, column in enumerate(columns)
    )
    body = [
        " ".join(f"{row[index]:<{widths[index]}}" for index in range(len(columns)))
        for row in text_rows
    ]
    return "\n".join([header, *body]) if columns else ""
