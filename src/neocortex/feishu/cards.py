"""Feishu card builders."""

from __future__ import annotations

from datetime import date, datetime
from numbers import Real
from typing import Any


def build_table_card(
    *,
    title: str,
    columns: tuple[str, ...],
    rows: tuple[tuple[object, ...], ...],
) -> dict[str, Any]:
    column_keys = tuple(f"col_{index}" for index, _ in enumerate(columns))
    row_height = _infer_row_height(len(columns))
    column_alignments = tuple(
        _infer_column_alignment(index, rows) for index, _ in enumerate(columns)
    )
    card_columns = tuple(
        {
            "name": key,
            "display_name": str(label),
            "data_type": "text",
            "width": "auto",
            "horizontal_align": alignment,
            "vertical_align": "top",
        }
        for key, label, alignment in zip(
            column_keys, columns, column_alignments, strict=True
        )
    )
    card_rows = tuple(
        {
            key: _format_cell_value(value)
            for key, value in zip(column_keys, row, strict=True)
        }
        for row in rows
    )
    return {
        "schema": "2.0",
        "header": {
            "title": {
                "tag": "plain_text",
                "content": title,
            }
        },
        "body": {
            "elements": [
                {
                    "tag": "table",
                    "page_size": 100,
                    "row_height": row_height,
                    "freeze_first_column": True,
                    "header_style": {
                        "text_align": "left",
                        "text_size": "normal",
                        "background_style": "none",
                        "text_color": "grey",
                        "bold": True,
                        "lines": 1,
                    },
                    "columns": list(card_columns),
                    # TODO: enforce Feishu card payload limits before very large tables
                    # are rendered into one outbound message.
                    "rows": list(card_rows),
                }
            ]
        },
    }


def _infer_column_alignment(
    column_index: int,
    rows: tuple[tuple[object, ...], ...],
) -> str:
    values = tuple(
        row[column_index]
        for row in rows
        if column_index < len(row) and row[column_index] is not None
    )
    if values and all(_is_numeric_value(value) for value in values):
        return "right"
    return "left"


def _infer_row_height(column_count: int) -> str:
    if column_count <= 4:
        return "medium"
    if column_count <= 8:
        return "low"
    return "low"


def _format_cell_value(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _is_numeric_value(value: object) -> bool:
    return isinstance(value, Real) and not isinstance(value, bool)
