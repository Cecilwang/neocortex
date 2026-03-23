"""Reusable SQLite query helpers for scripts and bot actions."""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Sequence

logger = logging.getLogger(__name__)


def build_query(*, sql: str | None, table: str | None, limit: int) -> str:
    """Return one read query from either raw SQL or table selection."""

    if sql is not None:
        logger.info(f"Built query from raw SQL: length={len(sql)}")
        return sql
    assert table is not None
    query = f"SELECT * FROM {table} LIMIT {limit}"
    logger.info(f"Built table query: table={table} limit={limit}")
    return query


def execute_query(
    db_path: str, query: str
) -> tuple[tuple[str, ...], list[tuple[object, ...]]]:
    """Execute one SQLite query and return headers plus rows."""

    logger.info(f"Executing SQLite query: db_path={db_path} length={len(query)}")
    with sqlite3.connect(db_path) as connection:
        cursor = connection.execute(query)
        columns = tuple(description[0] for description in cursor.description or ())
        rows = cursor.fetchall()
    logger.info(
        f"SQLite query completed: db_path={db_path} "
        f"columns={len(columns)} rows={len(rows)}"
    )
    return columns, rows


def render_table(columns: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    """Render one fixed-width table for terminal and bot output."""

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
