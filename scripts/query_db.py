#!/usr/bin/env python3
"""Query the shared SQLite database."""

from __future__ import annotations

import argparse
import sqlite3
from collections.abc import Sequence

from neocortex.storage import DEFAULT_DB_PATH


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="query_db")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))

    query_mode = parser.add_mutually_exclusive_group(required=True)
    query_mode.add_argument("--sql")
    query_mode.add_argument("--table")

    parser.add_argument("--limit", type=int, default=20)
    return parser


def build_query(*, sql: str | None, table: str | None, limit: int) -> str:
    if sql is not None:
        return sql
    assert table is not None
    return f"SELECT * FROM {table} LIMIT {limit}"


def execute_query(
    db_path: str, query: str
) -> tuple[tuple[str, ...], list[tuple[object, ...]]]:
    with sqlite3.connect(db_path) as connection:
        cursor = connection.execute(query)
        columns = tuple(description[0] for description in cursor.description or ())
        rows = cursor.fetchall()
    return columns, rows


def render_table(columns: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
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


def main() -> int:
    args = build_parser().parse_args()
    query = build_query(sql=args.sql, table=args.table, limit=args.limit)
    columns, rows = execute_query(args.db_path, query)
    output = render_table(columns, rows)
    if output:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
