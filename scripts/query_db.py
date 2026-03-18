#!/usr/bin/env python3
"""Query the shared SQLite database."""

from __future__ import annotations

import argparse

from neocortex.storage import DEFAULT_DB_PATH
from neocortex.storage.query import build_query, execute_query, render_table


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="query_db")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))

    query_mode = parser.add_mutually_exclusive_group(required=True)
    query_mode.add_argument("--sql")
    query_mode.add_argument("--table")

    parser.add_argument("--limit", type=int, default=20)
    return parser


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
