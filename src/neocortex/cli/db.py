"""Database query CLI commands."""

from __future__ import annotations

import argparse
import logging

from neocortex.serialization import to_pretty_json
from neocortex.storage.query import build_query, execute_query, render_table

logger = logging.getLogger(__name__)


def run_db_query(args: argparse.Namespace) -> int:
    query = build_query(sql=args.sql, table=args.table, limit=args.limit)
    logger.info(
        f"Running DB query command: db_path={args.db_path} "
        f"table={args.table} format={args.format}"
    )
    columns, rows = execute_query(args.db_path, query)
    if args.format == "json":
        print(
            to_pretty_json(
                {
                    "columns": columns,
                    "rows": rows,
                }
            )
        )
        return 0
    print(render_table(columns, rows))
    return 0


def add_db_commands(
    subcommands: argparse._SubParsersAction[argparse.ArgumentParser],
    *,
    default_db_path: str,
) -> None:
    db_parser = subcommands.add_parser("db")
    db_parser.add_argument("--db-path", type=str, default=default_db_path)
    db_commands = db_parser.add_subparsers(dest="command", required=True)

    query_parser = db_commands.add_parser("query")
    query_group = query_parser.add_mutually_exclusive_group(required=True)
    query_group.add_argument("--sql", default=None)
    query_group.add_argument("--table", default=None)
    query_parser.add_argument("--limit", type=int, default=20)
    query_parser.add_argument(
        "--format",
        choices=("table", "json"),
        default="table",
    )
    query_parser.set_defaults(handler=run_db_query)
