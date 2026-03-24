"""Registry-backed DB command definitions."""

from __future__ import annotations

import argparse
import logging

from neocortex.commands.core import (
    AuthPolicy,
    CommandContext,
    CommandResult,
    CommandSpec,
    ExecutionMode,
    Exposure,
)
from neocortex.storage.query import build_query, execute_query


logger = logging.getLogger(__name__)


def build_db_query_command_spec(*, default_db_path: str) -> CommandSpec:
    """Build the registry command spec for the DB query leaf command."""

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--db-path", type=str, default=default_db_path)
        query_group = parser.add_mutually_exclusive_group(required=True)
        query_group.add_argument("--sql", default=None)
        query_group.add_argument("--table", default=None)
        parser.add_argument("--limit", type=int, default=20)
        parser.add_argument(
            "--format",
            choices=("table", "json"),
            default="table",
        )

    def handler(args: argparse.Namespace, context: CommandContext) -> CommandResult:
        _ = context
        query = build_query(sql=args.sql, table=args.table, limit=args.limit)
        logger.info(
            "Running DB query command: "
            f"query_source={'sql' if args.sql is not None else 'table'} format={args.format}"
        )
        columns, rows = execute_query(args.db_path, query)
        payload = {
            "columns": columns,
            "rows": rows,
        }
        if args.format == "json":
            return CommandResult.json(payload)
        return CommandResult.table(
            columns=columns,
            rows=tuple(tuple(row) for row in rows),
            payload=payload,
        )

    return CommandSpec(
        id=("db", "query"),
        summary="Run a read-only SQLite query.",
        description="Run a read-only SQLite query against the local market DB.",
        exposure=Exposure.SHARED,
        auth=AuthPolicy.PUBLIC,
        execution=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=handler,
    )
