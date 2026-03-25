"""Reusable SQLite query helpers for scripts and bot actions."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
import re

logger = logging.getLogger(__name__)

_SQLITE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DENIED_SQLITE_ACTIONS = {
    getattr(sqlite3, name)
    for name in (
        "SQLITE_ATTACH",
        "SQLITE_DETACH",
        "SQLITE_ALTER_TABLE",
        "SQLITE_ANALYZE",
        "SQLITE_CREATE_INDEX",
        "SQLITE_CREATE_TABLE",
        "SQLITE_CREATE_TEMP_INDEX",
        "SQLITE_CREATE_TEMP_TABLE",
        "SQLITE_CREATE_TEMP_TRIGGER",
        "SQLITE_CREATE_TEMP_VIEW",
        "SQLITE_CREATE_TRIGGER",
        "SQLITE_CREATE_VIEW",
        "SQLITE_DELETE",
        "SQLITE_DROP_INDEX",
        "SQLITE_DROP_TABLE",
        "SQLITE_DROP_TEMP_INDEX",
        "SQLITE_DROP_TEMP_TABLE",
        "SQLITE_DROP_TEMP_TRIGGER",
        "SQLITE_DROP_TEMP_VIEW",
        "SQLITE_DROP_TRIGGER",
        "SQLITE_DROP_VIEW",
        "SQLITE_INSERT",
        "SQLITE_PRAGMA",
        "SQLITE_REINDEX",
        "SQLITE_TRANSACTION",
        "SQLITE_UPDATE",
        "SQLITE_VACUUM",
    )
    if hasattr(sqlite3, name)
}


def _ensure_positive_limit(limit: int) -> None:
    if limit <= 0:
        raise ValueError("--limit must be a positive integer.")


def _validate_table_name(table: str) -> None:
    if not _SQLITE_IDENTIFIER_PATTERN.fullmatch(table):
        raise ValueError("Table names must be bare SQLite identifiers.")


def _validate_read_only_query_shape(sql: str) -> None:
    stripped = sql.strip()
    if not stripped:
        raise ValueError("SQL query must not be empty.")
    if ";" in stripped.rstrip(";"):
        raise ValueError("Only one read-only SQLite statement is allowed.")
    first_token = stripped.split(maxsplit=1)[0].upper()
    if first_token not in {"SELECT", "WITH"}:
        raise ValueError("Only read-only SELECT queries are allowed.")


def _read_only_authorizer(
    action: int,
    param1: str | None,
    param2: str | None,
    db_name: str | None,
    trigger_name: str | None,
) -> int:
    _ = param1, param2, db_name, trigger_name
    if action in _DENIED_SQLITE_ACTIONS:
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def build_query(*, sql: str | None, table: str | None, limit: int) -> str:
    """Return one read query from either raw SQL or table selection."""

    _ensure_positive_limit(limit)
    if sql is not None:
        _validate_read_only_query_shape(sql)
        return sql
    assert table is not None
    _validate_table_name(table)
    query = f"SELECT * FROM {table} LIMIT {limit}"
    logger.info(f"Built table query: table={table} limit={limit}")
    return query


def execute_query(
    db_path: str, query: str
) -> tuple[tuple[str, ...], list[tuple[object, ...]]]:
    """Execute one SQLite query and return headers plus rows."""

    logger.info(f"Executing SQLite query: db_path={db_path} length={len(query)}")
    db_uri = f"{Path(db_path).resolve().as_uri()}?mode=ro"
    try:
        with sqlite3.connect(db_uri, uri=True) as connection:
            connection.execute("PRAGMA query_only = ON")
            connection.set_authorizer(_read_only_authorizer)
            cursor = connection.execute(query)
            columns = tuple(description[0] for description in cursor.description or ())
            rows = cursor.fetchall()
    except sqlite3.DatabaseError as exc:
        if "not authorized" in str(exc).lower() or "readonly" in str(exc).lower():
            raise ValueError("Only read-only SELECT queries are allowed.") from exc
        raise
    logger.info(
        f"SQLite query completed: db_path={db_path} "
        f"columns={len(columns)} rows={len(rows)}"
    )
    return columns, rows
