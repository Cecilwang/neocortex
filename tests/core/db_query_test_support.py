"""Shared SQLite helpers for CLI db query tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path


def seed_company_profiles_table(
    db_path: Path,
    *,
    rows: tuple[tuple[str, str], ...],
) -> Path:
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE company_profiles (
                symbol TEXT NOT NULL,
                company_name TEXT NOT NULL
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO company_profiles (symbol, company_name)
            VALUES (?, ?)
            """,
            rows,
        )
        connection.commit()
    return db_path


def seed_sample_rows_table(
    db_path: Path,
    *,
    rows: tuple[str, ...],
) -> Path:
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE sample_rows (name TEXT)")
        connection.executemany(
            "INSERT INTO sample_rows (name) VALUES (?)",
            tuple((row,) for row in rows),
        )
        connection.commit()
    return db_path
