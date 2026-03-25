import sqlite3

import pytest

from neocortex.storage import query as query_module
from neocortex.tabular import render_table


def test_build_query_uses_table_limit_by_default() -> None:
    query = query_module.build_query(sql=None, table="company_profiles", limit=5)

    assert query == "SELECT * FROM company_profiles LIMIT 5"


def test_build_query_rejects_non_identifier_table_names() -> None:
    with pytest.raises(ValueError) as exc_info:
        query_module.build_query(
            sql=None,
            table="company_profiles; drop table company_profiles",
            limit=5,
        )

    assert "Table names must be bare SQLite identifiers." in str(exc_info.value)


def test_execute_query_and_render_table(tmp_path) -> None:
    db_path = tmp_path / "query.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE company_profiles (
                symbol TEXT NOT NULL,
                company_name TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO company_profiles (symbol, company_name, fetched_at)
            VALUES ('000014', '沙河股份', '2026-03-18T00:00:00Z')
            """
        )
        connection.commit()

    columns, rows = query_module.execute_query(
        str(db_path),
        "SELECT symbol, company_name, fetched_at FROM company_profiles",
    )
    rendered = render_table(columns, rows)
    lines = rendered.splitlines()

    assert columns == ("symbol", "company_name", "fetched_at")
    assert rows == [("000014", "沙河股份", "2026-03-18T00:00:00Z")]
    assert lines[0].split() == ["symbol", "company_name", "fetched_at"]
    assert lines[1].split() == ["000014", "沙河股份", "2026-03-18T00:00:00Z"]


def test_execute_query_rejects_write_statements(tmp_path) -> None:
    db_path = tmp_path / "query.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute("create table sample_rows (name text)")
        connection.execute("insert into sample_rows (name) values ('alpha')")
        connection.commit()

    with pytest.raises(ValueError) as exc_info:
        query_module.execute_query(str(db_path), "DELETE FROM sample_rows")

    assert "Only read-only SELECT queries are allowed." in str(exc_info.value)
