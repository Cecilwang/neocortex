import importlib.util
import sqlite3
from pathlib import Path


SCRIPT_PATH = Path("/tmp/stock-akshare-profile-cache/scripts/query_db.py")


def _load_module():
    spec = importlib.util.spec_from_file_location("query_db_script", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_build_query_uses_table_limit_by_default() -> None:
    module = _load_module()

    query = module.build_query(sql=None, table="company_profiles", limit=5)

    assert query == "SELECT * FROM company_profiles LIMIT 5"


def test_execute_query_and_render_table(tmp_path) -> None:
    module = _load_module()
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

    columns, rows = module.execute_query(
        str(db_path),
        "SELECT symbol, company_name, fetched_at FROM company_profiles",
    )
    rendered = module.render_table(columns, rows)
    lines = rendered.splitlines()

    assert columns == ("symbol", "company_name", "fetched_at")
    assert rows == [("000014", "沙河股份", "2026-03-18T00:00:00Z")]
    assert lines[0].split() == ["symbol", "company_name", "fetched_at"]
    assert lines[1].split() == ["000014", "沙河股份", "2026-03-18T00:00:00Z"]
