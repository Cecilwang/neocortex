import sqlite3
from pathlib import Path

import pandas as pd

from neocortex.storage import company_profiles as company_profiles_module
from neocortex.storage.config import (
    DEFAULT_DB_PATH,
    DEFAULT_STORAGE_CONFIG,
)
from neocortex.storage.company_profiles import BackfillStats
from neocortex.models import CompanyProfile, SecurityId


class FakeConnector:
    last_timeout: float | None = None

    def __init__(self, *, timeout: float | None = None) -> None:
        type(self).last_timeout = timeout

    def get_company_profile(self, security_id: SecurityId) -> CompanyProfile:
        profiles = {
            "600519": CompanyProfile(
                security_id=security_id,
                company_name="贵州茅台",
                sector="酿酒行业",
                industry="酿酒行业",
                country="CN",
                currency="CNY",
            ),
            "000001": CompanyProfile(
                security_id=security_id,
                company_name="平安银行股份有限公司",
                sector="银行",
                industry="银行",
                country="CN",
                currency="CNY",
            ),
        }
        return profiles[security_id.symbol]


class FailingConnector(FakeConnector):
    def get_company_profile(self, security_id: SecurityId) -> CompanyProfile:
        if security_id.symbol == "600519":
            raise RuntimeError("temporary upstream failure")
        return super().get_company_profile(security_id)


def _rows(connection: sqlite3.Connection, query: str) -> list[tuple]:
    return connection.execute(query).fetchall()


def test_backfill_company_profiles_creates_schema_and_supported_aliases(
    monkeypatch,
    tmp_path,
) -> None:
    db_path = tmp_path / "profiles.sqlite"
    code_name_frame = pd.DataFrame(
        {
            "code": ["600519", "430001", "000001"],
            "name": ["贵州茅台", "北交测试", "平安银行"],
        }
    )
    timestamps = iter(
        [
            "2026-03-17T00:00:01Z",
            "2026-03-17T00:00:02Z",
            "2026-03-17T00:00:03Z",
            "2026-03-17T00:00:04Z",
        ]
    )
    monkeypatch.setattr(
        company_profiles_module,
        "utc_now_iso",
        lambda: next(timestamps),
    )

    stats = company_profiles_module.backfill_company_profiles(
        db_path,
        timeout=3.0,
        limit=2,
        workers=1,
        code_name_frame=code_name_frame,
        connector_factory=FakeConnector,
    )

    assert FakeConnector.last_timeout == 3.0
    assert stats == BackfillStats(
        processed=2,
        fetched=2,
        skipped_unsupported=1,
        failed=0,
    )

    with sqlite3.connect(db_path) as connection:
        assert _rows(
            connection,
            """
            SELECT market, exchange, symbol, source, last_seen_at
            FROM securities
            ORDER BY symbol
            """,
        ) == [
            (
                "CN",
                "XSHE",
                "000001",
                "akshare",
                "2026-03-17T00:00:02Z",
            ),
            (
                "CN",
                "XSHG",
                "600519",
                "akshare",
                "2026-03-17T00:00:01Z",
            ),
        ]
        assert _rows(
            connection,
            """
            SELECT market, exchange, symbol, company_name, fetched_at
            FROM company_profiles
            ORDER BY symbol
            """,
        ) == [
            (
                "CN",
                "XSHE",
                "000001",
                "平安银行股份有限公司",
                "2026-03-17T00:00:04Z",
            ),
            (
                "CN",
                "XSHG",
                "600519",
                "贵州茅台",
                "2026-03-17T00:00:03Z",
            ),
        ]
        assert _rows(
            connection,
            """
            SELECT symbol, alias, alias_norm, language
            FROM security_aliases
            ORDER BY symbol, alias
            """,
        ) == [
            (
                "000001",
                "平安银行",
                "平安银行",
                "zh",
            ),
            (
                "000001",
                "平安银行股份有限公司",
                "平安银行股份有限公司",
                "zh",
            ),
            (
                "600519",
                "贵州茅台",
                "贵州茅台",
                "zh",
            ),
        ]


def test_backfill_company_profiles_updates_latest_timestamps(
    monkeypatch,
    tmp_path,
) -> None:
    db_path = tmp_path / "profiles.sqlite"
    code_name_frame = pd.DataFrame({"code": ["600519"], "name": ["贵州茅台"]})
    timestamps = iter(
        [
            "2026-03-17T00:00:01Z",
            "2026-03-17T00:00:02Z",
            "2026-03-18T00:00:01Z",
            "2026-03-18T00:00:02Z",
        ]
    )
    monkeypatch.setattr(
        company_profiles_module,
        "utc_now_iso",
        lambda: next(timestamps),
    )

    company_profiles_module.backfill_company_profiles(
        db_path,
        workers=1,
        code_name_frame=code_name_frame,
        connector_factory=FakeConnector,
    )
    company_profiles_module.backfill_company_profiles(
        db_path,
        workers=1,
        code_name_frame=code_name_frame,
        connector_factory=FakeConnector,
    )

    with sqlite3.connect(db_path) as connection:
        assert _rows(
            connection,
            """
            SELECT last_seen_at
            FROM securities
            WHERE market = 'CN' AND exchange = 'XSHG' AND symbol = '600519'
            """,
        ) == [("2026-03-18T00:00:01Z",)]
        assert _rows(
            connection,
            """
            SELECT fetched_at
            FROM company_profiles
            WHERE market = 'CN' AND exchange = 'XSHG' AND symbol = '600519'
            """,
        ) == [("2026-03-18T00:00:02Z",)]
        assert _rows(
            connection,
            """
            SELECT alias, updated_at
            FROM security_aliases
            WHERE market = 'CN' AND exchange = 'XSHG' AND symbol = '600519'
            """,
        ) == [("贵州茅台", "2026-03-18T00:00:02Z")]


def test_backfill_company_profiles_keeps_processing_after_profile_failures(
    monkeypatch,
    tmp_path,
) -> None:
    db_path = tmp_path / "profiles.sqlite"
    code_name_frame = pd.DataFrame(
        {
            "code": ["600519", "000001"],
            "name": ["贵州茅台", "平安银行"],
        }
    )
    timestamps = iter(
        [
            "2026-03-17T00:00:01Z",
            "2026-03-17T00:00:02Z",
            "2026-03-17T00:00:03Z",
        ]
    )
    monkeypatch.setattr(
        company_profiles_module,
        "utc_now_iso",
        lambda: next(timestamps),
    )

    stats = company_profiles_module.backfill_company_profiles(
        db_path,
        retry_count=1,
        workers=1,
        code_name_frame=code_name_frame,
        connector_factory=FailingConnector,
    )

    assert stats == BackfillStats(
        processed=2,
        fetched=1,
        skipped_unsupported=0,
        failed=1,
    )

    with sqlite3.connect(db_path) as connection:
        assert _rows(
            connection,
            "SELECT symbol FROM securities ORDER BY symbol",
        ) == [("000001",), ("600519",)]
        assert _rows(
            connection,
            "SELECT symbol FROM company_profiles ORDER BY symbol",
        ) == [("000001",)]


def test_backfill_company_profiles_supports_multithreaded_fetching(
    monkeypatch,
    tmp_path,
) -> None:
    db_path = tmp_path / "profiles.sqlite"
    code_name_frame = pd.DataFrame(
        {
            "code": ["600519", "000001"],
            "name": ["贵州茅台", "平安银行"],
        }
    )
    timestamps = iter(
        [
            "2026-03-17T00:00:01Z",
            "2026-03-17T00:00:02Z",
            "2026-03-17T00:00:03Z",
            "2026-03-17T00:00:04Z",
        ]
    )
    monkeypatch.setattr(
        company_profiles_module,
        "utc_now_iso",
        lambda: next(timestamps),
    )

    stats = company_profiles_module.backfill_company_profiles(
        db_path,
        workers=2,
        code_name_frame=code_name_frame,
        connector_factory=FakeConnector,
    )

    assert stats == BackfillStats(
        processed=2,
        fetched=2,
        skipped_unsupported=0,
        failed=0,
    )

    with sqlite3.connect(db_path) as connection:
        assert _rows(
            connection,
            "SELECT COUNT(*) FROM company_profiles",
        ) == [(2,)]
        assert _rows(
            connection,
            "SELECT COUNT(*) FROM security_aliases",
        ) == [(3,)]


def test_default_db_path_points_to_shared_repo_database() -> None:
    expected = Path("/tmp/stock-akshare-profile-cache") / "data" / "neocortex.sqlite3"

    assert DEFAULT_DB_PATH == expected
    assert DEFAULT_STORAGE_CONFIG.db_path == expected
