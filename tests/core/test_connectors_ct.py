import sqlite3
from datetime import date

import pytest
from neocortex.connectors.types import (
    DailyPriceBarRecord,
    FundamentalSnapshotRecord,
    SecurityListing,
    TradingDateRecord,
)
from neocortex.markets import get_market_context
from neocortex.models import (
    Exchange,
    Market,
    SecurityId,
)
from neocortex.storage import market_store as market_store_module
from neocortex.storage.market_store import MarketDataStore


@pytest.fixture
def security_id() -> SecurityId:
    return SecurityId(symbol="AAPL", market=Market.US, exchange=Exchange.XNAS)


def test_get_market_context_preserves_raw_key_error_for_missing_market() -> None:
    with pytest.raises(KeyError) as exc_info:
        get_market_context("EU")  # type: ignore[arg-type]

    assert exc_info.value.args == ("EU",)


def test_market_data_store_creates_single_table_schema(tmp_path) -> None:
    db_path = tmp_path / "market.sqlite3"
    store = MarketDataStore(db_path)

    store.ensure_schema()

    with sqlite3.connect(db_path) as connection:
        table_names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }

    assert {
        "daily_price_bars",
        "disclosure_sections",
        "fundamental_snapshots",
        "intraday_price_bars",
        "macro_points",
        "securities",
        "security_aliases",
        "security_profiles",
        "trading_dates",
    }.issubset(table_names)


def test_seed_security_listing_keeps_single_security_row_across_sources(
    monkeypatch,
    tmp_path,
) -> None:
    db_path = tmp_path / "market.sqlite3"
    store = MarketDataStore(db_path)
    store.ensure_schema()
    listing = SecurityListing(
        security_id=SecurityId(
            symbol="600519",
            market=Market.CN,
            exchange=Exchange.XSHG,
        ),
        name="贵州茅台",
    )
    timestamps = iter(["2026-03-19T00:00:01Z", "2026-03-19T00:00:02Z"])
    monkeypatch.setattr(market_store_module, "utc_now_iso", lambda: next(timestamps))

    store.seed_security_listing(listing, source="efinance")
    store.seed_security_listing(listing, source="baostock")

    with sqlite3.connect(db_path) as connection:
        security_rows = connection.execute(
            """
            SELECT market, exchange, symbol, last_seen_at
            FROM securities
            """
        ).fetchall()
        alias_rows = connection.execute(
            """
            SELECT symbol, alias, source
            FROM security_aliases
            ORDER BY source
            """
        ).fetchall()

    assert security_rows == [("CN", "XSHG", "600519", "2026-03-19T00:00:02Z")]
    assert alias_rows == [
        ("600519", "贵州茅台", "baostock"),
        ("600519", "贵州茅台", "efinance"),
    ]


def test_seed_security_listing_skips_alias_when_name_is_missing(tmp_path) -> None:
    db_path = tmp_path / "market.sqlite3"
    store = MarketDataStore(db_path)
    store.ensure_schema()

    store.seed_security_listing(
        SecurityListing(
            security_id=SecurityId(
                symbol="600519",
                market=Market.CN,
                exchange=Exchange.XSHG,
            ),
            name=None,
        ),
        source="seed",
    )

    with sqlite3.connect(db_path) as connection:
        alias_rows = connection.execute(
            "SELECT symbol, alias, source FROM security_aliases"
        ).fetchall()

    assert alias_rows == []


def test_daily_price_bar_repository_aggregates_weekly_and_monthly(tmp_path) -> None:
    db_path = tmp_path / "market.sqlite3"
    store = MarketDataStore(db_path)
    store.ensure_schema()
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    store.securities.upsert(
        SecurityListing(security_id=security_id, name="贵州茅台"),
        observed_at="2026-03-19T00:00:00Z",
    )
    records = (
        DailyPriceBarRecord(
            source="efinance",
            security_id=security_id,
            trade_date="2026-01-05",
            open=10.0,
            high=11.0,
            low=9.0,
            close=10.5,
            volume=100.0,
            amount=1000.0,
        ),
        DailyPriceBarRecord(
            source="efinance",
            security_id=security_id,
            trade_date="2026-01-06",
            open=10.5,
            high=12.0,
            low=10.0,
            close=11.5,
            volume=150.0,
            amount=1500.0,
        ),
        DailyPriceBarRecord(
            source="efinance",
            security_id=security_id,
            trade_date="2026-02-02",
            open=12.0,
            high=13.0,
            low=11.0,
            close=12.5,
            volume=200.0,
            amount=2200.0,
        ),
    )

    store.daily_price_bars.upsert_many(records, fetched_at="2026-03-19T00:00:00Z")

    weekly = store.daily_price_bars.aggregate_bars(
        source="efinance",
        security_id=security_id,
        interval="1w",
    )
    monthly = store.daily_price_bars.aggregate_bars(
        source="efinance",
        security_id=security_id,
        interval="1mo",
    )

    assert weekly == (
        DailyPriceBarRecord(
            source="efinance",
            security_id=security_id,
            trade_date="2026-01-06",
            open=10.0,
            high=12.0,
            low=9.0,
            close=11.5,
            volume=250.0,
            amount=2500.0,
        ),
        DailyPriceBarRecord(
            source="efinance",
            security_id=security_id,
            trade_date="2026-02-02",
            open=12.0,
            high=13.0,
            low=11.0,
            close=12.5,
            volume=200.0,
            amount=2200.0,
        ),
    )
    assert monthly == (
        DailyPriceBarRecord(
            source="efinance",
            security_id=security_id,
            trade_date="2026-01-06",
            open=10.0,
            high=12.0,
            low=9.0,
            close=11.5,
            volume=250.0,
            amount=2500.0,
        ),
        DailyPriceBarRecord(
            source="efinance",
            security_id=security_id,
            trade_date="2026-02-02",
            open=12.0,
            high=13.0,
            low=11.0,
            close=12.5,
            volume=200.0,
            amount=2200.0,
        ),
    )


def test_trading_date_repository_round_trips_records(tmp_path) -> None:
    db_path = tmp_path / "market.sqlite3"
    store = MarketDataStore(db_path)
    store.ensure_schema()
    records = (
        TradingDateRecord(
            source="baostock",
            market=Market.CN,
            calendar="XSHG",
            trade_date="2026-03-20",
            is_trading_day=True,
        ),
        TradingDateRecord(
            source="baostock",
            market=Market.CN,
            calendar="XSHG",
            trade_date="2026-03-21",
            is_trading_day=False,
        ),
    )

    store.trading_dates.upsert_many(records, fetched_at="2026-03-23T00:00:00Z")

    loaded = store.trading_dates.get_range(
        source="baostock",
        market=Market.CN,
        calendar="XSHG",
        start_date=date.fromisoformat("2026-03-20"),
        end_date=date.fromisoformat("2026-03-21"),
    )

    assert loaded == records


def test_trading_date_repository_rejects_non_contiguous_batch(tmp_path) -> None:
    db_path = tmp_path / "market.sqlite3"
    store = MarketDataStore(db_path)
    store.ensure_schema()

    with pytest.raises(ValueError):
        store.trading_dates.upsert_many(
            (
                TradingDateRecord(
                    source="baostock",
                    market=Market.CN,
                    calendar="XSHG",
                    trade_date="2026-03-20",
                    is_trading_day=True,
                ),
                TradingDateRecord(
                    source="baostock",
                    market=Market.CN,
                    calendar="XSHG",
                    trade_date="2026-03-22",
                    is_trading_day=False,
                ),
            ),
            fetched_at="2026-03-23T00:00:00Z",
        )


def test_trading_date_repository_rejects_disconnected_incremental_upsert(
    tmp_path,
) -> None:
    db_path = tmp_path / "market.sqlite3"
    store = MarketDataStore(db_path)
    store.ensure_schema()
    store.trading_dates.upsert_many(
        (
            TradingDateRecord(
                source="baostock",
                market=Market.CN,
                calendar="XSHG",
                trade_date="2026-03-20",
                is_trading_day=True,
            ),
            TradingDateRecord(
                source="baostock",
                market=Market.CN,
                calendar="XSHG",
                trade_date="2026-03-21",
                is_trading_day=False,
            ),
        ),
        fetched_at="2026-03-23T00:00:00Z",
    )

    with pytest.raises(ValueError):
        store.trading_dates.upsert_many(
            (
                TradingDateRecord(
                    source="baostock",
                    market=Market.CN,
                    calendar="XSHG",
                    trade_date="2026-03-24",
                    is_trading_day=True,
                ),
            ),
            fetched_at="2026-03-24T00:00:00Z",
        )


def test_trading_date_repository_returns_next_and_previous_trading_dates(
    tmp_path,
) -> None:
    db_path = tmp_path / "market.sqlite3"
    store = MarketDataStore(db_path)
    store.ensure_schema()
    store.trading_dates.upsert_many(
        (
            TradingDateRecord(
                source="baostock",
                market=Market.CN,
                calendar="XSHG",
                trade_date="2026-01-04",
                is_trading_day=False,
            ),
            TradingDateRecord(
                source="baostock",
                market=Market.CN,
                calendar="XSHG",
                trade_date="2026-01-05",
                is_trading_day=True,
            ),
            TradingDateRecord(
                source="baostock",
                market=Market.CN,
                calendar="XSHG",
                trade_date="2026-01-06",
                is_trading_day=True,
            ),
        ),
        fetched_at="2026-03-23T00:00:00Z",
    )

    assert store.trading_dates.next_trading_date(
        source="baostock",
        market=Market.CN,
        calendar="XSHG",
        trade_date=date(2026, 1, 4),
    ) == date(2026, 1, 5)
    assert (
        store.trading_dates.previous_trading_date(
            source="baostock",
            market=Market.CN,
            calendar="XSHG",
            trade_date=date(2026, 1, 4),
        )
        is None
    )
    assert store.trading_dates.previous_trading_date(
        source="baostock",
        market=Market.CN,
        calendar="XSHG",
        trade_date=date(2026, 1, 6),
    ) == date(2026, 1, 6)


def test_fundamental_snapshots_accept_non_enumerated_statement_kind(tmp_path) -> None:
    db_path = tmp_path / "market.sqlite3"
    store = MarketDataStore(db_path)
    store.ensure_schema()
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    store.securities.upsert(
        SecurityListing(security_id=security_id, name="贵州茅台"),
        observed_at="2026-03-19T00:00:00Z",
    )

    written = store.fundamental_snapshots.upsert_many(
        (
            FundamentalSnapshotRecord(
                source="baostock",
                security_id=security_id,
                period_end_date="2025-12-31",
                canonical_period_label="2025FY",
                statement_kind="valuation",
                provider_period_label="2025年报",
                report_date="2026-03-10",
                currency="CNY",
                raw_items_json='{"pe": 20.0}',
                derived_metrics_json='{"roe": 0.18}',
            ),
        ),
        fetched_at="2026-03-19T00:00:00Z",
    )

    with sqlite3.connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT statement_kind, provider_period_label, raw_items_json, derived_metrics_json
            FROM fundamental_snapshots
            """
        ).fetchall()

    assert written == 1
    assert rows == [("valuation", "2025年报", '{"pe": 20.0}', '{"roe": 0.18}')]
