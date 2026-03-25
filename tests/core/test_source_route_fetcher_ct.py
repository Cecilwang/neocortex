from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

import neocortex.market_data_provider.source_fetcher as source_fetcher_module
from tests.core._market_data_provider_fakes import FakeSourceConnector

from neocortex.connectors.types import (
    DailyPriceBarRecord,
    SecurityProfileSnapshot,
    TradingDateRecord,
)
from neocortex.market_data_provider import (
    RESOURCE_COMPANY_PROFILE,
    RESOURCE_DAILY_PRICE_BARS,
    RESOURCE_TRADING_DATES,
    SourceRouteFetcher,
    SourceRoutingError,
)
from neocortex.models import Exchange, Market, SecurityId
from neocortex.storage.market_store import MarketDataStore


def test_source_route_fetcher_aggregates_retryable_failures_across_sources(
    tmp_path,
) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    fetcher = SourceRouteFetcher(
        store=store,
        source_connectors={
            "baostock": FakeSourceConnector(profile_error=NotImplementedError()),
            "efinance": FakeSourceConnector(profile_error=KeyError(security_id)),
        },
        source_priority={
            Market.CN: {RESOURCE_COMPANY_PROFILE: ("baostock", "efinance")},
        },
    )

    with pytest.raises(SourceRoutingError) as exc_info:
        fetcher.get_company_profile(security_id=security_id)

    assert exc_info.value.resource_type == RESOURCE_COMPANY_PROFILE
    assert exc_info.value.target == security_id


def test_source_route_fetcher_surfaces_non_retryable_source_errors(tmp_path) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    later_source = FakeSourceConnector(
        profile=SecurityProfileSnapshot(
            source="efinance",
            security_id=security_id,
            provider_company_name="should-not-be-called",
        )
    )
    fetcher = SourceRouteFetcher(
        store=store,
        source_connectors={
            "baostock": FakeSourceConnector(profile_error=ValueError("bad payload")),
            "efinance": later_source,
        },
        source_priority={
            Market.CN: {RESOURCE_COMPANY_PROFILE: ("baostock", "efinance")},
        },
    )

    with pytest.raises(ValueError, match="bad payload"):
        fetcher.get_company_profile(security_id=security_id)

    assert later_source.profile_calls == 0


def test_source_route_fetcher_fetches_and_persists_trading_dates(tmp_path) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    store.trading_dates.upsert_many(
        (
            TradingDateRecord(
                source="baostock",
                market=Market.CN,
                calendar="XSHG",
                trade_date="2026-03-19",
                is_trading_day=True,
            ),
            TradingDateRecord(
                source="baostock",
                market=Market.CN,
                calendar="XSHG",
                trade_date="2026-03-20",
                is_trading_day=False,
            ),
        ),
        fetched_at="2026-03-21T00:00:00Z",
    )
    baostock = FakeSourceConnector(
        trading_date_records=(
            TradingDateRecord(
                source="baostock",
                market=Market.CN,
                calendar="XSHG",
                trade_date="2026-03-21",
                is_trading_day=False,
            ),
            TradingDateRecord(
                source="baostock",
                market=Market.CN,
                calendar="XSHG",
                trade_date="2026-03-22",
                is_trading_day=False,
            ),
            TradingDateRecord(
                source="baostock",
                market=Market.CN,
                calendar="XSHG",
                trade_date="2026-03-23",
                is_trading_day=True,
            ),
        ),
    )
    fetcher = SourceRouteFetcher(
        store=store,
        source_connectors={"baostock": baostock},
        source_priority={Market.CN: {RESOURCE_TRADING_DATES: ("baostock",)}},
    )

    trading_dates = fetcher.get_trading_dates(
        market=Market.CN,
        start_date=date(2026, 3, 19),
        end_date=date(2026, 3, 23),
    )

    assert tuple(record.trade_date for record in trading_dates) == (
        "2026-03-19",
        "2026-03-20",
        "2026-03-21",
        "2026-03-22",
        "2026-03-23",
    )
    assert baostock.trading_dates_calls == 1
    assert baostock.trading_date_request_ranges == [
        (date(2026, 3, 21), date(2026, 3, 23))
    ]
    assert len(store.dump_table("trading_dates")) == 5


def test_source_route_fetcher_writes_back_current_day_after_market_close(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            current = datetime(2026, 3, 19, 15, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
            return current if tz is None else current.astimezone(tz)

    monkeypatch.setattr(source_fetcher_module, "datetime", FrozenDateTime)

    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    security_id = SecurityId(symbol="688981", market=Market.CN, exchange=Exchange.XSHG)
    connector = FakeSourceConnector(
        daily_records=(
            DailyPriceBarRecord(
                source="fake",
                security_id=security_id,
                trade_date="2026-03-19",
                open=10.0,
                high=11.0,
                low=9.0,
                close=10.5,
                volume=100.0,
            ),
        ),
    )
    fetcher = SourceRouteFetcher(
        store=store,
        source_connectors={"fake": connector},
        source_priority={Market.CN: {RESOURCE_DAILY_PRICE_BARS: ("fake",)}},
    )

    fetched = fetcher.get_raw_daily_records(
        security_id=security_id,
        start_date=date(2026, 3, 19),
        end_date=date(2026, 3, 19),
    )

    assert len(fetched) == 1
    assert len(store.dump_table("daily_price_bars")) == 1


def test_source_route_fetcher_does_not_write_back_current_day_before_market_close_in_market_timezone(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            current = datetime(2026, 3, 20, 4, 30, tzinfo=ZoneInfo("Asia/Tokyo"))
            return current if tz is None else current.astimezone(tz)

    monkeypatch.setattr(source_fetcher_module, "datetime", FrozenDateTime)

    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    security_id = SecurityId(symbol="AAPL", market=Market.US, exchange=Exchange.XNAS)
    connector = FakeSourceConnector(
        daily_records=(
            DailyPriceBarRecord(
                source="fake",
                security_id=security_id,
                trade_date="2026-03-19",
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=100.0,
            ),
        ),
    )
    fetcher = SourceRouteFetcher(
        store=store,
        source_connectors={"fake": connector},
        source_priority={Market.US: {RESOURCE_DAILY_PRICE_BARS: ("fake",)}},
    )

    fetched = fetcher.get_raw_daily_records(
        security_id=security_id,
        start_date=date(2026, 3, 19),
        end_date=date(2026, 3, 19),
    )

    assert len(fetched) == 1
    assert store.dump_table("daily_price_bars") == []
