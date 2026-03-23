from datetime import date

import pytest

from tests.core._market_data_provider_fakes import FakeSourceConnector

from neocortex.connectors.types import (
    DailyPriceBarRecord,
    SecurityListing,
    SecurityProfileSnapshot,
    TradingDateRecord,
)
from neocortex.market_data_provider import (
    DBRouteReader,
    RESOURCE_COMPANY_PROFILE,
    RESOURCE_DAILY_PRICE_BARS,
    RESOURCE_TRADING_DATES,
    SourceRoutingError,
)
from neocortex.models import Exchange, Market, SecurityId
from neocortex.storage.market_store import MarketDataStore


def test_db_route_reader_prefers_later_source_profile_hit_without_network_call(
    tmp_path,
) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    store.seed_security_listing(
        SecurityListing(security_id=security_id, name="贵州茅台"),
        source="seed",
    )
    store.security_profiles.upsert(
        SecurityProfileSnapshot(
            source="efinance",
            security_id=security_id,
            provider_company_name="贵州茅台股份有限公司",
            sector="白酒",
            industry="白酒",
            country="CN",
            currency="CNY",
        ),
        fetched_at="2026-03-19T00:00:00Z",
    )
    baostock = FakeSourceConnector(
        profile=SecurityProfileSnapshot(
            source="baostock",
            security_id=security_id,
            provider_company_name="should-not-be-used",
        )
    )
    efinance = FakeSourceConnector(
        profile=SecurityProfileSnapshot(
            source="efinance",
            security_id=security_id,
            provider_company_name="should-not-be-called",
        )
    )
    reader = DBRouteReader(
        store=store,
        source_connectors={"baostock": baostock, "efinance": efinance},
        source_priority={
            Market.CN: {RESOURCE_COMPANY_PROFILE: ("baostock", "efinance")},
        },
    )

    profile = reader.get_company_profile(security_id=security_id)

    assert profile.company_name == "贵州茅台股份有限公司"
    assert baostock.profile_calls == 0
    assert efinance.profile_calls == 0


def test_db_route_reader_returns_trading_dates_from_later_source_hit(tmp_path) -> None:
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
        fetched_at="2026-03-23T00:00:00Z",
    )
    reader = DBRouteReader(
        store=store,
        source_connectors={
            "efinance": FakeSourceConnector(),
            "baostock": FakeSourceConnector(),
        },
        source_priority={
            Market.CN: {RESOURCE_TRADING_DATES: ("efinance", "baostock")},
        },
    )

    trading_dates = reader.get_trading_dates(
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


def test_db_route_reader_rejects_incomplete_trading_date_coverage(tmp_path) -> None:
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
        fetched_at="2026-03-23T00:00:00Z",
    )
    reader = DBRouteReader(
        store=store,
        source_connectors={"baostock": FakeSourceConnector()},
        source_priority={Market.CN: {RESOURCE_TRADING_DATES: ("baostock",)}},
    )

    with pytest.raises(SourceRoutingError) as exc_info:
        reader.get_trading_dates(
            market=Market.CN,
            start_date=date(2026, 3, 19),
            end_date=date(2026, 3, 23),
        )

    assert exc_info.value.resource_type == RESOURCE_TRADING_DATES


def test_db_route_reader_accepts_raw_daily_when_requested_start_is_non_trading_day(
    tmp_path,
) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    store.seed_security_listing(
        SecurityListing(security_id=security_id, name="贵州茅台"),
        source="seed",
    )
    store.trading_dates.upsert_many(
        (
            TradingDateRecord(
                source="baostock",
                market=Market.CN,
                calendar="XSHG",
                trade_date="2026-01-05",
                is_trading_day=True,
            ),
        ),
        fetched_at="2026-03-23T00:00:00Z",
    )
    store.daily_price_bars.upsert_many(
        (
            DailyPriceBarRecord(
                source="baostock",
                security_id=security_id,
                trade_date="2026-01-05",
                open=10.0,
                high=11.0,
                low=9.0,
                close=10.0,
                volume=100.0,
            ),
        ),
        fetched_at="2026-03-23T00:00:00Z",
    )
    reader = DBRouteReader(
        store=store,
        source_connectors={"baostock": FakeSourceConnector()},
        source_priority={Market.CN: {RESOURCE_DAILY_PRICE_BARS: ("baostock",)}},
    )

    bars = reader.get_raw_daily_price_bars(
        security_id=security_id,
        start_date=date(2026, 1, 4),
        end_date=date(2026, 1, 5),
    )

    assert bars.closes.tolist() == [10.0]
