from datetime import date

import pytest

from tests.core._market_data_provider_fakes import FakeSourceConnector

from neocortex.connectors.types import TradingDateRecord
from neocortex.market_data_provider import (
    RESOURCE_COMPANY_PROFILE,
    RESOURCE_TRADING_DATES,
    SourceRouteFetcher,
    SourceRoutingError,
)
from neocortex.models import Exchange, Market, SecurityId
from neocortex.storage.market_store import MarketDataStore


def test_source_route_fetcher_aggregates_failures_across_sources(tmp_path) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    fetcher = SourceRouteFetcher(
        store=store,
        source_connectors={
            "baostock": FakeSourceConnector(profile_error=NotImplementedError()),
            "efinance": FakeSourceConnector(profile_error=ValueError("bad payload")),
        },
        source_priority={
            Market.CN: {RESOURCE_COMPANY_PROFILE: ("baostock", "efinance")},
        },
    )

    with pytest.raises(SourceRoutingError) as exc_info:
        fetcher.get_company_profile(security_id=security_id)

    assert exc_info.value.resource_type == RESOURCE_COMPANY_PROFILE
    assert exc_info.value.target == security_id


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
