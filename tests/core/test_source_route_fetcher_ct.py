from datetime import date

import pytest

from tests.core._market_data_provider_fakes import FakeSourceConnector

from neocortex.connectors.types import AdjustmentFactorRecord, DailyPriceBarRecord
from neocortex.market_data_provider import (
    RESOURCE_DAILY_PRICE_BARS,
    RESOURCE_COMPANY_PROFILE,
    SourceRouteFetcher,
    SourceRoutingError,
)
from neocortex.models import Exchange, Market, SecurityId
from neocortex.storage.market_store import MarketDataStore


def test_source_route_fetcher_fetches_factor_adjusted_bars_without_persisting_factors(
    tmp_path,
) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    baostock = FakeSourceConnector(
        store=store,
        daily_records=(
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
        factor_records=(
            AdjustmentFactorRecord(
                source="baostock",
                security_id=security_id,
                trade_date="2026-01-05",
                adjustment_type="qfq",
                factor=3.0,
            ),
        ),
    )
    fetcher = SourceRouteFetcher(
        store=store,
        source_connectors={"baostock": baostock},
        source_priority={
            Market.CN: {RESOURCE_DAILY_PRICE_BARS: ("baostock",)},
        },
        today=lambda: date(2026, 3, 19),
    )

    bars = fetcher.get_adjusted_daily_price_bars(
        security_id=security_id,
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 5),
        adjust="qfq",
    )

    assert bars.closes.tolist() == [30.0]
    assert baostock.daily_calls == 1
    assert baostock.factor_calls == 1
    assert baostock.apply_adjustment_calls == 1
    assert len(store.dump_table("daily_price_bars")) == 1


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

    assert [source for source, _ in exc_info.value.failures] == [
        "baostock",
        "efinance",
    ]
    assert isinstance(exc_info.value.failures[0][1], NotImplementedError)
    assert isinstance(exc_info.value.failures[1][1], ValueError)
