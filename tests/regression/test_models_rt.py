"""Deterministic model and protocol baselines.

These cases live under regression/ because they pin normalized output shapes and
derived convenience behavior as snapshot-style baselines, even though they do
not talk to live upstream systems.
"""

from datetime import date, datetime

import pytest

from neocortex.markets import get_market_context
from neocortex.market_data_provider import MarketDataProvider
from neocortex.models.core import (
    CompanyProfile,
    DataProvider,
    Exchange,
    FundamentalStatement,
    FundamentalSnapshot,
    FundamentalValueOrigin,
    Market,
    MarketContext,
    PRICE_BAR_TIMESTAMP,
    PriceBar,
    PriceSeries,
    SecurityId,
    SectorBenchmark,
    TradingCalendar,
)
from tests.core._market_data_provider_fakes import InMemoryMarketDataProvider


@pytest.fixture
def security_id() -> SecurityId:
    return SecurityId(symbol="AAPL", market=Market.US, exchange=Exchange.XNAS)


@pytest.fixture
def in_memory_provider(security_id: SecurityId) -> InMemoryMarketDataProvider:
    return InMemoryMarketDataProvider(
        company_profiles={
            security_id: CompanyProfile(
                security_id=security_id,
                company_name="Apple Inc.",
                sector="Technology",
                industry="Consumer Electronics",
                country="US",
                currency="USD",
            )
        },
        price_bars={
            security_id: PriceSeries(
                security_id=security_id,
                bars=(
                    PriceBar(
                        security_id=security_id,
                        timestamp=datetime(2026, 3, 14, 16, 0),
                        open=210.0,
                        high=212.0,
                        low=209.5,
                        close=211.4,
                        volume=10_000_000,
                    ),
                ),
            )
        },
    )


def test_market_data_provider_protocol_can_back_normalized_models(
    in_memory_provider: InMemoryMarketDataProvider,
    security_id: SecurityId,
) -> None:
    provider: MarketDataProvider = in_memory_provider

    profile = provider.get_company_profile(security_id)

    assert profile.company_name == "Apple Inc."


def test_core_models_store_normalized_entities(security_id: SecurityId) -> None:
    profile = CompanyProfile(
        security_id=security_id,
        company_name="Apple Inc.",
        sector="Technology",
        industry="Consumer Electronics",
        country="US",
        currency="USD",
    )
    market_context = MarketContext(
        market=Market.US,
        region="North America",
        timezone="America/New_York",
        trading_currency="USD",
        benchmark_symbol="SPY",
        trading_calendar=TradingCalendar.XNYS,
    )
    bar = PriceBar(
        security_id=security_id,
        timestamp=datetime(2026, 3, 15, 9, 30),
        open=210.0,
        high=212.0,
        low=208.5,
        close=211.4,
        volume=10_000_000,
    )
    benchmark = SectorBenchmark(
        market=Market.US,
        sector="Technology",
        as_of_date=date(2026, 3, 15),
        metric_averages={"roe": 0.18},
        constituents=("AAPL", "MSFT"),
    )
    series = PriceSeries(security_id=security_id, bars=(bar,))

    assert profile.security_id.ticker == "US:AAPL"
    assert market_context.trading_calendar is TradingCalendar.XNYS
    assert bar.close == 211.4
    assert series.closes.tolist() == [211.4]
    assert benchmark.metric_averages["roe"] == 0.18


def test_price_series_bars_uses_price_bar_columns(security_id: SecurityId) -> None:
    series = PriceSeries(
        security_id=security_id,
        bars=(
            PriceBar(
                security_id=security_id,
                timestamp=datetime(2026, 3, 15, 9, 30),
                open=210.0,
                high=212.0,
                low=208.5,
                close=211.4,
                volume=10_000_000,
            ),
        ),
    )

    frame = series.bars.copy()
    frame[PRICE_BAR_TIMESTAMP] = frame[PRICE_BAR_TIMESTAMP].map(
        lambda value: value.isoformat()
    )

    assert frame.to_dict(orient="records") == [
        {
            "timestamp": "2026-03-15T09:30:00",
            "open": 210.0,
            "high": 212.0,
            "low": 208.5,
            "close": 211.4,
            "volume": 10_000_000,
        }
    ]


def test_fundamental_snapshot_tracks_source_provider_without_symbol_mapping(
    security_id: SecurityId,
) -> None:
    snapshot = FundamentalSnapshot(
        security_id=security_id,
        report_date=date(2025, 12, 31),
        ann_date=date(2026, 3, 15),
        fetch_at=datetime(2026, 3, 15, 12, 0),
        statement=FundamentalStatement.ROE,
        value=0.18,
        value_origin=FundamentalValueOrigin.FETCHED,
        source=DataProvider.YAHOO_FINANCE,
    )

    assert snapshot.source is DataProvider.YAHOO_FINANCE
    assert snapshot.security_id.ticker == "US:AAPL"


@pytest.mark.parametrize(
    ("market", "symbol", "expected_ticker"),
    [
        (Market.US, "AAPL", "US:AAPL"),
        (Market.JP, "7203", "JP:7203"),
        (Market.HK, "0700", "HK:0700"),
    ],
)
def test_security_id_builds_market_scoped_ticker(
    market: Market,
    symbol: str,
    expected_ticker: str,
) -> None:
    assert (
        SecurityId(symbol=symbol, market=market, exchange=Exchange.XNYS).ticker
        == expected_ticker
    )


def test_market_context_registry_baseline() -> None:
    baseline = {
        market.value: {
            "region": context.region,
            "timezone": context.timezone,
            "currency": context.trading_currency,
            "benchmark": context.benchmark_symbol,
            "calendar": context.trading_calendar.value,
        }
        for market in Market
        for context in (get_market_context(market),)
    }

    assert baseline == {
        "US": {
            "region": "North America",
            "timezone": "America/New_York",
            "currency": "USD",
            "benchmark": "SPY",
            "calendar": "XNYS",
        },
        "JP": {
            "region": "Japan",
            "timezone": "Asia/Tokyo",
            "currency": "JPY",
            "benchmark": "TOPIX",
            "calendar": "XTKS",
        },
        "HK": {
            "region": "Hong Kong",
            "timezone": "Asia/Hong_Kong",
            "currency": "HKD",
            "benchmark": "HSI",
            "calendar": "XHKG",
        },
        "CN": {
            "region": "China",
            "timezone": "Asia/Shanghai",
            "currency": "CNY",
            "benchmark": "000300.SH",
            "calendar": "XSHG",
        },
    }


def test_exchange_enum_values_baseline() -> None:
    assert {member.name: member.value for member in Exchange} == {
        "XNAS": "XNAS",
        "XNYS": "XNYS",
        "XTKS": "XTKS",
        "XHKG": "XHKG",
        "XSHG": "XSHG",
        "XSHE": "XSHE",
        "XBJS": "XBJS",
    }


def test_trading_calendar_enum_values_baseline() -> None:
    assert {member.name: member.value for member in TradingCalendar} == {
        "XNAS": "XNAS",
        "XNYS": "XNYS",
        "XTKS": "XTKS",
        "XHKG": "XHKG",
        "XSHG": "XSHG",
        "XSHE": "XSHE",
        "XBJS": "XBJS",
    }
