from datetime import date, datetime

import pytest

from alphaforge.connectors import (
    InMemoryConnector,
    MarketDataConnector,
    from_provider_ticker,
    to_provider_ticker,
)
from alphaforge.models import (
    CompanyProfile,
    DataProvider,
    Market,
    MarketContext,
    PriceBar,
    SecurityId,
)


@pytest.mark.parametrize(
    ("security_id", "provider", "expected_ticker"),
    [
        (
            SecurityId(symbol="AAPL", market=Market.US, exchange="NASDAQ"),
            DataProvider.YAHOO_FINANCE,
            "AAPL",
        ),
        (
            SecurityId(symbol="7203", market=Market.JP, exchange="TSE"),
            DataProvider.YAHOO_FINANCE,
            "7203.T",
        ),
        (
            SecurityId(symbol="0700", market=Market.HK, exchange="HKEX"),
            DataProvider.YAHOO_FINANCE,
            "0700.HK",
        ),
        (
            SecurityId(symbol="600519", market=Market.CN, exchange="SSE"),
            DataProvider.YAHOO_FINANCE,
            "600519.SS",
        ),
        (
            SecurityId(symbol="000001", market=Market.CN, exchange="SZSE"),
            DataProvider.AKSHARE,
            "sz000001",
        ),
    ],
)
def test_to_provider_ticker_formats_market_specific_symbol(
    security_id: SecurityId,
    provider: DataProvider,
    expected_ticker: str,
) -> None:
    assert to_provider_ticker(security_id, provider) == expected_ticker


@pytest.mark.parametrize(
    ("ticker", "provider", "kwargs", "expected_security_id"),
    [
        (
            "AAPL",
            DataProvider.YAHOO_FINANCE,
            {"market": Market.US, "exchange": "NASDAQ"},
            SecurityId(symbol="AAPL", market=Market.US, exchange="NASDAQ"),
        ),
        (
            "7203.T",
            DataProvider.YAHOO_FINANCE,
            {},
            SecurityId(symbol="7203", market=Market.JP, exchange="TSE"),
        ),
        (
            "0700.HK",
            DataProvider.YAHOO_FINANCE,
            {},
            SecurityId(symbol="0700", market=Market.HK, exchange="HKEX"),
        ),
        (
            "600519.SS",
            DataProvider.YAHOO_FINANCE,
            {},
            SecurityId(symbol="600519", market=Market.CN, exchange="SSE"),
        ),
        (
            "sz000001",
            DataProvider.AKSHARE,
            {},
            SecurityId(symbol="000001", market=Market.CN, exchange="SZSE"),
        ),
        (
            "US:AAPL",
            DataProvider.MANUAL,
            {"exchange": "NASDAQ"},
            SecurityId(symbol="AAPL", market=Market.US, exchange="NASDAQ"),
        ),
    ],
)
def test_from_provider_ticker_restores_canonical_security_id(
    ticker: str,
    provider: DataProvider,
    kwargs: dict[str, str | Market],
    expected_security_id: SecurityId,
) -> None:
    assert from_provider_ticker(ticker, provider, **kwargs) == expected_security_id


def test_cn_prefixed_codec_rejects_non_cn_market() -> None:
    security_id = SecurityId(symbol="AAPL", market=Market.US, exchange="NASDAQ")

    with pytest.raises(
        ValueError, match="not supported by the CN-prefixed ticker codec"
    ):
        to_provider_ticker(security_id, DataProvider.AKSHARE)


@pytest.fixture
def security_id() -> SecurityId:
    return SecurityId(symbol="AAPL", market=Market.US, exchange="NASDAQ")


@pytest.fixture
def in_memory_connector(security_id: SecurityId) -> InMemoryConnector:
    return InMemoryConnector(
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
        market_contexts={
            Market.US: MarketContext(
                market=Market.US,
                region="North America",
                timezone="America/New_York",
                trading_currency="USD",
                benchmark_symbol="SPY",
                trading_calendar="XNYS",
            )
        },
        price_bars={
            security_id: (
                PriceBar(
                    security_id=security_id,
                    timestamp=datetime(2026, 3, 14, 16, 0),
                    open=210.0,
                    high=212.0,
                    low=209.5,
                    close=211.4,
                    volume=10_000_000,
                ),
                PriceBar(
                    security_id=security_id,
                    timestamp=datetime(2026, 3, 15, 16, 0),
                    open=211.5,
                    high=214.0,
                    low=210.8,
                    close=213.6,
                    volume=12_000_000,
                ),
            )
        },
    )


def test_market_data_connector_protocol_can_back_normalized_models(
    in_memory_connector: InMemoryConnector,
    security_id: SecurityId,
) -> None:
    connector: MarketDataConnector = in_memory_connector

    profile = connector.get_company_profile(security_id)
    market_context = connector.get_market_context(Market.US)

    assert profile.company_name == "Apple Inc."
    assert market_context.benchmark_symbol == "SPY"


def test_in_memory_connector_filters_price_bars_by_date(
    in_memory_connector: InMemoryConnector,
    security_id: SecurityId,
) -> None:
    bars = in_memory_connector.get_price_bars(
        security_id,
        start_date=date(2026, 3, 15),
        end_date=date(2026, 3, 15),
    )

    assert len(bars) == 1
    assert bars[0].close == 213.6


def test_in_memory_connector_rejects_unsupported_interval(
    in_memory_connector: InMemoryConnector,
    security_id: SecurityId,
) -> None:
    with pytest.raises(ValueError, match="supports only the 1d interval"):
        in_memory_connector.get_price_bars(
            security_id,
            start_date=date(2026, 3, 14),
            end_date=date(2026, 3, 15),
            interval="1h",
        )
