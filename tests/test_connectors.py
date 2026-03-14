from datetime import date

import pytest

from alphaforge.connectors import (
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


def test_market_data_connector_protocol_can_back_normalized_models() -> None:
    class DummyConnector:
        def get_company_profile(self, security_id: SecurityId) -> CompanyProfile:
            return CompanyProfile(
                security_id=security_id,
                company_name="Apple Inc.",
                sector="Technology",
                industry="Consumer Electronics",
                country="US",
                currency="USD",
            )

        def get_price_bars(
            self,
            security_id: SecurityId,
            *,
            start_date: date,
            end_date: date,
            interval: str = "1d",
        ) -> tuple[PriceBar, ...]:
            return ()

        def get_market_context(self, market: Market) -> MarketContext:
            return MarketContext(
                market=market,
                region="North America",
                timezone="America/New_York",
                trading_currency="USD",
                benchmark_symbol="SPY",
                trading_calendar="XNYS",
            )

    connector: MarketDataConnector = DummyConnector()

    profile = connector.get_company_profile(
        SecurityId(symbol="AAPL", market=Market.US, exchange="NASDAQ")
    )
    market_context = connector.get_market_context(Market.US)

    assert profile.company_name == "Apple Inc."
    assert market_context.benchmark_symbol == "SPY"
