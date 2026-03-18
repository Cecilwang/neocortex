from datetime import date, datetime

import pytest

from neocortex.connectors import InMemoryConnector
from neocortex.markets import get_market_context
from neocortex.models import (
    CompanyProfile,
    Exchange,
    Market,
    PriceBar,
    PriceSeries,
    SecurityId,
)


@pytest.fixture
def security_id() -> SecurityId:
    return SecurityId(symbol="AAPL", market=Market.US, exchange=Exchange.XNAS)


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
                    PriceBar(
                        security_id=security_id,
                        timestamp=datetime(2026, 3, 15, 16, 0),
                        open=211.5,
                        high=214.0,
                        low=210.8,
                        close=213.6,
                        volume=12_000_000,
                    ),
                ),
            )
        },
    )


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
    assert bars.closes.tolist() == [213.6]


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


def test_in_memory_connector_preserves_raw_key_error_for_missing_profile() -> None:
    connector = InMemoryConnector()
    security_id = SecurityId(symbol="AAPL", market=Market.US, exchange=Exchange.XNAS)

    with pytest.raises(KeyError) as exc_info:
        connector.get_company_profile(security_id)

    assert exc_info.value.args == (security_id,)


def test_in_memory_connector_preserves_raw_key_error_for_missing_price_bars() -> None:
    connector = InMemoryConnector()
    security_id = SecurityId(symbol="AAPL", market=Market.US, exchange=Exchange.XNAS)

    with pytest.raises(KeyError) as exc_info:
        connector.get_price_bars(
            security_id,
            start_date=date(2026, 3, 14),
            end_date=date(2026, 3, 15),
        )

    assert exc_info.value.args == (security_id,)


def test_get_market_context_preserves_raw_key_error_for_missing_market() -> None:
    with pytest.raises(KeyError) as exc_info:
        get_market_context("EU")  # type: ignore[arg-type]

    assert exc_info.value.args == ("EU",)
