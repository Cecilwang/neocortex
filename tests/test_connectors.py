from datetime import date, datetime

import pandas as pd
import pytest

from neocortex.connectors import (
    AkShareConnector,
    InMemoryConnector,
    MarketDataConnector,
)
from neocortex.markets import get_market_context
from neocortex.models import (
    CompanyProfile,
    Exchange,
    Market,
    PriceBar,
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

    assert profile.company_name == "Apple Inc."


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


def test_in_memory_connector_rejects_adjusted_price_request(
    in_memory_connector: InMemoryConnector,
    security_id: SecurityId,
) -> None:
    with pytest.raises(ValueError, match="does not support adjusted price series"):
        in_memory_connector.get_price_bars(
            security_id,
            start_date=date(2026, 3, 14),
            end_date=date(2026, 3, 15),
            adjust="qfq",
        )


class FakeAkShareAPI:
    def stock_individual_info_em(
        self, symbol: str, timeout: float | None
    ) -> pd.DataFrame:
        assert symbol == "600519"
        assert timeout == 3.0
        return pd.DataFrame(
            {
                "item": ["股票代码", "股票简称", "行业", "上市时间"],
                "value": ["600519", "贵州茅台", "酿酒行业", "20010827"],
            }
        )

    def stock_zh_a_hist(
        self,
        symbol: str,
        period: str,
        start_date: str,
        end_date: str,
        adjust: str,
        timeout: float | None,
    ) -> pd.DataFrame:
        assert symbol == "600519"
        assert period == "daily"
        assert start_date == "20260314"
        assert end_date == "20260315"
        assert adjust == "qfq"
        assert timeout == 3.0
        return pd.DataFrame(
            {
                "日期": [date(2026, 3, 14), date(2026, 3, 15)],
                "开盘": [1500.0, 1510.0],
                "收盘": [1515.0, 1528.0],
                "最高": [1520.0, 1533.0],
                "最低": [1498.0, 1505.0],
                "成交量": [120000.0, 110000.0],
            }
        )


def test_akshare_connector_normalizes_cn_profile_and_daily_bars() -> None:
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    connector = AkShareConnector(timeout=3.0, api=FakeAkShareAPI())

    profile = connector.get_company_profile(security_id)
    bars = connector.get_price_bars(
        security_id,
        start_date=date(2026, 3, 14),
        end_date=date(2026, 3, 15),
        adjust="qfq",
    )
    market_context = get_market_context(Market.CN)

    assert profile.company_name == "贵州茅台"
    assert profile.industry == "酿酒行业"
    assert profile.sector == "酿酒行业"
    assert profile.currency == "CNY"
    assert len(bars) == 2
    assert bars[0].timestamp == datetime(2026, 3, 14, 15, 0)
    assert bars[1].close == 1528.0
    assert bars[1].adjusted_close == 1528.0
    assert market_context.timezone == "Asia/Shanghai"
    assert market_context.benchmark_symbol == "000300.SH"


def test_akshare_connector_rejects_non_cn_security() -> None:
    connector = AkShareConnector(api=FakeAkShareAPI())
    security_id = SecurityId(symbol="AAPL", market=Market.US, exchange=Exchange.XNAS)

    with pytest.raises(ValueError, match="supports only CN securities"):
        connector.get_company_profile(security_id)


def test_akshare_connector_rejects_non_daily_interval() -> None:
    connector = AkShareConnector(api=FakeAkShareAPI())
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)

    with pytest.raises(ValueError, match="supports only the 1d interval"):
        connector.get_price_bars(
            security_id,
            start_date=date(2026, 3, 14),
            end_date=date(2026, 3, 15),
            interval="1h",
        )


def test_akshare_connector_rejects_unsupported_cn_exchange() -> None:
    connector = AkShareConnector(api=FakeAkShareAPI())
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XBJS)

    with pytest.raises(ValueError, match="requires an XSHG or XSHE listing exchange"):
        connector.get_company_profile(security_id)
