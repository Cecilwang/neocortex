from datetime import date
import importlib

import pandas as pd
import pytest

from neocortex.connectors import EFinanceConnector
from neocortex.connectors.types import (
    DailyPriceBarRecord,
    SecurityListing,
    SecurityProfileSnapshot,
)
from neocortex.models import Exchange, Market, SecurityId


class FakeEFinanceStockAPI:
    last_fqt: int | None = None

    @staticmethod
    def get_realtime_quotes(scope: str) -> pd.DataFrame:
        assert scope == "沪深A股"
        return pd.DataFrame(
            {
                "股票代码": ["600519", "000001"],
                "股票名称": ["贵州茅台", "平安银行"],
            }
        )

    @staticmethod
    def get_base_info(symbol: str) -> pd.DataFrame:
        assert symbol == "600519"
        return pd.DataFrame(
            [
                {
                    "股票名称": "贵州茅台",
                    "所处行业": "白酒",
                }
            ]
        )

    @staticmethod
    def get_quote_history(
        symbol: str,
        *,
        beg: str,
        end: str,
        klt: int,
        fqt: int,
    ) -> pd.DataFrame:
        FakeEFinanceStockAPI.last_fqt = fqt
        assert symbol == "600519"
        assert beg == "20260101"
        assert end == "20260131"
        assert klt == 101
        return pd.DataFrame(
            {
                "日期": ["2026-01-05", "2026-01-06"],
                "开盘": [100.0 + fqt, 101.0 + fqt],
                "最高": [110.0 + fqt, 111.0 + fqt],
                "最低": [99.0 + fqt, 100.0 + fqt],
                "收盘": [108.0 + fqt, 109.0 + fqt],
                "成交量": [1000.0, 1100.0],
                "成交额": [108000.0, 119900.0],
            }
        )


class FakeEFinanceAPI:
    stock = FakeEFinanceStockAPI()


class IncompleteProfileEFinanceStockAPI(FakeEFinanceStockAPI):
    @staticmethod
    def get_base_info(symbol: str) -> pd.DataFrame:
        assert symbol == "600519"
        return pd.DataFrame([{"股票名称": "贵州茅台", "所处行业": ""}])


class IncompleteProfileEFinanceAPI:
    stock = IncompleteProfileEFinanceStockAPI()


def test_efinance_connector_lists_cn_securities() -> None:
    connector = EFinanceConnector(api=FakeEFinanceAPI())

    listings = connector.list_securities(market=Market.CN)

    assert listings == (
        SecurityListing(
            security_id=SecurityId(
                symbol="600519",
                market=Market.CN,
                exchange=Exchange.XSHG,
            ),
            name="贵州茅台",
        ),
        SecurityListing(
            security_id=SecurityId(
                symbol="000001",
                market=Market.CN,
                exchange=Exchange.XSHE,
            ),
            name="平安银行",
        ),
    )


def test_efinance_connector_fetches_profile_snapshot() -> None:
    connector = EFinanceConnector(api=FakeEFinanceAPI())
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)

    snapshot = connector.get_security_profile_snapshot(security_id)

    assert snapshot == SecurityProfileSnapshot(
        source="efinance",
        security_id=security_id,
        provider_company_name="贵州茅台",
        sector="白酒",
        industry="白酒",
        country="CN",
        currency="CNY",
        primary_listing=True,
    )


def test_efinance_connector_fetches_daily_price_bars() -> None:
    FakeEFinanceStockAPI.last_fqt = None
    connector = EFinanceConnector(api=FakeEFinanceAPI())
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)

    records = connector.get_daily_price_bars(
        security_id,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
    )

    assert FakeEFinanceStockAPI.last_fqt == 0
    assert records == (
        DailyPriceBarRecord(
            source="efinance",
            security_id=security_id,
            trade_date="2026-01-05",
            open=100.0,
            high=110.0,
            low=99.0,
            close=108.0,
            volume=1000.0,
            amount=108000.0,
        ),
        DailyPriceBarRecord(
            source="efinance",
            security_id=security_id,
            trade_date="2026-01-06",
            open=101.0,
            high=111.0,
            low=100.0,
            close=109.0,
            volume=1100.0,
            amount=119900.0,
        ),
    )


def test_efinance_connector_fetches_adjusted_daily_price_bars() -> None:
    FakeEFinanceStockAPI.last_fqt = None
    connector = EFinanceConnector(api=FakeEFinanceAPI())
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)

    records = connector.get_adjusted_daily_price_bars(
        security_id,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        adjustment_type="qfq",
    )

    assert FakeEFinanceStockAPI.last_fqt == 1
    assert records == (
        DailyPriceBarRecord(
            source="efinance",
            security_id=security_id,
            trade_date="2026-01-05",
            open=101.0,
            high=111.0,
            low=100.0,
            close=109.0,
            volume=1000.0,
            amount=108000.0,
        ),
        DailyPriceBarRecord(
            source="efinance",
            security_id=security_id,
            trade_date="2026-01-06",
            open=102.0,
            high=112.0,
            low=101.0,
            close=110.0,
            volume=1100.0,
            amount=119900.0,
        ),
    )


def test_efinance_connector_rejects_incomplete_profile_data() -> None:
    connector = EFinanceConnector(api=IncompleteProfileEFinanceAPI())
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)

    with pytest.raises(ValueError, match="Incomplete profile data"):
        connector.get_security_profile_snapshot(security_id)


def test_efinance_connector_reports_missing_dependency(monkeypatch) -> None:
    connector = EFinanceConnector()
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)

    def _raise_missing(name: str):
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(importlib, "import_module", _raise_missing)

    with pytest.raises(RuntimeError, match="optional 'efinance' dependency"):
        connector.get_security_profile_snapshot(security_id)
