from datetime import date

import pandas as pd
import pytest

from neocortex.connectors import AkShareConnector
from neocortex.models import Exchange, Market, SecurityId


MOUTAI_SNAPSHOT_START_DATE = date(2026, 3, 9)
MOUTAI_SNAPSHOT_END_DATE = date(2026, 3, 13)
MOUTAI_WEEKLY_BAR_SNAPSHOT = [
    {
        "trade_date": "2026-03-09",
        "open": 1390.00,
        "high": 1404.90,
        "low": 1383.20,
        "close": 1397.00,
    },
    {
        "trade_date": "2026-03-10",
        "open": 1404.90,
        "high": 1409.49,
        "low": 1398.00,
        "close": 1401.88,
    },
    {
        "trade_date": "2026-03-11",
        "open": 1402.99,
        "high": 1405.99,
        "low": 1398.02,
        "close": 1400.00,
    },
    {
        "trade_date": "2026-03-12",
        "open": 1395.00,
        "high": 1403.95,
        "low": 1391.01,
        "close": 1392.00,
    },
    {
        "trade_date": "2026-03-13",
        "open": 1392.48,
        "high": 1417.62,
        "low": 1392.00,
        "close": 1413.64,
    },
]


class FakeAkShareAPI:
    def stock_individual_info_em(
        self, symbol: str, timeout: float | None
    ) -> pd.DataFrame:
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


def test_akshare_connector_normalized_output_baseline() -> None:
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    connector = AkShareConnector(timeout=3.0, api=FakeAkShareAPI())

    profile = connector.get_security_profile_snapshot(security_id)
    bars = connector.get_adjusted_daily_price_bars(
        security_id,
        start_date=date(2026, 3, 14),
        end_date=date(2026, 3, 15),
        adjustment_type="qfq",
    )

    assert {
        "ticker": profile.security_id.ticker,
        "company_name": profile.provider_company_name,
        "sector": profile.sector,
        "industry": profile.industry,
        "currency": profile.currency,
    } == {
        "ticker": "CN:600519",
        "company_name": "贵州茅台",
        "sector": "酿酒行业",
        "industry": "酿酒行业",
        "currency": "CNY",
    }
    assert [
        {
            "trade_date": record.trade_date,
            "open": record.open,
            "high": record.high,
            "low": record.low,
            "close": record.close,
            "volume": record.volume,
        }
        for record in bars
    ] == [
        {
            "trade_date": "2026-03-14",
            "open": 1500.0,
            "high": 1520.0,
            "low": 1498.0,
            "close": 1515.0,
            "volume": 12_000_000.0,
        },
        {
            "trade_date": "2026-03-15",
            "open": 1510.0,
            "high": 1533.0,
            "low": 1505.0,
            "close": 1528.0,
            "volume": 11_000_000.0,
        },
    ]


def test_akshare_connector_fetches_moutai_profile_and_fixed_week_bars() -> None:
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    connector = AkShareConnector(timeout=10.0)

    try:
        profile = connector.get_security_profile_snapshot(security_id)
        raw_bars = connector.get_daily_price_bars(
            security_id,
            start_date=MOUTAI_SNAPSHOT_START_DATE,
            end_date=MOUTAI_SNAPSHOT_END_DATE,
        )
    except Exception as exc:  # pragma: no cover - depends on live upstream behavior
        pytest.skip(f"AkShare live fetch unavailable: {type(exc).__name__}: {exc}")

    assert profile.provider_company_name
    assert "茅台" in profile.provider_company_name
    assert profile.currency == "CNY"
    baseline_rows = [
        {
            "trade_date": record.trade_date,
            "open": record.open,
            "high": record.high,
            "low": record.low,
            "close": record.close,
        }
        for record in raw_bars
    ]
    assert baseline_rows == MOUTAI_WEEKLY_BAR_SNAPSHOT
    assert all(record.volume is None or record.volume > 0 for record in raw_bars)
