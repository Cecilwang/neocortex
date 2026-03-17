from datetime import date
import time

import pandas as pd
import pytest

from neocortex.connectors import AkShareConnector
from neocortex.models import Exchange, Market, SecurityId


MOUTAI_SNAPSHOT_START_DATE = date(2026, 3, 9)
MOUTAI_SNAPSHOT_END_DATE = date(2026, 3, 13)
MOUTAI_WEEKLY_BAR_SNAPSHOT = [
    {
        "timestamp": "2026-03-09T15:00:00",
        "open": 1390.00,
        "high": 1404.90,
        "low": 1383.20,
        "close": 1397.00,
        "volume": 3_740_000.0,
    },
    {
        "timestamp": "2026-03-10T15:00:00",
        "open": 1404.90,
        "high": 1409.49,
        "low": 1398.00,
        "close": 1401.88,
        "volume": 2_460_000.0,
    },
    {
        "timestamp": "2026-03-11T15:00:00",
        "open": 1402.99,
        "high": 1405.99,
        "low": 1398.02,
        "close": 1400.00,
        "volume": 2_450_000.0,
    },
    {
        "timestamp": "2026-03-12T15:00:00",
        "open": 1395.00,
        "high": 1403.95,
        "low": 1391.01,
        "close": 1392.00,
        "volume": 2_760_000.0,
    },
    {
        "timestamp": "2026-03-13T15:00:00",
        "open": 1392.48,
        "high": 1417.62,
        "low": 1392.00,
        "close": 1413.64,
        "volume": 3_360_000.0,
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


def _fetch_moutai_snapshot_with_retries(
    connector: AkShareConnector,
    security_id: SecurityId,
    *,
    max_attempts: int = 3,
    sleep_seconds: float = 1.0,
):
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            profile = connector.get_company_profile(security_id)
            bars = connector.get_price_bars(
                security_id,
                start_date=MOUTAI_SNAPSHOT_START_DATE,
                end_date=MOUTAI_SNAPSHOT_END_DATE,
            )
            return profile, bars
        except Exception as exc:  # pragma: no cover - depends on live upstream behavior
            last_exc = exc
            if attempt < max_attempts - 1:
                time.sleep(sleep_seconds)
    pytest.skip(
        f"AkShare live fetch unavailable after {max_attempts} attempts: {last_exc}"
    )


def test_akshare_connector_normalized_output_baseline() -> None:
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    connector = AkShareConnector(timeout=3.0, api=FakeAkShareAPI())

    profile = connector.get_company_profile(security_id)
    bars = connector.get_price_bars(
        security_id,
        start_date=date(2026, 3, 14),
        end_date=date(2026, 3, 15),
        adjust="qfq",
    )

    assert {
        "ticker": profile.security_id.ticker,
        "company_name": profile.company_name,
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
            "timestamp": bar.timestamp.isoformat(),
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "adjusted_close": bar.adjusted_close,
        }
        for bar in bars
    ] == [
        {
            "timestamp": "2026-03-14T15:00:00",
            "open": 1500.0,
            "high": 1520.0,
            "low": 1498.0,
            "close": 1515.0,
            "volume": 12_000_000.0,
            "adjusted_close": 1515.0,
        },
        {
            "timestamp": "2026-03-15T15:00:00",
            "open": 1510.0,
            "high": 1533.0,
            "low": 1505.0,
            "close": 1528.0,
            "volume": 11_000_000.0,
            "adjusted_close": 1528.0,
        },
    ]


def test_akshare_connector_fetches_moutai_profile_and_fixed_week_bars() -> None:
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    connector = AkShareConnector(timeout=10.0)

    profile, raw_bars = _fetch_moutai_snapshot_with_retries(
        connector,
        security_id,
    )

    assert profile.company_name
    assert "茅台" in profile.company_name
    assert profile.currency == "CNY"
    assert [
        {
            "timestamp": bar.timestamp.isoformat(),
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
        }
        for bar in raw_bars
    ] == MOUTAI_WEEKLY_BAR_SNAPSHOT
