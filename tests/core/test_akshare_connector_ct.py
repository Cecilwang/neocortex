from datetime import date

import pandas as pd
import pytest

from neocortex.connectors import AkShareConnector
from neocortex.connectors.types import SecurityListing
from neocortex.markets import get_market_context
from neocortex.models import Exchange, Market, SecurityId


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

    def stock_individual_basic_info_xq(
        self, symbol: str, timeout: float | None
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "item": [
                    "org_name_cn",
                    "org_short_name_cn",
                    "affiliate_industry",
                ],
                "value": [
                    "贵州茅台酒股份有限公司",
                    "贵州茅台",
                    {"ind_code": "BK0088", "ind_name": "白酒"},
                ],
            }
        )


class RecordingAkShareAPI(FakeAkShareAPI):
    def __init__(self) -> None:
        self.last_adjust: str | None = None

    def stock_zh_a_hist(
        self,
        symbol: str,
        period: str,
        start_date: str,
        end_date: str,
        adjust: str,
        timeout: float | None,
    ) -> pd.DataFrame:
        self.last_adjust = adjust
        assert symbol == "600519"
        assert period == "daily"
        assert start_date == "20260314"
        assert end_date == "20260315"
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


class EmptyBarsAkShareAPI(FakeAkShareAPI):
    def stock_zh_a_hist(
        self,
        symbol: str,
        period: str,
        start_date: str,
        end_date: str,
        adjust: str,
        timeout: float | None,
    ) -> pd.DataFrame:
        return pd.DataFrame()


class MissingProfileFieldAkShareAPI(FakeAkShareAPI):
    def __init__(self, missing_field: str) -> None:
        self.missing_field = missing_field

    def stock_individual_info_em(
        self, symbol: str, timeout: float | None
    ) -> pd.DataFrame:
        assert symbol == "600519"
        return pd.DataFrame(
            {
                "item": [
                    field
                    for field in ["股票代码", "股票简称", "行业", "上市时间"]
                    if field != self.missing_field
                ],
                "value": [
                    value
                    for field, value in [
                        ("股票代码", "600519"),
                        ("股票简称", "贵州茅台"),
                        ("行业", "酿酒行业"),
                        ("上市时间", "20010827"),
                    ]
                    if field != self.missing_field
                ],
            }
        )


class FailingEastmoneyProfileAkShareAPI(FakeAkShareAPI):
    def __init__(self) -> None:
        self.last_xueqiu_symbol: str | None = None

    def stock_individual_info_em(
        self, symbol: str, timeout: float | None
    ) -> pd.DataFrame:
        raise RuntimeError("eastmoney unavailable")

    def stock_individual_basic_info_xq(
        self, symbol: str, timeout: float | None
    ) -> pd.DataFrame:
        self.last_xueqiu_symbol = symbol
        assert timeout == 3.0
        return pd.DataFrame(
            {
                "item": [
                    "org_name_cn",
                    "org_short_name_cn",
                    "affiliate_industry",
                ],
                "value": [
                    "贵州茅台酒股份有限公司",
                    "贵州茅台",
                    {"ind_code": "BK0088", "ind_name": "白酒"},
                ],
            }
        )


@pytest.mark.parametrize(
    ("security_id", "expected_symbol"),
    [
        (
            SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG),
            "SH600519",
        ),
        (
            SecurityId(symbol="000001", market=Market.CN, exchange=Exchange.XSHE),
            "SZ000001",
        ),
    ],
)
def test_akshare_connector_falls_back_to_xueqiu_company_profile(
    security_id: SecurityId,
    expected_symbol: str,
) -> None:
    api = FailingEastmoneyProfileAkShareAPI()
    connector = AkShareConnector(timeout=3.0, api=api)

    profile = connector.get_security_profile_snapshot(security_id)

    assert api.last_xueqiu_symbol == expected_symbol
    assert profile.provider_company_name == "贵州茅台"
    assert profile.industry == "白酒"
    assert profile.sector == "白酒"
    assert profile.currency == "CNY"


class UniverseAkShareAPI(FakeAkShareAPI):
    def stock_info_a_code_name(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "code": ["600519", "000001"],
                "name": ["贵州茅台", "平安银行"],
            }
        )


def test_akshare_connector_fetches_cn_security_universe() -> None:
    connector = AkShareConnector(api=UniverseAkShareAPI())

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


def test_akshare_connector_normalizes_cn_profile_and_adjusted_daily_bars() -> None:
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    connector = AkShareConnector(
        timeout=3.0,
        api=FakeAkShareAPI(),
    )

    profile = connector.get_security_profile_snapshot(security_id)
    records = connector.get_adjusted_daily_price_bars(
        security_id,
        start_date=date(2026, 3, 14),
        end_date=date(2026, 3, 15),
        adjustment_type="qfq",
    )
    market_context = get_market_context(Market.CN)

    assert profile.provider_company_name == "贵州茅台"
    assert profile.industry == "酿酒行业"
    assert profile.sector == "酿酒行业"
    assert profile.currency == "CNY"
    assert [record.trade_date for record in records] == ["2026-03-14", "2026-03-15"]
    assert [record.close for record in records] == [1515.0, 1528.0]
    assert [record.volume for record in records] == [12_000_000.0, 11_000_000.0]
    assert market_context.timezone == "Asia/Shanghai"
    assert market_context.benchmark_symbol == "000300.SH"


def test_akshare_connector_rejects_non_cn_security() -> None:
    connector = AkShareConnector(api=FakeAkShareAPI())
    security_id = SecurityId(symbol="AAPL", market=Market.US, exchange=Exchange.XNAS)

    with pytest.raises(ValueError, match="supports only CN securities"):
        connector.get_security_profile_snapshot(security_id)


def test_akshare_connector_rejects_unsupported_cn_exchange() -> None:
    connector = AkShareConnector(api=FakeAkShareAPI())
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XBJS)

    with pytest.raises(ValueError, match="requires an XSHG or XSHE listing exchange"):
        connector.get_security_profile_snapshot(security_id)


def test_akshare_connector_uses_unadjusted_series_when_adjust_is_none() -> None:
    api = RecordingAkShareAPI()
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    connector = AkShareConnector(timeout=3.0, api=api)

    records = connector.get_daily_price_bars(
        security_id,
        start_date=date(2026, 3, 14),
        end_date=date(2026, 3, 15),
    )

    assert api.last_adjust == ""
    assert [record.close for record in records] == [1515.0, 1528.0]


def test_akshare_connector_fetches_adjusted_daily_price_bars() -> None:
    api = RecordingAkShareAPI()
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    connector = AkShareConnector(timeout=3.0, api=api)

    records = connector.get_adjusted_daily_price_bars(
        security_id,
        start_date=date(2026, 3, 14),
        end_date=date(2026, 3, 15),
        adjustment_type="hfq",
    )

    assert api.last_adjust == "hfq"
    assert records[0].trade_date == "2026-03-14"
    assert records[0].close == 1515.0
    assert records[1].volume == 11_000_000.0


def test_akshare_connector_raises_when_provider_has_no_bars() -> None:
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    connector = AkShareConnector(api=EmptyBarsAkShareAPI())

    with pytest.raises(KeyError):
        connector.get_daily_price_bars(
            security_id,
            start_date=date(2026, 3, 14),
            end_date=date(2026, 3, 15),
        )


@pytest.mark.parametrize("missing_field", ["股票简称", "行业"])
def test_akshare_connector_falls_back_to_xueqiu_when_em_profile_is_incomplete(
    missing_field: str,
) -> None:
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    connector = AkShareConnector(
        timeout=3.0,
        api=MissingProfileFieldAkShareAPI(missing_field),
    )

    snapshot = connector.get_security_profile_snapshot(security_id)

    assert snapshot.provider_company_name == "贵州茅台"
    assert snapshot.industry == "白酒"
    assert snapshot.sector == "白酒"
