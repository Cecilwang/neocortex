from datetime import date
import importlib

import pandas as pd
import pytest

from neocortex.connectors import BaoStockConnector
from neocortex.connectors.types import (
    AdjustmentFactorRecord,
    DailyPriceBarRecord,
    FundamentalSnapshotRecord,
    MacroPointRecord,
    SecurityListing,
    SecurityProfileSnapshot,
    TradingDateRecord,
)
from neocortex.models import Exchange, Market, SecurityId
from neocortex.models import FundamentalStatement, FundamentalValueOrigin
from neocortex.storage.market_store import MarketDataStore


class _Result:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.error_code = "0"
        self.error_msg = ""
        self._frame = frame

    def get_data(self) -> pd.DataFrame:
        return self._frame


class _StreamingResult:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.error_code = "0"
        self.error_msg = ""
        self.fields = list(frame.columns)
        self._rows = frame.astype(str).values.tolist()
        self._index = -1

    def next(self) -> bool:
        self._index += 1
        return self._index < len(self._rows)

    def get_row_data(self) -> list[str]:
        return self._rows[self._index]


class FakeBaoStockAPI:
    def __init__(self) -> None:
        self.login_calls = 0
        self.logout_calls = 0

    def login(self):
        self.login_calls += 1
        return _Result(pd.DataFrame())

    def logout(self) -> None:
        self.logout_calls += 1
        return None

    def query_stock_basic(self, code: str = "", code_name: str = ""):
        assert code_name == ""
        if code == "":
            return _Result(
                pd.DataFrame(
                    {
                        "code": ["sh.600519", "sz.000001", "sh.000300", "bj.430047"],
                        "code_name": ["贵州茅台", "平安银行", "沪深300", "诺思兰德"],
                        "type": ["1", "1", "2", "1"],
                        "status": ["1", "1", "1", "1"],
                    }
                )
            )
        assert code == "sh.600519"
        return _Result(pd.DataFrame({"code_name": ["贵州茅台"]}))

    def query_stock_industry(self, *, code: str):
        assert code == "sh.600519"
        return _Result(pd.DataFrame({"industry": ["白酒"]}))

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        *,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ):
        assert code == "sh.600519"
        assert fields == "date,open,high,low,close,volume,amount"
        assert start_date == "2026-01-01"
        assert end_date == "2026-01-31"
        assert frequency == "d"
        assert adjustflag == "3"
        return _Result(
            pd.DataFrame(
                {
                    "date": ["2026-01-05"],
                    "open": ["100.0"],
                    "high": ["110.0"],
                    "low": ["99.0"],
                    "close": ["108.0"],
                    "volume": ["1000"],
                    "amount": ["108000"],
                }
            )
        )

    def query_adjust_factor(
        self,
        code: str,
        *,
        start_date: str,
        end_date: str,
    ):
        assert code == "sh.600519"
        assert start_date == "2026-01-01"
        assert end_date == "2026-01-31"
        return _Result(
            pd.DataFrame(
                {
                    "dividOperateDate": ["2026-01-05"],
                    "foreAdjustFactor": ["1.1"],
                    "backAdjustFactor": ["0.9"],
                }
            )
        )

    def query_profit_data(self, *, code: str, year: int, quarter: int):
        assert code == "sh.600519"
        return _Result(
            self._fundamental_frame(
                year,
                quarter,
                {
                    "pubDate": "2026-03-10"
                    if (year, quarter) == (2025, 4)
                    else "2025-10-25",
                    "statDate": "2025-12-31"
                    if (year, quarter) == (2025, 4)
                    else "2025-09-30",
                    "roeAvg": "0.18" if (year, quarter) == (2025, 4) else "0.16",
                    "npMargin": "0.21" if (year, quarter) == (2025, 4) else "0.19",
                    "gpMargin": "0.32" if (year, quarter) == (2025, 4) else "0.30",
                    "netProfit": "80000000000"
                    if (year, quarter) == (2025, 4)
                    else "72000000000",
                    "epsTTM": "4.5" if (year, quarter) == (2025, 4) else "4.0",
                    "MBRevenue": "180000000000"
                    if (year, quarter) == (2025, 4)
                    else "165000000000",
                    "totalShare": "1256197800",
                    "liqaShare": "1256197800",
                },
            )
        )

    def query_operation_data(self, *, code: str, year: int, quarter: int):
        _ = code
        return _Result(
            self._fundamental_frame(
                year,
                quarter,
                {
                    "pubDate": "2026-03-10"
                    if (year, quarter) == (2025, 4)
                    else "2025-10-25",
                    "statDate": "2025-12-31"
                    if (year, quarter) == (2025, 4)
                    else "2025-09-30",
                    "NRTurnRatio": "9.0" if (year, quarter) == (2025, 4) else "8.5",
                    "NRTurnDays": "40" if (year, quarter) == (2025, 4) else "43",
                    "INVTurnRatio": "10.5" if (year, quarter) == (2025, 4) else "9.6",
                    "AssetTurnRatio": "0.8" if (year, quarter) == (2025, 4) else "0.7",
                    "INVTurnDays": "35" if (year, quarter) == (2025, 4) else "38",
                    "CATurnRatio": "1.7" if (year, quarter) == (2025, 4) else "1.6",
                },
            )
        )

    def query_growth_data(self, *, code: str, year: int, quarter: int):
        _ = code
        return _Result(
            self._fundamental_frame(
                year,
                quarter,
                {
                    "pubDate": "2026-03-10"
                    if (year, quarter) == (2025, 4)
                    else "2025-10-25",
                    "statDate": "2025-12-31"
                    if (year, quarter) == (2025, 4)
                    else "2025-09-30",
                    "YOYEquity": "0.11" if (year, quarter) == (2025, 4) else "0.09",
                    "YOYAsset": "0.08" if (year, quarter) == (2025, 4) else "0.07",
                    "YOYNI": "0.13" if (year, quarter) == (2025, 4) else "0.10",
                    "YOYOr": "0.10" if (year, quarter) == (2025, 4) else "0.08",
                    "YOYEPSBasic": "0.12" if (year, quarter) == (2025, 4) else "0.09",
                    "YOYPNI": "0.14" if (year, quarter) == (2025, 4) else "0.11",
                },
            )
        )

    def query_balance_data(self, *, code: str, year: int, quarter: int):
        _ = code
        return _Result(
            self._fundamental_frame(
                year,
                quarter,
                {
                    "pubDate": "2026-03-10"
                    if (year, quarter) == (2025, 4)
                    else "2025-10-25",
                    "statDate": "2025-12-31"
                    if (year, quarter) == (2025, 4)
                    else "2025-09-30",
                    "currentRatio": "2.1" if (year, quarter) == (2025, 4) else "2.0",
                    "quickRatio": "1.4" if (year, quarter) == (2025, 4) else "1.3",
                    "cashRatio": "1.0" if (year, quarter) == (2025, 4) else "0.9",
                    "YOYLiability": "0.06" if (year, quarter) == (2025, 4) else "0.05",
                    "liabilityToAsset": "0.45"
                    if (year, quarter) == (2025, 4)
                    else "0.48",
                    "assetToEquity": "1.82" if (year, quarter) == (2025, 4) else "1.92",
                },
            )
        )

    def query_cash_flow_data(self, *, code: str, year: int, quarter: int):
        _ = code
        return _Result(
            self._fundamental_frame(
                year,
                quarter,
                {
                    "pubDate": "2026-03-10"
                    if (year, quarter) == (2025, 4)
                    else "2025-10-25",
                    "statDate": "2025-12-31"
                    if (year, quarter) == (2025, 4)
                    else "2025-09-30",
                    "CAToAsset": "0.52" if (year, quarter) == (2025, 4) else "0.51",
                    "NCAToAsset": "0.48" if (year, quarter) == (2025, 4) else "0.49",
                    "tangibleAssetToAsset": "0.74"
                    if (year, quarter) == (2025, 4)
                    else "0.73",
                    "ebitToInterest": "45" if (year, quarter) == (2025, 4) else "42",
                    "CFOToOR": "0.29" if (year, quarter) == (2025, 4) else "0.27",
                    "CFOToNP": "1.21" if (year, quarter) == (2025, 4) else "1.16",
                    "CFOToGr": "0.24" if (year, quarter) == (2025, 4) else "0.22",
                },
            )
        )

    def query_dupont_data(self, *, code: str, year: int, quarter: int):
        _ = code
        return _Result(
            self._fundamental_frame(
                year,
                quarter,
                {
                    "pubDate": "2026-03-10"
                    if (year, quarter) == (2025, 4)
                    else "2025-10-25",
                    "statDate": "2025-12-31"
                    if (year, quarter) == (2025, 4)
                    else "2025-09-30",
                    "dupontROE": "0.18" if (year, quarter) == (2025, 4) else "0.16",
                    "dupontAssetSto498": "1.82"
                    if (year, quarter) == (2025, 4)
                    else "1.92",
                    "dupontAssetTurn": "0.8" if (year, quarter) == (2025, 4) else "0.7",
                    "dupontPnitoni": "0.21" if (year, quarter) == (2025, 4) else "0.19",
                    "dupontNitogr": "0.23" if (year, quarter) == (2025, 4) else "0.20",
                    "dupontTaxBurden": "0.88"
                    if (year, quarter) == (2025, 4)
                    else "0.87",
                    "dupontIntburden": "0.98"
                    if (year, quarter) == (2025, 4)
                    else "0.97",
                    "dupontEbittogr": "0.26"
                    if (year, quarter) == (2025, 4)
                    else "0.24",
                },
            )
        )

    @staticmethod
    def _fundamental_frame(
        year: int,
        quarter: int,
        row: dict[str, str],
    ) -> pd.DataFrame:
        if (year, quarter) not in {(2025, 4), (2025, 3)}:
            return pd.DataFrame(columns=row.keys())
        return pd.DataFrame([row])

    def query_money_supply_data_month(self, *, start_date: str, end_date: str):
        assert start_date == "2026-03-01"
        assert end_date == "2026-03-19"
        return _Result(
            pd.DataFrame(
                {
                    "statYear": ["2026-03"],
                    "moneySupplyM2": ["8.8"],
                }
            )
        )

    def query_required_reserve_ratio_data(self, *, start_date: str, end_date: str):
        assert start_date == "2026-03-01"
        assert end_date == "2026-03-19"
        return _Result(
            pd.DataFrame(
                {
                    "effectiveDate": ["2026-03-15"],
                    "largeRRR": ["7.0"],
                }
            )
        )

    def query_trade_dates(self, *, start_date: str, end_date: str):
        assert start_date == "2026-03-20"
        assert end_date == "2026-03-23"
        return _StreamingResult(
            pd.DataFrame(
                {
                    "calendar_date": ["2026-03-20", "2026-03-21", "2026-03-23"],
                    "is_trading_day": ["1", "0", "1"],
                }
            )
        )


class FakeBaoStockNoTtmEpsAPI(FakeBaoStockAPI):
    def query_profit_data(self, *, code: str, year: int, quarter: int):
        assert code == "sh.600519"
        return _Result(
            self._fundamental_frame(
                year,
                quarter,
                {
                    "pubDate": "2026-03-10"
                    if (year, quarter) == (2025, 4)
                    else "2025-10-25",
                    "statDate": "2025-12-31"
                    if (year, quarter) == (2025, 4)
                    else "2025-09-30",
                    "roeAvg": "0.18" if (year, quarter) == (2025, 4) else "0.16",
                    "npMargin": "0.21" if (year, quarter) == (2025, 4) else "0.19",
                    "epsBasic": "1.1" if (year, quarter) == (2025, 4) else "1.0",
                },
            )
        )


class MissingProfileBaoStockAPI(FakeBaoStockAPI):
    def query_stock_basic(self, code: str = "", code_name: str = ""):
        assert code_name == ""
        if code == "":
            return super().query_stock_basic(code=code, code_name=code_name)
        assert code == "sh.600519"
        return _Result(pd.DataFrame({"code_name": [""]}))

    def query_stock_industry(self, *, code: str):
        assert code == "sh.600519"
        return _Result(pd.DataFrame({"industry": [""]}))


class FakeBaoStockAdjustedAPI:
    def __init__(self) -> None:
        self.adjust_factor_calls: list[tuple[str, str]] = []
        self.kline_calls: list[tuple[str, str, str]] = []
        self.login_calls = 0
        self.logout_calls = 0

    def login(self):
        self.login_calls += 1
        return _Result(pd.DataFrame())

    def logout(self) -> None:
        self.logout_calls += 1
        return None

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        *,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ):
        assert code == "sh.600519"
        assert fields == "date,open,high,low,close,volume,amount"
        assert frequency == "d"
        assert adjustflag == "3"
        self.kline_calls.append((start_date, end_date, adjustflag))
        return _Result(
            pd.DataFrame(
                {
                    "date": ["2026-01-04", "2026-01-05", "2026-01-06"],
                    "open": ["10.0", "20.0", "30.0"],
                    "high": ["11.0", "21.0", "31.0"],
                    "low": ["9.0", "19.0", "29.0"],
                    "close": ["10.0", "20.0", "30.0"],
                    "volume": ["100", "200", "300"],
                    "amount": ["1000", "2000", "3000"],
                }
            )
        )

    def query_adjust_factor(
        self,
        code: str,
        *,
        start_date: str,
        end_date: str,
    ):
        assert code == "sh.600519"
        self.adjust_factor_calls.append((start_date, end_date))
        if start_date > "2026-01-05":
            return _Result(
                pd.DataFrame(
                    columns=["dividOperateDate", "foreAdjustFactor", "backAdjustFactor"]
                )
            )
        return _Result(
            pd.DataFrame(
                {
                    "dividOperateDate": ["2026-01-05"],
                    "foreAdjustFactor": ["1.1"],
                    "backAdjustFactor": ["0.9"],
                }
            )
        )


class FakeBaoStockDirectAdjustedAPI:
    def __init__(self) -> None:
        self.login_calls = 0
        self.logout_calls = 0
        self.kline_calls: list[tuple[str, str, str]] = []

    def login(self):
        self.login_calls += 1
        return _Result(pd.DataFrame())

    def logout(self) -> None:
        self.logout_calls += 1
        return None

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        *,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ):
        assert code == "sh.600519"
        assert fields == "date,open,high,low,close,volume,amount"
        assert start_date == "2026-01-01"
        assert end_date == "2026-01-31"
        assert frequency == "d"
        self.kline_calls.append((start_date, end_date, adjustflag))
        close = "120.0" if adjustflag == "2" else "95.0"
        return _Result(
            pd.DataFrame(
                {
                    "date": ["2026-01-05"],
                    "open": ["100.0"],
                    "high": ["121.0"],
                    "low": ["99.0"],
                    "close": [close],
                    "volume": ["1000"],
                    "amount": ["108000"],
                }
            )
        )


class FakeBaoStockFactorGapAPI(FakeBaoStockAdjustedAPI):
    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        *,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ):
        assert code == "sh.600519"
        assert fields == "date,open,high,low,close,volume,amount"
        assert frequency == "d"
        assert adjustflag == "3"
        self.kline_calls.append((start_date, end_date, adjustflag))
        return _Result(
            pd.DataFrame(
                {
                    "date": ["2026-01-31"],
                    "open": ["10.0"],
                    "high": ["10.0"],
                    "low": ["10.0"],
                    "close": ["10.0"],
                    "volume": ["100"],
                    "amount": ["1000"],
                }
            )
        )

    def query_adjust_factor(
        self,
        code: str,
        *,
        start_date: str,
        end_date: str,
    ):
        assert code == "sh.600519"
        self.adjust_factor_calls.append((start_date, end_date))
        if start_date == "2026-01-06" and end_date == "2026-01-31":
            return _Result(
                pd.DataFrame(
                    {
                        "dividOperateDate": ["2026-01-20"],
                        "foreAdjustFactor": ["1.2"],
                        "backAdjustFactor": ["0.8"],
                    }
                )
            )
        return _Result(
            pd.DataFrame(
                columns=["dividOperateDate", "foreAdjustFactor", "backAdjustFactor"]
            )
        )


def test_baostock_connector_lists_cn_securities() -> None:
    api = FakeBaoStockAPI()
    connector = BaoStockConnector(api=api)

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
    assert api.login_calls == 1
    assert api.logout_calls == 1


def test_baostock_connector_fetches_profile_snapshot() -> None:
    connector = BaoStockConnector(api=FakeBaoStockAPI())
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)

    snapshot = connector.get_security_profile_snapshot(security_id)

    assert snapshot == SecurityProfileSnapshot(
        source="baostock",
        security_id=security_id,
        provider_company_name="贵州茅台",
        sector="白酒",
        industry="白酒",
        country="CN",
        currency="CNY",
        primary_listing=True,
    )


def test_baostock_connector_fetches_daily_bars_and_adjustment_factors() -> None:
    connector = BaoStockConnector(api=FakeBaoStockAPI())
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)

    bars = connector.get_daily_price_bars(
        security_id,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
    )
    factors = connector.get_adjustment_factors(
        security_id,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
    )

    assert bars == (
        DailyPriceBarRecord(
            source="baostock",
            security_id=security_id,
            trade_date="2026-01-05",
            open=100.0,
            high=110.0,
            low=99.0,
            close=108.0,
            volume=1000.0,
            amount=108000.0,
        ),
    )
    assert factors == (
        AdjustmentFactorRecord(
            source="baostock",
            security_id=security_id,
            trade_date="2026-01-05",
            adjustment_type="qfq",
            factor=1.1,
        ),
        AdjustmentFactorRecord(
            source="baostock",
            security_id=security_id,
            trade_date="2026-01-05",
            adjustment_type="hfq",
            factor=0.9,
        ),
    )


def test_baostock_connector_fetches_cn_trading_dates() -> None:
    connector = BaoStockConnector(api=FakeBaoStockAPI())

    records = connector.get_trading_dates(
        market=Market.CN,
        start_date=date(2026, 3, 20),
        end_date=date(2026, 3, 23),
    )

    assert records == (
        TradingDateRecord(
            source="baostock",
            market=Market.CN,
            calendar="XSHG",
            trade_date="2026-03-20",
            is_trading_day=True,
        ),
        TradingDateRecord(
            source="baostock",
            market=Market.CN,
            calendar="XSHG",
            trade_date="2026-03-21",
            is_trading_day=False,
        ),
        TradingDateRecord(
            source="baostock",
            market=Market.CN,
            calendar="XSHG",
            trade_date="2026-03-23",
            is_trading_day=True,
        ),
    )


def test_baostock_connector_fetches_direct_adjusted_daily_bars() -> None:
    connector = BaoStockConnector(api=FakeBaoStockDirectAdjustedAPI())
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)

    qfq_bars = connector.get_adjusted_daily_price_bars(
        security_id,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        adjustment_type="qfq",
    )
    hfq_bars = connector.get_adjusted_daily_price_bars(
        security_id,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        adjustment_type="hfq",
    )

    assert qfq_bars[0].close == 120.0
    assert hfq_bars[0].close == 95.0


def test_baostock_connector_fetches_adjusted_daily_price_bars_with_official_factor_matching(
    tmp_path,
) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    api = FakeBaoStockAdjustedAPI()
    connector = BaoStockConnector(api=api)
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    raw_records = (
        DailyPriceBarRecord(
            source="baostock",
            security_id=security_id,
            trade_date="2026-01-04",
            open=10.0,
            high=11.0,
            low=9.0,
            close=10.0,
            volume=100.0,
            amount=1000.0,
        ),
        DailyPriceBarRecord(
            source="baostock",
            security_id=security_id,
            trade_date="2026-01-05",
            open=20.0,
            high=21.0,
            low=19.0,
            close=20.0,
            volume=200.0,
            amount=2000.0,
        ),
        DailyPriceBarRecord(
            source="baostock",
            security_id=security_id,
            trade_date="2026-01-06",
            open=30.0,
            high=31.0,
            low=29.0,
            close=30.0,
            volume=300.0,
            amount=3000.0,
        ),
    )
    qfq_records = connector.apply_adjustment(
        security_id,
        adjustment_type="qfq",
        raw_daily_records=raw_records,
    )
    assert api.adjust_factor_calls == [("2016-01-01", "2026-01-06")]
    assert [record.close for record in qfq_records] == [10.0, 22.0, 33.0]
    assert qfq_records[2].close != 30.0 * 1.0
    assert store.dump_table("daily_price_bars") == []


def test_baostock_connector_apply_adjustment_raises_without_raw_records() -> None:
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    connector = BaoStockConnector(api=FakeBaoStockAdjustedAPI())

    with pytest.raises(KeyError):
        connector.apply_adjustment(
            security_id,
            adjustment_type="qfq",
            raw_daily_records=(),
        )


def test_baostock_connector_apply_adjustment_returns_raw_when_no_factor_records() -> (
    None
):
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    api = FakeBaoStockAdjustedAPI()
    connector = BaoStockConnector(api=api)
    raw_records = (
        DailyPriceBarRecord(
            source="baostock",
            security_id=security_id,
            trade_date="2026-01-04",
            open=10.0,
            high=11.0,
            low=9.0,
            close=10.0,
            volume=100.0,
            amount=1000.0,
        ),
    )

    adjusted_records = connector.apply_adjustment(
        security_id,
        adjustment_type="qfq",
        raw_daily_records=raw_records,
    )

    assert adjusted_records == raw_records


def test_baostock_connector_fetches_fundamentals_and_macro_points() -> None:
    api = FakeBaoStockAPI()
    connector = BaoStockConnector(api=api)
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)

    snapshots = connector.get_fundamental_snapshots(
        security_id,
        as_of_date=date(2026, 3, 19),
    )
    macro_points = connector.get_macro_points(
        market=Market.CN,
        as_of_date=date(2026, 3, 19),
    )

    latest_snapshots = [
        snapshot
        for snapshot in snapshots
        if snapshot.report_date == "2025-12-31" and snapshot.ann_date == "2026-03-10"
    ]
    previous_snapshots = [
        snapshot
        for snapshot in snapshots
        if snapshot.report_date == "2025-09-30" and snapshot.ann_date == "2025-10-25"
    ]

    assert len(latest_snapshots) >= 20
    assert previous_snapshots
    assert {snapshot.fetch_at for snapshot in snapshots} == {snapshots[0].fetch_at}
    assert (
        FundamentalSnapshotRecord(
            source="baostock",
            security_id=security_id,
            report_date="2025-12-31",
            ann_date="2026-03-10",
            fetch_at=snapshots[0].fetch_at,
            statement=FundamentalStatement.NET_MARGIN,
            value=0.21,
            value_origin=FundamentalValueOrigin.FETCHED,
        )
        in snapshots
    )
    assert (
        FundamentalSnapshotRecord(
            source="baostock",
            security_id=security_id,
            report_date="2025-12-31",
            ann_date="2026-03-10",
            fetch_at=snapshots[0].fetch_at,
            statement=FundamentalStatement.EQUITY_RATIO,
            value=0.55,
            value_origin=FundamentalValueOrigin.DERIVED,
        )
        in snapshots
    )
    assert (
        FundamentalSnapshotRecord(
            source="baostock",
            security_id=security_id,
            report_date="2025-12-31",
            ann_date="2026-03-10",
            fetch_at=snapshots[0].fetch_at,
            statement=FundamentalStatement.GP_MARGIN,
            value=0.32,
            value_origin=FundamentalValueOrigin.FETCHED,
        )
        in snapshots
    )
    assert (
        FundamentalSnapshotRecord(
            source="baostock",
            security_id=security_id,
            report_date="2025-12-31",
            ann_date="2026-03-10",
            fetch_at=snapshots[0].fetch_at,
            statement=FundamentalStatement.CURRENT_RATIO,
            value=2.1,
            value_origin=FundamentalValueOrigin.FETCHED,
        )
        in snapshots
    )
    assert (
        FundamentalSnapshotRecord(
            source="baostock",
            security_id=security_id,
            report_date="2025-12-31",
            ann_date="2026-03-10",
            fetch_at=snapshots[0].fetch_at,
            statement=FundamentalStatement.CFO_TO_OPERATING_REVENUE,
            value=0.29,
            value_origin=FundamentalValueOrigin.FETCHED,
        )
        in snapshots
    )
    assert (
        FundamentalSnapshotRecord(
            source="baostock",
            security_id=security_id,
            report_date="2025-12-31",
            ann_date="2026-03-10",
            fetch_at=snapshots[0].fetch_at,
            statement=FundamentalStatement.DUPONT_ROE,
            value=0.18,
            value_origin=FundamentalValueOrigin.FETCHED,
        )
        in snapshots
    )
    assert (
        FundamentalSnapshotRecord(
            source="baostock",
            security_id=security_id,
            report_date="2025-09-30",
            ann_date="2025-10-25",
            fetch_at=snapshots[0].fetch_at,
            statement=FundamentalStatement.NET_MARGIN,
            value=0.19,
            value_origin=FundamentalValueOrigin.FETCHED,
        )
        in snapshots
    )
    assert macro_points == (
        MacroPointRecord(
            source="baostock",
            market=Market.CN,
            series_key="cn_money_supply.moneySupplyM2",
            observed_at="2026-03",
            series_name="moneySupplyM2",
            value=8.8,
            unit="value",
            frequency="monthly",
            category="macro",
            change_pct=None,
            yoy_change_pct=None,
        ),
        MacroPointRecord(
            source="baostock",
            market=Market.CN,
            series_key="cn_rrr.largeRRR",
            observed_at="2026-03-15",
            series_name="largeRRR",
            value=7.0,
            unit="value",
            frequency="monthly",
            category="rates",
            change_pct=None,
            yoy_change_pct=None,
        ),
    )
    assert api.login_calls == 2
    assert api.logout_calls == 2


def test_baostock_connector_quant_eps_requires_ttm_field() -> None:
    api = FakeBaoStockNoTtmEpsAPI()
    connector = BaoStockConnector(api=api)
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)

    snapshots = connector.get_fundamental_snapshots(
        security_id,
        as_of_date=date(2026, 3, 19),
    )

    assert not any(
        snapshot.statement is FundamentalStatement.EPS_TTM for snapshot in snapshots
    )


def test_baostock_connector_rejects_disclosure_sections() -> None:
    connector = BaoStockConnector(api=FakeBaoStockAPI())
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)

    with pytest.raises(
        NotImplementedError,
        match="does not expose qualitative disclosure sections",
    ):
        connector.get_disclosure_sections(
            security_id,
            as_of_date=date(2026, 3, 19),
        )


def test_baostock_connector_rejects_incomplete_profile_data() -> None:
    connector = BaoStockConnector(api=MissingProfileBaoStockAPI())
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)

    with pytest.raises(ValueError, match="profile is missing required fields"):
        connector.get_security_profile_snapshot(security_id)


def test_baostock_connector_reports_missing_dependency(monkeypatch) -> None:
    connector = BaoStockConnector()
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)

    def _raise_missing(name: str):
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(importlib, "import_module", _raise_missing)

    with pytest.raises(RuntimeError, match="optional 'baostock' dependency"):
        connector.get_security_profile_snapshot(security_id)
