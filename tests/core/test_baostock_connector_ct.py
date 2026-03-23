from datetime import date

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
        assert year == 2026
        assert quarter == 1
        return _Result(pd.DataFrame({"roeAvg": ["0.18"]}))

    def query_operation_data(self, *, code: str, year: int, quarter: int):
        return _Result(pd.DataFrame({"NRTurnRatio": ["1.2"]}))

    def query_growth_data(self, *, code: str, year: int, quarter: int):
        return _Result(pd.DataFrame({"YOYNI": ["0.15"]}))

    def query_balance_data(self, *, code: str, year: int, quarter: int):
        return _Result(pd.DataFrame({"currentRatio": ["2.0"]}))

    def query_cash_flow_data(self, *, code: str, year: int, quarter: int):
        return _Result(pd.DataFrame({"CAToAsset": ["0.3"]}))

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
    connector = BaoStockConnector(api=api, store=store)
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

    assert snapshots == (
        FundamentalSnapshotRecord(
            source="baostock",
            security_id=security_id,
            period_end_date="2026-03-31",
            canonical_period_label="2026Q1",
            statement_kind="profitability",
            provider_period_label="2026Q1",
            report_date=None,
            currency="CNY",
            raw_items_json='[{"roeAvg":"0.18"}]',
            derived_metrics_json="{}",
        ),
        FundamentalSnapshotRecord(
            source="baostock",
            security_id=security_id,
            period_end_date="2026-03-31",
            canonical_period_label="2026Q1",
            statement_kind="operating_efficiency",
            provider_period_label="2026Q1",
            report_date=None,
            currency="CNY",
            raw_items_json='[{"NRTurnRatio":"1.2"}]',
            derived_metrics_json="{}",
        ),
        FundamentalSnapshotRecord(
            source="baostock",
            security_id=security_id,
            period_end_date="2026-03-31",
            canonical_period_label="2026Q1",
            statement_kind="growth",
            provider_period_label="2026Q1",
            report_date=None,
            currency="CNY",
            raw_items_json='[{"YOYNI":"0.15"}]',
            derived_metrics_json="{}",
        ),
        FundamentalSnapshotRecord(
            source="baostock",
            security_id=security_id,
            period_end_date="2026-03-31",
            canonical_period_label="2026Q1",
            statement_kind="balance_sheet",
            provider_period_label="2026Q1",
            report_date=None,
            currency="CNY",
            raw_items_json='[{"currentRatio":"2.0"}]',
            derived_metrics_json="{}",
        ),
        FundamentalSnapshotRecord(
            source="baostock",
            security_id=security_id,
            period_end_date="2026-03-31",
            canonical_period_label="2026Q1",
            statement_kind="cash_flow",
            provider_period_label="2026Q1",
            report_date=None,
            currency="CNY",
            raw_items_json='[{"CAToAsset":"0.3"}]',
            derived_metrics_json="{}",
        ),
    )
    assert macro_points == (
        MacroPointRecord(
            source="baostock",
            market="CN",
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
            market="CN",
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
