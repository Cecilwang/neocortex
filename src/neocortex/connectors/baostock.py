"""baostock-backed connector for market-data ingestion."""

from __future__ import annotations

import importlib
import json
from contextlib import contextmanager
from datetime import date
from functools import wraps
import logging
from typing import Any

import pandas as pd
from neocortex.connectors.base import BaseSourceConnector
from neocortex.connectors.common import (
    infer_cn_exchange,
    log_daily_records_access,
    optional_float,
)
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
from neocortex.utils.retry import connector_retry

logger = logging.getLogger(__name__)
_CN_TRADING_CALENDAR = "XSHG"


def _security_id_from_baostock(code: str) -> SecurityId:
    _exchange_code, symbol = code.split(".")
    return SecurityId(
        symbol=symbol,
        market=Market.CN,
        exchange=infer_cn_exchange(symbol),
    )


def _to_baostock_code(security_id: SecurityId) -> str:
    if security_id.exchange is Exchange.XSHG:
        return f"sh.{security_id.symbol}"
    if security_id.exchange is Exchange.XSHE:
        return f"sz.{security_id.symbol}"
    raise NotImplementedError("BaoStockConnector currently supports only XSHG/XSHE.")


@contextmanager
def _baostock_session(client: "_BaoStockApiClient"):
    logger.debug("Opening BaoStock session.")
    api = client._api()
    result = api.login()
    if getattr(result, "error_code", "0") != "0":
        raise RuntimeError(f"BaoStock login failed: {result.error_msg}")
    try:
        yield api
    finally:
        logger.debug("Closing BaoStock session.")
        api.logout()


def _with_baostock_session(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        with _baostock_session(self) as api:
            return func(self, *args, api=api, **kwargs)

    return wrapper


class _BaoStockApiClient:
    source_name = "baostock"

    def __init__(self, api: Any | None = None) -> None:
        self.api = api

    def _api(self) -> Any:
        if self.api is not None:
            return self.api
        return importlib.import_module("baostock")

    def _result_to_frame(self, result: Any) -> pd.DataFrame:
        if getattr(result, "error_code", "0") != "0":
            raise RuntimeError(f"BaoStock query failed: {result.error_msg}")
        if all(
            hasattr(result, attribute)
            for attribute in ("fields", "next", "get_row_data")
        ):
            rows: list[list[str]] = []
            while result.next():
                rows.append(result.get_row_data())
            return pd.DataFrame(rows, columns=list(result.fields))
        return result.get_data()

    @connector_retry(source_name=source_name)
    @_with_baostock_session
    def list_securities(
        self,
        *,
        market: Market,
        api: Any,
    ) -> tuple[SecurityListing, ...]:
        if market is not Market.CN:
            raise NotImplementedError("BaoStockConnector currently supports only CN.")
        logger.info(f"Fetching BaoStock security universe: market={market.value}")
        frame = self._result_to_frame(api.query_stock_basic())
        listings: list[SecurityListing] = []
        for _, row in frame.iterrows():
            code = str(row["code"]).strip().lower()
            if not code.startswith(("sh.", "sz.")):
                continue
            if "type" in row and str(row["type"]).strip() not in {"1", ""}:
                continue
            if "status" in row and str(row["status"]).strip() not in {"1", ""}:
                continue
            listings.append(
                SecurityListing(
                    security_id=_security_id_from_baostock(code),
                    name=str(row.get("code_name", "")).strip(),
                )
            )
        return tuple(listings)

    @connector_retry(source_name=source_name)
    @_with_baostock_session
    def get_security_profile_snapshot(
        self,
        security_id: SecurityId,
        *,
        api: Any,
    ) -> SecurityProfileSnapshot:
        code = _to_baostock_code(security_id)
        logger.info(f"Fetching BaoStock profile: security={security_id.ticker}")
        basic = self._result_to_frame(api.query_stock_basic(code=code))
        industry = self._result_to_frame(api.query_stock_industry(code=code))
        company_name = None
        if not basic.empty:
            company_name = str(basic.iloc[0].get("code_name", "")).strip() or None
        industry_name = None
        if not industry.empty:
            industry_name = str(industry.iloc[0].get("industry", "")).strip() or None
        assert company_name and industry_name, (
            "BaoStock profile is missing required fields."
        )
        return SecurityProfileSnapshot(
            source="baostock",
            security_id=security_id,
            provider_company_name=company_name,
            sector=industry_name,
            industry=industry_name,
            country="CN",
            currency="CNY",
            primary_listing=True,
        )

    @connector_retry(source_name=source_name)
    @_with_baostock_session
    def get_daily_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        api: Any,
    ) -> tuple[DailyPriceBarRecord, ...]:
        logger.info(
            f"Fetching BaoStock raw daily bars: security={security_id.ticker} "
            f"start={start_date} end={end_date}"
        )
        return self._get_daily_price_bars(
            security_id,
            start_date=start_date,
            end_date=end_date,
            adjustflag="3",
            api=api,
        )

    @connector_retry(source_name=source_name)
    @_with_baostock_session
    def get_adjusted_daily_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        adjustment_type: str,
        api: Any,
    ) -> tuple[DailyPriceBarRecord, ...]:
        if adjustment_type not in {"qfq", "hfq"}:
            raise ValueError(
                "BaoStockConnector supports only qfq or hfq adjusted bars."
            )
        logger.info(
            f"Fetching BaoStock adjusted daily bars: security={security_id.ticker} "
            f"start={start_date} end={end_date} adjust={adjustment_type}"
        )
        return self._get_daily_price_bars(
            security_id,
            start_date=start_date,
            end_date=end_date,
            adjustflag="2" if adjustment_type == "qfq" else "1",
            api=api,
        )

    def _get_daily_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        adjustflag: str,
        api: Any,
    ) -> tuple[DailyPriceBarRecord, ...]:
        frame = self._result_to_frame(
            api.query_history_k_data_plus(
                _to_baostock_code(security_id),
                "date,open,high,low,close,volume,amount",
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                frequency="d",
                adjustflag=adjustflag,
            )
        )
        records: list[DailyPriceBarRecord] = []
        for _, row in frame.iterrows():
            records.append(
                DailyPriceBarRecord(
                    source="baostock",
                    security_id=security_id,
                    trade_date=str(row["date"]),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=optional_float(row.get("volume")),
                    amount=optional_float(row.get("amount")),
                )
            )
        fetched_records = tuple(records)
        log_daily_records_access(
            source_name=self.source_name,
            security_id=security_id,
            requested_start_date=start_date,
            requested_end_date=end_date,
            records=fetched_records,
            adjust_label={"3": "raw", "2": "qfq", "1": "hfq"}[adjustflag],
        )
        return fetched_records

    @connector_retry(source_name=source_name)
    @_with_baostock_session
    def get_adjustment_factors(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        api: Any,
    ) -> tuple[AdjustmentFactorRecord, ...]:
        logger.info(
            f"Fetching BaoStock adjustment factors: security={security_id.ticker} "
            f"start={start_date} end={end_date}"
        )
        frame = self._result_to_frame(
            api.query_adjust_factor(
                _to_baostock_code(security_id),
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
            )
        )
        records: list[AdjustmentFactorRecord] = []
        for _, row in frame.iterrows():
            trade_date = str(
                row.get("dividOperateDate") or row.get("adjustDate") or row.get("date")
            )
            assert trade_date, (
                "BaoStock adjustment factor record is missing trade date."
            )
            qfq = row.get("foreAdjustFactor")
            hfq = row.get("backAdjustFactor")
            if qfq is not None and qfq != "":
                records.append(
                    AdjustmentFactorRecord(
                        source="baostock",
                        security_id=security_id,
                        trade_date=trade_date,
                        adjustment_type="qfq",
                        factor=float(qfq),
                    )
                )
            if hfq is not None and hfq != "":
                records.append(
                    AdjustmentFactorRecord(
                        source="baostock",
                        security_id=security_id,
                        trade_date=trade_date,
                        adjustment_type="hfq",
                        factor=float(hfq),
                    )
                )
        return tuple(records)

    @connector_retry(source_name=source_name)
    @_with_baostock_session
    def get_fundamental_snapshots(
        self,
        security_id: SecurityId,
        *,
        as_of_date: date,
        api: Any,
    ) -> tuple[FundamentalSnapshotRecord, ...]:
        code = _to_baostock_code(security_id)
        logger.info(
            f"Fetching BaoStock fundamentals: security={security_id.ticker} "
            f"as_of_date={as_of_date}"
        )
        year = as_of_date.year
        quarter = (as_of_date.month + 2) // 3
        fetchers = {
            "profitability": api.query_profit_data,
            "operating_efficiency": api.query_operation_data,
            "growth": api.query_growth_data,
            "balance_sheet": api.query_balance_data,
            "cash_flow": api.query_cash_flow_data,
        }
        frames = {
            kind: self._result_to_frame(fetcher(code=code, year=year, quarter=quarter))
            for kind, fetcher in fetchers.items()
        }
        period_end_date = _quarter_period_end(year, quarter)
        label = f"{year}Q{quarter}"
        records: list[FundamentalSnapshotRecord] = []
        for kind, frame in frames.items():
            raw_json = frame.to_json(orient="records", force_ascii=False)
            records.append(
                FundamentalSnapshotRecord(
                    source="baostock",
                    security_id=security_id,
                    period_end_date=period_end_date,
                    canonical_period_label=label,
                    statement_kind=kind,
                    provider_period_label=label,
                    raw_items_json=raw_json if raw_json != "[]" else "{}",
                    derived_metrics_json=json.dumps({}, ensure_ascii=False),
                    currency="CNY",
                )
            )
        return tuple(records)

    @connector_retry(source_name=source_name)
    @_with_baostock_session
    def get_macro_points(
        self,
        *,
        market: Market,
        as_of_date: date,
        api: Any,
    ) -> tuple[MacroPointRecord, ...]:
        if market is not Market.CN:
            raise NotImplementedError("BaoStockConnector currently supports only CN.")
        logger.info(
            f"Fetching BaoStock macro points: market={market.value} as_of_date={as_of_date}"
        )
        money_supply = self._result_to_frame(
            api.query_money_supply_data_month(
                start_date=(as_of_date.replace(day=1)).isoformat(),
                end_date=as_of_date.isoformat(),
            )
        )
        reserve_ratio = self._result_to_frame(
            api.query_required_reserve_ratio_data(
                start_date=(as_of_date.replace(day=1)).isoformat(),
                end_date=as_of_date.isoformat(),
            )
        )
        records: list[MacroPointRecord] = []
        for frame, series_prefix, category in (
            (money_supply, "cn_money_supply", "macro"),
            (reserve_ratio, "cn_rrr", "rates"),
        ):
            if frame.empty:
                continue
            last_row = frame.iloc[-1]
            observed_at = str(last_row.iloc[0])
            for column in frame.columns[1:]:
                value = optional_float(last_row.get(column))
                if value is None:
                    continue
                records.append(
                    MacroPointRecord(
                        source="baostock",
                        market=market.value,
                        series_key=f"{series_prefix}.{column}",
                        observed_at=observed_at,
                        series_name=column,
                        value=value,
                        unit="value",
                        frequency="monthly",
                        category=category,
                    )
                )
        return tuple(records)

    @connector_retry(source_name=source_name)
    @_with_baostock_session
    def get_trading_dates(
        self,
        *,
        market: Market,
        start_date: date,
        end_date: date,
        api: Any,
    ) -> tuple[TradingDateRecord, ...]:
        if market is not Market.CN:
            raise NotImplementedError("BaoStockConnector currently supports only CN.")
        logger.info(
            f"Fetching BaoStock trading dates: market={market.value} "
            f"start={start_date} end={end_date}"
        )
        frame = self._result_to_frame(
            api.query_trade_dates(
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
            )
        )
        records: list[TradingDateRecord] = []
        for _, row in frame.iterrows():
            records.append(
                TradingDateRecord(
                    source=self.source_name,
                    market=market,
                    calendar=_CN_TRADING_CALENDAR,
                    trade_date=str(row["calendar_date"]),
                    is_trading_day=str(row["is_trading_day"]).strip() == "1",
                )
            )
        return tuple(records)


class BaoStockConnector(BaseSourceConnector):
    """Minimal CN ingestion connector backed by baostock."""

    source_name = "baostock"
    supported_markets = frozenset({Market.CN})
    supports_adjustment_factors = True
    supports_adjusted_daily_bars = True

    def __init__(
        self,
        api: Any | None = None,
        *,
        store=None,
    ) -> None:
        super().__init__(store=store)
        self._client = _BaoStockApiClient(api)

    def list_securities(self, *, market: Market) -> tuple[SecurityListing, ...]:
        return self._client.list_securities(market=market)

    def get_security_profile_snapshot(
        self,
        security_id: SecurityId,
    ) -> SecurityProfileSnapshot:
        return self._client.get_security_profile_snapshot(security_id)

    def get_daily_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
    ) -> tuple[DailyPriceBarRecord, ...]:
        return self._client.get_daily_price_bars(
            security_id,
            start_date=start_date,
            end_date=end_date,
        )

    def get_adjustment_factors(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
    ) -> tuple[AdjustmentFactorRecord, ...]:
        return self._client.get_adjustment_factors(
            security_id,
            start_date=start_date,
            end_date=end_date,
        )

    def get_adjusted_daily_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        adjustment_type: str,
    ) -> tuple[DailyPriceBarRecord, ...]:
        return self._client.get_adjusted_daily_price_bars(
            security_id,
            start_date=start_date,
            end_date=end_date,
            adjustment_type=adjustment_type,
        )

    def apply_adjustment(
        self,
        security_id: SecurityId,
        *,
        adjustment_type: str,
        raw_daily_records: tuple[DailyPriceBarRecord, ...],
    ) -> tuple[DailyPriceBarRecord, ...]:
        if adjustment_type not in {"qfq", "hfq"}:
            raise ValueError(
                "BaoStockConnector supports only qfq or hfq adjusted bars."
            )
        if not raw_daily_records:
            raise KeyError(security_id)
        if any(record.source != self.source_name for record in raw_daily_records):
            raise ValueError(
                "BaoStockConnector adjusted bars require raw_daily_records from the same source."
            )
        trade_dates = [
            date.fromisoformat(record.trade_date) for record in raw_daily_records
        ]
        start_date = min(trade_dates)
        end_date = max(trade_dates)
        factor_start_date = date(start_date.year - 10, 1, 1)
        factor_records = tuple(
            record
            for record in self._client.get_adjustment_factors(
                security_id,
                start_date=factor_start_date,
                end_date=end_date,
            )
            if record.adjustment_type == adjustment_type
        )
        if not factor_records:
            logger.info(
                "BaoStock returned no adjustment factors; using raw daily bars "
                f"unchanged: security={security_id.ticker} adjust={adjustment_type} "
                f"start={start_date} end={end_date}"
            )
            return raw_daily_records
        if adjustment_type == "qfq":
            return self._apply_fwd_adjustment(
                security_id=security_id,
                raw_records=raw_daily_records,
                factor_records=factor_records,
            )
        else:
            raise NotImplementedError(
                "BaoStockConnector currently supports only qfq adjusted bars."
            )

    def get_fundamental_snapshots(
        self,
        security_id: SecurityId,
        *,
        as_of_date: date,
    ) -> tuple[FundamentalSnapshotRecord, ...]:
        return self._client.get_fundamental_snapshots(
            security_id,
            as_of_date=as_of_date,
        )

    def get_disclosure_sections(
        self,
        security_id: SecurityId,
        *,
        as_of_date: date,
    ) -> tuple:
        _ = security_id, as_of_date
        raise NotImplementedError(
            "BaoStockConnector does not expose qualitative disclosure sections."
        )

    def get_macro_points(
        self,
        *,
        market: Market,
        as_of_date: date,
    ) -> tuple[MacroPointRecord, ...]:
        return self._client.get_macro_points(market=market, as_of_date=as_of_date)

    def get_trading_dates(
        self,
        *,
        market: Market,
        start_date: date,
        end_date: date,
    ) -> tuple[TradingDateRecord, ...]:
        return self._client.get_trading_dates(
            market=market,
            start_date=start_date,
            end_date=end_date,
        )

    def _apply_fwd_adjustment(
        self,
        *,
        security_id: SecurityId,
        raw_records: tuple[DailyPriceBarRecord, ...],
        factor_records: tuple[AdjustmentFactorRecord, ...],
    ) -> tuple[DailyPriceBarRecord, ...]:
        sorted_factors = sorted(factor_records, key=lambda record: record.trade_date)
        adjusted: list[DailyPriceBarRecord] = []
        factor_index = 0
        current_factor = 1.0
        for record in raw_records:
            trade_date = date.fromisoformat(record.trade_date)
            while factor_index < len(sorted_factors):
                factor_record = sorted_factors[factor_index]
                if date.fromisoformat(factor_record.trade_date) <= trade_date:
                    current_factor = factor_record.factor
                    factor_index += 1
                    continue
                break
            adjusted.append(
                DailyPriceBarRecord(
                    source=self.source_name,
                    security_id=security_id,
                    trade_date=record.trade_date,
                    open=record.open * current_factor,
                    high=record.high * current_factor,
                    low=record.low * current_factor,
                    close=record.close * current_factor,
                    volume=record.volume,
                    amount=record.amount,
                )
            )
        return tuple(adjusted)


def _quarter_period_end(year: int, quarter: int) -> str:
    month = quarter * 3
    day = 31 if month in (3, 12) else 30
    return date(year, month, day).isoformat()
