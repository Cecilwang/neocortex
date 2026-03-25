"""efinance-backed connector for market-data ingestion."""

from __future__ import annotations

import importlib
from datetime import date
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
    DailyPriceBarRecord,
    SecurityListing,
    SecurityProfileSnapshot,
)
from neocortex.models import Market, SecurityId
from neocortex.utils.retry import connector_retry


_EFINANCE_CN_UNIVERSE = "沪深A股"
logger = logging.getLogger(__name__)


class _EFinanceApiClient:
    source_name = "efinance"

    def __init__(self, api: Any | None = None) -> None:
        self.api = api

    def _api(self) -> Any:
        if self.api is not None:
            return self.api
        try:
            return importlib.import_module("efinance")
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "EFinanceConnector requires the optional 'efinance' dependency."
            ) from exc

    @connector_retry(source_name=source_name)
    def list_securities(self, *, market: Market) -> tuple[SecurityListing, ...]:
        if market is not Market.CN:
            raise NotImplementedError("EFinanceConnector currently supports only CN.")
        logger.info(f"Fetching EFinance security universe: market={market.value}")
        frame = self._api().stock.get_realtime_quotes(_EFINANCE_CN_UNIVERSE)
        listings: list[SecurityListing] = []
        for _, row in frame.iterrows():
            symbol = str(row["股票代码"]).zfill(6)
            try:
                exchange = infer_cn_exchange(symbol)
            except ValueError:
                continue
            listings.append(
                SecurityListing(
                    security_id=SecurityId(
                        symbol=symbol,
                        market=Market.CN,
                        exchange=exchange,
                    ),
                    name=str(row["股票名称"]).strip(),
                )
            )
        return tuple(listings)

    @connector_retry(source_name=source_name)
    def get_security_profile_snapshot(
        self,
        security_id: SecurityId,
    ) -> SecurityProfileSnapshot:
        if security_id.market is not Market.CN:
            raise NotImplementedError("EFinanceConnector currently supports only CN.")
        logger.info(f"Fetching EFinance profile: security={security_id.ticker}")
        raw = self._api().stock.get_base_info(security_id.symbol)
        if isinstance(raw, pd.DataFrame):
            series = raw.iloc[0]
        else:
            series = raw
        company_name = _series_value(series, "股票名称")
        industry = _series_value(series, "所处行业")
        if not company_name or not industry:
            raise ValueError(f"Incomplete profile data for {security_id}: {raw}")
        return SecurityProfileSnapshot(
            source=self.source_name,
            security_id=security_id,
            provider_company_name=company_name,
            sector=industry,
            industry=industry,
            country="CN",
            currency="CNY",
            primary_listing=True,
        )

    @connector_retry(source_name=source_name)
    def get_daily_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
    ) -> tuple[DailyPriceBarRecord, ...]:
        logger.info(
            f"Fetching EFinance raw daily bars: security={security_id.ticker} "
            f"start={start_date} end={end_date}"
        )
        return self._get_daily_price_bars(
            security_id,
            start_date=start_date,
            end_date=end_date,
            fqt=0,
        )

    @connector_retry(source_name=source_name)
    def get_adjusted_daily_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        adjustment_type: str,
    ) -> tuple[DailyPriceBarRecord, ...]:
        if adjustment_type not in {"qfq", "hfq"}:
            raise ValueError(
                "EFinanceConnector supports only qfq or hfq adjusted bars."
            )
        logger.info(
            f"Fetching EFinance adjusted daily bars: security={security_id.ticker} "
            f"start={start_date} end={end_date} adjust={adjustment_type}"
        )
        return self._get_daily_price_bars(
            security_id,
            start_date=start_date,
            end_date=end_date,
            fqt=1 if adjustment_type == "qfq" else 2,
        )

    def _get_daily_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        fqt: int,
    ) -> tuple[DailyPriceBarRecord, ...]:
        if security_id.market is not Market.CN:
            raise NotImplementedError("EFinanceConnector currently supports only CN.")
        frame = self._api().stock.get_quote_history(
            security_id.symbol,
            beg=start_date.strftime("%Y%m%d"),
            end=end_date.strftime("%Y%m%d"),
            klt=101,
            fqt=fqt,
        )
        if frame.empty:
            raise KeyError(security_id)
        records: list[DailyPriceBarRecord] = []
        for _, row in frame.iterrows():
            records.append(
                DailyPriceBarRecord(
                    source=self.source_name,
                    security_id=security_id,
                    trade_date=str(row["日期"]),
                    open=float(row["开盘"]),
                    high=float(row["最高"]),
                    low=float(row["最低"]),
                    close=float(row["收盘"]),
                    volume=optional_float(row.get("成交量")),
                    amount=optional_float(row.get("成交额")),
                )
            )
        fetched_records = tuple(records)
        log_daily_records_access(
            source_name=self.source_name,
            security_id=security_id,
            requested_start_date=start_date,
            requested_end_date=end_date,
            records=fetched_records,
            adjust_label="raw" if fqt == 0 else ("qfq" if fqt == 1 else "hfq"),
        )
        return fetched_records


class EFinanceConnector(BaseSourceConnector):
    """Minimal CN ingestion connector backed by efinance."""

    source_name = "efinance"
    supported_markets = frozenset({Market.CN})
    supports_adjustment_factors = False
    supports_adjusted_daily_bars = True

    def __init__(
        self,
        *,
        api: Any | None = None,
    ) -> None:
        super().__init__()
        self.api = api
        self._client = _EFinanceApiClient(api=api)

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


def _series_value(series: pd.Series, key: str) -> str | None:
    value = series.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None
