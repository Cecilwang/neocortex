"""AkShare-backed connector for minimal China A-share market data."""

from __future__ import annotations

import importlib
import logging
from datetime import date, datetime
from typing import Any

import pandas as pd

from neocortex.connectors.base import BaseSourceConnector
from neocortex.connectors.common import infer_cn_exchange
from neocortex.connectors.common import log_daily_records_access
from neocortex.connectors.types import (
    DailyPriceBarRecord,
    SecurityListing,
    SecurityProfileSnapshot,
)
from neocortex.models.core import Exchange, Market, SecurityId
from neocortex.utils.retry import connector_retry


logger = logging.getLogger(__name__)
_SUPPORTED_CN_EXCHANGES = frozenset({Exchange.XSHG, Exchange.XSHE})
_AKSHARE_DAILY_PERIOD = "daily"
_AKSHARE_DATE_FORMAT = "%Y%m%d"
_EM_PROFILE_NAME_FIELD = "股票简称"
_EM_PROFILE_INDUSTRY_FIELD = "行业"
_XUEQIU_NAME_FIELD = "org_short_name_cn"
_XUEQIU_LEGAL_NAME_FIELD = "org_name_cn"
_XUEQIU_INDUSTRY_FIELD = "affiliate_industry"
_BAR_DATE_FIELD = "日期"
_BAR_OPEN_FIELD = "开盘"
_BAR_HIGH_FIELD = "最高"
_BAR_LOW_FIELD = "最低"
_BAR_CLOSE_FIELD = "收盘"
_BAR_VOLUME_FIELD = "成交量"
_AKSHARE_VOLUME_LOT_SIZE = 100.0


class _AkShareApiClient:
    source_name = "akshare"

    def __init__(self, *, timeout: float | None = None, api: Any | None = None) -> None:
        self.timeout = timeout
        self.api = api

    def _api(self) -> Any:
        if self.api is not None:
            return self.api
        try:
            return importlib.import_module("akshare")
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "AkShareConnector requires the optional 'akshare' dependency."
            ) from exc

    def _symbol_for_request(self, security_id: SecurityId) -> str:
        if security_id.market is not Market.CN:
            raise ValueError("AkShareConnector supports only CN securities.")
        if security_id.exchange not in _SUPPORTED_CN_EXCHANGES:
            raise ValueError(
                "AkShareConnector requires an XSHG or XSHE listing exchange."
            )
        return security_id.symbol

    def _xueqiu_symbol_for_request(self, security_id: SecurityId) -> str:
        symbol = self._symbol_for_request(security_id)
        if security_id.exchange is Exchange.XSHG:
            return f"SH{symbol}"
        return f"SZ{symbol}"

    @connector_retry(source_name=source_name)
    def list_securities(self, *, market: Market) -> tuple[SecurityListing, ...]:
        if market is not Market.CN:
            raise NotImplementedError("AkShareConnector currently supports only CN.")
        logger.info("Fetching AkShare CN security universe.")
        frame = self._api().stock_info_a_code_name()
        listings: list[SecurityListing] = []
        for _, row in frame.iterrows():
            symbol = str(row["code"]).zfill(6)
            listings.append(
                SecurityListing(
                    security_id=SecurityId(
                        symbol=symbol,
                        market=Market.CN,
                        exchange=infer_cn_exchange(symbol),
                    ),
                    name=str(row["name"]).strip(),
                )
            )
        return tuple(listings)

    def get_security_profile_snapshot(
        self,
        security_id: SecurityId,
    ) -> SecurityProfileSnapshot:
        try:
            company_name, industry = self._get_company_profile_from_em(security_id)
        except Exception:
            logger.warning(
                f"Eastmoney company profile request failed for {security_id.ticker}; "
                "falling back to Xueqiu."
            )
            company_name, industry = self._get_company_profile_from_xueqiu(security_id)
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
    def _get_company_profile_from_em(
        self, security_id: SecurityId
    ) -> tuple[str | None, str | None]:
        symbol = self._symbol_for_request(security_id)
        logger.info(
            f"Fetching AkShare company profile for {security_id.ticker} from Eastmoney."
        )
        raw_profile = self._api().stock_individual_info_em(
            symbol=symbol,
            timeout=self.timeout,
        )
        profile_items = self._profile_items(raw_profile)
        company_name = str(profile_items[_EM_PROFILE_NAME_FIELD]).strip()
        industry = str(profile_items[_EM_PROFILE_INDUSTRY_FIELD]).strip()
        if not company_name or not industry:
            raise ValueError("Eastmoney profile is missing required fields.")
        return company_name, industry

    @connector_retry(source_name=source_name)
    def _get_company_profile_from_xueqiu(
        self, security_id: SecurityId
    ) -> tuple[str | None, str | None]:
        logger.info(
            f"Fetching AkShare company profile for {security_id.ticker} from Xueqiu."
        )
        raw_profile = self._api().stock_individual_basic_info_xq(
            symbol=self._xueqiu_symbol_for_request(security_id),
            timeout=self.timeout,
        )
        profile_items = self._profile_items(raw_profile)
        company_name = str(
            profile_items.get(_XUEQIU_NAME_FIELD)
            or profile_items[_XUEQIU_LEGAL_NAME_FIELD]
        )
        industry_value = profile_items[_XUEQIU_INDUSTRY_FIELD]
        if (
            isinstance(industry_value, dict)
            and industry_value.get("ind_name") is not None
        ):
            industry = str(industry_value["ind_name"])
        else:
            industry = str(industry_value)
        if not company_name or not industry:
            raise ValueError("Xueqiu profile is missing required fields.")
        return company_name, industry

    def get_daily_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
    ) -> tuple[DailyPriceBarRecord, ...]:
        return self._fetch_daily_price_records(
            security_id,
            start_date=start_date,
            end_date=end_date,
            adjust=None,
        )

    def get_adjusted_daily_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        adjustment_type: str,
    ) -> tuple[DailyPriceBarRecord, ...]:
        if adjustment_type not in {"qfq", "hfq"}:
            raise ValueError("AkShareConnector supports only qfq or hfq adjusted bars.")
        return self._fetch_daily_price_records(
            security_id,
            start_date=start_date,
            end_date=end_date,
            adjust=adjustment_type,
        )

    @staticmethod
    def _profile_items(frame: pd.DataFrame) -> dict[str, Any]:
        return {
            str(row["item"]): row["value"]
            for _, row in frame.iterrows()
            if row["item"] is not None and row["value"] is not None
        }

    @connector_retry(source_name=source_name)
    def _fetch_daily_price_records(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        adjust: str | None,
    ) -> tuple[DailyPriceBarRecord, ...]:
        symbol = self._symbol_for_request(security_id)
        provider_adjust = adjust or ""
        logger.info(
            f"Fetching AkShare price bars for {security_id.ticker} between "
            f"{start_date} and {end_date}."
        )
        frame = self._api().stock_zh_a_hist(
            symbol=symbol,
            period=_AKSHARE_DAILY_PERIOD,
            start_date=start_date.strftime(_AKSHARE_DATE_FORMAT),
            end_date=end_date.strftime(_AKSHARE_DATE_FORMAT),
            adjust=provider_adjust,
            timeout=self.timeout,
        )
        if frame.empty:
            raise KeyError(security_id)

        records: list[DailyPriceBarRecord] = []
        for _, row in frame.iterrows():
            trading_date = row[_BAR_DATE_FIELD]
            if isinstance(trading_date, datetime):
                trade_date = trading_date.date().isoformat()
            elif isinstance(trading_date, date):
                trade_date = trading_date.isoformat()
            else:
                trade_date = str(trading_date)
            records.append(
                DailyPriceBarRecord(
                    source=self.source_name,
                    security_id=security_id,
                    trade_date=trade_date,
                    open=float(row[_BAR_OPEN_FIELD]),
                    high=float(row[_BAR_HIGH_FIELD]),
                    low=float(row[_BAR_LOW_FIELD]),
                    close=float(row[_BAR_CLOSE_FIELD]),
                    volume=float(row[_BAR_VOLUME_FIELD]) * _AKSHARE_VOLUME_LOT_SIZE,
                    amount=None,
                )
            )
        fetched_records = tuple(records)
        log_daily_records_access(
            source_name=self.source_name,
            security_id=security_id,
            requested_start_date=start_date,
            requested_end_date=end_date,
            records=fetched_records,
            adjust_label=adjust or "raw",
        )
        return fetched_records


class AkShareConnector(BaseSourceConnector):
    """Fetch normalized China A-share data via AkShare."""

    source_name = "akshare"
    supported_markets = frozenset({Market.CN})
    supports_adjustment_factors = False
    supports_adjusted_daily_bars = True

    def __init__(
        self,
        *,
        timeout: float | None = None,
        api: Any | None = None,
    ) -> None:
        super().__init__()
        self.timeout = timeout
        self.api = api
        self._client = _AkShareApiClient(timeout=timeout, api=api)

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
