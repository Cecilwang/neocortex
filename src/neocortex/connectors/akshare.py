"""AkShare-backed connector for minimal China A-share market data."""

from __future__ import annotations

import importlib
import logging
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import TYPE_CHECKING, Any

from neocortex.connectors.base import DAILY_BAR_INTERVAL
from neocortex.models.core import (
    CompanyProfile,
    Exchange,
    Market,
    PriceBar,
    PriceSeries,
    SecurityId,
)

if TYPE_CHECKING:
    import pandas as pd


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


@dataclass(slots=True)
class AkShareConnector:
    """Fetch normalized China A-share data via AkShare."""

    timeout: float | None = None
    api: Any | None = None

    def get_company_profile(self, security_id: SecurityId) -> CompanyProfile:
        """Return a normalized company profile for one China A-share."""

        symbol = self._symbol_for_request(security_id)
        logger.debug("Fetching AkShare company profile for %s.", security_id.ticker)
        try:
            raw_profile = self._api().stock_individual_info_em(
                symbol=symbol,
                timeout=self.timeout,
            )
        except Exception as exc:
            logger.warning(
                "Eastmoney company profile request failed for %s (%s: %s); falling back to Xueqiu.",
                security_id.ticker,
                type(exc).__name__,
                exc,
            )
            logger.debug(
                "Eastmoney company profile traceback for %s.",
                security_id.ticker,
                exc_info=True,
            )
            return self._get_company_profile_from_xueqiu(security_id)
        profile_items = self._profile_items(raw_profile)
        company_name = str(profile_items[_EM_PROFILE_NAME_FIELD])
        industry = str(profile_items[_EM_PROFILE_INDUSTRY_FIELD])
        return CompanyProfile(
            security_id=security_id,
            company_name=company_name,
            sector=industry,
            industry=industry,
            country="CN",
            currency="CNY",
        )

    def get_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        interval: str = DAILY_BAR_INTERVAL,
        adjust: str | None = None,
    ) -> PriceSeries:
        """Return normalized daily bars for one China A-share."""

        if interval != DAILY_BAR_INTERVAL:
            raise ValueError(
                f"AkShareConnector currently supports only the {DAILY_BAR_INTERVAL} interval."
            )

        symbol = self._symbol_for_request(security_id)
        provider_adjust = adjust or ""
        logger.debug(
            "Fetching AkShare price bars for %s between %s and %s.",
            security_id.ticker,
            start_date,
            end_date,
        )
        raw_bars = self._api().stock_zh_a_hist(
            symbol=symbol,
            period=_AKSHARE_DAILY_PERIOD,
            start_date=start_date.strftime(_AKSHARE_DATE_FORMAT),
            end_date=end_date.strftime(_AKSHARE_DATE_FORMAT),
            adjust=provider_adjust,
            timeout=self.timeout,
        )
        return self._normalize_price_bars(
            security_id,
            raw_bars,
            adjust=provider_adjust,
        )

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

    def _get_company_profile_from_xueqiu(
        self, security_id: SecurityId
    ) -> CompanyProfile:
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
        if isinstance(industry_value, dict) and industry_value.get("ind_name") is not None:
            industry = str(industry_value["ind_name"])
        else:
            industry = str(industry_value)
        return CompanyProfile(
            security_id=security_id,
            company_name=company_name,
            sector=industry,
            industry=industry,
            country="CN",
            currency="CNY",
        )

    @staticmethod
    def _profile_items(frame: pd.DataFrame) -> dict[str, Any]:
        return {
            str(row["item"]): row["value"]
            for _, row in frame.iterrows()
            if row["item"] is not None and row["value"] is not None
        }

    def _normalize_price_bars(
        self,
        security_id: SecurityId,
        frame: pd.DataFrame,
        *,
        adjust: str,
    ) -> PriceSeries:
        if frame.empty:
            return PriceSeries(security_id=security_id, bars=())

        bars: list[PriceBar] = []
        for _, row in frame.iterrows():
            trading_date = row[_BAR_DATE_FIELD]
            if isinstance(trading_date, datetime):
                bar_timestamp = trading_date
            else:
                bar_timestamp = datetime.combine(trading_date, time(15, 0))

            close = float(row[_BAR_CLOSE_FIELD])
            bars.append(
                PriceBar(
                    security_id=security_id,
                    timestamp=bar_timestamp,
                    open=float(row[_BAR_OPEN_FIELD]),
                    high=float(row[_BAR_HIGH_FIELD]),
                    low=float(row[_BAR_LOW_FIELD]),
                    close=close,
                    volume=float(row[_BAR_VOLUME_FIELD]),
                    adjusted_close=close if adjust else None,
                )
            )
        return PriceSeries(security_id=security_id, bars=tuple(bars))
