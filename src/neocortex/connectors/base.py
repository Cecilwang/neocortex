"""Shared base class for network-source connectors."""

from __future__ import annotations

from datetime import date
from typing import ClassVar

from neocortex.connectors.types import (
    AdjustmentFactorRecord,
    DailyPriceBarRecord,
    DisclosureSectionRecord,
    FundamentalSnapshotRecord,
    MacroPointRecord,
    SecurityListing,
    SecurityProfileSnapshot,
    TradingDateRecord,
)
from neocortex.models.core import SecurityId
from neocortex.models.core import Market

DAILY_BAR_INTERVAL = "1d"


class BaseSourceConnector:
    """Common runtime helpers and declared capabilities for source connectors."""

    source_name: ClassVar[str]
    supported_markets: ClassVar[frozenset[Market]] = frozenset()
    supports_adjustment_factors: ClassVar[bool] = False
    supports_adjusted_daily_bars: ClassVar[bool] = False

    def __init__(
        self,
    ) -> None:
        if not getattr(self, "source_name", "").strip():
            raise ValueError("Source connectors must define a non-empty source_name.")

    def supports_market(self, market: Market) -> bool:
        return not self.supported_markets or market in self.supported_markets

    def list_securities(self, *, market: Market) -> tuple[SecurityListing, ...]:
        _ = market
        raise NotImplementedError

    def get_security_profile_snapshot(
        self,
        security_id: SecurityId,
    ) -> SecurityProfileSnapshot:
        _ = security_id
        raise NotImplementedError

    def get_daily_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
    ) -> tuple[DailyPriceBarRecord, ...]:
        _ = security_id, start_date, end_date
        raise NotImplementedError

    def get_adjusted_daily_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        adjustment_type: str,
    ) -> tuple[DailyPriceBarRecord, ...]:
        _ = security_id, start_date, end_date, adjustment_type
        raise NotImplementedError

    def get_adjustment_factors(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
    ) -> tuple[AdjustmentFactorRecord, ...]:
        _ = security_id, start_date, end_date
        raise NotImplementedError

    def apply_adjustment(
        self,
        security_id: SecurityId,
        *,
        adjustment_type: str,
        raw_daily_records: tuple[DailyPriceBarRecord, ...],
    ) -> tuple[DailyPriceBarRecord, ...]:
        _ = security_id, adjustment_type, raw_daily_records
        raise NotImplementedError

    def get_fundamental_snapshots(
        self,
        security_id: SecurityId,
        *,
        as_of_date: date,
    ) -> tuple[FundamentalSnapshotRecord, ...]:
        _ = security_id, as_of_date
        raise NotImplementedError

    def get_disclosure_sections(
        self,
        security_id: SecurityId,
        *,
        as_of_date: date,
    ) -> tuple[DisclosureSectionRecord, ...]:
        _ = security_id, as_of_date
        raise NotImplementedError

    def get_macro_points(
        self,
        *,
        market: Market,
        as_of_date: date,
    ) -> tuple[MacroPointRecord, ...]:
        _ = market, as_of_date
        raise NotImplementedError

    def get_trading_dates(
        self,
        *,
        market: Market,
        start_date: date,
        end_date: date,
    ) -> tuple[TradingDateRecord, ...]:
        _ = market, start_date, end_date
        raise NotImplementedError
