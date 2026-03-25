"""Typed records for connector-backed source fetching and persistence."""

from __future__ import annotations

from dataclasses import dataclass

from neocortex.models import Market
from neocortex.models import SecurityId


@dataclass(frozen=True, slots=True)
class SecurityListing:
    """One canonical security observed in a connector universe."""

    security_id: SecurityId
    name: str | None = None


@dataclass(frozen=True, slots=True)
class SecurityProfileSnapshot:
    """One source-specific company profile snapshot."""

    source: str
    security_id: SecurityId
    provider_company_name: str | None = None
    sector: str | None = None
    industry: str | None = None
    country: str | None = None
    currency: str | None = None
    primary_listing: bool = True


@dataclass(frozen=True, slots=True)
class DailyPriceBarRecord:
    """One source-specific daily OHLCV record."""

    source: str
    security_id: SecurityId
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None
    amount: float | None = None


@dataclass(frozen=True, slots=True)
class IntradayPriceBarRecord:
    """One source-specific intraday OHLCV record."""

    source: str
    security_id: SecurityId
    interval: str
    bar_time: str
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None
    amount: float | None = None


@dataclass(frozen=True, slots=True)
class AdjustmentFactorRecord:
    """One source-specific adjustment factor record."""

    source: str
    security_id: SecurityId
    trade_date: str
    adjustment_type: str
    factor: float


@dataclass(frozen=True, slots=True)
class FundamentalSnapshotRecord:
    """One source-specific structured fundamental snapshot."""

    source: str
    security_id: SecurityId
    period_end_date: str
    canonical_period_label: str
    statement_kind: str
    provider_period_label: str | None = None
    report_date: str | None = None
    currency: str | None = None
    raw_items_json: str = "{}"
    derived_metrics_json: str = "{}"


@dataclass(frozen=True, slots=True)
class DisclosureSectionRecord:
    """One source-specific qualitative disclosure section."""

    source: str
    security_id: SecurityId
    report_date: str
    section_kind: str
    content: str


@dataclass(frozen=True, slots=True)
class MacroPointRecord:
    """One source-specific macro or market time-series point."""

    source: str
    market: Market
    series_key: str
    observed_at: str
    series_name: str
    unit: str
    frequency: str
    category: str
    value: float | None = None
    change_pct: float | None = None
    yoy_change_pct: float | None = None


@dataclass(frozen=True, slots=True)
class TradingDateRecord:
    """One source-specific trading-date calendar record."""

    source: str
    market: Market
    calendar: str
    trade_date: str
    is_trading_day: bool
