"""Provider-agnostic core data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import Any


JsonDict = dict[str, Any]


class Market(StrEnum):
    """Supported stock markets in the initial product scope."""

    US = "US"
    JP = "JP"
    HK = "HK"
    CN = "CN"


class DataProvider(StrEnum):
    """External providers that may use incompatible symbol formats."""

    AKSHARE = "akshare"
    YAHOO_FINANCE = "yahoo_finance"
    MANUAL = "manual"


@dataclass(frozen=True, slots=True)
class SecurityId:
    """Canonical stock identifier across multiple markets."""

    symbol: str
    market: Market
    exchange: str

    @property
    def ticker(self) -> str:
        """Human-readable symbol scoped to a market."""

        return f"{self.market}:{self.symbol}"


@dataclass(frozen=True, slots=True)
class MarketContext:
    """Market-level configuration used by connectors, agents, and UI."""

    market: Market
    region: str
    timezone: str
    trading_currency: str
    benchmark_symbol: str
    trading_calendar: str


@dataclass(frozen=True, slots=True)
class CompanyProfile:
    """Stable company identity and classification metadata."""

    security_id: SecurityId
    company_name: str
    sector: str
    industry: str
    country: str
    currency: str
    primary_listing: bool = True


@dataclass(frozen=True, slots=True)
class PriceBar:
    """Daily or higher-frequency OHLCV market data."""

    security_id: SecurityId
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    adjusted_close: float | None = None


@dataclass(frozen=True, slots=True)
class FundamentalSnapshot:
    """Normalized company fundamentals as of a point in time."""

    security_id: SecurityId
    as_of_date: date
    period_label: str
    raw_items: JsonDict = field(default_factory=dict)
    derived_metrics: JsonDict = field(default_factory=dict)
    source: DataProvider | None = None


@dataclass(frozen=True, slots=True)
class NewsItem:
    """Structured news item used by the news agent and UI."""

    security_id: SecurityId
    published_at: datetime
    source: str
    title: str
    summary: str
    url: str
    sentiment_tags: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class MacroSeriesPoint:
    """Single observation in a macroeconomic time series."""

    market: Market
    series_name: str
    observed_at: date
    value: float | None
    unit: str
    frequency: str
    change_pct: float | None = None
    yoy_change_pct: float | None = None


@dataclass(frozen=True, slots=True)
class SectorBenchmark:
    """Sector aggregates used to contextualize stock-level metrics."""

    market: Market
    sector: str
    as_of_date: date
    metric_averages: JsonDict = field(default_factory=dict)
    metric_medians: JsonDict = field(default_factory=dict)
    metric_percentiles: JsonDict = field(default_factory=dict)
    constituents: tuple[str, ...] = ()
