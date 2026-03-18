"""Provider-agnostic core data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from collections.abc import Sequence
from typing import Any

import pandas as pd


JsonDict = dict[str, Any]

PRICE_BAR_TIMESTAMP = "timestamp"
PRICE_BAR_OPEN = "open"
PRICE_BAR_HIGH = "high"
PRICE_BAR_LOW = "low"
PRICE_BAR_CLOSE = "close"
PRICE_BAR_VOLUME = "volume"
PRICE_BAR_ADJUSTED_CLOSE = "adjusted_close"

PRICE_BAR_VALUE_COLUMNS = (
    PRICE_BAR_TIMESTAMP,
    PRICE_BAR_OPEN,
    PRICE_BAR_HIGH,
    PRICE_BAR_LOW,
    PRICE_BAR_CLOSE,
    PRICE_BAR_VOLUME,
    PRICE_BAR_ADJUSTED_CLOSE,
)


class Market(StrEnum):
    """Supported stock markets in the initial product scope."""

    US = "US"
    JP = "JP"
    HK = "HK"
    CN = "CN"


class Exchange(StrEnum):
    """Canonical listing exchange identifiers using ISO 10383 MIC values."""

    XNAS = "XNAS"  # 纳斯达克证券交易所
    XNYS = "XNYS"  # 纽约证券交易所
    XTKS = "XTKS"  # 东京证券交易所
    XHKG = "XHKG"  # 香港交易所
    XSHG = "XSHG"  # 上海证券交易所
    XSHE = "XSHE"  # 深圳证券交易所
    XBJS = "XBJS"  # 北京证券交易所


class TradingCalendar(StrEnum):
    """Canonical trading calendar identifiers using MIC-style values."""

    XNAS = "XNAS"  # 纳斯达克交易日历
    XNYS = "XNYS"  # 纽交所交易日历
    XTKS = "XTKS"  # 东京交易所交易日历
    XHKG = "XHKG"  # 香港交易所交易日历
    XSHG = "XSHG"  # 上交所交易日历
    XSHE = "XSHE"  # 深交所交易日历
    XBJS = "XBJS"  # 北交所交易日历


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
    exchange: Exchange

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
    trading_calendar: TradingCalendar


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


@dataclass(frozen=True, slots=True, init=False)
class PriceSeries:
    """Time-ordered OHLCV frame for one security."""

    security_id: SecurityId
    data: pd.DataFrame

    def __init__(
        self,
        *,
        security_id: SecurityId,
        bars: Sequence[PriceBar] = (),
        data: pd.DataFrame | None = None,
    ) -> None:
        if data is not None and bars:
            raise ValueError("PriceSeries accepts either bars or data, but not both.")
        if data is None:
            normalized_data = _price_series_frame_from_bars(security_id, bars)
        else:
            normalized_data = _normalize_price_series_frame(data)
        _validate_price_series_frame(security_id, normalized_data)
        object.__setattr__(self, "security_id", security_id)
        object.__setattr__(self, "data", normalized_data)

    @property
    def bars(self) -> pd.DataFrame:
        return self.data

    def __len__(self) -> int:
        return len(self.data)

    @property
    def start_timestamp(self) -> object | None:
        if self.data.empty:
            return None
        return self.data[PRICE_BAR_TIMESTAMP].iloc[0]

    @property
    def end_timestamp(self) -> object | None:
        if self.data.empty:
            return None
        return self.data[PRICE_BAR_TIMESTAMP].iloc[-1]

    @property
    def closes(self) -> pd.Series:
        return self.data.loc[:, PRICE_BAR_CLOSE]

    @property
    def timestamps(self) -> pd.Series:
        return self.data.loc[:, PRICE_BAR_TIMESTAMP]


def _empty_price_series_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=PRICE_BAR_VALUE_COLUMNS)


def _price_series_frame_from_bars(
    security_id: SecurityId,
    bars: Sequence[PriceBar],
) -> pd.DataFrame:
    if not bars:
        return _empty_price_series_frame()
    records: list[dict[str, object]] = []
    for bar in bars:
        if bar.security_id != security_id:
            raise ValueError("PriceSeries bars must all share the same security_id.")
        records.append(
            {
                PRICE_BAR_TIMESTAMP: bar.timestamp,
                PRICE_BAR_OPEN: bar.open,
                PRICE_BAR_HIGH: bar.high,
                PRICE_BAR_LOW: bar.low,
                PRICE_BAR_CLOSE: bar.close,
                PRICE_BAR_VOLUME: bar.volume,
                PRICE_BAR_ADJUSTED_CLOSE: bar.adjusted_close,
            }
        )
    return pd.DataFrame(records, columns=PRICE_BAR_VALUE_COLUMNS)


def _normalize_price_series_frame(data: pd.DataFrame) -> pd.DataFrame:
    frame = data.copy()
    if PRICE_BAR_TIMESTAMP not in frame.columns:
        raise ValueError(
            f"PriceSeries data is missing required columns: {PRICE_BAR_TIMESTAMP}."
        )
    frame[PRICE_BAR_TIMESTAMP] = pd.to_datetime(frame[PRICE_BAR_TIMESTAMP])
    missing_columns = [
        column for column in PRICE_BAR_VALUE_COLUMNS if column not in frame.columns
    ]
    if missing_columns:
        raise ValueError(
            f"PriceSeries data is missing required columns: {', '.join(missing_columns)}."
        )
    frame = frame.loc[:, PRICE_BAR_VALUE_COLUMNS]
    return frame


def _validate_price_series_frame(
    security_id: SecurityId,
    frame: pd.DataFrame,
) -> None:
    _ = security_id
    timestamps = frame[PRICE_BAR_TIMESTAMP]
    if not timestamps.is_monotonic_increasing:
        raise ValueError("PriceSeries bars must be sorted by timestamp.")
    if timestamps.duplicated().any():
        raise ValueError("PriceSeries bars must not contain duplicate timestamps.")


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
