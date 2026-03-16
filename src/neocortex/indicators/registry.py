"""Registry of built-in indicator implementations."""

from __future__ import annotations

from neocortex.indicators.core import (
    Indicator,
    IndicatorSeries,
    IndicatorSpec,
)
from neocortex.indicators.ema import ema
from neocortex.indicators.rsi import rsi
from neocortex.indicators.sma import sma
from neocortex.models.core import PriceSeries


_INDICATORS: dict[str, Indicator] = {
    "sma": sma,
    "ema": ema,
    "rsi": rsi,
}


def list_indicators() -> tuple[Indicator, ...]:
    """Return all built-in indicators in stable registry order."""

    return tuple(_INDICATORS.values())


def get_indicator(key: str) -> Indicator:
    """Return one concrete indicator implementation."""

    return _INDICATORS[key]


def list_indicator_specs() -> tuple[IndicatorSpec, ...]:
    """Return the public metadata for all built-in indicators."""

    return tuple(indicator.spec for indicator in list_indicators())


def get_indicator_spec(key: str) -> IndicatorSpec:
    """Return one indicator metadata record."""

    return get_indicator(key).spec


def calculate_indicator(
    key: str,
    bars: PriceSeries,
    *,
    parameters: object | None = None,
) -> IndicatorSeries:
    """Calculate one indicator series over normalized price bars."""

    indicator = get_indicator(key)
    return indicator.calculate(bars, parameters=parameters)
