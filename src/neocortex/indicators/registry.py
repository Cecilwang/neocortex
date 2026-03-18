"""Registry of built-in indicator implementations."""

from __future__ import annotations

from neocortex.indicators.core import Indicator, IndicatorSpec
from neocortex.indicators.ema import ema
from neocortex.indicators.macd import macd
from neocortex.indicators.rsi import rsi
from neocortex.indicators.sma import sma
from neocortex.models.core import PriceSeries


_INDICATORS: dict[str, IndicatorSpec] = {
    "sma": sma,
    "ema": ema,
    "rsi": rsi,
    "macd": macd,
}


def list_indicator_specs() -> tuple[IndicatorSpec, ...]:
    """Return all built-in indicators in stable registry order."""

    return tuple(_INDICATORS.values())


def get_indicator_spec(key: str) -> IndicatorSpec:
    """Return one concrete indicator implementation."""

    return _INDICATORS[key]


def calculate_indicator(
    key: str,
    bars: PriceSeries,
    *,
    parameters: object | None = None,
) -> Indicator:
    """Calculate one indicator series over normalized price bars."""

    indicator = get_indicator_spec(key)
    return indicator.calculate(bars, parameters=parameters)
