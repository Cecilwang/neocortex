"""Registry of built-in indicator implementations."""

from __future__ import annotations

import logging

from neocortex.indicators.core import Indicator, IndicatorSpec
from neocortex.indicators.ema import ema
from neocortex.indicators.kdj import kdj
from neocortex.indicators.macd import macd
from neocortex.indicators.roc import roc
from neocortex.indicators.rsi import rsi
from neocortex.indicators.sma import sma
from neocortex.models.core import PriceSeries


logger = logging.getLogger(__name__)


_INDICATORS: dict[str, IndicatorSpec] = {
    "sma": sma,
    "ema": ema,
    "roc": roc,
    "rsi": rsi,
    "macd": macd,
    "kdj": kdj,
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

    logger.debug(
        f"Calculating indicator from registry: indicator={key} "
        f"security={bars.security_id.ticker} bar_count={len(bars)} parameters={parameters}"
    )
    indicator = get_indicator_spec(key)
    result = indicator.calculate(bars, parameters=parameters)
    logger.debug(
        f"Calculated indicator from registry: indicator={key} "
        f"security={bars.security_id.ticker} row_count={len(result.data)}"
    )
    return result
