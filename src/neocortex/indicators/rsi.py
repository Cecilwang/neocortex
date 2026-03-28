"""Relative strength index indicator."""

from __future__ import annotations

from dataclasses import dataclass

from neocortex.indicators.core import (
    Indicator,
    IndicatorParams,
    IndicatorSpec,
    coerce_indicator_params,
    log_indicator_calculation,
)
from neocortex.indicators.ta_lib_backend import rsi as calculate_rsi_series
from neocortex.models.core import PRICE_BAR_TIMESTAMP, PriceSeries
import pandas as pd


@dataclass(frozen=True, slots=True)
class RSIParams(IndicatorParams):
    """Parameters for relative strength index."""

    period: int = 14

    def __post_init__(self) -> None:
        if not isinstance(self.period, int):
            raise ValueError("period must be an integer.")
        if self.period <= 0:
            raise ValueError("period must be a positive integer.")


@dataclass(frozen=True, slots=True)
class RSIIndicator(IndicatorSpec):
    """Calculate Wilder-style RSI over close prices."""

    key: str = "rsi"
    display_name: str = "Relative Strength Index"
    category: str = "momentum"
    formula: str = (
        "Wilder-style ratio of average gains to average losses over N periods."
    )
    interpretation: str = (
        "Momentum oscillator often used to spot overbought and oversold regimes."
    )

    def calculate(
        self,
        bars: PriceSeries,
        *,
        parameters: RSIParams | dict[str, object] | None = None,
    ) -> RSI:
        resolved_parameters = coerce_indicator_params(RSIParams, parameters)
        log_indicator_calculation(
            indicator_key=self.key,
            bars=bars,
            parameters=resolved_parameters,
        )
        period = resolved_parameters.period
        closes = bars.closes
        if closes.empty:
            frame = pd.DataFrame(
                {
                    PRICE_BAR_TIMESTAMP: pd.Series(dtype="datetime64[ns]"),
                    "value": pd.Series(dtype=object),
                }
            )
            return RSI(spec=self, parameters=resolved_parameters, data=frame)

        values = calculate_rsi_series(closes, period=period)
        frame = pd.DataFrame(
            {
                PRICE_BAR_TIMESTAMP: bars.timestamps,
                "value": values,
            }
        )
        return RSI(
            spec=self,
            parameters=resolved_parameters,
            data=frame,
        )


@dataclass(frozen=True, slots=True)
class RSI(Indicator):
    @property
    def rsi(self) -> pd.Series:
        return self.data["value"]


rsi = RSIIndicator()
