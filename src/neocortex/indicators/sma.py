"""Simple moving average indicator."""

from __future__ import annotations

from dataclasses import dataclass
import pandas as pd

from neocortex.indicators.core import (
    Indicator,
    IndicatorParams,
    IndicatorSpec,
    coerce_indicator_params,
    log_indicator_calculation,
)
from neocortex.models.core import PRICE_BAR_TIMESTAMP, PriceSeries


@dataclass(frozen=True, slots=True)
class SMAParams(IndicatorParams):
    """Parameters for simple moving average."""

    window: int = 20

    def __post_init__(self) -> None:
        if not isinstance(self.window, int):
            raise ValueError("window must be an integer.")
        if self.window <= 0:
            raise ValueError("window must be a positive integer.")


@dataclass(frozen=True, slots=True)
class SMAIndicator(IndicatorSpec):
    """Calculate a simple moving average over close prices."""

    key: str = "sma"
    display_name: str = "Simple Moving Average"
    category: str = "trend"
    formula: str = "Average of the last N closing prices."
    interpretation: str = (
        "Smooths price noise and highlights the prevailing trend direction."
    )

    def calculate(
        self,
        bars: PriceSeries,
        *,
        parameters: SMAParams | dict[str, object] | None = None,
    ) -> SMA:
        resolved_parameters = coerce_indicator_params(SMAParams, parameters)
        log_indicator_calculation(
            indicator_key=self.key,
            bars=bars,
            parameters=resolved_parameters,
        )
        window = resolved_parameters.window
        values = bars.closes.rolling(window=window).mean()
        frame = pd.DataFrame(
            {
                PRICE_BAR_TIMESTAMP: bars.timestamps,
                "value": values.astype(object).where(values.notna(), None),
            }
        )
        return SMA(
            spec=self,
            parameters=resolved_parameters,
            data=frame,
        )


@dataclass(frozen=True, slots=True)
class SMA(Indicator):
    @property
    def sma(self) -> pd.Series:
        return self.data["value"]


sma = SMAIndicator()
