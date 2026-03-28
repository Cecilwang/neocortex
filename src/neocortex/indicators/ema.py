"""Exponential moving average indicator."""

from __future__ import annotations

from dataclasses import dataclass

from neocortex.indicators.core import (
    Indicator,
    IndicatorParams,
    IndicatorSpec,
    coerce_indicator_params,
    log_indicator_calculation,
)
from neocortex.indicators.ta_lib_backend import ema as calculate_ema_series
from neocortex.models.core import PRICE_BAR_TIMESTAMP, PriceSeries
import pandas as pd


@dataclass(frozen=True, slots=True)
class EMAParams(IndicatorParams):
    """Parameters for exponential moving average."""

    window: int = 20

    def __post_init__(self) -> None:
        if not isinstance(self.window, int):
            raise ValueError("window must be an integer.")
        if self.window <= 0:
            raise ValueError("window must be a positive integer.")


@dataclass(frozen=True, slots=True)
class EMAIndicator(IndicatorSpec):
    """Calculate an exponential moving average over close prices."""

    key: str = "ema"
    display_name: str = "Exponential Moving Average"
    category: str = "trend"
    formula: str = (
        "Recursively weighted average that gives more weight to recent closes."
    )
    interpretation: str = (
        "Responds faster to recent prices than SMA while still tracking trend."
    )

    def calculate(
        self,
        bars: PriceSeries,
        *,
        parameters: EMAParams | dict[str, object] | None = None,
    ) -> EMA:
        resolved_parameters = coerce_indicator_params(EMAParams, parameters)
        log_indicator_calculation(
            indicator_key=self.key,
            bars=bars,
            parameters=resolved_parameters,
        )
        window = resolved_parameters.window
        closes = bars.closes
        if closes.empty:
            frame = pd.DataFrame(
                {
                    PRICE_BAR_TIMESTAMP: pd.Series(dtype="datetime64[ns]"),
                    "value": pd.Series(dtype=object),
                }
            )
            return EMA(spec=self, parameters=resolved_parameters, data=frame)

        values = calculate_ema_series(closes, window=window)

        frame = pd.DataFrame(
            {
                PRICE_BAR_TIMESTAMP: bars.timestamps,
                "value": values,
            }
        )
        return EMA(
            spec=self,
            parameters=resolved_parameters,
            data=frame,
        )


@dataclass(frozen=True, slots=True)
class EMA(Indicator):
    @property
    def ema(self) -> pd.Series:
        return self.data["value"]


ema = EMAIndicator()
