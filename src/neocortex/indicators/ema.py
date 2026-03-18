"""Exponential moving average indicator."""

from __future__ import annotations

from dataclasses import dataclass

from neocortex.indicators.core import (
    Indicator,
    IndicatorParams,
    IndicatorSpec,
)
from neocortex.models.core import PRICE_BAR_TIMESTAMP, PriceSeries
import pandas as pd


@dataclass(frozen=True, slots=True)
class EMAParams(IndicatorParams):
    """Parameters for exponential moving average."""

    window: int = 20

    def __post_init__(self) -> None:
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
        resolved_parameters = _coerce_params(parameters)
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

        values = calculate_ema_series(closes, window)

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


def calculate_ema_series(values: pd.Series, window: int) -> pd.Series:
    if len(values) < window:
        return pd.Series([None] * len(values), dtype=object)

    alpha = 2.0 / (window + 1)
    ema_values: list[float | None] = [None] * (window - 1)
    window_values = values.iloc[:window]
    ema = float(sum(window_values) / window)
    ema_values.append(ema)
    trailing_values = values.iloc[window:]
    for value in trailing_values:
        ema = (alpha * float(value)) + ((1 - alpha) * ema)
        ema_values.append(ema)
    return pd.Series(ema_values, dtype=object)


def _coerce_params(parameters: EMAParams | dict[str, object] | None) -> EMAParams:
    if parameters is None:
        return EMAParams()
    if isinstance(parameters, EMAParams):
        return parameters
    if isinstance(parameters, dict):
        return EMAParams.from_dict(parameters)
    raise TypeError("EMAIndicator parameters must be EMAParams, dict, or None.")


ema = EMAIndicator()
