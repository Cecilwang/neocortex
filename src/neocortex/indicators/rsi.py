"""Relative strength index indicator."""

from __future__ import annotations

from dataclasses import dataclass

from neocortex.indicators.core import (
    Indicator,
    IndicatorParams,
    IndicatorSpec,
    log_indicator_calculation,
)
from neocortex.models.core import PRICE_BAR_TIMESTAMP, PriceSeries
import pandas as pd


def _rsi_value(average_gain: float, average_loss: float) -> float:
    if average_loss == 0:
        return 100.0
    relative_strength = average_gain / average_loss
    return 100 - (100 / (1 + relative_strength))


@dataclass(frozen=True, slots=True)
class RSIParams(IndicatorParams):
    """Parameters for relative strength index."""

    period: int = 14

    def __post_init__(self) -> None:
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
        resolved_parameters = _coerce_params(parameters)
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

        values: list[float | None] = [None] * len(closes)
        if len(closes) <= period:
            frame = pd.DataFrame(
                {
                    PRICE_BAR_TIMESTAMP: bars.timestamps,
                    "value": pd.Series(values, dtype=object),
                }
            )
            return RSI(
                spec=self,
                parameters=resolved_parameters,
                data=frame,
            )

        changes = closes.diff().iloc[1:]
        gains = [max(change, 0.0) for change in changes]
        losses = [abs(min(change, 0.0)) for change in changes]

        average_gain = sum(gains[:period]) / period
        average_loss = sum(losses[:period]) / period
        values[period] = _rsi_value(average_gain, average_loss)

        for index in range(period + 1, len(bars)):
            gain = gains[index - 1]
            loss = losses[index - 1]
            average_gain = ((average_gain * (period - 1)) + gain) / period
            average_loss = ((average_loss * (period - 1)) + loss) / period
            values[index] = _rsi_value(average_gain, average_loss)

        frame = pd.DataFrame(
            {
                PRICE_BAR_TIMESTAMP: bars.timestamps,
                "value": pd.Series(values, dtype=object),
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


def _coerce_params(parameters: RSIParams | dict[str, object] | None) -> RSIParams:
    if parameters is None:
        return RSIParams()
    if isinstance(parameters, RSIParams):
        return parameters
    if isinstance(parameters, dict):
        return RSIParams.from_dict(parameters)
    raise TypeError("RSIIndicator parameters must be RSIParams, dict, or None.")


rsi = RSIIndicator()
