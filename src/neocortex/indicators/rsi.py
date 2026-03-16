"""Relative strength index indicator."""

from __future__ import annotations

from dataclasses import dataclass

from neocortex.indicators.core import (
    IndicatorParams,
    IndicatorPoint,
    IndicatorSeries,
    IndicatorSpec,
)
from neocortex.models.core import PriceSeries


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
class RSIIndicator:
    """Calculate Wilder-style RSI over close prices."""

    spec: IndicatorSpec = IndicatorSpec(
        key="rsi",
        display_name="Relative Strength Index",
        category="momentum",
        input_field="close",
        formula="Wilder-style ratio of average gains to average losses over N periods.",
        interpretation="Momentum oscillator often used to spot overbought and oversold regimes.",
    )

    def calculate(
        self,
        bars: PriceSeries,
        *,
        parameters: RSIParams | dict[str, object] | None = None,
    ) -> IndicatorSeries:
        resolved_parameters = _coerce_params(parameters)
        period = resolved_parameters.period
        closes = bars.closes
        timestamps = bars.timestamps
        if not closes:
            return IndicatorSeries(
                spec=self.spec,
                parameters=resolved_parameters,
                points=(),
            )

        points = [
            IndicatorPoint(timestamp=timestamp, value=None)
            for timestamp in timestamps
        ]
        if len(closes) <= period:
            return IndicatorSeries(
                spec=self.spec,
                parameters=resolved_parameters,
                points=tuple(points),
            )

        changes = [current - previous for previous, current in zip(closes, closes[1:])]
        gains = [max(change, 0.0) for change in changes]
        losses = [abs(min(change, 0.0)) for change in changes]

        average_gain = sum(gains[:period]) / period
        average_loss = sum(losses[:period]) / period
        points[period] = IndicatorPoint(
            timestamp=timestamps[period],
            value=_rsi_value(average_gain, average_loss),
        )

        for index in range(period + 1, len(bars)):
            gain = gains[index - 1]
            loss = losses[index - 1]
            average_gain = ((average_gain * (period - 1)) + gain) / period
            average_loss = ((average_loss * (period - 1)) + loss) / period
            points[index] = IndicatorPoint(
                timestamp=timestamps[index],
                value=_rsi_value(average_gain, average_loss),
            )

        return IndicatorSeries(
            spec=self.spec,
            parameters=resolved_parameters,
            points=tuple(points),
        )


def _coerce_params(parameters: RSIParams | dict[str, object] | None) -> RSIParams:
    if parameters is None:
        return RSIParams()
    if isinstance(parameters, RSIParams):
        return parameters
    if isinstance(parameters, dict):
        return RSIParams.from_dict(parameters)
    raise TypeError("RSIIndicator parameters must be RSIParams, dict, or None.")


rsi = RSIIndicator()
