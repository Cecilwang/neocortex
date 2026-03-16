"""Exponential moving average indicator."""

from __future__ import annotations

from dataclasses import dataclass

from neocortex.indicators.core import (
    IndicatorParams,
    IndicatorPoint,
    IndicatorSeries,
    IndicatorSpec,
)
from neocortex.models.core import PriceSeries


@dataclass(frozen=True, slots=True)
class EMAParams(IndicatorParams):
    """Parameters for exponential moving average."""

    window: int = 20

    def __post_init__(self) -> None:
        if self.window <= 0:
            raise ValueError("window must be a positive integer.")


@dataclass(frozen=True, slots=True)
class EMAIndicator:
    """Calculate an exponential moving average over close prices."""

    spec: IndicatorSpec = IndicatorSpec(
        key="ema",
        display_name="Exponential Moving Average",
        category="trend",
        input_field="close",
        formula="Recursively weighted average that gives more weight to recent closes.",
        interpretation="Responds faster to recent prices than SMA while still tracking trend.",
    )

    def calculate(
        self,
        bars: PriceSeries,
        *,
        parameters: EMAParams | dict[str, object] | None = None,
    ) -> IndicatorSeries:
        resolved_parameters = _coerce_params(parameters)
        window = resolved_parameters.window
        closes = bars.closes
        timestamps = bars.timestamps
        if not closes:
            return IndicatorSeries(
                spec=self.spec,
                parameters=resolved_parameters,
                points=(),
            )
        if len(closes) < window:
            return IndicatorSeries(
                spec=self.spec,
                parameters=resolved_parameters,
                points=tuple(
                    IndicatorPoint(timestamp=timestamp, value=None)
                    for timestamp in timestamps
                ),
            )

        alpha = 2 / (window + 1)
        seed = sum(closes[:window]) / window
        points: list[IndicatorPoint] = [
            IndicatorPoint(timestamp=timestamp, value=None)
            for timestamp in timestamps[: window - 1]
        ]
        ema = seed
        points.append(IndicatorPoint(timestamp=timestamps[window - 1], value=ema))
        for timestamp, close in zip(timestamps[window:], closes[window:]):
            ema = (close * alpha) + (ema * (1 - alpha))
            points.append(IndicatorPoint(timestamp=timestamp, value=ema))

        return IndicatorSeries(
            spec=self.spec,
            parameters=resolved_parameters,
            points=tuple(points),
        )


def _coerce_params(parameters: EMAParams | dict[str, object] | None) -> EMAParams:
    if parameters is None:
        return EMAParams()
    if isinstance(parameters, EMAParams):
        return parameters
    if isinstance(parameters, dict):
        return EMAParams.from_dict(parameters)
    raise TypeError("EMAIndicator parameters must be EMAParams, dict, or None.")


ema = EMAIndicator()
