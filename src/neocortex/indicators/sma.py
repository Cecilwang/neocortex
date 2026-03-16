"""Simple moving average indicator."""

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
class SMAParams(IndicatorParams):
    """Parameters for simple moving average."""

    window: int = 20

    def __post_init__(self) -> None:
        if self.window <= 0:
            raise ValueError("window must be a positive integer.")


@dataclass(frozen=True, slots=True)
class SMAIndicator:
    """Calculate a simple moving average over close prices."""

    spec: IndicatorSpec = IndicatorSpec(
        key="sma",
        display_name="Simple Moving Average",
        category="trend",
        input_field="close",
        formula="Average of the last N closing prices.",
        interpretation="Smooths price noise and highlights the prevailing trend direction.",
    )

    def calculate(
        self,
        bars: PriceSeries,
        *,
        parameters: SMAParams | dict[str, object] | None = None,
    ) -> IndicatorSeries:
        resolved_parameters = _coerce_params(parameters)
        window = resolved_parameters.window
        closes = bars.closes
        timestamps = bars.timestamps
        points: list[IndicatorPoint] = []
        for index, timestamp in enumerate(timestamps):
            if index + 1 < window:
                points.append(IndicatorPoint(timestamp=timestamp, value=None))
                continue
            window_values = closes[index - window + 1 : index + 1]
            points.append(
                IndicatorPoint(
                    timestamp=timestamp,
                    value=sum(window_values) / window,
                )
            )

        return IndicatorSeries(
            spec=self.spec,
            parameters=resolved_parameters,
            points=tuple(points),
        )


def _coerce_params(parameters: SMAParams | dict[str, object] | None) -> SMAParams:
    if parameters is None:
        return SMAParams()
    if isinstance(parameters, SMAParams):
        return parameters
    if isinstance(parameters, dict):
        return SMAParams.from_dict(parameters)
    raise TypeError("SMAIndicator parameters must be SMAParams, dict, or None.")


sma = SMAIndicator()
