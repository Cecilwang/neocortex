"""Rate of change indicator."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from neocortex.indicators.core import (
    Indicator,
    IndicatorParams,
    IndicatorSpec,
    log_indicator_calculation,
)
from neocortex.models.core import PRICE_BAR_TIMESTAMP, PriceSeries


@dataclass(frozen=True, slots=True)
class ROCParams(IndicatorParams):
    """Parameters for rate of change."""

    period: int = 20

    def __post_init__(self) -> None:
        if self.period <= 0:
            raise ValueError("period must be a positive integer.")


@dataclass(frozen=True, slots=True)
class ROCIndicator(IndicatorSpec):
    """Calculate percentage change versus the close N periods ago."""

    key: str = "roc"
    display_name: str = "Rate of Change"
    category: str = "momentum"
    formula: str = "((close_t - close_t-n) / close_t-n) * 100"
    interpretation: str = (
        "Momentum oscillator showing percentage price change over a fixed lookback."
    )

    def calculate(
        self,
        bars: PriceSeries,
        *,
        parameters: ROCParams | dict[str, object] | None = None,
    ) -> ROC:
        resolved_parameters = _coerce_params(parameters)
        log_indicator_calculation(
            indicator_key=self.key,
            bars=bars,
            parameters=resolved_parameters,
        )
        period = resolved_parameters.period
        closes = bars.closes
        base = closes.shift(period)
        values = ((closes - base) / base) * 100.0
        values = values.where(base != 0)
        frame = pd.DataFrame(
            {
                PRICE_BAR_TIMESTAMP: bars.timestamps,
                "value": values.astype(object).where(values.notna(), None),
            }
        )
        return ROC(
            spec=self,
            parameters=resolved_parameters,
            data=frame,
        )


@dataclass(frozen=True, slots=True)
class ROC(Indicator):
    @property
    def roc(self) -> pd.Series:
        return self.data["value"]


def _coerce_params(parameters: ROCParams | dict[str, object] | None) -> ROCParams:
    if parameters is None:
        return ROCParams()
    if isinstance(parameters, ROCParams):
        return parameters
    if isinstance(parameters, dict):
        return ROCParams.from_dict(parameters)
    raise TypeError("ROCIndicator parameters must be ROCParams, dict, or None.")


roc = ROCIndicator()
