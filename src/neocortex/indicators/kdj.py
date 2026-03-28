"""KDJ indicator."""

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
from neocortex.indicators.ta_lib_backend import kdj as calculate_kdj_series
from neocortex.models.core import (
    PRICE_BAR_HIGH,
    PRICE_BAR_LOW,
    PRICE_BAR_TIMESTAMP,
    PriceSeries,
)


@dataclass(frozen=True, slots=True)
class KDJParams(IndicatorParams):
    """Parameters for KDJ."""

    window: int = 9
    signal_window: int = 3

    def __post_init__(self) -> None:
        if not isinstance(self.window, int):
            raise ValueError("window must be an integer.")
        if not isinstance(self.signal_window, int):
            raise ValueError("signal_window must be an integer.")
        if self.window <= 0:
            raise ValueError("window must be a positive integer.")
        if self.signal_window <= 0:
            raise ValueError("signal_window must be a positive integer.")


@dataclass(frozen=True, slots=True)
class KDJIndicator(IndicatorSpec):
    """Calculate %K, %D, and %J over rolling price windows."""

    key: str = "kdj"
    display_name: str = "KDJ"
    category: str = "momentum"
    formula: str = "RSV over N periods, then K and D are recursively smoothed with (signal_window - 1) / signal_window previous value plus 1 / signal_window current input; J = 3 * K - 2 * D."
    interpretation: str = (
        "Oscillator family used to gauge short-term momentum and turning points."
    )

    def calculate(
        self,
        bars: PriceSeries,
        *,
        parameters: KDJParams | dict[str, object] | None = None,
    ) -> KDJ:
        resolved_parameters = coerce_indicator_params(KDJParams, parameters)
        log_indicator_calculation(
            indicator_key=self.key,
            bars=bars,
            parameters=resolved_parameters,
        )
        if bars.bars.empty:
            frame = pd.DataFrame(
                {
                    PRICE_BAR_TIMESTAMP: pd.Series(dtype="datetime64[ns]"),
                    "k": pd.Series(dtype=object),
                    "d": pd.Series(dtype=object),
                    "j": pd.Series(dtype=object),
                }
            )
            return KDJ(spec=self, parameters=resolved_parameters, data=frame)

        frame = bars.bars
        k, d, j = calculate_kdj_series(
            frame[PRICE_BAR_HIGH],
            frame[PRICE_BAR_LOW],
            bars.closes,
            window=resolved_parameters.window,
            signal_window=resolved_parameters.signal_window,
        )

        result_frame = pd.DataFrame(
            {
                PRICE_BAR_TIMESTAMP: bars.timestamps,
                "k": k,
                "d": d,
                "j": j,
            }
        )
        return KDJ(spec=self, parameters=resolved_parameters, data=result_frame)


@dataclass(frozen=True, slots=True)
class KDJ(Indicator):
    @property
    def k(self) -> pd.Series:
        return self.data["k"]

    @property
    def d(self) -> pd.Series:
        return self.data["d"]

    @property
    def j(self) -> pd.Series:
        return self.data["j"]


kdj = KDJIndicator()
