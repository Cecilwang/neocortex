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
from neocortex.models.core import (
    PRICE_BAR_CLOSE,
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

        window = resolved_parameters.window
        signal_window = resolved_parameters.signal_window
        previous_weight = (signal_window - 1) / signal_window
        current_weight = 1.0 / signal_window
        k: list[float | None] = [None] * len(bars)
        d: list[float | None] = [None] * len(bars)
        j: list[float | None] = [None] * len(bars)
        frame = bars.bars
        previous_rsv: float = 50
        previous_k: float = 50
        previous_d: float = 50

        for end in range(window - 1, len(bars)):
            window_frame = frame.iloc[end - window + 1 : end + 1]
            highest = float(window_frame[PRICE_BAR_HIGH].max())
            lowest = float(window_frame[PRICE_BAR_LOW].min())
            if highest == lowest:
                rsv = previous_rsv
            else:
                latest_close = float(window_frame[PRICE_BAR_CLOSE].iloc[-1])
                rsv = 100.0 * ((latest_close - lowest) / (highest - lowest))

            latest_k = (previous_weight * previous_k) + (current_weight * rsv)
            latest_d = (previous_weight * previous_d) + (current_weight * latest_k)
            latest_j = (3 * latest_k) - (2 * latest_d)

            k[end] = latest_k
            d[end] = latest_d
            j[end] = latest_j
            previous_rsv = rsv
            previous_k = latest_k
            previous_d = latest_d

        result_frame = pd.DataFrame(
            {
                PRICE_BAR_TIMESTAMP: bars.timestamps,
                "k": pd.Series(k, dtype=object),
                "d": pd.Series(d, dtype=object),
                "j": pd.Series(j, dtype=object),
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
