"""Moving average convergence divergence indicator."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from neocortex.indicators.core import (
    Indicator,
    IndicatorParams,
    IndicatorSpec,
    coerce_indicator_params,
    log_indicator_calculation,
)
from neocortex.indicators.ema import calculate_ema_series
from neocortex.models.core import PRICE_BAR_TIMESTAMP, PriceSeries
import pandas as pd


@dataclass(frozen=True, slots=True)
class MACDParams(IndicatorParams):
    """Parameters for MACD line, signal line, and histogram."""

    fast_window: int = 12
    slow_window: int = 26
    signal_window: int = 9
    normalization: Literal["close", "slow", "fast"] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.fast_window, int):
            raise ValueError("fast_window must be an integer.")
        if not isinstance(self.slow_window, int):
            raise ValueError("slow_window must be an integer.")
        if not isinstance(self.signal_window, int):
            raise ValueError("signal_window must be an integer.")
        if self.fast_window <= 0:
            raise ValueError("fast_window must be a positive integer.")
        if self.slow_window <= 0:
            raise ValueError("slow_window must be a positive integer.")
        if self.signal_window <= 0:
            raise ValueError("signal_window must be a positive integer.")
        if self.fast_window >= self.slow_window:
            raise ValueError("fast_window must be smaller than slow_window.")
        if self.normalization not in (None, "close", "slow", "fast"):
            raise ValueError(
                "normalization must be one of None, 'close', 'slow', or 'fast'."
            )


@dataclass(frozen=True, slots=True)
class MACDIndicator(IndicatorSpec):
    """Calculate the normalized MACD line over close prices."""

    key: str = "macd"
    display_name: str = "Moving Average Convergence Divergence"
    category: str = "momentum"
    formula: str = "EMA_fast - EMA_slow, paired with EMA signal line and histogram."
    interpretation: str = "Momentum/trend oscillator commonly read together with its signal line and histogram."

    def calculate(
        self,
        bars: PriceSeries,
        *,
        parameters: MACDParams | dict[str, object] | None = None,
    ) -> MACD:
        resolved_parameters = coerce_indicator_params(MACDParams, parameters)
        log_indicator_calculation(
            indicator_key=self.key,
            bars=bars,
            parameters=resolved_parameters,
        )
        closes = bars.closes
        if closes.empty:
            frame = pd.DataFrame(
                {
                    PRICE_BAR_TIMESTAMP: pd.Series(dtype="datetime64[ns]"),
                    "macd": pd.Series(dtype=object),
                    "signal": pd.Series(dtype=object),
                    "hist": pd.Series(dtype=object),
                }
            )
            return MACD(spec=self, parameters=resolved_parameters, data=frame)

        fast_ema = calculate_ema_series(closes, resolved_parameters.fast_window)
        slow_ema = calculate_ema_series(closes, resolved_parameters.slow_window)
        macd_values: list[float | None] = []
        for fast, slow, close in zip(
            fast_ema.tolist(), slow_ema.tolist(), closes, strict=False
        ):
            if fast is None or slow is None:
                macd_values.append(None)
                continue
            diff = fast - slow
            if resolved_parameters.normalization is None:
                macd_values.append(diff)
                continue

            if resolved_parameters.normalization == "close":
                divisor = float(close)
            elif resolved_parameters.normalization == "slow":
                divisor = slow
            elif resolved_parameters.normalization == "fast":
                divisor = fast
            else:
                raise ValueError(
                    "normalization must be one of None, 'close', 'slow', or 'fast'."
                )
            macd_values.append(0.0 if divisor == 0 else diff / divisor)
        macd_series = pd.Series(macd_values, dtype=object)

        filtered_values = macd_series[macd_series.notna()]
        if filtered_values.empty:
            signal_series = pd.Series([None] * len(macd_series), dtype=object)
        else:
            raw_signal_values = calculate_ema_series(
                filtered_values, resolved_parameters.signal_window
            )
            signal_values: list[float | None] = []
            signal_index = 0
            for value in macd_series.tolist():
                if value is None:
                    signal_values.append(None)
                    continue
                signal_values.append(raw_signal_values.iloc[signal_index])
                signal_index += 1
            signal_series = pd.Series(signal_values, dtype=object)
        hist_values = [
            None
            if macd_value is None or signal_value is None
            else macd_value - signal_value
            for macd_value, signal_value in zip(
                macd_series.tolist(), signal_series.tolist(), strict=False
            )
        ]

        frame = pd.DataFrame(
            {
                PRICE_BAR_TIMESTAMP: bars.timestamps,
                "macd": macd_series,
                "signal": signal_series,
                "hist": pd.Series(hist_values, dtype=object),
            }
        )
        return MACD(
            spec=self,
            parameters=resolved_parameters,
            data=frame,
        )


@dataclass(frozen=True, slots=True)
class MACD(Indicator):
    @property
    def macd(self) -> pd.Series:
        return self.data["macd"]

    @property
    def signal(self) -> pd.Series:
        return self.data["signal"]

    @property
    def hist(self) -> pd.Series:
        return self.data["hist"]
macd = MACDIndicator()
