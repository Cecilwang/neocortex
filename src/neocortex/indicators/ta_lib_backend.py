"""Thin TA-Lib wrapper shared by indicator implementations."""

from __future__ import annotations

from typing import Literal, TypeAlias

import numpy as np
import pandas as pd
import talib


NumericSeries: TypeAlias = pd.Series
MACDNormalization: TypeAlias = Literal["close", "slow", "fast"] | None


def to_float_array(values: NumericSeries) -> np.ndarray:
    """Convert a numeric pandas series into a float array for TA-Lib."""

    return values.astype(float).to_numpy(dtype=float, copy=False)


def to_nullable_series(
    values: np.ndarray | list[float],
    *,
    index: pd.Index | None = None,
) -> pd.Series:
    """Convert TA-Lib output arrays into object series with None warmups."""

    series = pd.Series(values, index=index)
    return series.astype(object).where(series.notna(), None)


def sma(values: NumericSeries, *, window: int) -> pd.Series:
    return to_nullable_series(
        talib.SMA(to_float_array(values), timeperiod=window),
        index=values.index,
    )


def ema(values: NumericSeries, *, window: int) -> pd.Series:
    return to_nullable_series(
        talib.EMA(to_float_array(values), timeperiod=window),
        index=values.index,
    )


def roc(values: NumericSeries, *, period: int) -> pd.Series:
    return to_nullable_series(
        talib.ROC(to_float_array(values), timeperiod=period),
        index=values.index,
    )


def rsi(values: NumericSeries, *, period: int) -> pd.Series:
    return to_nullable_series(
        talib.RSI(to_float_array(values), timeperiod=period),
        index=values.index,
    )


def macd(
    values: NumericSeries,
    *,
    fast_window: int,
    slow_window: int,
    signal_window: int,
    normalization: MACDNormalization = None,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    if normalization is not None:
        return normalized_macd(
            values,
            fast_window=fast_window,
            slow_window=slow_window,
            signal_window=signal_window,
            normalization=normalization,
        )

    macd_values, signal_values, hist_values = talib.MACD(
        to_float_array(values),
        fastperiod=fast_window,
        slowperiod=slow_window,
        signalperiod=signal_window,
    )
    return (
        to_nullable_series(macd_values, index=values.index),
        to_nullable_series(signal_values, index=values.index),
        to_nullable_series(hist_values, index=values.index),
    )


def rolling_max(values: NumericSeries, *, window: int) -> pd.Series:
    return to_nullable_series(
        talib.MAX(to_float_array(values), timeperiod=window),
        index=values.index,
    )


def rolling_min(values: NumericSeries, *, window: int) -> pd.Series:
    return to_nullable_series(
        talib.MIN(to_float_array(values), timeperiod=window),
        index=values.index,
    )


def normalized_macd(
    values: NumericSeries,
    *,
    fast_window: int,
    slow_window: int,
    signal_window: int,
    normalization: Literal["close", "slow", "fast"],
) -> tuple[pd.Series, pd.Series, pd.Series]:
    fast_values = ema(values, window=fast_window)
    slow_values = ema(values, window=slow_window)

    normalized_values: list[float | None] = []
    for fast, slow, close in zip(
        fast_values.tolist(),
        slow_values.tolist(),
        values.tolist(),
        strict=False,
    ):
        if fast is None or slow is None:
            normalized_values.append(None)
            continue
        diff = fast - slow
        if normalization == "close":
            divisor = float(close)
        elif normalization == "slow":
            divisor = slow
        else:
            divisor = fast
        normalized_values.append(0.0 if divisor == 0 else diff / divisor)

    macd_series = pd.Series(normalized_values, index=values.index, dtype=object)
    signal_series = _ema_over_defined_values(macd_series, window=signal_window)
    hist_series = pd.Series(
        [
            None
            if macd_value is None or signal_value is None
            else macd_value - signal_value
            for macd_value, signal_value in zip(
                macd_series.tolist(),
                signal_series.tolist(),
                strict=False,
            )
        ],
        index=values.index,
        dtype=object,
    )
    return macd_series, signal_series, hist_series


def kdj(
    highs: NumericSeries,
    lows: NumericSeries,
    closes: NumericSeries,
    *,
    window: int,
    signal_window: int,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    highest_values = rolling_max(highs, window=window)
    lowest_values = rolling_min(lows, window=window)
    rsv_values = _rsv(highest_values, lowest_values, closes)
    k_values = _seeded_smooth(rsv_values, window=signal_window)
    d_values = _seeded_smooth(k_values, window=signal_window)
    j_values = pd.Series(
        [
            None
            if k_value is None or d_value is None
            else (3 * k_value) - (2 * d_value)
            for k_value, d_value in zip(
                k_values.tolist(),
                d_values.tolist(),
                strict=False,
            )
        ],
        index=closes.index,
        dtype=object,
    )
    return k_values, d_values, j_values


def _ema_over_defined_values(values: pd.Series, *, window: int) -> pd.Series:
    defined_values = values[values.notna()].astype(float)
    if defined_values.empty:
        return pd.Series([None] * len(values), index=values.index, dtype=object)

    raw_signal_values = ema(defined_values, window=window)
    signal_values: list[float | None] = []
    signal_index = 0
    for value in values.tolist():
        if value is None:
            signal_values.append(None)
            continue
        signal_values.append(raw_signal_values.iloc[signal_index])
        signal_index += 1
    return pd.Series(signal_values, index=values.index, dtype=object)


def _rsv(
    highest_values: pd.Series,
    lowest_values: pd.Series,
    closes: NumericSeries,
) -> pd.Series:
    rsv_values: list[float | None] = []
    previous_rsv = 50.0
    for highest, lowest, close in zip(
        highest_values.tolist(),
        lowest_values.tolist(),
        closes.tolist(),
        strict=False,
    ):
        if highest is None or lowest is None:
            rsv_values.append(None)
            continue
        if highest == lowest:
            current_rsv = previous_rsv
        else:
            current_rsv = 100.0 * ((float(close) - lowest) / (highest - lowest))
        rsv_values.append(current_rsv)
        previous_rsv = current_rsv
    return pd.Series(rsv_values, index=closes.index, dtype=object)


def _seeded_smooth(values: pd.Series, *, window: int) -> pd.Series:
    previous_weight = (window - 1) / window
    current_weight = 1.0 / window
    previous_value = 50.0
    smoothed_values: list[float | None] = []

    for value in values.tolist():
        if value is None:
            smoothed_values.append(None)
            continue
        latest_value = (previous_weight * previous_value) + (current_weight * value)
        smoothed_values.append(latest_value)
        previous_value = latest_value

    return pd.Series(smoothed_values, index=values.index, dtype=object)
