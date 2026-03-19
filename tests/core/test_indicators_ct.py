from dataclasses import asdict
from datetime import datetime

import pytest

from neocortex.models import (
    Exchange,
    Market,
    PriceBar,
    PriceSeries,
    SecurityId,
)


def _build_bar(
    security_id: SecurityId,
    day: int,
    close: float,
) -> PriceBar:
    return PriceBar(
        security_id=security_id,
        timestamp=datetime(2026, 3, day, 15, 0),
        open=close - 1.0,
        high=close + 1.0,
        low=close - 2.0,
        close=close,
        volume=1_000_000.0 + day,
    )


@pytest.fixture
def sample_bars() -> PriceSeries:
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    closes = (100.0, 102.0, 101.0, 105.0, 107.0)
    return PriceSeries(
        security_id=security_id,
        bars=tuple(
            _build_bar(security_id, day=10 + index, close=close)
            for index, close in enumerate(closes)
        ),
    )


@pytest.fixture
def flat_kdj_sample_bars() -> PriceSeries:
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    bars = tuple(
        PriceBar(
            security_id=security_id,
            timestamp=datetime(2026, 4, 1 + index, 15, 0),
            open=100.0,
            high=100.0,
            low=100.0,
            close=100.0,
            volume=1_000_000.0 + index,
        )
        for index in range(11)
    )
    return PriceSeries(security_id=security_id, bars=bars)


@pytest.fixture
def kdj_sample_bars() -> PriceSeries:
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    closes = (
        100.0,
        102.0,
        101.0,
        105.0,
        107.0,
        106.0,
        108.0,
        109.0,
        110.0,
        111.0,
        109.0,
        112.0,
    )
    return PriceSeries(
        security_id=security_id,
        bars=tuple(
            _build_bar(security_id, day=10 + index, close=close)
            for index, close in enumerate(closes)
        ),
    )


def test_indicator_registry_exposes_minimal_metadata() -> None:
    from neocortex.indicators import list_indicator_specs
    from neocortex.indicators.ema import EMAParams
    from neocortex.indicators.kdj import KDJParams
    from neocortex.indicators.macd import MACDParams
    from neocortex.indicators.rsi import RSIParams
    from neocortex.indicators.sma import SMAParams

    indicators = {indicator.key: indicator for indicator in list_indicator_specs()}

    assert tuple(indicators) == ("sma", "ema", "rsi", "macd", "kdj")
    assert indicators["sma"].display_name == "Simple Moving Average"
    assert asdict(SMAParams()) == {"window": 20}
    assert "average" in indicators["sma"].formula.lower()
    assert asdict(EMAParams()) == {"window": 20}
    assert "recent prices" in indicators["ema"].interpretation.lower()
    assert asdict(RSIParams()) == {"period": 14}
    assert "momentum" in indicators["rsi"].interpretation.lower()
    assert asdict(MACDParams()) == {
        "fast_window": 12,
        "slow_window": 26,
        "signal_window": 9,
        "normalization": None,
    }
    assert "signal line" in indicators["macd"].formula.lower()
    assert asdict(KDJParams()) == {"window": 9, "signal_window": 3}
    assert "rsv" in indicators["kdj"].formula.lower()
    assert "j = 3 * k - 2 * d" in indicators["kdj"].formula.lower()


def test_indicator_params_can_be_built_from_dict() -> None:
    from neocortex.indicators.ema import EMAParams
    from neocortex.indicators.kdj import KDJParams
    from neocortex.indicators.macd import MACDParams
    from neocortex.indicators.rsi import RSIParams
    from neocortex.indicators.sma import SMAParams

    assert SMAParams.from_dict(None) == SMAParams(window=20)
    assert SMAParams.from_dict({"window": 3}) == SMAParams(window=3)
    assert EMAParams.from_dict({"window": 5}) == EMAParams(window=5)
    assert RSIParams.from_dict({"period": 7}) == RSIParams(period=7)
    assert KDJParams.from_dict({"window": 10, "signal_window": 5}) == KDJParams(
        window=10,
        signal_window=5,
    )
    assert MACDParams.from_dict(
        {
            "fast_window": 3,
            "slow_window": 4,
            "signal_window": 2,
            "normalization": "close",
        }
    ) == MACDParams(
        fast_window=3,
        slow_window=4,
        signal_window=2,
        normalization="close",
    )


def test_calculate_indicator_returns_aligned_sma_series(
    sample_bars: PriceSeries,
) -> None:
    from neocortex.indicators.sma import SMAParams, sma

    result = sma.calculate(sample_bars, parameters=SMAParams(window=3))

    assert result.spec.key == "sma"
    assert result.parameters == SMAParams(window=3)
    assert [timestamp.isoformat() for timestamp in result.timestamp] == [
        "2026-03-10T15:00:00",
        "2026-03-11T15:00:00",
        "2026-03-12T15:00:00",
        "2026-03-13T15:00:00",
        "2026-03-14T15:00:00",
    ]
    assert result.sma.tolist() == [
        None,
        None,
        101.0,
        102.66666666666667,
        104.33333333333333,
    ]


def test_calculate_indicator_returns_ema_series(
    sample_bars: PriceSeries,
) -> None:
    from neocortex.indicators.ema import EMAParams, ema

    result = ema.calculate(sample_bars, parameters=EMAParams(window=3))

    assert result.ema.tolist() == [
        None,
        None,
        101.0,
        103.0,
        105.0,
    ]


def test_calculate_indicator_returns_rsi_series(
    sample_bars: PriceSeries,
) -> None:
    from neocortex.indicators import calculate_indicator

    result = calculate_indicator("rsi", sample_bars, parameters={"period": 3})

    assert result.rsi.tolist() == [
        None,
        None,
        None,
        85.71428571428571,
        90.0,
    ]


def test_calculate_macd_indicator_returns_aligned_series(
    sample_bars: PriceSeries,
) -> None:
    from neocortex.indicators import calculate_indicator
    from neocortex.indicators.macd import MACDParams

    result = calculate_indicator(
        "macd",
        sample_bars,
        parameters=MACDParams(fast_window=3, slow_window=4, signal_window=2),
    )

    assert result.macd.tolist() == [
        None,
        None,
        None,
        1.0,
        1.0,
    ]
    assert result.signal.tolist() == [
        None,
        None,
        None,
        None,
        1.0,
    ]
    assert result.hist.tolist() == [
        None,
        None,
        None,
        None,
        0.0,
    ]


def test_calculate_macd_indicator_supports_close_normalization(
    sample_bars: PriceSeries,
) -> None:
    from neocortex.indicators import calculate_indicator

    result = calculate_indicator(
        "macd",
        sample_bars,
        parameters={
            "fast_window": 3,
            "slow_window": 4,
            "signal_window": 2,
            "normalization": "close",
        },
    )

    assert result.macd.tolist() == [
        None,
        None,
        None,
        0.009523809523809525,
        0.009345794392523364,
    ]
    assert result.signal.tolist() == [
        None,
        None,
        None,
        None,
        0.009434801958166445,
    ]
    assert result.hist.tolist() == [
        None,
        None,
        None,
        None,
        -8.900756564308131e-05,
    ]


def test_calculate_kdj_indicator_returns_aligned_series(
    kdj_sample_bars: PriceSeries,
) -> None:
    from neocortex.indicators import calculate_indicator
    from neocortex.indicators.kdj import KDJParams

    result = calculate_indicator(
        "kdj",
        kdj_sample_bars,
        parameters=KDJParams(window=9, signal_window=3),
    )

    assert result.k.tolist() == [
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        64.10256410256409,
        73.5042735042735,
        74.64387464387464,
        79.76258309591643,
    ]
    assert result.d.tolist() == [
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        54.70085470085469,
        60.968660968660956,
        65.52706552706552,
        70.27223805001582,
    ]
    assert result.j.tolist() == [
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        82.90598290598288,
        98.57549857549859,
        92.87749287749287,
        98.74327318771765,
    ]


def test_calculate_kdj_indicator_uses_fifty_seed_when_range_is_flat(
    flat_kdj_sample_bars: PriceSeries,
) -> None:
    from neocortex.indicators import calculate_indicator

    result = calculate_indicator("kdj", flat_kdj_sample_bars)

    assert result.k.tolist() == [
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        49.99999999999999,
        49.99999999999999,
        49.99999999999999,
    ]
    assert result.d.tolist() == [
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        49.99999999999999,
        49.99999999999999,
        49.99999999999999,
    ]
    assert result.j.tolist() == [
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        49.999999999999986,
        49.999999999999986,
        49.999999999999986,
    ]


def test_calculate_indicator_returns_empty_series_for_empty_input() -> None:
    from neocortex.indicators.sma import SMAParams, sma
    from neocortex.models import PriceSeries

    result = sma.calculate(
        PriceSeries(
            security_id=SecurityId(
                symbol="600519",
                market=Market.CN,
                exchange=Exchange.XSHG,
            ),
            bars=(),
        ),
        parameters=SMAParams(window=3),
    )

    assert result.data.empty
    assert result.parameters == SMAParams(window=3)


@pytest.mark.parametrize(
    ("key", "parameters"),
    [
        ("unknown", None),
        ("sma", {"window": 0}),
        ("ema", {"window": -1}),
        ("rsi", {"period": 0}),
        ("kdj", {"window": 0}),
        ("kdj", {"window": 9, "signal_window": 0}),
        ("macd", {"fast_window": 5, "slow_window": 5, "signal_window": 2}),
        (
            "macd",
            {
                "fast_window": 3,
                "slow_window": 5,
                "signal_window": 2,
                "normalization": "weird",
            },
        ),
    ],
)
def test_calculate_indicator_rejects_unknown_or_invalid_requests(
    sample_bars: PriceSeries,
    key: str,
    parameters: dict[str, int] | None,
) -> None:
    from neocortex.indicators import calculate_indicator

    if key == "unknown":
        with pytest.raises(KeyError) as exc_info:
            calculate_indicator(key, sample_bars, parameters=parameters)

        assert exc_info.value.args == ("unknown",)
        return

    with pytest.raises(ValueError):
        calculate_indicator(key, sample_bars, parameters=parameters)


def test_price_series_preserves_sequence_invariants() -> None:
    from neocortex.models import PriceSeries
    from neocortex.models import PRICE_BAR_CLOSE

    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    first_bar = _build_bar(security_id, day=10, close=100.0)
    second_bar = _build_bar(security_id, day=11, close=101.0)
    series = PriceSeries(security_id=security_id, bars=(first_bar, second_bar))

    assert len(series) == 2
    assert series.start_timestamp == first_bar.timestamp
    assert series.end_timestamp == second_bar.timestamp
    assert series.timestamps.tolist() == [
        first_bar.timestamp,
        second_bar.timestamp,
    ]
    assert series.closes.tolist() == [100.0, 101.0]
    assert series.bars[PRICE_BAR_CLOSE].tolist() == [100.0, 101.0]


def test_price_series_rejects_mismatched_security_or_unsorted_bars() -> None:
    from neocortex.models import PriceSeries

    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    other_security_id = SecurityId(
        symbol="000001",
        market=Market.CN,
        exchange=Exchange.XSHE,
    )
    with pytest.raises(ValueError, match="same security_id"):
        PriceSeries(
            security_id=security_id,
            bars=(
                _build_bar(security_id, day=10, close=100.0),
                _build_bar(other_security_id, day=11, close=101.0),
            ),
        )

    with pytest.raises(ValueError, match="sorted by timestamp"):
        PriceSeries(
            security_id=security_id,
            bars=(
                _build_bar(security_id, day=11, close=101.0),
                _build_bar(security_id, day=10, close=100.0),
            ),
        )
