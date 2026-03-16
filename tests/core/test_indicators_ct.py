from dataclasses import asdict
from datetime import datetime

import pytest

from neocortex.models import Exchange, Market, PriceBar, PriceSeries, SecurityId


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


def test_indicator_registry_exposes_minimal_metadata() -> None:
    from neocortex.indicators import list_indicator_specs
    from neocortex.indicators.ema import EMAParams
    from neocortex.indicators.rsi import RSIParams
    from neocortex.indicators.sma import SMAParams

    specs = {spec.key: spec for spec in list_indicator_specs()}

    assert tuple(specs) == ("sma", "ema", "rsi")
    assert specs["sma"].display_name == "Simple Moving Average"
    assert asdict(SMAParams()) == {"window": 20}
    assert "average" in specs["sma"].formula.lower()
    assert asdict(EMAParams()) == {"window": 20}
    assert "recent prices" in specs["ema"].interpretation.lower()
    assert asdict(RSIParams()) == {"period": 14}
    assert "momentum" in specs["rsi"].interpretation.lower()


def test_indicator_params_can_be_built_from_dict() -> None:
    from neocortex.indicators.ema import EMAParams
    from neocortex.indicators.rsi import RSIParams
    from neocortex.indicators.sma import SMAParams

    assert SMAParams.from_dict(None) == SMAParams(window=20)
    assert SMAParams.from_dict({"window": 3}) == SMAParams(window=3)
    assert EMAParams.from_dict({"window": 5}) == EMAParams(window=5)
    assert RSIParams.from_dict({"period": 7}) == RSIParams(period=7)


def test_calculate_indicator_returns_aligned_sma_series(
    sample_bars: PriceSeries,
) -> None:
    from neocortex.indicators.sma import SMAParams, sma

    series = sma.calculate(sample_bars, parameters=SMAParams(window=3))

    assert series.spec.key == "sma"
    assert series.parameters == SMAParams(window=3)
    assert [point.timestamp.isoformat() for point in series.points] == [
        "2026-03-10T15:00:00",
        "2026-03-11T15:00:00",
        "2026-03-12T15:00:00",
        "2026-03-13T15:00:00",
        "2026-03-14T15:00:00",
    ]
    assert [point.value for point in series.points] == [
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

    series = ema.calculate(sample_bars, parameters=EMAParams(window=3))

    assert [point.value for point in series.points] == [
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

    series = calculate_indicator("rsi", sample_bars, parameters={"period": 3})

    assert [point.value for point in series.points] == [
        None,
        None,
        None,
        85.71428571428571,
        90.0,
    ]


def test_calculate_indicator_returns_empty_series_for_empty_input() -> None:
    from neocortex.indicators.sma import SMAParams, sma
    from neocortex.models import PriceSeries

    series = sma.calculate(
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

    assert series.points == ()
    assert series.parameters == SMAParams(window=3)


@pytest.mark.parametrize(
    ("key", "parameters"),
    [
        ("unknown", None),
        ("sma", {"window": 0}),
        ("ema", {"window": -1}),
        ("rsi", {"period": 0}),
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

    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    first_bar = _build_bar(security_id, day=10, close=100.0)
    second_bar = _build_bar(security_id, day=11, close=101.0)
    series = PriceSeries(security_id=security_id, bars=(first_bar, second_bar))

    assert len(series) == 2
    assert series[0] == first_bar
    assert series.start_timestamp == first_bar.timestamp
    assert series.end_timestamp == second_bar.timestamp
    assert series.timestamps == (first_bar.timestamp, second_bar.timestamp)
    assert series.closes == (100.0, 101.0)


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
