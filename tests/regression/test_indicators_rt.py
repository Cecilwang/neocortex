from dataclasses import asdict
from datetime import datetime

from neocortex.models import Exchange, Market, PriceBar, PriceSeries, SecurityId


def _build_bar(
    security_id: SecurityId,
    day: int,
    close: float,
    volume: float,
) -> PriceBar:
    return PriceBar(
        security_id=security_id,
        timestamp=datetime(2026, 2, day, 15, 0),
        open=close - 1.0,
        high=close + 2.0,
        low=close - 2.0,
        close=close,
        volume=volume,
    )


def test_indicator_output_baseline() -> None:
    from neocortex.indicators import calculate_indicator

    security_id = SecurityId(symbol="AAPL", market=Market.US, exchange=Exchange.XNAS)
    bars = PriceSeries(
        security_id=security_id,
        bars=(
            _build_bar(security_id, 3, 210.0, 10_000_000.0),
            _build_bar(security_id, 4, 212.0, 12_000_000.0),
            _build_bar(security_id, 5, 211.0, 11_000_000.0),
            _build_bar(security_id, 6, 214.0, 13_000_000.0),
            _build_bar(security_id, 7, 216.0, 14_000_000.0),
            _build_bar(security_id, 10, 215.0, 12_500_000.0),
        ),
    )

    assert {
        "sma_3": [point.value for point in calculate_indicator("sma", bars, parameters={"window": 3}).points],
        "ema_3": [point.value for point in calculate_indicator("ema", bars, parameters={"window": 3}).points],
        "rsi_3": [point.value for point in calculate_indicator("rsi", bars, parameters={"period": 3}).points],
        "params": {
            "sma": asdict(calculate_indicator("sma", bars, parameters={"window": 3}).parameters),
            "ema": asdict(calculate_indicator("ema", bars, parameters={"window": 3}).parameters),
            "rsi": asdict(calculate_indicator("rsi", bars, parameters={"period": 3}).parameters),
        },
    } == {
        "sma_3": [
            None,
            None,
            211.0,
            212.33333333333334,
            213.66666666666666,
            215.0,
        ],
        "ema_3": [
            None,
            None,
            211.0,
            212.5,
            214.25,
            214.625,
        ],
        "rsi_3": [
            None,
            None,
            None,
            83.33333333333334,
            88.88888888888889,
            71.11111111111111,
        ],
        "params": {
            "sma": {"window": 3},
            "ema": {"window": 3},
            "rsi": {"period": 3},
        },
    }
