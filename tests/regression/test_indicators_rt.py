"""Deterministic indicator output baselines.

These are regression-style golden outputs for known input series. They are kept
under regression/ to make the pinned numeric baselines easy to locate alongside
other snapshot-style expectations.
"""

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

    sma_result = calculate_indicator("sma", bars, parameters={"window": 3})
    ema_result = calculate_indicator("ema", bars, parameters={"window": 3})
    roc_result = calculate_indicator("roc", bars, parameters={"period": 3})
    rsi_result = calculate_indicator("rsi", bars, parameters={"period": 3})
    macd_result = calculate_indicator(
        "macd",
        bars,
        parameters={"fast_window": 3, "slow_window": 4, "signal_window": 2},
    )

    assert {
        "sma_3": sma_result.sma.tolist(),
        "ema_3": ema_result.ema.tolist(),
        "roc_3": roc_result.roc.tolist(),
        "rsi_3": rsi_result.rsi.tolist(),
        "macd_3_4_2": macd_result.macd.tolist(),
        "signal_3_4_2": macd_result.signal.tolist(),
        "hist_3_4_2": macd_result.hist.tolist(),
        "params": {
            "sma": asdict(sma_result.parameters),
            "ema": asdict(ema_result.parameters),
            "roc": asdict(roc_result.parameters),
            "rsi": asdict(rsi_result.parameters),
            "macd": asdict(macd_result.parameters),
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
        "roc_3": [
            None,
            None,
            None,
            1.9047619047619049,
            1.8867924528301887,
            1.8957345971563981,
        ],
        "rsi_3": [
            None,
            None,
            None,
            83.33333333333334,
            88.88888888888889,
            71.11111111111111,
        ],
        "macd_3_4_2": [
            None,
            None,
            None,
            0.75,
            0.8000000000000114,
            0.5550000000000068,
        ],
        "signal_3_4_2": [
            None,
            None,
            None,
            None,
            0.7750000000000057,
            0.6283333333333398,
        ],
        "hist_3_4_2": [
            None,
            None,
            None,
            None,
            0.025000000000005684,
            -0.07333333333333303,
        ],
        "params": {
            "sma": {"window": 3},
            "ema": {"window": 3},
            "roc": {"period": 3},
            "rsi": {"period": 3},
            "macd": {
                "fast_window": 3,
                "slow_window": 4,
                "signal_window": 2,
                "normalization": None,
            },
        },
    }


def test_kdj_output_baseline() -> None:
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
            _build_bar(security_id, 11, 217.0, 12_800_000.0),
            _build_bar(security_id, 12, 218.0, 12_600_000.0),
            _build_bar(security_id, 13, 220.0, 13_100_000.0),
            _build_bar(security_id, 14, 221.0, 13_400_000.0),
            _build_bar(security_id, 17, 219.0, 13_000_000.0),
            _build_bar(security_id, 18, 222.0, 13_700_000.0),
        ),
    )

    result = calculate_indicator("kdj", bars)

    assert {
        "k": result.k.tolist(),
        "d": result.d.tolist(),
        "j": result.j.tolist(),
        "params": asdict(result.parameters),
    } == {
        "k": [
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            61.9047619047619,
            69.84126984126983,
            70.37037037037037,
            74.69135802469135,
        ],
        "d": [
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            53.96825396825396,
            59.25925925925925,
            62.96296296296295,
            66.87242798353908,
        ],
        "j": [
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            77.77777777777777,
            91.00529100529101,
            85.18518518518519,
            90.32921810699591,
        ],
        "params": {"window": 9, "signal_window": 3},
    }
