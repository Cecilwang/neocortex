"""Common helpers shared by source connectors and market-data provider."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

import pandas as pd

from neocortex.connectors.types import DailyPriceBarRecord
from neocortex.models import Exchange


def optional_float(value: object) -> float | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    return float(value)


def daily_records_cover_requested_range(
    *,
    records: Sequence[DailyPriceBarRecord],
    start_date: date,
    end_date: date,
) -> bool:
    if not records:
        return False
    first = date.fromisoformat(records[0].trade_date)
    last = date.fromisoformat(records[-1].trade_date)
    return first <= start_date and last >= end_date


def infer_cn_exchange(symbol: str) -> Exchange:
    if symbol.startswith("6"):
        return Exchange.XSHG
    if symbol.startswith(("0", "3")):
        return Exchange.XSHE
    raise ValueError(f"Unsupported CN symbol for exchange inference: {symbol}")
