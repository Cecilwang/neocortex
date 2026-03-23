"""Common helpers shared by source connectors and market-data provider."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
import logging

import pandas as pd

from neocortex.connectors.types import DailyPriceBarRecord
from neocortex.models import Exchange, SecurityId


logger = logging.getLogger(__name__)


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


def log_daily_records_access(
    *,
    source_name: str,
    security_id: SecurityId,
    requested_start_date: date,
    requested_end_date: date,
    records: Sequence[DailyPriceBarRecord],
    adjust_label: str,
) -> None:
    if not records:
        logger.info(
            f"Fetched {adjust_label} daily bars: source={source_name} "
            f"security={security_id.ticker} requested_start={requested_start_date} "
            f"requested_end={requested_end_date} count=0"
        )
        return
    actual_start_date = records[0].trade_date
    actual_end_date = records[-1].trade_date
    logger.info(
        f"Fetched {adjust_label} daily bars: source={source_name} "
        f"security={security_id.ticker} requested_start={requested_start_date} "
        f"requested_end={requested_end_date} actual_start={actual_start_date} "
        f"actual_end={actual_end_date} count={len(records)}"
    )


def infer_cn_exchange(symbol: str) -> Exchange:
    if symbol.startswith("6"):
        return Exchange.XSHG
    if symbol.startswith(("0", "3")):
        return Exchange.XSHE
    raise ValueError(f"Unsupported CN symbol for exchange inference: {symbol}")
