"""Shared date parsing and market-aware default date helpers."""

from __future__ import annotations

import argparse
from datetime import date, datetime, time
import logging
from zoneinfo import ZoneInfo

from neocortex.market_data_provider import ReadThroughMarketDataProvider
from neocortex.models import Market


logger = logging.getLogger(__name__)
_BEIJING_TIMEZONE = ZoneInfo("Asia/Shanghai")
_CN_BAOSTOCK_DATA_READY_TIME = time(18, 30)


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _beijing_now(now: datetime | None = None) -> datetime:
    if now is None:
        return datetime.now(_BEIJING_TIMEZONE)
    if now.tzinfo is None:
        return now.replace(tzinfo=_BEIJING_TIMEZONE)
    return now.astimezone(_BEIJING_TIMEZONE)


def default_end_date(
    *,
    market: Market,
    provider: ReadThroughMarketDataProvider | None = None,
    now: datetime | None = None,
) -> date:
    if market is not Market.CN:
        if now is None:
            return date.today()
        return now.date()
    if provider is None:
        raise ValueError("provider is required when resolving default CN end dates.")
    current_time = _beijing_now(now)
    candidate_date = current_time.date()
    if not provider.is_trading_day(market=market, trade_date=candidate_date):
        return provider.get_previous_trading_date(
            market=market,
            trade_date=candidate_date,
        )
    if current_time.time() >= _CN_BAOSTOCK_DATA_READY_TIME:
        return candidate_date
    return provider.get_previous_trading_date(
        market=market,
        trade_date=candidate_date,
    )


def default_start_date(*, end_date: date | None = None) -> date:
    resolved_end_date = end_date or date.today()
    try:
        return resolved_end_date.replace(year=resolved_end_date.year - 10)
    except ValueError:
        return resolved_end_date.replace(
            year=resolved_end_date.year - 10,
            day=28,
        )


def resolve_date_range(
    *,
    market: Market,
    provider: ReadThroughMarketDataProvider | None = None,
    start_date: date | None,
    end_date: date | None,
    now: datetime | None = None,
) -> tuple[date, date]:
    resolved_end_date = end_date or default_end_date(
        market=market,
        provider=provider,
        now=now,
    )
    resolved_start_date = start_date or default_start_date(end_date=resolved_end_date)
    if resolved_start_date > resolved_end_date:
        raise ValueError("start_date cannot be later than end_date.")
    if start_date != resolved_start_date:
        logger.info(
            f"Resolved default start_date from {start_date} to {resolved_start_date}."
        )
    if end_date != resolved_end_date:
        logger.info(
            f"Resolved default end_date from {end_date} to {resolved_end_date}."
        )
    return resolved_start_date, resolved_end_date


def resolve_as_of_date(
    *,
    market: Market,
    provider: ReadThroughMarketDataProvider | None = None,
    as_of_date: date | None,
    now: datetime | None = None,
) -> date:
    resolved = as_of_date or default_end_date(
        market=market,
        provider=provider,
        now=now,
    )
    if resolved != as_of_date:
        logger.info(f"Resolved default as_of_date to {resolved}.")
    return resolved


def add_date_range_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--start-date",
        type=parse_date,
        default=None,
        help="Defaults to 10 years before --end-date.",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=None,
        help=(
            "Defaults to today's date for non-CN markets. For CN, defaults to "
            "today after BaoStock data is expected to be available at Beijing "
            "18:30 on trading days; otherwise the previous trading day."
        ),
    )


def add_as_of_date_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--as-of-date",
        type=parse_date,
        default=None,
        help=(
            "Defaults to today's date for non-CN markets. For CN, defaults to "
            "today after BaoStock data is expected to be available at Beijing "
            "18:30 on trading days; otherwise the previous trading day."
        ),
    )
