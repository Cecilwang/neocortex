"""Shared CLI helpers."""

from __future__ import annotations

import argparse
from datetime import date, datetime, time
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

from neocortex.connectors.common import infer_cn_exchange
from neocortex.config import get_config
from neocortex.market_data_provider import ReadThroughMarketDataProvider
from neocortex.models import Exchange, Market, SecurityId
from neocortex.storage import MarketDataStore

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
        logger.info(f"Resolved default start_date to {resolved_start_date} based on end_date {resolved_end_date}.")
    if end_date != resolved_end_date:
        logger.info(f"Resolved default end_date to {resolved_end_date} based on start_date {resolved_start_date}.")
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


def build_security_id(args: argparse.Namespace) -> SecurityId:
    if not args.symbol:
        raise ValueError("--symbol is required when resolving a security by symbol.")
    return SecurityId(
        symbol=args.symbol,
        market=Market(args.market),
        exchange=resolve_exchange(
            symbol=args.symbol,
            exchange=args.exchange,
            market=Market(args.market),
        ),
    )


def build_security_id_for_market(
    *,
    symbol: str,
    exchange: str | None,
    market: Market,
) -> SecurityId:
    return SecurityId(
        symbol=symbol,
        market=market,
        exchange=resolve_exchange(
            symbol=symbol,
            exchange=exchange,
            market=market,
        ),
    )


def resolve_exchange(
    *,
    symbol: str,
    exchange: str | None,
    market: Market,
) -> Exchange:
    if exchange:
        return Exchange(exchange)
    if market is Market.CN:
        return infer_cn_exchange(symbol)
    raise ValueError(f"--exchange is required for market {market.value}.")


def parse_cli_ticker(value: str, *, market: Market) -> SecurityId:
    symbol, separator, exchange = value.partition(".")
    if not separator or not symbol or not exchange:
        raise ValueError(
            "Ticker must use the format <symbol>.<exchange>, for example 600519.XSHG."
        )
    return build_security_id_for_market(
        symbol=symbol,
        exchange=exchange,
        market=market,
    )


def resolve_cli_ticker_or_name(
    value: str,
    *,
    market: Market,
    db_path: str | Path | None = None,
) -> SecurityId:
    try:
        return parse_cli_ticker(value, market=market)
    except ValueError:
        return _resolve_unique_security_match(
            matches=find_security_ids_by_name(
                name=value,
                market=market,
                db_path=db_path,
            ),
            market=market,
            query=value,
        )


def _resolve_unique_security_match(
    *,
    matches: tuple[tuple[SecurityId, str], ...],
    market: Market,
    query: str,
) -> SecurityId:
    if not matches:
        raise KeyError(
            f"No security matched {query!r} in market {market.value}."
        ) from None
    if len(matches) > 1:
        choices = ", ".join(
            f"{security_id.ticker} ({alias})" for security_id, alias in matches[:5]
        )
        raise ValueError(f"Multiple securities matched {query!r}: {choices}.") from None
    logger.info(f"Resolved {query!r} to ticker {matches[0][0].ticker!r}.")
    return matches[0][0]


def market_data_db_path(args: argparse.Namespace) -> str:
    return str(getattr(args, "db_path", get_config().storage.market_data_db_path))


def market_data_provider(args: argparse.Namespace) -> ReadThroughMarketDataProvider:
    return ReadThroughMarketDataProvider.from_defaults(market_data_db_path(args))


def find_security_ids_by_name(
    *,
    name: str,
    market: Market,
    db_path: str | Path | None = None,
    limit: int = 10,
) -> tuple[tuple[SecurityId, str], ...]:
    store = MarketDataStore(db_path or get_config().storage.market_data_db_path)
    return store.aliases.search_security_ids(
        market=market,
        query=name,
        limit=limit,
    )


def resolve_security_id(args: argparse.Namespace) -> SecurityId:
    has_symbol = bool(getattr(args, "symbol", None))
    has_name = bool(getattr(args, "name", None))
    if has_symbol and has_name:
        raise ValueError("Choose exactly one of --symbol or --name.")
    if has_symbol:
        return build_security_id(args)
    if not has_name:
        raise ValueError("Provide either --symbol or --name.")

    return _resolve_unique_security_match(
        matches=find_security_ids_by_name(
            name=args.name,
            market=Market(args.market),
            db_path=market_data_db_path(args),
        ),
        market=Market(args.market),
        query=args.name,
    )


def add_security_identity_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--market", default="CN", choices=("CN", "US", "JP", "HK"))
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--exchange", default=None)
    parser.add_argument("--name", default=None)
