"""Shared helpers for security identity arguments and resolution."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from neocortex.config import get_config
from neocortex.connectors.common import infer_cn_exchange
from neocortex.models import Exchange, Market, SecurityId
from neocortex.storage import MarketDataStore


logger = logging.getLogger(__name__)


def add_security_identity_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--market", default="CN", choices=("CN", "US", "JP", "HK"))
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--exchange", default=None)
    parser.add_argument("--name", default=None)


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


def find_security_ids_by_name(
    *,
    name: str,
    market: Market,
    db_path: str | Path,
    limit: int = 10,
) -> tuple[tuple[SecurityId, str], ...]:
    store = MarketDataStore(db_path)
    return store.aliases.search_security_ids(
        market=market,
        query=name,
        limit=limit,
    )


def resolve_unique_security_match(
    *,
    matches: tuple[tuple[SecurityId, str], ...],
    market: Market,
    query: str,
) -> SecurityId:
    if not matches:
        raise KeyError(f"No security matched {query!r} in market {market.value}.")
    if len(matches) > 1:
        choices = ", ".join(
            f"{security_id.ticker} ({alias})" for security_id, alias in matches[:5]
        )
        raise ValueError(f"Multiple securities matched {query!r}: {choices}.")
    logger.info(f"Resolved {query!r} to ticker {matches[0][0].ticker!r}.")
    return matches[0][0]


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


def parse_ticker(value: str, *, market: Market) -> SecurityId:
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


def resolve_ticker_or_name(
    value: str,
    *,
    market: Market,
    db_path: str | Path | None = None,
) -> SecurityId:
    try:
        return parse_ticker(value, market=market)
    except ValueError:
        return resolve_unique_security_match(
            matches=find_security_ids_by_name(
                name=value,
                market=market,
                db_path=db_path or get_config().storage.market_data_db_path,
            ),
            market=market,
            query=value,
        )


def resolve_security_id(
    args: argparse.Namespace,
    *,
    db_path: str | Path | None = None,
) -> SecurityId:
    market = Market(args.market)
    has_symbol = bool(getattr(args, "symbol", None))
    has_name = bool(getattr(args, "name", None))
    if has_symbol and has_name:
        raise ValueError("Choose exactly one of --symbol or --name.")
    if has_symbol:
        return SecurityId(
            symbol=args.symbol,
            market=market,
            exchange=resolve_exchange(
                symbol=args.symbol,
                exchange=args.exchange,
                market=market,
            ),
        )
    if not has_name:
        raise ValueError("Provide either --symbol or --name.")
    return resolve_unique_security_match(
        matches=find_security_ids_by_name(
            name=args.name,
            market=market,
            db_path=db_path
            or getattr(args, "db_path", get_config().storage.market_data_db_path),
        ),
        market=market,
        query=args.name,
    )
