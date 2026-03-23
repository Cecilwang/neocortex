"""Market-data sync CLI commands."""

from __future__ import annotations

import argparse
from datetime import date
import logging

from neocortex.market_data_provider import ReadThroughMarketDataProvider
from neocortex.models import Market, SecurityId
from neocortex.serialization import to_pretty_json

from neocortex.cli.common import (
    add_date_range_arguments,
    market_data_provider,
    resolve_date_range,
    resolve_cli_ticker_or_name,
    resolve_security_id,
)

logger = logging.getLogger(__name__)
_CN_TRADING_DATE_SYNC_START = date(1990, 12, 19)


def run_market_data_provider_sync_securities(args: argparse.Namespace) -> int:
    logger.info(f"Running provider sync securities command: market={args.market}")
    provider = market_data_provider(args)
    security_ids = provider.list_securities(market=Market(args.market))
    print(
        to_pretty_json(
            {
                "market": args.market,
                "synced_security_count": len(security_ids),
                "tickers": [security_id.ticker for security_id in security_ids],
            }
        )
    )
    return 0


def _resolve_sync_security_ids(
    args: argparse.Namespace,
    *,
    provider: ReadThroughMarketDataProvider,
) -> tuple[SecurityId, ...]:
    market = Market(args.market)
    has_single = bool(args.symbol or args.exchange or args.name)
    has_collection = bool(args.ticker)
    has_all = bool(args.all_securities)

    selected_modes = int(has_single) + int(has_collection) + int(has_all)
    if selected_modes != 1:
        raise ValueError(
            "Choose exactly one sync target mode: --symbol/--name, --ticker, or --all-securities."
        )

    if has_single:
        return (resolve_security_id(args),)

    if has_collection:
        return tuple(
            resolve_cli_ticker_or_name(
                ticker,
                market=market,
                db_path=args.db_path,
            )
            for ticker in args.ticker
        )

    return provider.list_securities(market=market)


def run_market_data_provider_sync_bars(args: argparse.Namespace) -> int:
    provider = market_data_provider(args)
    start_date, end_date = resolve_date_range(
        market=Market(args.market),
        provider=provider,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    logger.info(
        f"Running provider sync bars command: market={args.market} "
        f"start={start_date} end={end_date}"
    )
    security_ids = _resolve_sync_security_ids(args, provider=provider)
    total_bar_count = 0
    synced_tickers: list[str] = []
    for security_id in security_ids:
        logger.info(
            f"Syncing raw daily bars: security={security_id.ticker} "
            f"start={start_date} end={end_date}"
        )
        price_series = provider.get_price_bars(
            security_id,
            start_date=start_date,
            end_date=end_date,
        )
        synced_tickers.append(security_id.ticker)
        total_bar_count += len(price_series)
    print(
        to_pretty_json(
            {
                "market": args.market,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "synced_security_count": len(security_ids),
                "synced_bar_count": total_bar_count,
                "tickers": synced_tickers,
            }
        )
    )
    return 0


def run_market_data_provider_sync_trading_dates(args: argparse.Namespace) -> int:
    end_date = date.today()
    logger.info(
        f"Running provider sync trading dates command: market=CN "
        f"start={_CN_TRADING_DATE_SYNC_START} end={end_date}"
    )
    provider = market_data_provider(args)
    records = provider.get_trading_dates(
        market=Market.CN,
        start_date=_CN_TRADING_DATE_SYNC_START,
        end_date=end_date,
    )
    print(
        to_pretty_json(
            {
                "market": "CN",
                "start_date": _CN_TRADING_DATE_SYNC_START.isoformat(),
                "end_date": end_date.isoformat(),
                "synced_record_count": len(records),
                "trading_day_count": sum(
                    1 for record in records if record.is_trading_day
                ),
            }
        )
    )
    return 0


def add_sync_commands(
    subcommands: argparse._SubParsersAction[argparse.ArgumentParser],
    *,
    default_db_path: str,
) -> None:
    sync_parser = subcommands.add_parser("sync")
    sync_parser.add_argument("--db-path", type=str, default=default_db_path)
    sync_commands = sync_parser.add_subparsers(dest="command", required=True)

    sync_securities = sync_commands.add_parser("securities")
    sync_securities.add_argument(
        "--market",
        default="CN",
        choices=("CN", "US", "JP", "HK"),
    )
    sync_securities.set_defaults(handler=run_market_data_provider_sync_securities)

    sync_bars = sync_commands.add_parser("bars")
    sync_bars.add_argument(
        "--market",
        default="CN",
        choices=("CN", "US", "JP", "HK"),
    )
    add_date_range_arguments(sync_bars)
    sync_bars.add_argument("--symbol", default=None)
    sync_bars.add_argument("--name", default=None)
    sync_bars.add_argument("--exchange", default=None)
    sync_bars.add_argument(
        "--ticker",
        action="append",
        default=[],
        help="Repeatable ticker in <symbol>.<exchange> format, for example 600519.XSHG.",
    )
    sync_bars.add_argument(
        "--all-securities",
        action="store_true",
        help="Sync bars for all securities visible in the target market.",
    )
    sync_bars.set_defaults(handler=run_market_data_provider_sync_bars)

    sync_trading_dates = sync_commands.add_parser("trading-dates")
    sync_trading_dates.set_defaults(handler=run_market_data_provider_sync_trading_dates)
