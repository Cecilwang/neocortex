"""Registry-backed sync command definitions."""

from __future__ import annotations

import argparse
from datetime import date
import logging

from neocortex.commands.core import (
    AuthPolicy,
    CommandContext,
    CommandResult,
    CommandSpec,
    ExecutionMode,
    Exposure,
)
from neocortex.date_resolution import add_date_range_arguments, resolve_date_range
from neocortex.market_data_provider import ReadThroughMarketDataProvider
from neocortex.models import Market, SecurityId
from neocortex.security_resolution import (
    add_security_identity_arguments,
    resolve_ticker_or_name,
    resolve_security_id,
)


logger = logging.getLogger(__name__)
_CN_TRADING_DATE_SYNC_START = date(1990, 12, 19)


def _flatten_ticker_values(
    ticker_groups: list[str] | list[list[str]] | None,
) -> tuple[str, ...]:
    if not ticker_groups:
        return ()
    flattened: list[str] = []
    for group in ticker_groups:
        if isinstance(group, list):
            flattened.extend(group)
        else:
            flattened.append(group)
    return tuple(flattened)


def build_sync_securities_command_spec(
    *,
    default_db_path: str,
) -> CommandSpec:
    """Build the registry command spec for sync securities."""

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--db-path", type=str, default=default_db_path)
        parser.add_argument(
            "--market",
            default="CN",
            choices=("CN", "US", "JP", "HK"),
        )

    def handler(args: argparse.Namespace, context: CommandContext) -> CommandResult:
        _ = context
        logger.info(f"Running provider sync securities command: market={args.market}")
        provider = ReadThroughMarketDataProvider.from_defaults(args.db_path)
        security_ids = provider.list_securities(market=Market(args.market))
        return CommandResult.json(
            {
                "market": args.market,
                "synced_security_count": len(security_ids),
                "tickers": [security_id.ticker for security_id in security_ids],
            }
        )

    return CommandSpec(
        id=("sync", "securities"),
        summary="Sync the visible security universe for one market.",
        description="Sync the visible security universe for one market.",
        exposure=Exposure.SHARED,
        auth=AuthPolicy.PUBLIC,
        execution=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=handler,
    )


def _resolve_sync_security_ids(
    args: argparse.Namespace,
    *,
    provider: ReadThroughMarketDataProvider,
) -> tuple[SecurityId, ...]:
    market = Market(args.market)
    tickers = _flatten_ticker_values(args.ticker)
    has_single = bool(args.symbol or args.exchange or args.name)
    has_collection = bool(tickers)
    has_all = bool(args.all_securities)

    selected_modes = int(has_single) + int(has_collection) + int(has_all)
    if selected_modes != 1:
        raise ValueError(
            "Choose exactly one sync target mode: --symbol/--name, --ticker, or --all-securities."
        )

    if has_single:
        return (resolve_security_id(args, db_path=args.db_path),)

    if has_collection:
        return tuple(
            resolve_ticker_or_name(
                ticker,
                market=market,
                db_path=args.db_path,
            )
            for ticker in tickers
        )

    return provider.list_securities(market=market)


def build_sync_bars_command_spec(
    *,
    default_db_path: str,
) -> CommandSpec:
    """Build the registry command spec for sync bars."""

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--db-path", type=str, default=default_db_path)
        add_date_range_arguments(parser)
        add_security_identity_arguments(parser)
        parser.add_argument(
            "--ticker",
            action="append",
            nargs="+",
            default=[],
            help=(
                "One or more tickers in <symbol>.<exchange> format, for example "
                "600519.XSHG. Repeat --ticker or pass multiple values after one "
                "flag."
            ),
        )
        parser.add_argument(
            "--all-securities",
            action="store_true",
            help="Sync bars for all securities visible in the target market.",
        )

    def handler(args: argparse.Namespace, context: CommandContext) -> CommandResult:
        _ = context
        provider = ReadThroughMarketDataProvider.from_defaults(args.db_path)
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
        return CommandResult.json(
            {
                "market": args.market,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "synced_security_count": len(security_ids),
                "synced_bar_count": total_bar_count,
                "tickers": synced_tickers,
            }
        )

    return CommandSpec(
        id=("sync", "bars"),
        summary="Sync raw daily price bars into the local market-data store.",
        description="Sync raw daily price bars into the local market-data store.",
        exposure=Exposure.SHARED,
        auth=AuthPolicy.PUBLIC,
        execution=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=handler,
    )


def build_sync_trading_dates_command_spec(
    *,
    default_db_path: str,
) -> CommandSpec:
    """Build the registry command spec for sync trading-dates."""

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--db-path", type=str, default=default_db_path)

    def handler(args: argparse.Namespace, context: CommandContext) -> CommandResult:
        _ = context, args
        end_date = date.today()
        logger.info(
            f"Running provider sync trading dates command: market=CN "
            f"start={_CN_TRADING_DATE_SYNC_START} end={end_date}"
        )
        provider = ReadThroughMarketDataProvider.from_defaults(args.db_path)
        records = provider.get_trading_dates(
            market=Market.CN,
            start_date=_CN_TRADING_DATE_SYNC_START,
            end_date=end_date,
        )
        return CommandResult.json(
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

    return CommandSpec(
        id=("sync", "trading-dates"),
        summary="Sync the full CN trading-date calendar into the local store.",
        description="Sync the full CN trading-date calendar into the local store.",
        exposure=Exposure.SHARED,
        auth=AuthPolicy.PUBLIC,
        execution=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=handler,
    )
