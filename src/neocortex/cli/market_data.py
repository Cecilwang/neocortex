"""Market-data provider CLI commands."""

from __future__ import annotations

import argparse
from datetime import date
import logging

from neocortex.models import Market
from neocortex.serialization import to_pretty_json
from neocortex.storage import MarketDataStore

from neocortex.cli.common import (
    add_as_of_date_argument,
    add_date_range_arguments,
    add_security_identity_arguments,
    market_data_provider,
    parse_date,
    resolve_as_of_date,
    resolve_date_range,
    resolve_security_id,
)

logger = logging.getLogger(__name__)


def run_market_data_provider_init_db(args: argparse.Namespace) -> int:
    logger.info(f"Initializing market-data DB: path={args.db_path}")
    store = MarketDataStore(args.db_path)
    store.ensure_schema()
    print(f"Initialized market data database at {args.db_path}")
    return 0


def run_market_data_provider_securities(args: argparse.Namespace) -> int:
    logger.info(f"Running provider securities command: market={args.market}")
    provider = market_data_provider(args)
    print(to_pretty_json(provider.list_securities(market=Market(args.market))))
    return 0


def run_market_data_provider_profile(args: argparse.Namespace) -> int:
    security_id = resolve_security_id(args)
    logger.info(f"Running provider profile command: security={security_id.ticker}")
    provider = market_data_provider(args)
    print(to_pretty_json(provider.get_company_profile(security_id)))
    return 0


def run_market_data_provider_bars(args: argparse.Namespace) -> int:
    security_id = resolve_security_id(args)
    provider = market_data_provider(args)
    start_date, end_date = resolve_date_range(
        market=security_id.market,
        provider=provider,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    logger.info(
        f"Running provider bars command: security={security_id.ticker} "
        f"start={start_date} end={end_date} adjust={args.adjust}"
    )
    bars = provider.get_price_bars(
        security_id,
        start_date=start_date,
        end_date=end_date,
        adjust=args.adjust,
    )
    print(bars.bars.to_string(index=False))
    return 0


def run_market_data_provider_fundamentals(args: argparse.Namespace) -> int:
    security_id = resolve_security_id(args)
    provider = market_data_provider(args)
    as_of_date = resolve_as_of_date(
        market=security_id.market,
        provider=provider,
        as_of_date=args.as_of_date,
    )
    logger.info(
        f"Running provider fundamentals command: security={security_id.ticker} "
        f"as_of_date={as_of_date}"
    )
    print(
        to_pretty_json(
            provider.get_fundamental_snapshots(
                security_id,
                as_of_date=as_of_date,
            )
        )
    )
    return 0


def run_market_data_provider_disclosures(args: argparse.Namespace) -> int:
    security_id = resolve_security_id(args)
    provider = market_data_provider(args)
    as_of_date = resolve_as_of_date(
        market=security_id.market,
        provider=provider,
        as_of_date=args.as_of_date,
    )
    logger.info(
        f"Running provider disclosures command: security={security_id.ticker} "
        f"as_of_date={as_of_date}"
    )
    print(
        to_pretty_json(
            provider.get_disclosure_sections(
                security_id,
                as_of_date=as_of_date,
            )
        )
    )
    return 0


def run_market_data_provider_macro(args: argparse.Namespace) -> int:
    provider = market_data_provider(args)
    market = Market(args.market)
    as_of_date = resolve_as_of_date(
        market=market,
        provider=provider,
        as_of_date=args.as_of_date,
    )
    logger.info(
        f"Running provider macro command: market={args.market} as_of_date={as_of_date}"
    )
    print(
        to_pretty_json(
            provider.get_macro_points(
                market=market,
                as_of_date=as_of_date,
            )
        )
    )
    return 0


def run_market_data_provider_trading_dates(args: argparse.Namespace) -> int:
    provider = market_data_provider(args)
    if args.date is not None:
        if args.start_date is not None or args.end_date is not None:
            raise ValueError("Choose either --date or --start-date/--end-date.")
        start_date = args.date
        end_date = args.date
    else:
        start_date, end_date = resolve_date_range(
            market=Market(args.market),
            provider=provider,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    today = date.today()
    if start_date > today or end_date > today:
        raise ValueError("Future dates are not supported for trading-date queries.")
    logger.info(
        f"Running provider trading dates command: market={args.market} "
        f"start={start_date} end={end_date}"
    )
    records = provider.get_trading_dates(
        market=Market(args.market),
        start_date=start_date,
        end_date=end_date,
    )
    if args.date is not None:
        print(to_pretty_json(records[0]))
    else:
        print(to_pretty_json(records))
    return 0


def add_market_data_provider_commands(
    subcommands: argparse._SubParsersAction[argparse.ArgumentParser],
    *,
    default_db_path: str,
) -> None:
    provider_parser = subcommands.add_parser("market-data-provider")
    provider_parser.add_argument("--db-path", type=str, default=default_db_path)
    provider_commands = provider_parser.add_subparsers(dest="command", required=True)

    provider_init = provider_commands.add_parser("init-db")
    provider_init.set_defaults(handler=run_market_data_provider_init_db)

    provider_securities = provider_commands.add_parser("securities")
    provider_securities.add_argument(
        "--market",
        default="CN",
        choices=("CN", "US", "JP", "HK"),
    )
    provider_securities.set_defaults(handler=run_market_data_provider_securities)

    provider_profile = provider_commands.add_parser("profile")
    add_security_identity_arguments(provider_profile)
    provider_profile.set_defaults(handler=run_market_data_provider_profile)

    provider_bars = provider_commands.add_parser("bars")
    add_security_identity_arguments(provider_bars)
    add_date_range_arguments(provider_bars)
    provider_bars.add_argument(
        "--adjust",
        default=None,
        help="Defaults to raw prices.",
    )
    provider_bars.set_defaults(handler=run_market_data_provider_bars)

    provider_fundamentals = provider_commands.add_parser("fundamentals")
    add_security_identity_arguments(provider_fundamentals)
    add_as_of_date_argument(provider_fundamentals)
    provider_fundamentals.set_defaults(handler=run_market_data_provider_fundamentals)

    provider_disclosures = provider_commands.add_parser("disclosures")
    add_security_identity_arguments(provider_disclosures)
    add_as_of_date_argument(provider_disclosures)
    provider_disclosures.set_defaults(handler=run_market_data_provider_disclosures)

    provider_macro = provider_commands.add_parser("macro")
    provider_macro.add_argument(
        "--market",
        default="CN",
        choices=("CN", "US", "JP", "HK"),
    )
    add_as_of_date_argument(provider_macro)
    provider_macro.set_defaults(handler=run_market_data_provider_macro)

    provider_trading_dates = provider_commands.add_parser("trading-dates")
    provider_trading_dates.add_argument(
        "--market",
        default="CN",
        choices=("CN",),
    )
    provider_trading_dates.add_argument("--date", type=parse_date, default=None)
    provider_trading_dates.add_argument(
        "--start-date",
        type=parse_date,
        default=None,
        help="Defaults to 10 years before --end-date.",
    )
    provider_trading_dates.add_argument(
        "--end-date",
        type=parse_date,
        default=None,
        help="Defaults to today.",
    )
    provider_trading_dates.set_defaults(handler=run_market_data_provider_trading_dates)
