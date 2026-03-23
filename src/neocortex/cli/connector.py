"""Connector CLI commands."""

from __future__ import annotations

import argparse
import logging

from neocortex.connectors import AkShareConnector, BaoStockConnector, EFinanceConnector
from neocortex.models import Market
from neocortex.serialization import to_pretty_json

from neocortex.cli.common import (
    add_as_of_date_argument,
    add_date_range_arguments,
    add_security_identity_arguments,
    market_data_provider,
    resolve_as_of_date,
    resolve_date_range,
    resolve_security_id,
)

logger = logging.getLogger(__name__)


def connector_for_name(args: argparse.Namespace):
    if args.connector_name == "akshare":
        return AkShareConnector(timeout=getattr(args, "timeout", None))
    if args.connector_name == "baostock":
        return BaoStockConnector()
    if args.connector_name == "efinance":
        return EFinanceConnector()
    raise ValueError(f"Unsupported connector: {args.connector_name}")


def run_connector_securities(args: argparse.Namespace) -> int:
    logger.info(
        f"Running connector securities command: connector={args.connector_name} market={args.market}"
    )
    connector = connector_for_name(args)
    listings = connector.list_securities(market=Market(args.market))
    print(to_pretty_json(listings))
    return 0


def run_connector_profile(args: argparse.Namespace) -> int:
    security_id = resolve_security_id(args)
    logger.info(
        f"Running connector profile command: connector={args.connector_name} security={security_id.ticker}"
    )
    connector = connector_for_name(args)
    snapshot = connector.get_security_profile_snapshot(security_id)
    print(to_pretty_json(snapshot))
    return 0


def run_connector_daily(args: argparse.Namespace) -> int:
    security_id = resolve_security_id(args)
    provider = market_data_provider(args)
    start_date, end_date = resolve_date_range(
        market=security_id.market,
        provider=provider,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    logger.info(
        f"Running connector daily command: connector={args.connector_name} "
        f"security={security_id.ticker} start={start_date} end={end_date}"
    )
    connector = connector_for_name(args)
    records = connector.get_daily_price_bars(
        security_id,
        start_date=start_date,
        end_date=end_date,
    )
    print(to_pretty_json(records))
    return 0


def run_connector_adjusted_daily(args: argparse.Namespace) -> int:
    security_id = resolve_security_id(args)
    provider = market_data_provider(args)
    start_date, end_date = resolve_date_range(
        market=security_id.market,
        provider=provider,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    logger.info(
        f"Running connector adjusted-daily command: connector={args.connector_name} "
        f"security={security_id.ticker} start={start_date} end={end_date} "
        f"adjust={args.adjustment_type}"
    )
    connector = connector_for_name(args)
    records = connector.get_adjusted_daily_price_bars(
        security_id,
        start_date=start_date,
        end_date=end_date,
        adjustment_type=args.adjustment_type,
    )
    print(to_pretty_json(records))
    return 0


def run_connector_adjustment_factors(args: argparse.Namespace) -> int:
    security_id = resolve_security_id(args)
    provider = market_data_provider(args)
    start_date, end_date = resolve_date_range(
        market=security_id.market,
        provider=provider,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    logger.info(
        f"Running connector adjustment-factors command: connector={args.connector_name} "
        f"security={security_id.ticker} start={start_date} end={end_date}"
    )
    connector = connector_for_name(args)
    records = connector.get_adjustment_factors(
        security_id,
        start_date=start_date,
        end_date=end_date,
    )
    print(to_pretty_json(records))
    return 0


def run_connector_fundamentals(args: argparse.Namespace) -> int:
    security_id = resolve_security_id(args)
    provider = market_data_provider(args)
    as_of_date = resolve_as_of_date(
        market=security_id.market,
        provider=provider,
        as_of_date=args.as_of_date,
    )
    logger.info(
        f"Running connector fundamentals command: connector={args.connector_name} "
        f"security={security_id.ticker} as_of_date={as_of_date}"
    )
    connector = connector_for_name(args)
    records = connector.get_fundamental_snapshots(
        security_id,
        as_of_date=as_of_date,
    )
    print(to_pretty_json(records))
    return 0


def run_connector_macro(args: argparse.Namespace) -> int:
    provider = market_data_provider(args)
    market = Market(args.market)
    as_of_date = resolve_as_of_date(
        market=market,
        provider=provider,
        as_of_date=args.as_of_date,
    )
    logger.info(
        f"Running connector macro command: connector={args.connector_name} "
        f"market={args.market} as_of_date={as_of_date}"
    )
    connector = connector_for_name(args)
    records = connector.get_macro_points(
        market=market,
        as_of_date=as_of_date,
    )
    print(to_pretty_json(records))
    return 0


def _maybe_add_timeout_argument(
    parser: argparse.ArgumentParser,
    *,
    supports_timeout: bool,
) -> None:
    if supports_timeout:
        parser.add_argument("--timeout", type=float, default=None)


def _add_securities_command(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
    *,
    supports_timeout: bool = False,
) -> None:
    parser = commands.add_parser("securities")
    parser.add_argument("--market", default="CN", choices=("CN",))
    _maybe_add_timeout_argument(parser, supports_timeout=supports_timeout)
    parser.set_defaults(handler=run_connector_securities)


def _add_profile_command(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
    *,
    supports_timeout: bool = False,
) -> None:
    parser = commands.add_parser("profile")
    add_security_identity_arguments(parser)
    _maybe_add_timeout_argument(parser, supports_timeout=supports_timeout)
    parser.set_defaults(handler=run_connector_profile)


def _add_daily_command(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
    *,
    supports_timeout: bool = False,
) -> None:
    parser = commands.add_parser("daily")
    add_security_identity_arguments(parser)
    add_date_range_arguments(parser)
    _maybe_add_timeout_argument(parser, supports_timeout=supports_timeout)
    parser.set_defaults(handler=run_connector_daily)


def _add_adjusted_daily_command(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
    *,
    supports_timeout: bool = False,
) -> None:
    parser = commands.add_parser("adjusted-daily")
    add_security_identity_arguments(parser)
    add_date_range_arguments(parser)
    parser.add_argument(
        "--adjustment-type",
        required=True,
        choices=("qfq", "hfq"),
    )
    _maybe_add_timeout_argument(parser, supports_timeout=supports_timeout)
    parser.set_defaults(handler=run_connector_adjusted_daily)


def _add_adjustment_factors_command(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = commands.add_parser("adjustment-factors")
    add_security_identity_arguments(parser)
    add_date_range_arguments(parser)
    parser.set_defaults(handler=run_connector_adjustment_factors)


def _add_fundamentals_command(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = commands.add_parser("fundamentals")
    add_security_identity_arguments(parser)
    add_as_of_date_argument(parser)
    parser.set_defaults(handler=run_connector_fundamentals)


def _add_macro_command(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = commands.add_parser("macro")
    parser.add_argument("--market", default="CN", choices=("CN",))
    add_as_of_date_argument(parser)
    parser.set_defaults(handler=run_connector_macro)


def _add_connector_command_group(
    connector_commands: argparse._SubParsersAction[argparse.ArgumentParser],
    *,
    connector_name: str,
    supports_timeout: bool = False,
    supports_adjustment_factors: bool = False,
    supports_fundamentals: bool = False,
    supports_macro: bool = False,
) -> None:
    parser = connector_commands.add_parser(connector_name)
    commands = parser.add_subparsers(dest="command", required=True)
    _add_securities_command(commands, supports_timeout=supports_timeout)
    _add_profile_command(commands, supports_timeout=supports_timeout)
    _add_daily_command(commands, supports_timeout=supports_timeout)
    _add_adjusted_daily_command(commands, supports_timeout=supports_timeout)
    if supports_adjustment_factors:
        _add_adjustment_factors_command(commands)
    if supports_fundamentals:
        _add_fundamentals_command(commands)
    if supports_macro:
        _add_macro_command(commands)


def add_connector_commands(
    subcommands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    connector_parser = subcommands.add_parser("connector")
    connector_commands = connector_parser.add_subparsers(
        dest="connector_name",
        required=True,
    )

    _add_connector_command_group(
        connector_commands,
        connector_name="akshare",
        supports_timeout=True,
    )
    _add_connector_command_group(
        connector_commands,
        connector_name="efinance",
    )
    _add_connector_command_group(
        connector_commands,
        connector_name="baostock",
        supports_adjustment_factors=True,
        supports_fundamentals=True,
        supports_macro=True,
    )
