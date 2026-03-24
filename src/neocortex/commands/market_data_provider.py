"""Registry-backed market-data-provider command definitions."""

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
from neocortex.date_resolution import (
    add_as_of_date_argument,
    add_date_range_arguments,
    parse_date,
    resolve_as_of_date,
    resolve_date_range,
)
from neocortex.market_data_provider import ReadThroughMarketDataProvider
from neocortex.models import Market
from neocortex.security_resolution import (
    add_security_identity_arguments,
    resolve_security_id,
)
from neocortex.storage import MarketDataStore


logger = logging.getLogger(__name__)


def build_market_data_provider_init_db_command_spec(
    *,
    default_db_path: str,
) -> CommandSpec:
    """Build the registry command spec for market-data-provider init-db."""

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--db-path", type=str, default=default_db_path)

    def handler(args: argparse.Namespace, context: CommandContext) -> CommandResult:
        _ = context
        logger.info("Initializing market-data DB schema.")
        store = MarketDataStore(args.db_path)
        store.ensure_schema()
        return CommandResult.text(f"Initialized market data database at {args.db_path}")

    return CommandSpec(
        id=("market-data-provider", "init-db"),
        summary="Initialize the local market data database schema.",
        description="Initialize the local market data database schema.",
        exposure=Exposure.SHARED,
        auth=AuthPolicy.PUBLIC,
        execution=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=handler,
    )


def build_market_data_provider_securities_command_spec(
    *,
    default_db_path: str,
) -> CommandSpec:
    """Build the registry command spec for market-data-provider securities."""

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--db-path", type=str, default=default_db_path)
        parser.add_argument(
            "--market",
            default="CN",
            choices=("CN", "US", "JP", "HK"),
        )

    def handler(args: argparse.Namespace, context: CommandContext) -> CommandResult:
        _ = context
        logger.info(f"Running provider securities command: market={args.market}")
        provider = ReadThroughMarketDataProvider.from_defaults(args.db_path)
        securities = provider.list_securities(market=Market(args.market))
        return CommandResult.table(
            columns=("symbol", "market", "exchange"),
            rows=tuple(
                (
                    security_id.symbol,
                    security_id.market.value,
                    security_id.exchange.value,
                )
                for security_id in securities
            ),
        )

    return CommandSpec(
        id=("market-data-provider", "securities"),
        summary="List securities from the runtime market data provider.",
        description="List securities from the runtime market data provider.",
        exposure=Exposure.SHARED,
        auth=AuthPolicy.PUBLIC,
        execution=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=handler,
    )


def build_market_data_provider_bars_command_spec(
    *,
    default_db_path: str,
) -> CommandSpec:
    """Build the registry command spec for market-data-provider bars."""

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--db-path", type=str, default=default_db_path)
        add_security_identity_arguments(parser)
        add_date_range_arguments(parser)
        parser.add_argument(
            "--adjust",
            default=None,
            help="Defaults to raw prices.",
        )

    def handler(args: argparse.Namespace, context: CommandContext) -> CommandResult:
        _ = context
        security_id = resolve_security_id(args, db_path=args.db_path)
        provider = ReadThroughMarketDataProvider.from_defaults(args.db_path)
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
        return CommandResult.text(bars.bars.to_string(index=False), payload=bars)

    return CommandSpec(
        id=("market-data-provider", "bars"),
        summary="Fetch historical price bars from the runtime market data provider.",
        description="Fetch historical price bars from the runtime market data provider.",
        exposure=Exposure.SHARED,
        auth=AuthPolicy.PUBLIC,
        execution=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=handler,
    )


def build_market_data_provider_fundamentals_command_spec(
    *,
    default_db_path: str,
) -> CommandSpec:
    """Build the registry command spec for market-data-provider fundamentals."""

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--db-path", type=str, default=default_db_path)
        add_security_identity_arguments(parser)
        add_as_of_date_argument(parser)

    def handler(args: argparse.Namespace, context: CommandContext) -> CommandResult:
        _ = context
        security_id = resolve_security_id(args, db_path=args.db_path)
        provider = ReadThroughMarketDataProvider.from_defaults(args.db_path)
        as_of_date = resolve_as_of_date(
            market=security_id.market,
            provider=provider,
            as_of_date=args.as_of_date,
        )
        logger.info(
            f"Running provider fundamentals command: security={security_id.ticker} "
            f"as_of_date={as_of_date}"
        )
        return CommandResult.json(
            provider.get_fundamental_snapshots(
                security_id,
                as_of_date=as_of_date,
            )
        )

    return CommandSpec(
        id=("market-data-provider", "fundamentals"),
        summary="Fetch fundamental snapshots from the runtime market data provider.",
        description="Fetch fundamental snapshots from the runtime market data provider.",
        exposure=Exposure.SHARED,
        auth=AuthPolicy.PUBLIC,
        execution=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=handler,
    )


def build_market_data_provider_profile_command_spec(
    *,
    default_db_path: str,
) -> CommandSpec:
    """Build the registry command spec for market-data-provider profile."""

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--db-path", type=str, default=default_db_path)
        add_security_identity_arguments(parser)

    def handler(args: argparse.Namespace, context: CommandContext) -> CommandResult:
        _ = context
        security_id = resolve_security_id(args, db_path=args.db_path)
        logger.info(f"Running provider profile command: security={security_id.ticker}")
        provider = ReadThroughMarketDataProvider.from_defaults(args.db_path)
        return CommandResult.json(provider.get_company_profile(security_id))

    return CommandSpec(
        id=("market-data-provider", "profile"),
        summary="Fetch one company profile from the runtime market data provider.",
        description="Fetch one company profile from the runtime market data provider.",
        exposure=Exposure.SHARED,
        auth=AuthPolicy.PUBLIC,
        execution=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=handler,
    )


def build_market_data_provider_disclosures_command_spec(
    *,
    default_db_path: str,
) -> CommandSpec:
    """Build the registry command spec for market-data-provider disclosures."""

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--db-path", type=str, default=default_db_path)
        add_security_identity_arguments(parser)
        add_as_of_date_argument(parser)

    def handler(args: argparse.Namespace, context: CommandContext) -> CommandResult:
        _ = context
        security_id = resolve_security_id(args, db_path=args.db_path)
        provider = ReadThroughMarketDataProvider.from_defaults(args.db_path)
        as_of_date = resolve_as_of_date(
            market=security_id.market,
            provider=provider,
            as_of_date=args.as_of_date,
        )
        logger.info(
            f"Running provider disclosures command: security={security_id.ticker} "
            f"as_of_date={as_of_date}"
        )
        return CommandResult.json(
            provider.get_disclosure_sections(
                security_id,
                as_of_date=as_of_date,
            )
        )

    return CommandSpec(
        id=("market-data-provider", "disclosures"),
        summary="Fetch disclosure sections from the runtime market data provider.",
        description="Fetch disclosure sections from the runtime market data provider.",
        exposure=Exposure.SHARED,
        auth=AuthPolicy.PUBLIC,
        execution=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=handler,
    )


def build_market_data_provider_macro_command_spec(
    *,
    default_db_path: str,
) -> CommandSpec:
    """Build the registry command spec for market-data-provider macro."""

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--db-path", type=str, default=default_db_path)
        parser.add_argument(
            "--market",
            default="CN",
            choices=("CN", "US", "JP", "HK"),
        )
        add_as_of_date_argument(parser)

    def handler(args: argparse.Namespace, context: CommandContext) -> CommandResult:
        _ = context
        provider = ReadThroughMarketDataProvider.from_defaults(args.db_path)
        market = Market(args.market)
        as_of_date = resolve_as_of_date(
            market=market,
            provider=provider,
            as_of_date=args.as_of_date,
        )
        logger.info(
            f"Running provider macro command: market={args.market} as_of_date={as_of_date}"
        )
        return CommandResult.json(
            provider.get_macro_points(
                market=market,
                as_of_date=as_of_date,
            )
        )

    return CommandSpec(
        id=("market-data-provider", "macro"),
        summary="Fetch macro points from the runtime market data provider.",
        description="Fetch macro points from the runtime market data provider.",
        exposure=Exposure.SHARED,
        auth=AuthPolicy.PUBLIC,
        execution=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=handler,
    )


def build_market_data_provider_trading_dates_command_spec(
    *,
    default_db_path: str,
) -> CommandSpec:
    """Build the registry command spec for market-data-provider trading-dates."""

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--db-path", type=str, default=default_db_path)
        parser.add_argument(
            "--market",
            default="CN",
            choices=("CN",),
        )
        parser.add_argument("--date", type=parse_date, default=None)
        add_date_range_arguments(parser)

    def handler(args: argparse.Namespace, context: CommandContext) -> CommandResult:
        _ = context
        provider = ReadThroughMarketDataProvider.from_defaults(args.db_path)
        market = Market(args.market)
        if args.date is not None:
            if args.start_date is not None or args.end_date is not None:
                raise ValueError("Choose either --date or --start-date/--end-date.")
            start_date = args.date
            end_date = args.date
        else:
            start_date, end_date = resolve_date_range(
                market=market,
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
            market=market,
            start_date=start_date,
            end_date=end_date,
        )
        rows = tuple(
            (
                record.source,
                record.market.value,
                record.calendar,
                record.trade_date,
                record.is_trading_day,
            )
            for record in records
        )
        return CommandResult.table(
            columns=("source", "market", "calendar", "trade_date", "is_trading_day"),
            rows=rows,
            payload=records[0] if args.date is not None else records,
        )

    return CommandSpec(
        id=("market-data-provider", "trading-dates"),
        summary="Fetch trading-date records from the runtime market data provider.",
        description="Fetch trading-date records from the runtime market data provider.",
        exposure=Exposure.SHARED,
        auth=AuthPolicy.PUBLIC,
        execution=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=handler,
    )
