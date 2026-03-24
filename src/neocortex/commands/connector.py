"""Registry-backed connector command definitions."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass

from neocortex.commands.core import (
    AuthPolicy,
    CommandContext,
    CommandResult,
    CommandSpec,
    ExecutionMode,
    Exposure,
)
from neocortex.connectors import AkShareConnector, BaoStockConnector, EFinanceConnector
from neocortex.date_resolution import (
    add_as_of_date_argument,
    add_date_range_arguments,
    resolve_as_of_date,
    resolve_date_range,
)
from neocortex.market_data_provider import ReadThroughMarketDataProvider
from neocortex.models import Market
from neocortex.security_resolution import (
    add_security_identity_arguments,
    resolve_security_id,
)


logger = logging.getLogger(__name__)


def _normalize_cell_value(value: object) -> object:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _flatten_record(
    record: Mapping[object, object],
    *,
    prefix: str = "",
) -> dict[str, object]:
    flattened: dict[str, object] = {}
    for key, value in record.items():
        key_text = str(key)
        field_name = f"{prefix}.{key_text}" if prefix else key_text
        if isinstance(value, Mapping):
            flattened.update(_flatten_record(value, prefix=field_name))
            continue
        flattened[field_name] = _normalize_cell_value(value)
    return flattened


def _record_to_mapping(record: object) -> dict[str, object]:
    if isinstance(record, Mapping):
        return _flatten_record(record)
    if is_dataclass(record):
        return _flatten_record(asdict(record))
    raise TypeError(
        f"Connector table rendering only supports mapping or dataclass records, got {type(record).__name__}."
    )


def _records_result(records: object) -> CommandResult:
    normalized_records = [_record_to_mapping(record) for record in records]
    if not normalized_records:
        return CommandResult.table(columns=(), rows=(), payload=records)
    column_names: list[str] = []
    for record in normalized_records:
        for column in record:
            if column not in column_names:
                column_names.append(column)
    columns = tuple(column_names)
    rows = tuple(
        tuple(record.get(column) for column in columns) for record in normalized_records
    )
    return CommandResult.table(
        columns=columns,
        rows=rows,
        payload=records,
    )


def _connector_for_name(source_name: str, args: argparse.Namespace):
    if source_name == "akshare":
        return AkShareConnector(timeout=getattr(args, "timeout", None))
    if source_name == "baostock":
        return BaoStockConnector()
    if source_name == "efinance":
        return EFinanceConnector()
    raise ValueError(f"Unsupported connector: {source_name}")


def _maybe_add_timeout_argument(
    parser: argparse.ArgumentParser,
    *,
    supports_timeout: bool,
) -> None:
    if supports_timeout:
        parser.add_argument("--timeout", type=float, default=None)


def build_connector_command_specs(
    *,
    default_db_path: str,
) -> tuple[CommandSpec, ...]:
    """Build registry command specs for all connector leaves."""

    specs: list[CommandSpec] = []

    for source_name, capabilities in (
        (
            "akshare",
            {
                "supports_timeout": True,
                "supports_adjustment_factors": False,
                "supports_fundamentals": False,
                "supports_macro": False,
            },
        ),
        (
            "efinance",
            {
                "supports_timeout": False,
                "supports_adjustment_factors": False,
                "supports_fundamentals": False,
                "supports_macro": False,
            },
        ),
        (
            "baostock",
            {
                "supports_timeout": False,
                "supports_adjustment_factors": True,
                "supports_fundamentals": True,
                "supports_macro": True,
            },
        ),
    ):
        supports_timeout = capabilities["supports_timeout"]

        def build_spec(
            command_name: str,
            *,
            configure_parser,
            handler,
            summary: str,
        ) -> None:
            specs.append(
                CommandSpec(
                    id=("connector", source_name, command_name),
                    summary=summary,
                    description=summary,
                    exposure=Exposure.SHARED,
                    auth=AuthPolicy.PUBLIC,
                    execution=ExecutionMode.SYNC,
                    configure_parser=configure_parser,
                    handler=handler,
                )
            )

        def configure_securities(
            parser: argparse.ArgumentParser,
            *,
            _default_db_path: str = default_db_path,
            _supports_timeout: bool = supports_timeout,
        ) -> None:
            parser.add_argument("--db-path", type=str, default=_default_db_path)
            parser.add_argument("--market", default="CN", choices=("CN",))
            _maybe_add_timeout_argument(parser, supports_timeout=_supports_timeout)

        def handle_securities(
            args: argparse.Namespace,
            context: CommandContext,
            *,
            _source_name: str = source_name,
        ) -> CommandResult:
            _ = context
            logger.info(
                f"Running connector securities command: connector={_source_name} market={args.market}"
            )
            connector = _connector_for_name(_source_name, args)
            records = connector.list_securities(market=Market(args.market))
            return _records_result(records)

        build_spec(
            "securities",
            configure_parser=configure_securities,
            handler=handle_securities,
            summary=f"List securities from the {source_name} connector.",
        )

        def configure_profile(
            parser: argparse.ArgumentParser,
            *,
            _default_db_path: str = default_db_path,
            _supports_timeout: bool = supports_timeout,
        ) -> None:
            parser.add_argument("--db-path", type=str, default=_default_db_path)
            add_security_identity_arguments(parser)
            _maybe_add_timeout_argument(parser, supports_timeout=_supports_timeout)

        def handle_profile(
            args: argparse.Namespace,
            context: CommandContext,
            *,
            _source_name: str = source_name,
        ) -> CommandResult:
            _ = context
            security_id = resolve_security_id(args, db_path=args.db_path)
            logger.info(
                f"Running connector profile command: connector={_source_name} security={security_id.ticker}"
            )
            connector = _connector_for_name(_source_name, args)
            return CommandResult.json(
                connector.get_security_profile_snapshot(security_id)
            )

        build_spec(
            "profile",
            configure_parser=configure_profile,
            handler=handle_profile,
            summary=f"Fetch one security profile from the {source_name} connector.",
        )

        def configure_daily(
            parser: argparse.ArgumentParser,
            *,
            _default_db_path: str = default_db_path,
            _supports_timeout: bool = supports_timeout,
        ) -> None:
            parser.add_argument("--db-path", type=str, default=_default_db_path)
            add_security_identity_arguments(parser)
            add_date_range_arguments(parser)
            _maybe_add_timeout_argument(parser, supports_timeout=_supports_timeout)

        def handle_daily(
            args: argparse.Namespace,
            context: CommandContext,
            *,
            _source_name: str = source_name,
        ) -> CommandResult:
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
                f"Running connector daily command: connector={_source_name} "
                f"security={security_id.ticker} start={start_date} end={end_date}"
            )
            connector = _connector_for_name(_source_name, args)
            records = connector.get_daily_price_bars(
                security_id,
                start_date=start_date,
                end_date=end_date,
            )
            return _records_result(records)

        build_spec(
            "daily",
            configure_parser=configure_daily,
            handler=handle_daily,
            summary=f"Fetch raw daily price bars from the {source_name} connector.",
        )

        def configure_adjusted_daily(
            parser: argparse.ArgumentParser,
            *,
            _default_db_path: str = default_db_path,
            _supports_timeout: bool = supports_timeout,
        ) -> None:
            parser.add_argument("--db-path", type=str, default=_default_db_path)
            add_security_identity_arguments(parser)
            add_date_range_arguments(parser)
            parser.add_argument(
                "--adjustment-type",
                required=True,
                choices=("qfq", "hfq"),
            )
            _maybe_add_timeout_argument(parser, supports_timeout=_supports_timeout)

        def handle_adjusted_daily(
            args: argparse.Namespace,
            context: CommandContext,
            *,
            _source_name: str = source_name,
        ) -> CommandResult:
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
                f"Running connector adjusted-daily command: connector={_source_name} "
                f"security={security_id.ticker} start={start_date} end={end_date} "
                f"adjust={args.adjustment_type}"
            )
            connector = _connector_for_name(_source_name, args)
            records = connector.get_adjusted_daily_price_bars(
                security_id,
                start_date=start_date,
                end_date=end_date,
                adjustment_type=args.adjustment_type,
            )
            return _records_result(records)

        build_spec(
            "adjusted-daily",
            configure_parser=configure_adjusted_daily,
            handler=handle_adjusted_daily,
            summary=f"Fetch adjusted daily price bars from the {source_name} connector.",
        )

        if capabilities["supports_adjustment_factors"]:

            def configure_adjustment_factors(
                parser: argparse.ArgumentParser,
                *,
                _default_db_path: str = default_db_path,
            ) -> None:
                parser.add_argument("--db-path", type=str, default=_default_db_path)
                add_security_identity_arguments(parser)
                add_date_range_arguments(parser)

            def handle_adjustment_factors(
                args: argparse.Namespace,
                context: CommandContext,
                *,
                _source_name: str = source_name,
            ) -> CommandResult:
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
                    f"Running connector adjustment-factors command: connector={_source_name} "
                    f"security={security_id.ticker} start={start_date} end={end_date}"
                )
                connector = _connector_for_name(_source_name, args)
                records = connector.get_adjustment_factors(
                    security_id,
                    start_date=start_date,
                    end_date=end_date,
                )
                return _records_result(records)

            build_spec(
                "adjustment-factors",
                configure_parser=configure_adjustment_factors,
                handler=handle_adjustment_factors,
                summary=f"Fetch adjustment factors from the {source_name} connector.",
            )

        if capabilities["supports_fundamentals"]:

            def configure_fundamentals(
                parser: argparse.ArgumentParser,
                *,
                _default_db_path: str = default_db_path,
            ) -> None:
                parser.add_argument("--db-path", type=str, default=_default_db_path)
                add_security_identity_arguments(parser)
                add_as_of_date_argument(parser)

            def handle_fundamentals(
                args: argparse.Namespace,
                context: CommandContext,
                *,
                _source_name: str = source_name,
            ) -> CommandResult:
                _ = context
                security_id = resolve_security_id(args, db_path=args.db_path)
                provider = ReadThroughMarketDataProvider.from_defaults(args.db_path)
                as_of_date = resolve_as_of_date(
                    market=security_id.market,
                    provider=provider,
                    as_of_date=args.as_of_date,
                )
                logger.info(
                    f"Running connector fundamentals command: connector={_source_name} "
                    f"security={security_id.ticker} as_of_date={as_of_date}"
                )
                connector = _connector_for_name(_source_name, args)
                records = connector.get_fundamental_snapshots(
                    security_id,
                    as_of_date=as_of_date,
                )
                return _records_result(records)

            build_spec(
                "fundamentals",
                configure_parser=configure_fundamentals,
                handler=handle_fundamentals,
                summary=f"Fetch fundamentals from the {source_name} connector.",
            )

        if capabilities["supports_macro"]:

            def configure_macro(
                parser: argparse.ArgumentParser,
                *,
                _default_db_path: str = default_db_path,
            ) -> None:
                parser.add_argument("--db-path", type=str, default=_default_db_path)
                parser.add_argument("--market", default="CN", choices=("CN",))
                add_as_of_date_argument(parser)

            def handle_macro(
                args: argparse.Namespace,
                context: CommandContext,
                *,
                _source_name: str = source_name,
            ) -> CommandResult:
                _ = context
                provider = ReadThroughMarketDataProvider.from_defaults(args.db_path)
                market = Market(args.market)
                as_of_date = resolve_as_of_date(
                    market=market,
                    provider=provider,
                    as_of_date=args.as_of_date,
                )
                logger.info(
                    f"Running connector macro command: connector={_source_name} "
                    f"market={args.market} as_of_date={as_of_date}"
                )
                connector = _connector_for_name(_source_name, args)
                records = connector.get_macro_points(
                    market=market,
                    as_of_date=as_of_date,
                )
                return _records_result(records)

            build_spec(
                "macro",
                configure_parser=configure_macro,
                handler=handle_macro,
                summary=f"Fetch macro points from the {source_name} connector.",
            )

    return tuple(specs)
