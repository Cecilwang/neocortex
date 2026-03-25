"""Registry-backed indicator command definitions."""

from __future__ import annotations

import argparse
import json
import logging
from typing import Any

from neocortex.commands.core import (
    AuthPolicy,
    CommandContext,
    CommandResult,
    CommandSpec,
    CommandUsageError,
    ExecutionMode,
    Exposure,
)
from neocortex.date_resolution import add_date_range_arguments, resolve_date_range
from neocortex.indicators import calculate_indicator, list_indicator_specs
from neocortex.market_data_provider import ReadThroughMarketDataProvider
from neocortex.models import PRICE_BAR_TIMESTAMP
from neocortex.security_resolution import (
    add_security_identity_arguments,
    resolve_security_id,
)


logger = logging.getLogger(__name__)


def _flatten_param_values(
    param_groups: list[str] | list[list[str]] | None,
) -> tuple[str, ...]:
    if not param_groups:
        return ()
    flattened: list[str] = []
    for group in param_groups:
        if isinstance(group, list):
            flattened.extend(group)
        else:
            flattened.append(group)
    return tuple(flattened)


def build_indicator_list_command_spec() -> CommandSpec:
    """Build the registry command spec for indicator list."""

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        _ = parser

    def handler(args: argparse.Namespace, context: CommandContext) -> CommandResult:
        _ = args, context
        logger.info("Running indicator list command.")
        return CommandResult.json(
            [
                {
                    "key": indicator.key,
                    "display_name": indicator.display_name,
                    "category": indicator.category,
                    "formula": indicator.formula,
                    "interpretation": indicator.interpretation,
                }
                for indicator in list_indicator_specs()
            ]
        )

    return CommandSpec(
        id=("indicator", "list"),
        summary="List supported indicators.",
        description="List supported indicators.",
        exposure=Exposure.SHARED,
        auth=AuthPolicy.PUBLIC,
        execution_mode=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=handler,
    )


def build_indicator_command_specs(
    *,
    default_db_path: str,
) -> tuple[CommandSpec, ...]:
    """Build registry command specs for all concrete indicator leaves."""

    specs: list[CommandSpec] = []
    for indicator in list_indicator_specs():

        def configure_parser(
            parser: argparse.ArgumentParser,
            *,
            _default_db_path: str = default_db_path,
        ) -> None:
            parser.add_argument("--db-path", type=str, default=_default_db_path)
            add_security_identity_arguments(parser)
            add_date_range_arguments(parser)
            parser.add_argument(
                "--adjust",
                default="qfq",
                help="Defaults to qfq.",
            )
            parser.add_argument(
                "--param",
                action="append",
                nargs="+",
                default=[],
                help=(
                    "One or more indicator parameters in key=value form. Repeat "
                    "--param or pass multiple values after one flag."
                ),
            )
            parser.add_argument(
                "--format",
                default="table",
                choices=("table", "json"),
            )

        def handler(
            args: argparse.Namespace,
            context: CommandContext,
            *,
            indicator_key: str = indicator.key,
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
            try:
                parameters = _parse_indicator_params(args.param)
            except ValueError as exc:
                raise CommandUsageError(str(exc)) from exc
            logger.info(
                f"Running indicator command: indicator={indicator_key} "
                f"security={security_id.ticker} start={start_date} end={end_date} "
                f"adjust={args.adjust} params={parameters} format={args.format}"
            )
            bars = provider.get_price_bars(
                security_id,
                start_date=start_date,
                end_date=end_date,
                adjust=args.adjust,
            )
            logger.info(
                f"Loaded price bars for indicator command: indicator={indicator_key} "
                f"security={security_id.ticker} bar_count={len(bars)}"
            )
            computed = calculate_indicator(
                indicator_key,
                bars,
                parameters=parameters or None,
            )
            logger.info(
                f"Calculated indicator: indicator={computed.spec.key} "
                f"security={security_id.ticker} row_count={len(computed.data)}"
            )
            if args.format == "table":
                return CommandResult.text(
                    computed.data.to_string(index=False),
                    payload=computed,
                )
            return CommandResult.json(
                {
                    "indicator": computed.spec.key,
                    "display_name": computed.spec.display_name,
                    "category": computed.spec.category,
                    "formula": computed.spec.formula,
                    "parameters": parameters,
                    "rows": _indicator_rows(computed.data),
                },
                payload=computed,
            )

        specs.append(
            CommandSpec(
                id=("indicator", indicator.key),
                summary=f"Calculate the {indicator.display_name} indicator.",
                description=f"Calculate the {indicator.display_name} indicator.",
                exposure=Exposure.SHARED,
                auth=AuthPolicy.PUBLIC,
                execution_mode=ExecutionMode.SYNC,
                configure_parser=configure_parser,
                handler=handler,
            )
        )

    return tuple(specs)


def _parse_indicator_params(values: list[str] | list[list[str]]) -> dict[str, object]:
    parameters: dict[str, object] = {}
    for raw_value in _flatten_param_values(values):
        key, separator, value = raw_value.partition("=")
        if not separator or not key:
            raise ValueError(
                f"Indicator parameter must use key=value format, got {raw_value!r}."
            )
        if key in parameters:
            raise ValueError(f"Duplicate indicator parameter key {key!r}.")
        parameters[key] = _coerce_param_value(value)
    return parameters


def _coerce_param_value(value: str) -> object:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _indicator_rows(data) -> list[dict[str, Any]]:
    frame = data.copy()
    if PRICE_BAR_TIMESTAMP in frame.columns:
        frame[PRICE_BAR_TIMESTAMP] = frame[PRICE_BAR_TIMESTAMP].map(
            lambda value: value.isoformat() if hasattr(value, "isoformat") else value
        )
    return frame.to_dict(orient="records")


def build_all_indicator_command_specs(
    *, default_db_path: str
) -> tuple[CommandSpec, ...]:
    return (
        build_indicator_list_command_spec(),
        *build_indicator_command_specs(default_db_path=default_db_path),
    )
