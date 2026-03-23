"""Indicator CLI commands."""

from __future__ import annotations

import argparse
import json
import logging
from typing import Any

from neocortex.indicators import calculate_indicator, list_indicator_specs
from neocortex.models import PRICE_BAR_TIMESTAMP
from neocortex.serialization import to_pretty_json

from neocortex.cli.common import (
    add_date_range_arguments,
    add_security_identity_arguments,
    market_data_provider,
    resolve_date_range,
    resolve_security_id,
)

logger = logging.getLogger(__name__)


def run_indicator_list(args: argparse.Namespace) -> int:
    _ = args
    logger.info("Running indicator list command.")
    print(
        to_pretty_json(
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
    )
    return 0


def run_indicator_command(args: argparse.Namespace) -> int:
    security_id = resolve_security_id(args)
    provider = market_data_provider(args)
    start_date, end_date = resolve_date_range(
        market=security_id.market,
        provider=provider,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    parameters = _parse_indicator_params(args.param)
    indicator_key = args.indicator_key
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
    indicator = calculate_indicator(
        indicator_key,
        bars,
        parameters=parameters or None,
    )
    logger.info(
        f"Calculated indicator: indicator={indicator.spec.key} "
        f"security={security_id.ticker} row_count={len(indicator.data)}"
    )
    if args.format == "table":
        print(indicator.data.to_string(index=False))
        return 0
    print(
        to_pretty_json(
            {
                "indicator": indicator.spec.key,
                "display_name": indicator.spec.display_name,
                "category": indicator.spec.category,
                "formula": indicator.spec.formula,
                "parameters": parameters,
                "rows": _indicator_rows(indicator.data),
            }
        )
    )
    return 0


def add_indicator_commands(
    subcommands: argparse._SubParsersAction[argparse.ArgumentParser],
    *,
    default_db_path: str,
) -> None:
    indicator_parser = subcommands.add_parser("indicator")
    indicator_parser.add_argument("--db-path", type=str, default=default_db_path)
    indicator_commands = indicator_parser.add_subparsers(dest="command", required=True)

    indicator_list = indicator_commands.add_parser("list")
    indicator_list.set_defaults(handler=run_indicator_list)

    for indicator in list_indicator_specs():
        indicator_parser = indicator_commands.add_parser(indicator.key)
        add_security_identity_arguments(indicator_parser)
        add_date_range_arguments(indicator_parser)
        indicator_parser.add_argument(
            "--adjust",
            default="qfq",
            help="Defaults to qfq.",
        )
        indicator_parser.add_argument(
            "--param",
            action="append",
            default=[],
            help="Repeatable indicator parameter in key=value form.",
        )
        indicator_parser.add_argument(
            "--format",
            default="table",
            choices=("table", "json"),
        )
        indicator_parser.set_defaults(
            handler=run_indicator_command,
            indicator_key=indicator.key,
        )


def _parse_indicator_params(values: list[str]) -> dict[str, object]:
    parameters: dict[str, object] = {}
    for raw_value in values:
        key, separator, value = raw_value.partition("=")
        if not separator or not key:
            raise ValueError(
                f"Indicator parameter must use key=value format, got {raw_value!r}."
            )
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
