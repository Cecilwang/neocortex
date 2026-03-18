"""Command-line interface for connector-backed market data access."""

from __future__ import annotations

import argparse
from datetime import date
from typing import Any, Sequence

import pandas as pd
from neocortex.config.env import load_dotenv
from neocortex.connectors import AkShareConnector
from neocortex.feishu import FeishuLongConnectionRunner, FeishuSettings
from neocortex.log import configure_logging
from neocortex.models import Exchange, Market, SecurityId
from neocortex.serialization import to_pretty_json


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _build_cn_security_id(args: argparse.Namespace) -> SecurityId:
    return SecurityId(
        symbol=args.symbol,
        market=Market.CN,
        exchange=Exchange(args.exchange),
    )


def _emit_json(payload: Any) -> None:
    print(to_pretty_json(payload))


def _emit_dataframe(payload: pd.DataFrame) -> None:
    print(payload.to_string(index=False))


def _run_akshare_profile(args: argparse.Namespace) -> int:
    connector = AkShareConnector(timeout=args.timeout)
    profile = connector.get_company_profile(_build_cn_security_id(args))
    _emit_json(profile)
    return 0


def _run_akshare_bars(args: argparse.Namespace) -> int:
    connector = AkShareConnector(timeout=args.timeout)
    bars = connector.get_price_bars(
        _build_cn_security_id(args),
        start_date=args.start_date,
        end_date=args.end_date,
        adjust=args.adjust,
    )
    _emit_dataframe(bars.bars)
    return 0


def _run_feishu_longconn(args: argparse.Namespace) -> int:
    if args.env_file is not None:
        load_dotenv(args.env_file, override=True)
    configure_logging(args.log_level)
    settings = FeishuSettings.from_env()
    runner = FeishuLongConnectionRunner(settings)
    runner.start()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="neocortex")
    subcommands = parser.add_subparsers(dest="provider", required=True)

    akshare_parser = subcommands.add_parser("akshare")
    akshare_commands = akshare_parser.add_subparsers(dest="command", required=True)

    profile_parser = akshare_commands.add_parser("profile")
    profile_parser.add_argument("--symbol", required=True)
    profile_parser.add_argument("--exchange", required=True, choices=("XSHG", "XSHE"))
    profile_parser.add_argument("--timeout", type=float, default=None)
    profile_parser.set_defaults(handler=_run_akshare_profile)

    bars_parser = akshare_commands.add_parser("bars")
    bars_parser.add_argument("--symbol", required=True)
    bars_parser.add_argument("--exchange", required=True, choices=("XSHG", "XSHE"))
    bars_parser.add_argument("--start-date", required=True, type=_parse_date)
    bars_parser.add_argument("--end-date", required=True, type=_parse_date)
    bars_parser.add_argument("--adjust", default=None)
    bars_parser.add_argument("--timeout", type=float, default=None)
    bars_parser.set_defaults(handler=_run_akshare_bars)

    feishu_parser = subcommands.add_parser("feishu")
    feishu_commands = feishu_parser.add_subparsers(dest="command", required=True)

    longconn_parser = feishu_commands.add_parser("longconn")
    longconn_parser.add_argument("--env-file", default=None)
    longconn_parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    longconn_parser.set_defaults(handler=_run_feishu_longconn)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.handler(args)
