"""Command-line interface for connector-backed market data access."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from typing import Any, Sequence

from neocortex.connectors import AkShareConnector
from neocortex.models import Exchange, Market, SecurityId


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _json_ready(value: Any) -> Any:
    if is_dataclass(value):
        return _json_ready(asdict(value))
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_json_ready(item) for item in value]
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


def _build_cn_security_id(args: argparse.Namespace) -> SecurityId:
    return SecurityId(
        symbol=args.symbol,
        market=Market.CN,
        exchange=Exchange(args.exchange),
    )


def _emit_json(payload: Any) -> None:
    print(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2))


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
    _emit_json(bars)
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

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.handler(args)
