"""Command-line interface for source connectors, market-data provider, agents, and Feishu."""

from __future__ import annotations

import argparse
from datetime import date
import logging
from typing import Any, Sequence

import pandas as pd

from neocortex.config import get_config, load_dotenv, reset_config_cache
from neocortex.connectors import AkShareConnector, BaoStockConnector, EFinanceConnector
from neocortex.feishu import FeishuLongConnectionRunner, FeishuSettings
from neocortex.log import configure_logging
from neocortex.market_data_provider import ReadThroughMarketDataProvider
from neocortex.models import AgentRole, Exchange, Market, SecurityId
from neocortex.pipeline.pipeline import Pipeline
from neocortex.serialization import to_pretty_json
from neocortex.storage import MarketDataStore

logger = logging.getLogger(__name__)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _build_security_id(args: argparse.Namespace) -> SecurityId:
    return SecurityId(
        symbol=args.symbol,
        market=Market(args.market),
        exchange=Exchange(args.exchange),
    )


def _emit_json(payload: Any) -> None:
    print(to_pretty_json(payload))


def _emit_dataframe(payload: pd.DataFrame) -> None:
    print(payload.to_string(index=False))


def _market_data_provider(args: argparse.Namespace) -> ReadThroughMarketDataProvider:
    return ReadThroughMarketDataProvider.from_defaults(args.db_path)


def _connector_for_name(args: argparse.Namespace):
    if args.connector_name == "akshare":
        return AkShareConnector(timeout=getattr(args, "timeout", None))
    if args.connector_name == "baostock":
        return BaoStockConnector()
    if args.connector_name == "efinance":
        return EFinanceConnector()
    raise ValueError(f"Unsupported connector: {args.connector_name}")


def _run_connector_securities(args: argparse.Namespace) -> int:
    logger.info(
        "Running connector securities command: connector=%s market=%s",
        args.connector_name,
        args.market,
    )
    connector = _connector_for_name(args)
    listings = connector.list_securities(market=Market(args.market))
    _emit_json(listings)
    return 0


def _run_connector_profile(args: argparse.Namespace) -> int:
    logger.info(
        "Running connector profile command: connector=%s security=%s.%s",
        args.connector_name,
        args.exchange,
        args.symbol,
    )
    connector = _connector_for_name(args)
    snapshot = connector.get_security_profile_snapshot(_build_security_id(args))
    _emit_json(snapshot)
    return 0


def _run_connector_daily(args: argparse.Namespace) -> int:
    logger.info(
        "Running connector daily command: connector=%s security=%s.%s start=%s end=%s",
        args.connector_name,
        args.exchange,
        args.symbol,
        args.start_date,
        args.end_date,
    )
    connector = _connector_for_name(args)
    records = connector.get_daily_price_bars(
        _build_security_id(args),
        start_date=args.start_date,
        end_date=args.end_date,
    )
    _emit_json(records)
    return 0


def _run_connector_adjusted_daily(args: argparse.Namespace) -> int:
    logger.info(
        "Running connector adjusted-daily command: connector=%s security=%s.%s start=%s end=%s adjust=%s",
        args.connector_name,
        args.exchange,
        args.symbol,
        args.start_date,
        args.end_date,
        args.adjustment_type,
    )
    connector = _connector_for_name(args)
    records = connector.get_adjusted_daily_price_bars(
        _build_security_id(args),
        start_date=args.start_date,
        end_date=args.end_date,
        adjustment_type=args.adjustment_type,
    )
    _emit_json(records)
    return 0


def _run_connector_adjustment_factors(args: argparse.Namespace) -> int:
    logger.info(
        "Running connector adjustment-factors command: connector=%s security=%s.%s start=%s end=%s",
        args.connector_name,
        args.exchange,
        args.symbol,
        args.start_date,
        args.end_date,
    )
    connector = _connector_for_name(args)
    records = connector.get_adjustment_factors(
        _build_security_id(args),
        start_date=args.start_date,
        end_date=args.end_date,
    )
    _emit_json(records)
    return 0


def _run_connector_fundamentals(args: argparse.Namespace) -> int:
    logger.info(
        "Running connector fundamentals command: connector=%s security=%s.%s as_of_date=%s",
        args.connector_name,
        args.exchange,
        args.symbol,
        args.as_of_date,
    )
    connector = _connector_for_name(args)
    records = connector.get_fundamental_snapshots(
        _build_security_id(args),
        as_of_date=args.as_of_date,
    )
    _emit_json(records)
    return 0


def _run_connector_macro(args: argparse.Namespace) -> int:
    logger.info(
        "Running connector macro command: connector=%s market=%s as_of_date=%s",
        args.connector_name,
        args.market,
        args.as_of_date,
    )
    connector = _connector_for_name(args)
    records = connector.get_macro_points(
        market=Market(args.market),
        as_of_date=args.as_of_date,
    )
    _emit_json(records)
    return 0


def _run_market_data_provider_init_db(args: argparse.Namespace) -> int:
    logger.info("Initializing market-data DB: path=%s", args.db_path)
    store = MarketDataStore(args.db_path)
    store.ensure_schema()
    print(f"Initialized market data database at {args.db_path}")
    return 0


def _run_market_data_provider_securities(args: argparse.Namespace) -> int:
    logger.info("Running provider securities command: market=%s", args.market)
    provider = _market_data_provider(args)
    _emit_json(provider.list_securities(market=Market(args.market)))
    return 0


def _run_market_data_provider_profile(args: argparse.Namespace) -> int:
    logger.info(
        "Running provider profile command: security=%s.%s",
        args.exchange,
        args.symbol,
    )
    provider = _market_data_provider(args)
    _emit_json(provider.get_company_profile(_build_security_id(args)))
    return 0


def _run_market_data_provider_bars(args: argparse.Namespace) -> int:
    logger.info(
        "Running provider bars command: security=%s.%s start=%s end=%s adjust=%s",
        args.exchange,
        args.symbol,
        args.start_date,
        args.end_date,
        args.adjust,
    )
    provider = _market_data_provider(args)
    bars = provider.get_price_bars(
        _build_security_id(args),
        start_date=args.start_date,
        end_date=args.end_date,
        adjust=args.adjust,
    )
    _emit_dataframe(bars.bars)
    return 0


def _run_market_data_provider_fundamentals(args: argparse.Namespace) -> int:
    logger.info(
        "Running provider fundamentals command: security=%s.%s as_of_date=%s",
        args.exchange,
        args.symbol,
        args.as_of_date,
    )
    provider = _market_data_provider(args)
    _emit_json(
        provider.get_fundamental_snapshots(
            _build_security_id(args),
            as_of_date=args.as_of_date,
        )
    )
    return 0


def _run_market_data_provider_disclosures(args: argparse.Namespace) -> int:
    logger.info(
        "Running provider disclosures command: security=%s.%s as_of_date=%s",
        args.exchange,
        args.symbol,
        args.as_of_date,
    )
    provider = _market_data_provider(args)
    _emit_json(
        provider.get_disclosure_sections(
            _build_security_id(args),
            as_of_date=args.as_of_date,
        )
    )
    return 0


def _run_market_data_provider_macro(args: argparse.Namespace) -> int:
    logger.info(
        "Running provider macro command: market=%s as_of_date=%s",
        args.market,
        args.as_of_date,
    )
    provider = _market_data_provider(args)
    _emit_json(
        provider.get_macro_points(
            market=Market(args.market),
            as_of_date=args.as_of_date,
        )
    )
    return 0


def _run_agent_render(args: argparse.Namespace) -> int:
    logger.info(
        "Running agent render command: role=%s security=%s.%s as_of_date=%s request_id=%s",
        args.role,
        args.exchange,
        args.symbol,
        args.as_of_date,
        args.request_id,
    )
    provider = _market_data_provider(args)
    pipeline = Pipeline(market_data=provider)
    role = AgentRole(args.role)
    agent = pipeline.get_agent(role)
    request = agent.build_request(
        request_id=args.request_id,
        security_id=_build_security_id(args),
        as_of_date=args.as_of_date,
    )
    _emit_json(
        {
            "request": request,
            "system_prompt": agent.get_system_prompt(request),
            "user_prompt": agent.get_user_prompt(request),
        }
    )
    return 0


def _run_feishu_longconn(args: argparse.Namespace) -> int:
    _ = args
    logger.info("Starting Feishu long connection runner.")
    settings = FeishuSettings.from_env()
    runner = FeishuLongConnectionRunner(settings)
    runner.start()
    return 0


def _add_security_identity_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--market", default="CN", choices=("CN", "US", "JP", "HK"))
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--exchange", required=True)


def build_parser() -> argparse.ArgumentParser:
    app_config = get_config()
    parser = argparse.ArgumentParser(prog="neocortex")
    parser.add_argument("--env-file", default=None)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    subcommands = parser.add_subparsers(dest="domain", required=True)

    connector_parser = subcommands.add_parser("connector")
    connector_commands = connector_parser.add_subparsers(
        dest="connector_name",
        required=True,
    )

    akshare_parser = connector_commands.add_parser("akshare")
    akshare_commands = akshare_parser.add_subparsers(dest="command", required=True)
    akshare_securities = akshare_commands.add_parser("securities")
    akshare_securities.add_argument("--market", default="CN", choices=("CN",))
    akshare_securities.add_argument("--timeout", type=float, default=None)
    akshare_securities.set_defaults(handler=_run_connector_securities)
    akshare_profile = akshare_commands.add_parser("profile")
    _add_security_identity_arguments(akshare_profile)
    akshare_profile.add_argument("--timeout", type=float, default=None)
    akshare_profile.set_defaults(handler=_run_connector_profile)
    akshare_daily = akshare_commands.add_parser("daily")
    _add_security_identity_arguments(akshare_daily)
    akshare_daily.add_argument("--start-date", required=True, type=_parse_date)
    akshare_daily.add_argument("--end-date", required=True, type=_parse_date)
    akshare_daily.add_argument("--timeout", type=float, default=None)
    akshare_daily.set_defaults(handler=_run_connector_daily)
    akshare_adjusted_daily = akshare_commands.add_parser("adjusted-daily")
    _add_security_identity_arguments(akshare_adjusted_daily)
    akshare_adjusted_daily.add_argument("--start-date", required=True, type=_parse_date)
    akshare_adjusted_daily.add_argument("--end-date", required=True, type=_parse_date)
    akshare_adjusted_daily.add_argument(
        "--adjustment-type",
        required=True,
        choices=("qfq", "hfq"),
    )
    akshare_adjusted_daily.add_argument("--timeout", type=float, default=None)
    akshare_adjusted_daily.set_defaults(handler=_run_connector_adjusted_daily)

    efinance_parser = connector_commands.add_parser("efinance")
    efinance_commands = efinance_parser.add_subparsers(dest="command", required=True)
    efinance_securities = efinance_commands.add_parser("securities")
    efinance_securities.add_argument("--market", default="CN", choices=("CN",))
    efinance_securities.set_defaults(handler=_run_connector_securities)
    efinance_profile = efinance_commands.add_parser("profile")
    _add_security_identity_arguments(efinance_profile)
    efinance_profile.set_defaults(handler=_run_connector_profile)
    efinance_daily = efinance_commands.add_parser("daily")
    _add_security_identity_arguments(efinance_daily)
    efinance_daily.add_argument("--start-date", required=True, type=_parse_date)
    efinance_daily.add_argument("--end-date", required=True, type=_parse_date)
    efinance_daily.set_defaults(handler=_run_connector_daily)
    efinance_adjusted_daily = efinance_commands.add_parser("adjusted-daily")
    _add_security_identity_arguments(efinance_adjusted_daily)
    efinance_adjusted_daily.add_argument(
        "--start-date", required=True, type=_parse_date
    )
    efinance_adjusted_daily.add_argument("--end-date", required=True, type=_parse_date)
    efinance_adjusted_daily.add_argument(
        "--adjustment-type",
        required=True,
        choices=("qfq", "hfq"),
    )
    efinance_adjusted_daily.set_defaults(handler=_run_connector_adjusted_daily)

    baostock_parser = connector_commands.add_parser("baostock")
    baostock_commands = baostock_parser.add_subparsers(dest="command", required=True)
    baostock_securities = baostock_commands.add_parser("securities")
    baostock_securities.add_argument("--market", default="CN", choices=("CN",))
    baostock_securities.set_defaults(handler=_run_connector_securities)
    baostock_profile = baostock_commands.add_parser("profile")
    _add_security_identity_arguments(baostock_profile)
    baostock_profile.set_defaults(handler=_run_connector_profile)
    baostock_daily = baostock_commands.add_parser("daily")
    _add_security_identity_arguments(baostock_daily)
    baostock_daily.add_argument("--start-date", required=True, type=_parse_date)
    baostock_daily.add_argument("--end-date", required=True, type=_parse_date)
    baostock_daily.set_defaults(handler=_run_connector_daily)
    baostock_adjusted_daily = baostock_commands.add_parser("adjusted-daily")
    _add_security_identity_arguments(baostock_adjusted_daily)
    baostock_adjusted_daily.add_argument(
        "--start-date", required=True, type=_parse_date
    )
    baostock_adjusted_daily.add_argument("--end-date", required=True, type=_parse_date)
    baostock_adjusted_daily.add_argument(
        "--adjustment-type",
        choices=("qfq", "hfq"),
        required=True,
    )
    baostock_adjusted_daily.set_defaults(handler=_run_connector_adjusted_daily)
    baostock_adjustment = baostock_commands.add_parser("adjustment-factors")
    _add_security_identity_arguments(baostock_adjustment)
    baostock_adjustment.add_argument("--start-date", required=True, type=_parse_date)
    baostock_adjustment.add_argument("--end-date", required=True, type=_parse_date)
    baostock_adjustment.set_defaults(handler=_run_connector_adjustment_factors)
    baostock_fundamentals = baostock_commands.add_parser("fundamentals")
    _add_security_identity_arguments(baostock_fundamentals)
    baostock_fundamentals.add_argument(
        "--as-of-date",
        type=_parse_date,
        default=date.today(),
    )
    baostock_fundamentals.set_defaults(handler=_run_connector_fundamentals)
    baostock_macro = baostock_commands.add_parser("macro")
    baostock_macro.add_argument("--market", default="CN", choices=("CN",))
    baostock_macro.add_argument("--as-of-date", type=_parse_date, default=date.today())
    baostock_macro.set_defaults(handler=_run_connector_macro)

    provider_parser = subcommands.add_parser("market-data-provider")
    provider_parser.add_argument(
        "--db-path",
        type=str,
        default=str(app_config.storage.market_data_db_path),
    )
    provider_commands = provider_parser.add_subparsers(dest="command", required=True)
    provider_init = provider_commands.add_parser("init-db")
    provider_init.set_defaults(handler=_run_market_data_provider_init_db)
    provider_securities = provider_commands.add_parser("securities")
    provider_securities.add_argument(
        "--market",
        default="CN",
        choices=("CN", "US", "JP", "HK"),
    )
    provider_securities.set_defaults(handler=_run_market_data_provider_securities)
    provider_profile = provider_commands.add_parser("profile")
    _add_security_identity_arguments(provider_profile)
    provider_profile.set_defaults(handler=_run_market_data_provider_profile)
    provider_bars = provider_commands.add_parser("bars")
    _add_security_identity_arguments(provider_bars)
    provider_bars.add_argument("--start-date", required=True, type=_parse_date)
    provider_bars.add_argument("--end-date", required=True, type=_parse_date)
    provider_bars.add_argument("--adjust", default=None)
    provider_bars.set_defaults(handler=_run_market_data_provider_bars)
    provider_fundamentals = provider_commands.add_parser("fundamentals")
    _add_security_identity_arguments(provider_fundamentals)
    provider_fundamentals.add_argument(
        "--as-of-date",
        type=_parse_date,
        default=date.today(),
    )
    provider_fundamentals.set_defaults(handler=_run_market_data_provider_fundamentals)
    provider_disclosures = provider_commands.add_parser("disclosures")
    _add_security_identity_arguments(provider_disclosures)
    provider_disclosures.add_argument(
        "--as-of-date",
        type=_parse_date,
        default=date.today(),
    )
    provider_disclosures.set_defaults(handler=_run_market_data_provider_disclosures)
    provider_macro = provider_commands.add_parser("macro")
    provider_macro.add_argument(
        "--market",
        default="CN",
        choices=("CN", "US", "JP", "HK"),
    )
    provider_macro.add_argument("--as-of-date", type=_parse_date, default=date.today())
    provider_macro.set_defaults(handler=_run_market_data_provider_macro)

    agent_parser = subcommands.add_parser("agent")
    agent_parser.add_argument(
        "--db-path",
        type=str,
        default=str(app_config.storage.market_data_db_path),
    )
    agent_commands = agent_parser.add_subparsers(dest="command", required=True)
    agent_render = agent_commands.add_parser("render")
    agent_render.add_argument(
        "--role",
        required=True,
        choices=tuple(role.value for role in AgentRole),
    )
    _add_security_identity_arguments(agent_render)
    agent_render.add_argument("--as-of-date", required=True, type=_parse_date)
    agent_render.add_argument("--request-id", default="cli-agent-render")
    agent_render.set_defaults(handler=_run_agent_render)

    feishu_parser = subcommands.add_parser("feishu")
    feishu_commands = feishu_parser.add_subparsers(dest="command", required=True)
    longconn_parser = feishu_commands.add_parser("longconn")
    longconn_parser.set_defaults(handler=_run_feishu_longconn)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--env-file", default=None)
    bootstrap_args, _ = bootstrap.parse_known_args(argv)
    if bootstrap_args.env_file is not None:
        load_dotenv(bootstrap_args.env_file, override=True)
        reset_config_cache()
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    logger.info(
        "CLI parsed command: domain=%s command=%s",
        getattr(args, "domain", None),
        getattr(args, "command", None),
    )
    return args.handler(args)
