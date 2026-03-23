"""CLI parser construction and main entrypoint."""

from __future__ import annotations

import argparse
import logging
from typing import Sequence

from neocortex.config import get_config, load_dotenv, reset_config_cache
from neocortex.log import configure_logging

from neocortex.cli.agent import add_agent_commands
from neocortex.cli.connector import add_connector_commands
from neocortex.cli.db import add_db_commands
from neocortex.cli.feishu import add_feishu_commands
from neocortex.cli.indicator import add_indicator_commands
from neocortex.cli.market_data import add_market_data_provider_commands
from neocortex.cli.sync import add_sync_commands

logger = logging.getLogger(__name__)


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

    add_db_commands(
        subcommands,
        default_db_path=str(app_config.storage.market_data_db_path),
    )
    add_connector_commands(subcommands)
    add_sync_commands(
        subcommands,
        default_db_path=str(app_config.storage.market_data_db_path),
    )
    add_market_data_provider_commands(
        subcommands,
        default_db_path=str(app_config.storage.market_data_db_path),
    )
    add_indicator_commands(
        subcommands,
        default_db_path=str(app_config.storage.market_data_db_path),
    )
    add_agent_commands(
        subcommands,
        default_db_path=str(app_config.storage.market_data_db_path),
    )
    add_feishu_commands(subcommands)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--env-file", default=None)
    bootstrap_args, _ = bootstrap.parse_known_args(argv)
    if bootstrap_args.env_file is None:
        loaded = load_dotenv(override=True)
    else:
        loaded = load_dotenv(bootstrap_args.env_file, override=True)
    if loaded:
        reset_config_cache()
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    logger.info(
        f"CLI parsed command: domain={getattr(args, 'domain', None)} "
        f"command={getattr(args, 'command', None)}"
    )
    return args.handler(args)
