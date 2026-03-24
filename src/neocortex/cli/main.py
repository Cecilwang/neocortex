"""CLI parser construction and main entrypoint."""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Sequence

from neocortex.commands import (
    CommandActor,
    CommandHelpRequested,
    CommandContext,
    CommandServices,
    CommandUsageError,
    InvocationSource,
    build_command_registry,
)
from neocortex.config import get_config, load_dotenv, reset_config_cache
from neocortex.log import configure_logging

from neocortex.cli.agent import add_agent_commands
from neocortex.cli.connector import add_connector_commands
from neocortex.cli.feishu import add_feishu_commands
from neocortex.cli.indicator import add_indicator_commands
from neocortex.cli.render import render_command_result

logger = logging.getLogger(__name__)


def build_legacy_parser() -> argparse.ArgumentParser:
    app_config = get_config()
    parser = argparse.ArgumentParser(prog="neocortex")
    parser.add_argument("--env-file", default=None)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    subcommands = parser.add_subparsers(dest="domain", required=True)

    add_connector_commands(subcommands)
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


def build_parser() -> argparse.ArgumentParser:
    """Return the current legacy parser during mixed-mode migration."""

    return build_legacy_parser()


def _build_bootstrap_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--env-file", default=None)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    return parser


def _run_cli_registry_command(
    command_tokens: Sequence[str],
    *,
    command_id: tuple[str, ...],
    log_level: str,
    registry,
) -> int:
    configure_logging(log_level)
    logger.debug(
        f"CLI dispatch selected registry path: command_id={' '.join(command_id)}"
    )
    context = CommandContext(
        actor=CommandActor(
            source=InvocationSource.CLI,
            user_id="cli",
            is_admin=True,
        ),
        config=get_config(),
        services=CommandServices(),
        request_id="cli",
    )
    try:
        result = registry.run(tuple(command_tokens), context)
    except CommandHelpRequested as exc:
        print(exc.help_text, end="")
        return 0
    except CommandUsageError as exc:
        print(exc.message, file=sys.stderr)
        if exc.help_text:
            print(exc.help_text, file=sys.stderr, end="")
        return exc.status
    return render_command_result(result)


def main(argv: Sequence[str] | None = None) -> int:
    bootstrap = _build_bootstrap_parser()
    bootstrap_args, _ = bootstrap.parse_known_args(argv)
    if bootstrap_args.env_file is None:
        loaded = load_dotenv(override=True)
    else:
        loaded = load_dotenv(bootstrap_args.env_file, override=True)
    if loaded:
        reset_config_cache()
    _, command_tokens = bootstrap.parse_known_args(argv)
    registry = build_command_registry()
    # Temporary mixed-mode split: registry-managed command paths opt into the new
    # kernel while all remaining paths still flow through legacy argparse wiring.
    matched_command = registry.match_command(command_tokens)
    root_command = command_tokens[0] if command_tokens else None
    if matched_command is not None or registry.manages_root(root_command):
        return _run_cli_registry_command(
            command_tokens,
            command_id=matched_command or (root_command,),
            log_level=bootstrap_args.log_level,
            registry=registry,
        )
    parser = build_legacy_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    logger.info(
        f"CLI dispatch selected legacy path: domain={getattr(args, 'domain', None)}"
    )
    logger.info(
        f"CLI parsed command: domain={getattr(args, 'domain', None)} "
        f"command={getattr(args, 'command', None)}"
    )
    return args.handler(args)
