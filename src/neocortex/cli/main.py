"""CLI parser construction and main entrypoint."""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Sequence

from dotenv import load_dotenv

from neocortex.commands import (
    CommandActor,
    CommandArgumentParser,
    CommandHelpRequested,
    CommandContext,
    CommandDispatcher,
    CommandServices,
    CommandSpec,
    CommandUsageError,
    InvocationSource,
    ParsedInvocation,
    build_command_registry,
)
from neocortex.config import get_config, reset_config_cache
from neocortex.log import configure_logging

from neocortex.cli.render import render_command_result

logger = logging.getLogger(__name__)


def build_base_parser() -> CommandArgumentParser:
    parser = CommandArgumentParser(prog="neocortex", add_help=False)
    parser.add_argument("--env-file", default=None)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    return parser


def _finalize_parser(
    parser: CommandArgumentParser,
    *,
    registry,
) -> None:
    parser.add_argument("-h", "--help", action="help")
    subcommands = parser.add_subparsers(dest="_command_root", required=True)
    registry.bind_subcommands(subcommands)


def _dispatch_cli_command(
    args: argparse.Namespace,
) -> int:
    spec = getattr(args, "_command_spec", None)
    if not isinstance(spec, CommandSpec):
        raise RuntimeError("CLI parser returned without a bound command spec.")
    configure_logging(args.log_level)
    logger.debug(f"CLI dispatch selected registry path: command_id={spec.path}")
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
    dispatcher = CommandDispatcher()
    result = dispatcher.dispatch(
        ParsedInvocation(spec=spec, args=args),
        context,
    )
    return render_command_result(result)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_base_parser()
    bootstrap_args, _ = parser.parse_known_args(argv)
    if bootstrap_args.env_file is None:
        loaded = load_dotenv(override=True)
    else:
        loaded = load_dotenv(bootstrap_args.env_file, override=True)
    if loaded:
        reset_config_cache()
    registry = build_command_registry()
    _finalize_parser(parser, registry=registry)
    try:
        args = parser.parse_args(argv)
    except CommandHelpRequested as exc:
        print(exc.help_text, end="")
        return 0
    except CommandUsageError as exc:
        print(exc.message, file=sys.stderr)
        if exc.help_text:
            print(exc.help_text, file=sys.stderr, end="")
        return exc.status
    return _dispatch_cli_command(args)
