"""Shared helpers for command-kernel tests."""

from __future__ import annotations

import argparse

from neocortex.commands import (
    AuthPolicy,
    CommandArgumentParser,
    CommandContext,
    CommandRegistry,
    CommandResult,
    CommandSpec,
    ExecutionMode,
    Exposure,
)


def demo_handler(args: argparse.Namespace, context: CommandContext) -> CommandResult:
    return CommandResult.text(
        f"{context.actor.source.value}:{args.target}:{args.count}:{args.verbose}:{len(args.labels)}"
    )


def simple_target_handler(
    args: argparse.Namespace, context: CommandContext
) -> CommandResult:
    _ = context
    return CommandResult.text(f"registry:{args.target}")


def demo_spec(
    *,
    auth: AuthPolicy = AuthPolicy.PUBLIC,
    execution_mode: ExecutionMode = ExecutionMode.SYNC,
    require_target_source: bool = False,
) -> CommandSpec:
    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("target")
        parser.add_argument(
            "--count", type=int, default=1, help="How many times to run."
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose output.",
        )
        parser.add_argument(
            "--label",
            dest="labels",
            action="append",
            default=[],
            help="Repeatable labels.",
        )
        if require_target_source:
            source_group = parser.add_mutually_exclusive_group(required=True)
            source_group.add_argument("--symbol", default=None)
            source_group.add_argument("--name", default=None)

    return CommandSpec(
        id=("demo", "run"),
        summary="Run a demo command.",
        description="Run a demo command with typed arguments.",
        exposure=Exposure.SHARED,
        auth=auth,
        execution_mode=execution_mode,
        configure_parser=configure_parser,
        handler=demo_handler,
    )


def inspect_spec(
    command_id: tuple[str, ...],
    *,
    description: str,
) -> CommandSpec:
    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("target")

    return CommandSpec(
        id=command_id,
        summary=f"Summary for {' '.join(command_id)}",
        description=description,
        exposure=Exposure.SHARED,
        auth=AuthPolicy.PUBLIC,
        execution_mode=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=demo_handler,
    )


def build_registry_parser(registry: CommandRegistry) -> CommandArgumentParser:
    parser = CommandArgumentParser(prog="neocortex")
    subcommands = parser.add_subparsers(dest="_command_root", required=True)
    registry.bind_subcommands(subcommands)
    return parser
