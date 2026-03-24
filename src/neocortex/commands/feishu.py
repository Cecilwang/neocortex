"""Registry-backed Feishu CLI commands."""

from __future__ import annotations

import argparse
import logging

from neocortex.commands.core import (
    AuthPolicy,
    CommandContext,
    CommandResult,
    CommandSpec,
    ExecutionMode,
    Exposure,
)
from neocortex.feishu import FeishuLongConnectionRunner, FeishuSettings


logger = logging.getLogger(__name__)


def _handle_feishu_longconn(
    args: argparse.Namespace,
    context: CommandContext,
) -> CommandResult:
    _ = args, context
    logger.info("Starting Feishu long connection runner.")
    settings = FeishuSettings.from_env()
    runner = FeishuLongConnectionRunner(settings)
    runner.start()
    return CommandResult.text("")


def build_feishu_longconn_command_spec() -> CommandSpec:
    def configure_parser(parser: argparse.ArgumentParser) -> None:
        _ = parser

    return CommandSpec(
        id=("feishu", "longconn"),
        summary="Start the Feishu long-connection bot runner.",
        description="Start the Feishu long-connection bot runner.",
        exposure=Exposure.CLI_ONLY,
        auth=AuthPolicy.PUBLIC,
        execution=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=_handle_feishu_longconn,
    )


def build_feishu_command_specs() -> tuple[CommandSpec, ...]:
    return (build_feishu_longconn_command_spec(),)
