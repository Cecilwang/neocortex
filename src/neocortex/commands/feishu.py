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
from neocortex.feishu.storage import FeishuBotStore


logger = logging.getLogger(__name__)


def _handle_feishu_longconn(
    args: argparse.Namespace,
    context: CommandContext,
) -> CommandResult:
    _ = args, context
    from neocortex.feishu.longconn import FeishuLongConnectionRunner
    from neocortex.feishu.settings import FeishuSettings

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
        execution_mode=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=_handle_feishu_longconn,
    )


def _handle_feishu_cleanup(
    args: argparse.Namespace,
    context: CommandContext,
) -> CommandResult:
    _ = context
    logger.info(
        "Running Feishu bot storage cleanup: "
        f"db_path={args.db_path} older_than_days={args.older_than_days}"
    )
    store = FeishuBotStore(args.db_path)
    receipts_deleted, jobs_deleted = store.cleanup_older_than(
        older_than_days=args.older_than_days
    )
    return CommandResult.text(
        "Feishu bot storage cleanup completed.\n"
        f"Event receipts deleted: {receipts_deleted}\n"
        f"Terminal jobs deleted: {jobs_deleted}"
    )


def build_feishu_cleanup_command_spec(*, default_db_path: str) -> CommandSpec:
    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--db-path",
            default=default_db_path,
            help="Path to the Feishu bot SQLite database.",
        )
        parser.add_argument(
            "--older-than-days",
            type=int,
            required=True,
            help="Delete receipts and terminal jobs older than this many days.",
        )

    return CommandSpec(
        id=("feishu", "cleanup"),
        summary="Delete old Feishu receipts and terminal jobs.",
        description="Delete old Feishu receipts and terminal jobs.",
        exposure=Exposure.CLI_ONLY,
        auth=AuthPolicy.PUBLIC,
        execution_mode=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=_handle_feishu_cleanup,
    )


def build_feishu_command_specs(*, default_db_path: str) -> tuple[CommandSpec, ...]:
    return (
        build_feishu_longconn_command_spec(),
        build_feishu_cleanup_command_spec(default_db_path=default_db_path),
    )
