"""Feishu CLI commands."""

from __future__ import annotations

import argparse
import logging

from neocortex.feishu import FeishuLongConnectionRunner, FeishuSettings

logger = logging.getLogger(__name__)


def run_feishu_longconn(args: argparse.Namespace) -> int:
    _ = args
    logger.info("Starting Feishu long connection runner.")
    settings = FeishuSettings.from_env()
    runner = FeishuLongConnectionRunner(settings)
    runner.start()
    return 0


def add_feishu_commands(
    subcommands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    feishu_parser = subcommands.add_parser("feishu")
    feishu_commands = feishu_parser.add_subparsers(dest="command", required=True)
    longconn_parser = feishu_commands.add_parser("longconn")
    longconn_parser.set_defaults(handler=run_feishu_longconn)
