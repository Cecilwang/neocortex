"""Bot action execution for supported Neocortex capabilities."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

from neocortex.connectors import AkShareConnector
from neocortex.feishu.commands import HELP_TEXT
from neocortex.feishu.models import BotCommand
from neocortex.feishu.storage import FeishuBotStore
from neocortex.models import Exchange, Market, SecurityId
from neocortex.serialization import to_pretty_json
from neocortex.storage import backfill_company_profiles
from neocortex.storage.query import build_query, execute_query, render_table


logger = logging.getLogger(__name__)


class BotActionRunner:
    """Run one parsed bot command against Neocortex capabilities."""

    def __init__(self, *, store: FeishuBotStore, db_path: str | Path) -> None:
        self.store = store
        self.db_path = Path(db_path)

    def run(self, command: BotCommand) -> str:
        """Execute one command and return user-facing text."""

        if command.name == "help":
            return HELP_TEXT
        if command.name == "profile":
            return self._run_profile(command)
        if command.name == "bars":
            return self._run_bars(command)
        if command.name == "db_table":
            return self._run_db_table(command)
        if command.name == "db_sql":
            return self._run_db_sql(command)
        if command.name == "backfill_profiles":
            return self._run_backfill_profiles(command)
        if command.name == "job_status":
            return self._run_job_status(command)
        if command.name == "pipeline_run":
            return (
                "Pipeline command is reserved but not available yet. "
                "The repository still lacks concrete agent modules, so only the bot "
                "integration hook is shipped in this change."
            )
        raise ValueError(f"Unsupported command: {command.name}")

    def _run_profile(self, command: BotCommand) -> str:
        connector = AkShareConnector(timeout=command.args["timeout"])
        security_id = _build_cn_security_id(command)
        profile = connector.get_company_profile(security_id)
        return to_pretty_json(profile)

    def _run_bars(self, command: BotCommand) -> str:
        connector = AkShareConnector(timeout=command.args["timeout"])
        bars = connector.get_price_bars(
            _build_cn_security_id(command),
            start_date=date.fromisoformat(str(command.args["start_date"])),
            end_date=date.fromisoformat(str(command.args["end_date"])),
            adjust=command.args["adjust"],
        )
        return bars.to_df().to_string(index=False)

    def _run_db_table(self, command: BotCommand) -> str:
        query = build_query(
            sql=None,
            table=str(command.args["table"]),
            limit=int(command.args["limit"]),
        )
        columns, rows = execute_query(str(self.db_path), query)
        rendered = render_table(columns, rows)
        return rendered or "No rows."

    def _run_db_sql(self, command: BotCommand) -> str:
        query = str(command.args["sql"])
        _ensure_read_only_sql(query)
        columns, rows = execute_query(str(self.db_path), query)
        rendered = render_table(columns, rows)
        return rendered or "No rows."

    def _run_backfill_profiles(self, command: BotCommand) -> str:
        stats = backfill_company_profiles(
            self.db_path,
            timeout=command.args["timeout"],
            limit=command.args["limit"],
            retry_count=int(command.args["retry_count"]),
            sleep_seconds=float(command.args["sleep_seconds"]),
            workers=int(command.args["workers"]),
        )
        return (
            "Backfill complete: "
            f"processed={stats.processed} "
            f"fetched={stats.fetched} "
            f"skipped_unsupported={stats.skipped_unsupported} "
            f"failed={stats.failed}"
        )

    def _run_job_status(self, command: BotCommand) -> str:
        job = self.store.get_job(int(command.args["job_id"]))
        if job is None:
            return "Job not found."
        parts = [
            f"job_id={job.id}",
            f"status={job.status.value}",
            f"command={job.command_name}",
            f"submitted_at={job.submitted_at}",
        ]
        if job.started_at is not None:
            parts.append(f"started_at={job.started_at}")
        if job.finished_at is not None:
            parts.append(f"finished_at={job.finished_at}")
        if job.result_text:
            parts.append(f"result={job.result_text}")
        if job.error_text:
            parts.append(f"error={job.error_text}")
        return "\n".join(parts)


def _build_cn_security_id(command: BotCommand) -> SecurityId:
    return SecurityId(
        symbol=str(command.args["symbol"]),
        market=Market.CN,
        exchange=Exchange(str(command.args["exchange"])),
    )


def _ensure_read_only_sql(query: str) -> None:
    normalized = " ".join(query.strip().lower().split())
    if ";" in normalized.rstrip(";"):
        raise ValueError("Only one SQL statement is allowed.")
    if not normalized.startswith(("select ", "with ", "explain ")):
        raise ValueError("Only read-only SQL is allowed.")
    blocked_keywords = (
        " insert ",
        " update ",
        " delete ",
        " drop ",
        " alter ",
        " pragma ",
        " attach ",
    )
    padded = f" {normalized} "
    if any(keyword in padded for keyword in blocked_keywords):
        raise ValueError("Only read-only SQL is allowed.")
