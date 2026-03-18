"""Feishu event handling and bot workflow orchestration."""

from __future__ import annotations

from concurrent.futures import Executor, ThreadPoolExecutor
import json
import logging
import re
from typing import Any

from neocortex.feishu.actions import BotActionRunner
from neocortex.feishu.client import FeishuClient
from neocortex.feishu.commands import parse_command
from neocortex.feishu.models import BotCommand, FeishuMessageEvent
from neocortex.feishu.settings import FeishuSettings
from neocortex.feishu.storage import FeishuBotStore


logger = logging.getLogger(__name__)
_AT_TAG_PATTERN = re.compile(r"<at\b[^>]*>.*?</at>")
_LEADING_AT_MENTION_PATTERN = re.compile(r"^(?:@\S+\s*)+")


class FeishuBotService:
    """Coordinate Feishu event intake, command routing, and async jobs."""

    def __init__(
        self,
        settings: FeishuSettings,
        *,
        client: FeishuClient | None = None,
        store: FeishuBotStore | None = None,
        action_runner: BotActionRunner | None = None,
        executor: Executor | None = None,
    ) -> None:
        self.settings = settings
        self.store = store or FeishuBotStore(settings.db_path)
        self.client = client or FeishuClient(settings)
        self.action_runner = action_runner or BotActionRunner(
            store=self.store,
            db_path=settings.db_path,
        )
        self.executor = executor or ThreadPoolExecutor(max_workers=settings.job_workers)
        logger.info(
            "Feishu bot service ready: db_path=%s admins=%s workers=%s",
            settings.db_path,
            len(settings.admin_open_ids),
            settings.job_workers,
        )

    def handle_event_payload(self, payload: dict[str, Any]) -> None:
        """Handle one Feishu event payload."""

        event = _extract_message_event(payload)
        if event is None:
            logger.debug("Ignoring unsupported Feishu payload.")
            return

        logger.info(
            "Received Feishu message: event_id=%s chat_id=%s sender=%s text=%r",
            event.event_id,
            event.chat_id,
            event.sender_open_id,
            event.text,
        )
        if not self.store.record_event(
            event_id=event.event_id, message_id=event.message_id
        ):
            logger.info("Skipping duplicate Feishu event %s.", event.event_id)
            return

        try:
            command = parse_command(event.text)
        except ValueError as exc:
            logger.warning("Invalid bot command from event %s: %s", event.event_id, exc)
            self._send_reply(event.chat_id, f"Invalid command.\n\n{exc}")
            return

        if command is None:
            logger.info("Ignoring non-command message %s.", event.message_id)
            return
        if (
            command.requires_admin
            and event.sender_open_id not in self.settings.admin_open_ids
        ):
            logger.warning(
                "Rejected admin command=%s from sender=%s",
                command.name,
                event.sender_open_id,
            )
            self._send_reply(event.chat_id, "Permission denied for this command.")
            return

        logger.info(
            "Dispatching command=%s async=%s sender=%s",
            command.name,
            command.asynchronous,
            event.sender_open_id,
        )
        if command.asynchronous:
            job = self.store.create_job(
                command_name=command.name,
                command_text=command.text,
                chat_id=event.chat_id,
                user_open_id=event.sender_open_id,
            )
            logger.info("Queued async job_id=%s command=%s", job.id, command.name)
            self._send_reply(
                event.chat_id,
                f"Accepted job {job.id}: {command.name}. Use `/neo job {job.id}` to query status.",
            )
            self.executor.submit(self._run_job, job.id, command)
            return

        try:
            result_text = self.action_runner.run(command)
        except Exception as exc:
            logger.exception("Quick bot command %s failed.", command.name)
            self._send_reply(
                event.chat_id,
                f"{command.name} failed.\n{type(exc).__name__}: {exc}",
            )
            return

        self._send_reply(event.chat_id, result_text)

    def _run_job(self, job_id: int, command: BotCommand) -> None:
        self.store.mark_job_running(job_id)
        logger.info("Running async job_id=%s command=%s", job_id, command.name)
        job = self.store.get_job(job_id)
        if job is None:
            return
        try:
            result_text = self.action_runner.run(command)
        except Exception as exc:
            error_text = f"{type(exc).__name__}: {exc}"
            self.store.mark_job_failed(job_id, error_text=error_text)
            logger.exception("Async bot job %s failed.", job_id)
            self._send_reply(job.chat_id, f"Job {job_id} failed.\n{error_text}")
            return

        self.store.mark_job_succeeded(job_id, result_text=result_text)
        logger.info("Async job_id=%s succeeded", job_id)
        self._send_reply(job.chat_id, f"Job {job_id} succeeded.\n{result_text}")

    def _send_reply(self, chat_id: str, text: str) -> None:
        logger.info("Sending reply to chat_id=%s chars=%s", chat_id, len(text))
        self.client.send_text(
            chat_id=chat_id, text=_truncate(text, self.settings.max_reply_chars)
        )


def _extract_message_event(payload: dict[str, Any]) -> FeishuMessageEvent | None:
    header = payload.get("header")
    event = payload.get("event")
    if not isinstance(header, dict) or not isinstance(event, dict):
        return None
    message = event.get("message")
    sender = event.get("sender")
    if not isinstance(message, dict) or not isinstance(sender, dict):
        return None
    if message.get("message_type") != "text":
        return None

    sender_id = sender.get("sender_id")
    if not isinstance(sender_id, dict):
        return None

    event_id = str(header.get("event_id") or "")
    message_id = str(message.get("message_id") or "")
    chat_id = str(message.get("chat_id") or "")
    chat_type = str(message.get("chat_type") or "")
    sender_open_id = str(sender_id.get("open_id") or sender_id.get("user_id") or "")
    raw_text = _extract_text_content(message.get("content"))
    normalized_text = _normalize_message_text(raw_text)
    if not all((event_id, message_id, chat_id, sender_open_id)):
        return None
    return FeishuMessageEvent(
        event_id=event_id,
        message_id=message_id,
        chat_id=chat_id,
        chat_type=chat_type,
        sender_open_id=sender_open_id,
        text=normalized_text,
    )


def _extract_text_content(raw_content: object) -> str:
    if isinstance(raw_content, str):
        try:
            document = json.loads(raw_content)
        except json.JSONDecodeError:
            return raw_content
        if isinstance(document, dict):
            text = document.get("text")
            if isinstance(text, str):
                return text
    return ""


def _normalize_message_text(raw_text: str) -> str:
    without_tags = " ".join(_AT_TAG_PATTERN.sub(" ", raw_text).split())
    return _LEADING_AT_MENTION_PATTERN.sub("", without_tags).strip()


def _truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 16]}\n\n...[truncated]"
