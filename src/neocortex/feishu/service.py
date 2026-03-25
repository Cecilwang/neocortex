"""Feishu event handling and bot workflow orchestration."""

from __future__ import annotations

from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass
import json
import logging
import re
import shlex
from typing import Any

from neocortex.commands import (
    CommandActor,
    CommandArgumentParser,
    CommandContext,
    CommandDispatcher,
    CommandHelpRequested,
    CommandServices,
    CommandSpec,
    CommandUsageError,
    Exposure,
    InvocationSource,
    ParsedInvocation,
    build_command_registry,
)
from neocortex.feishu.client import FeishuClient
from neocortex.feishu.models import BotRequest, FeishuMessageEvent
from neocortex.feishu.settings import FeishuSettings
from neocortex.feishu.storage import FeishuBotStore
from neocortex.serialization import to_pretty_json
from neocortex.tabular import render_table


logger = logging.getLogger(__name__)
HELP_TEXT = """Available commands:
help
job <job-id>
cli <full-cli-command>"""
_AT_TAG_PATTERN = re.compile(r"<at\b[^>]*>.*?</at>", re.DOTALL)
_LEADING_AT_TAGS_PATTERN = re.compile(r"^(?:\s*<at\b[^>]*>.*?</at>\s*)+", re.DOTALL)
_LEADING_PLACEHOLDER_MENTION_PATTERN = re.compile(r"^(?:@_user_\d+\s*)+")
_AT_TARGET_PATTERN = re.compile(r'(?:user_id|open_id|id)="([^"]+)"')


@dataclass(frozen=True, slots=True)
class _LeadingAtTags:
    targets: tuple[str, ...]
    remainder: str


@dataclass(frozen=True, slots=True)
class _CliExecutionOutcome:
    ok: bool
    text: str


class FeishuMessageNormalizer:
    """Extract supported text message events from Feishu payloads."""

    def extract(self, payload: dict[str, Any]) -> FeishuMessageEvent | None:
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
        if not all((event_id, message_id, chat_id, sender_open_id)):
            return None
        return FeishuMessageEvent(
            event_id=event_id,
            message_id=message_id,
            chat_id=chat_id,
            chat_type=chat_type,
            sender_open_id=sender_open_id,
            text=raw_text,
        )


class FeishuBotRouter:
    """Apply activation rules and route one chat message into a bot request."""

    def __init__(self, settings: FeishuSettings) -> None:
        self.settings = settings

    def parse(self, event: FeishuMessageEvent) -> BotRequest | None:
        command_text = self._extract_command_text(event)
        if command_text is None:
            return None
        stripped = command_text.strip()
        if not stripped:
            return BotRequest(kind="help", text="help")
        if stripped == "help":
            return BotRequest(kind="help", text="help")
        if stripped.startswith("job "):
            return BotRequest(kind="job", text=stripped)
        if stripped.startswith("cli "):
            return BotRequest(kind="cli", text=stripped)
        return BotRequest(kind="invalid", text=stripped)

    def _extract_command_text(self, event: FeishuMessageEvent) -> str | None:
        raw_text = event.text.strip()
        if event.chat_type == "p2p":
            command_text = _strip_leading_placeholder_mentions(raw_text)
            logger.info(
                f"Feishu p2p activation accepted: event_id={event.event_id} "
                f"sender={event.sender_open_id}"
            )
            return command_text.strip()

        leading_tags = _consume_leading_at_tags(raw_text)
        if leading_tags is not None:
            if self.settings.bot_open_id and self.settings.bot_open_id in leading_tags.targets:
                logger.info(
                    f"Feishu group activation matched bot_open_id: event_id={event.event_id} "
                    f"sender={event.sender_open_id}"
                )
                return _strip_leading_placeholder_mentions(leading_tags.remainder).strip()
            logger.info(
                f"Feishu group activation ignored unmatched at-tag: event_id={event.event_id} "
                f"sender={event.sender_open_id}"
            )
            return None

        stripped = _strip_leading_placeholder_mentions(raw_text)
        if stripped != raw_text:
            logger.warning(
                f"Feishu group message had placeholder mention without at-tag: event_id={event.event_id} "
                f"sender={event.sender_open_id}"
            )
            return None

        logger.info(
            f"Ignoring non-activated group message: event_id={event.event_id} "
            f"sender={event.sender_open_id}"
        )
        return None


class FeishuBotService:
    """Coordinate Feishu event intake, command routing, and async jobs."""

    def __init__(
        self,
        settings: FeishuSettings,
        *,
        client: FeishuClient | None = None,
        store: FeishuBotStore | None = None,
        executor: Executor | None = None,
    ) -> None:
        self.settings = settings
        self.store = store or FeishuBotStore(settings.db_path)
        self.client = client or FeishuClient(settings)
        self.executor = executor or ThreadPoolExecutor(max_workers=settings.job_workers)
        self.normalizer = FeishuMessageNormalizer()
        self.router = FeishuBotRouter(settings)
        logger.info(
            f"Feishu bot service ready: db_path={settings.db_path} "
            f"admins={len(settings.admin_open_ids)} workers={settings.job_workers}"
        )

    def handle_event_payload(self, payload: dict[str, Any]) -> None:
        """Handle one Feishu event payload."""

        event = self.normalizer.extract(payload)
        if event is None:
            logger.debug("Ignoring unsupported Feishu payload.")
            return

        logger.info(
            f"Received Feishu message: event_id={event.event_id} chat_id={event.chat_id} "
            f"chat_type={event.chat_type} sender={event.sender_open_id}"
        )
        if not self.store.record_event(
            event_id=event.event_id, message_id=event.message_id
        ):
            logger.info(f"Skipping duplicate Feishu event {event.event_id}.")
            return

        request = self.router.parse(event)
        if request is None:
            logger.info(f"Ignoring non-command message {event.message_id}.")
            return

        logger.info(
            f"Routed Feishu request: kind={request.kind} event_id={event.event_id}"
        )
        if request.kind == "cli":
            self._handle_cli_request(event, request)
            return
        if request.kind == "help":
            self._send_reply(event.chat_id, HELP_TEXT)
            return
        if request.kind == "job":
            self._send_reply(event.chat_id, self._render_job_status(request))
            return
        logger.warning(
            f"Invalid Feishu command: event_id={event.event_id} "
            f"chat_type={event.chat_type} sender={event.sender_open_id}"
        )
        self._send_reply(event.chat_id, f"Invalid command.\n\n{HELP_TEXT}")

    def _handle_cli_request(self, event: FeishuMessageEvent, request: BotRequest) -> None:
        logger.info(
            f"Handling Feishu cli request: event_id={event.event_id} sender={event.sender_open_id}"
        )
        try:
            invocation = _parse_cli_invocation(_cli_tokens(request))
        except CommandHelpRequested as exc:
            self._send_reply(event.chat_id, exc.help_text)
            return
        except CommandUsageError as exc:
            text = exc.message
            if exc.help_text:
                text = f"{text}\n\n{exc.help_text}"
            self._send_reply(event.chat_id, text)
            return
        except ValueError as exc:
            self._send_reply(event.chat_id, str(exc))
            return

        if invocation.spec.exposure is Exposure.CLI_ONLY:
            logger.warning(
                f"Rejected cli-only Feishu command: path={invocation.spec.path} "
                f"sender={event.sender_open_id}"
            )
            self._send_reply(
                event.chat_id,
                f"{invocation.spec.path} is only available from the CLI.",
            )
            return

        context = self._build_command_context(event)
        dispatcher = CommandDispatcher()
        execution_mode = invocation.spec.get_execution_mode(invocation.args)
        if execution_mode.value == "async":
            job = self.store.create_job(
                command_name=invocation.spec.path,
                command_text=request.text,
                chat_id=event.chat_id,
                user_open_id=event.sender_open_id,
            )
            logger.info(
                f"Queued Feishu cli async job_id={job.id} command={invocation.spec.path}"
            )
            self._send_reply(
                event.chat_id,
                f"Accepted job {job.id}: {invocation.spec.path}. Use `job {job.id}` to query status.",
            )
            self.executor.submit(self._run_cli_job, job.id, invocation, context)
            return

        outcome = self._execute_cli_invocation(invocation, context, dispatcher=dispatcher)
        self._send_reply(event.chat_id, outcome.text)

    def _build_command_context(self, event: FeishuMessageEvent) -> CommandContext:
        return CommandContext(
            actor=CommandActor(
                source=InvocationSource.FEISHU,
                user_id=event.sender_open_id,
                chat_id=event.chat_id,
                chat_type=event.chat_type,
                is_admin=event.sender_open_id in self.settings.admin_open_ids,
            ),
            services=CommandServices(),
            request_id=event.event_id,
        )

    def _run_cli_job(
        self,
        job_id: int,
        invocation: ParsedInvocation,
        context: CommandContext,
    ) -> None:
        self.store.mark_job_running(job_id)
        logger.info(f"Running Feishu cli job_id={job_id} command={invocation.spec.path}")
        job = self.store.get_job(job_id)
        if job is None:
            return
        dispatcher = CommandDispatcher()
        outcome = self._execute_cli_invocation(invocation, context, dispatcher=dispatcher)
        if not outcome.ok:
            self.store.mark_job_failed(job_id, error_text=outcome.text)
            logger.warning(f"Async Feishu cli job {job_id} failed.")
            self._send_reply(job.chat_id, f"Job {job_id} failed.\n{outcome.text}")
            return

        self.store.mark_job_succeeded(job_id, result_text=outcome.text)
        logger.info(f"Async Feishu cli job_id={job_id} succeeded")
        self._send_reply(job.chat_id, f"Job {job_id} succeeded.\n{outcome.text}")

    def _send_reply(self, chat_id: str, text: str) -> None:
        logger.info(f"Sending reply to chat_id={chat_id} chars={len(text)}")
        self.client.send_text(
            chat_id=chat_id, text=_truncate(text, self.settings.max_reply_chars)
        )

    def _render_job_status(self, request: BotRequest) -> str:
        parts = request.text.split()
        if len(parts) != 2 or not parts[1].isdigit():
            return "Usage: job <job-id>"
        job = self.store.get_job(int(parts[1]))
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

    def _execute_cli_invocation(
        self,
        invocation: ParsedInvocation,
        context: CommandContext,
        *,
        dispatcher: CommandDispatcher,
    ) -> _CliExecutionOutcome:
        try:
            result = dispatcher.dispatch(invocation, context)
        except PermissionError:
            return _CliExecutionOutcome(
                ok=False,
                text="Permission denied for this command.",
            )
        except Exception as exc:
            logger.exception(f"Feishu cli command failed: [{invocation.spec.path}]")
            return _CliExecutionOutcome(
                ok=False,
                text=f"{invocation.spec.path} failed.\n{type(exc).__name__}: {exc}",
            )
        return _CliExecutionOutcome(
            ok=True,
            text=_render_command_result_for_chat(result),
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


def _consume_leading_at_tags(raw_text: str) -> _LeadingAtTags | None:
    match = _LEADING_AT_TAGS_PATTERN.match(raw_text)
    if match is None:
        return None
    tag_block = match.group(0)
    targets = tuple(_AT_TARGET_PATTERN.findall(tag_block))
    remainder = raw_text[match.end() :].strip()
    return _LeadingAtTags(targets=targets, remainder=remainder)


def _strip_leading_placeholder_mentions(raw_text: str) -> str:
    without_tags = _AT_TAG_PATTERN.sub(" ", raw_text)
    return _LEADING_PLACEHOLDER_MENTION_PATTERN.sub("", without_tags, count=1).strip()


def _truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 16]}\n\n...[truncated]"


def _cli_tokens(request: BotRequest) -> list[str]:
    cli_text = request.text.removeprefix("cli").strip()
    if not cli_text:
        raise ValueError("Usage: cli <full-cli-command>")
    return shlex.split(cli_text)


def _parse_cli_invocation(tokens: list[str]) -> ParsedInvocation:
    registry = build_command_registry()
    parser = CommandArgumentParser(prog="neocortex")
    subcommands = parser.add_subparsers(dest="_command_root", required=True)
    registry.bind_subcommands(subcommands)
    args = parser.parse_args(tokens)
    spec = getattr(args, "_command_spec", None)
    if not isinstance(spec, CommandSpec):
        raise RuntimeError("Feishu cli parser returned without a bound command spec.")
    return ParsedInvocation(spec=spec, args=args)


def _render_command_result_for_chat(result) -> str:
    presentation = result.presentation
    if presentation.kind == "text":
        return presentation.text or ""
    if presentation.kind == "json":
        return to_pretty_json(presentation.json_value)
    if presentation.kind == "table":
        return render_table(list(presentation.columns), list(presentation.rows))
    raise ValueError(f"Unsupported presentation kind: {presentation.kind}")
