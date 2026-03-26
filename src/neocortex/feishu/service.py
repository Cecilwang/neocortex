"""Feishu event handling and bot workflow orchestration."""

from __future__ import annotations

from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass
from dataclasses import replace
import json
import logging
import re
import shlex
from typing import Any, Callable

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
from neocortex.feishu.cards import build_table_card
from neocortex.feishu.client import FeishuClient
from neocortex.feishu.models import (
    EventReceiptStatus,
    BotRequest,
    FeishuCardResp,
    FeishuDefaultHelpResp,
    FeishuFailedResp,
    FeishuFailedWithDefaultHelpResp,
    FeishuHelpResp,
    FeishuMention,
    FeishuMessageEvent,
    FeishuResp,
    FeishuMessageTarget,
    FeishuTextResp,
)
from neocortex.feishu.settings import FeishuSettings
from neocortex.feishu.storage import FeishuBotStore
from neocortex.serialization import to_pretty_json
from neocortex.tabular import render_table


logger = logging.getLogger(__name__)
_AT_TAG_PATTERN = re.compile(r"<at\b[^>]*>.*?</at>", re.DOTALL)
_LEADING_AT_TAGS_PATTERN = re.compile(r"^(?:\s*<at\b[^>]*>.*?</at>\s*)+", re.DOTALL)
_LEADING_PLACEHOLDER_MENTION_PATTERN = re.compile(r"^(?:@_user_\d+\s*)+")
_AT_TARGET_PATTERN = re.compile(r'(?:user_id|open_id|id)="([^"]+)"')
_REACTION_PROCESSING = "OneSecond"
_REACTION_SUCCEEDED = "CheckMark"
_REACTION_FAILED = "CrossMark"
_REACTION_IGNORED = "18X"


@dataclass(frozen=True, slots=True)
class _LeadingAtTags:
    targets: tuple[str, ...]
    remainder: str


@dataclass(frozen=True, slots=True)
class _LeadingPlaceholderMentions:
    keys: tuple[str, ...]
    remainder: str


@dataclass(frozen=True, slots=True)
class _BuiltResponse:
    response: FeishuResp
    after_send: Callable[[], None] | None = None


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
        thread_id = str(message.get("thread_id") or "")
        parent_id = str(message.get("parent_id") or "")
        root_id = str(message.get("root_id") or "")
        chat_id = str(message.get("chat_id") or "")
        chat_type = str(message.get("chat_type") or "")
        sender_id_value = sender_id.get("open_id")
        sender_id_text = str(sender_id_value or "")
        raw_text = _extract_text_content(message.get("content"))
        mentions = _extract_mentions(message.get("mentions"))
        if not all((event_id, message_id, chat_id, sender_id_text)):
            return None
        return FeishuMessageEvent(
            event_id=event_id,
            message_id=message_id,
            thread_id=thread_id,
            parent_id=parent_id,
            root_id=root_id,
            chat_id=chat_id,
            chat_type=chat_type,
            sender_id=sender_id_text,
            text=raw_text,
            mentions=mentions,
        )


class FeishuBotRouter:
    """Apply activation rules and route one chat message into a bot request."""

    def __init__(self, settings: FeishuSettings, *, bot_open_id: str) -> None:
        self.settings = settings
        self.bot_open_id = bot_open_id

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
                f"sender={event.sender_id}"
            )
            return command_text.strip()

        leading_tags = _consume_leading_at_tags(raw_text)
        if leading_tags is not None:
            if self.bot_open_id and self.bot_open_id in leading_tags.targets:
                logger.info(
                    f"Feishu group activation matched bot_open_id: event_id={event.event_id} "
                    f"sender={event.sender_id}"
                )
                return _strip_leading_placeholder_mentions(
                    leading_tags.remainder
                ).strip()
            logger.info(
                f"Feishu group activation ignored unmatched at-tag: event_id={event.event_id} "
                f"sender={event.sender_id}"
            )
            return None

        leading_placeholders = _consume_leading_placeholder_mentions(raw_text)
        if leading_placeholders is not None:
            mentioned_targets = tuple(
                mention.target_id
                for key in leading_placeholders.keys
                if (mention := (event.mentions or {}).get(key)) is not None
                and mention.id_type == "open_id"
            )
            if self.bot_open_id and self.bot_open_id in mentioned_targets:
                logger.info(
                    f"Feishu group activation matched mentions metadata: event_id={event.event_id} "
                    f"sender={event.sender_id}"
                )
                return leading_placeholders.remainder.strip()
            logger.warning(
                f"Feishu group message had placeholder mention without matching bot target: event_id={event.event_id} "
                f"sender={event.sender_id}"
            )
            return None

        logger.info(
            f"Ignoring non-activated group message: event_id={event.event_id} "
            f"sender={event.sender_id}"
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
        self._owns_client = client is None
        self._owns_executor = executor is None
        self.normalizer = FeishuMessageNormalizer()
        self._bot_open_id = self.client.get_bot_open_id()
        self.router = FeishuBotRouter(settings, bot_open_id=self._bot_open_id)
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
            f"chat_type={event.chat_type} sender={event.sender_id}"
        )
        is_new, receipt = self.store.begin_event(
            event_id=event.event_id, message_id=event.message_id
        )
        if not is_new:
            if receipt.status is EventReceiptStatus.SUCCEEDED:
                logger.info(f"Skipping duplicate Feishu event {event.event_id}.")
                return
            if receipt.status is EventReceiptStatus.FAILED:
                logger.info(
                    f"Skipping duplicate failed Feishu event {event.event_id}."
                )
                return
            logger.info(
                f"Skipping in-progress duplicate Feishu event {event.event_id}."
            )
            return

        try:
            request = self.router.parse(event)
            if request is None:
                logger.info(f"Ignoring non-command message {event.message_id}.")
                self._add_reaction_best_effort(
                    message_id=event.message_id, emoji_type=_REACTION_IGNORED
                )
                self.store.mark_event_succeeded(event.event_id)
                return

            logger.info(
                f"Routed Feishu request: kind={request.kind} event_id={event.event_id}"
            )
            self._add_reaction_best_effort(
                message_id=event.message_id, emoji_type=_REACTION_PROCESSING
            )
            built = self._build_response(event, request)
            self.client.send(built.response)
            if built.after_send is not None:
                built.after_send()
        except Exception as exc:
            error_text = f"{type(exc).__name__}: {exc}"
            logger.exception(f"Feishu event handling failed: event_id={event.event_id}")
            self.store.mark_event_failed(event.event_id, error_text=error_text)
            self._add_reaction_best_effort(
                message_id=event.message_id, emoji_type=_REACTION_FAILED
            )
            self._send_failure_best_effort(
                event,
                f"Failed to handle event.\n{error_text}",
            )
            return

        self.store.mark_event_succeeded(event.event_id)
        self._add_reaction_best_effort(
            message_id=event.message_id,
            emoji_type=_REACTION_SUCCEEDED if built.response.ok else _REACTION_FAILED,
        )

    def _build_response(
        self,
        event: FeishuMessageEvent,
        request: BotRequest,
    ) -> _BuiltResponse:
        if request.kind == "cli":
            return self._handle_cli_request(event, request)
        if request.kind == "help":
            return _BuiltResponse(response=FeishuDefaultHelpResp(target=event.target))
        if request.kind == "job":
            return _BuiltResponse(
                response=FeishuTextResp(
                    target=event.target, text=self._render_job_status(request)
                )
            )
        logger.warning(
            f"Invalid Feishu command: event_id={event.event_id} "
            f"chat_type={event.chat_type} sender={event.sender_id}"
        )
        return _BuiltResponse(
            response=FeishuFailedWithDefaultHelpResp(
                target=event.target,
                text="Invalid command.",
            )
        )

    def _handle_cli_request(
        self, event: FeishuMessageEvent, request: BotRequest
    ) -> _BuiltResponse:
        logger.info(
            f"Handling Feishu cli request: event_id={event.event_id} sender={event.sender_id}"
        )
        try:
            invocation = _parse_cli_invocation(_cli_tokens(request))
        except CommandHelpRequested as exc:
            return _BuiltResponse(
                response=FeishuHelpResp(target=event.target, text=exc.help_text)
            )
        except CommandUsageError as exc:
            text = exc.message
            if exc.help_text:
                text = f"{text}\n\n{exc.help_text}"
                return _BuiltResponse(
                    response=FeishuFailedResp(target=event.target, text=text)
                )
            else:
                return _BuiltResponse(
                    response=FeishuFailedWithDefaultHelpResp(
                        target=event.target, text=text
                    )
                )
        except ValueError as exc:
            return _BuiltResponse(
                response=FeishuFailedResp(target=event.target, text=str(exc))
            )

        if invocation.spec.exposure is Exposure.CLI_ONLY:
            logger.warning(
                f"Rejected cli-only Feishu command: path={invocation.spec.path} "
                f"sender={event.sender_id}"
            )
            return _BuiltResponse(
                response=FeishuFailedResp(
                    target=event.target,
                    text=f"{invocation.spec.path} is only available from the CLI.",
                )
            )

        context = self._build_command_context(event)
        dispatcher = CommandDispatcher()
        execution_mode = invocation.spec.get_execution_mode(invocation.args)
        if execution_mode.value == "async":
            job = self.store.create_job(
                command_name=invocation.spec.path,
                command_text=request.text,
                chat_id=event.chat_id,
                user_open_id=event.sender_id,
                reply_to_message_id=event.message_id if event.thread_id else None,
                reply_in_thread=bool(event.thread_id),
            )
            logger.info(
                f"Queued Feishu cli async job_id={job.id} command={invocation.spec.path}"
            )
            return _BuiltResponse(
                response=FeishuTextResp(
                    target=event.target,
                    text=(
                        f"Accepted job {job.id}: {invocation.spec.path}. "
                        f"Use `job {job.id}` to query status."
                    ),
                ),
                after_send=lambda: self.executor.submit(
                    self._run_cli_job, job.id, invocation, context
                ),
            )

        return _BuiltResponse(
            response=self._execute_cli_invocation(
                invocation, context, event.target, dispatcher=dispatcher
            )
        )

    def _build_command_context(self, event: FeishuMessageEvent) -> CommandContext:
        return CommandContext(
            actor=CommandActor(
                source=InvocationSource.FEISHU,
                user_id=event.sender_id,
                chat_id=event.chat_id,
                chat_type=event.chat_type,
                is_admin=event.sender_id in self.settings.admin_open_ids,
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
        logger.info(
            f"Running Feishu cli job_id={job_id} command={invocation.spec.path}"
        )
        job = self.store.get_job(job_id)
        if job is None:
            return
        dispatcher = CommandDispatcher()
        response = self._execute_cli_invocation(
            invocation, context, job.target, dispatcher=dispatcher
        )
        if not response.ok:
            self.store.mark_job_failed(job_id)
            logger.warning(f"Async Feishu cli job {job_id} failed.")
            self.client.send(replace(response, job_id=job_id))
            return

        self.store.mark_job_succeeded(job_id)
        logger.info(f"Async Feishu cli job_id={job_id} succeeded")
        self.client.send(replace(response, job_id=job.id))

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
        return "\n".join(parts)

    def _execute_cli_invocation(
        self,
        invocation: ParsedInvocation,
        context: CommandContext,
        target: FeishuMessageTarget,
        *,
        dispatcher: CommandDispatcher,
    ) -> FeishuResp:
        try:
            result = dispatcher.dispatch(invocation, context)
        except CommandHelpRequested as exc:
            return FeishuHelpResp(target=target, text=exc.help_text)
        except CommandUsageError as exc:
            text = exc.message
            if exc.help_text:
                text = f"{text}\n\n{exc.help_text}"
                return FeishuFailedResp(target=target, text=text)
            else:
                return FeishuFailedWithDefaultHelpResp(target=target, text=text)
        except PermissionError:
            return FeishuFailedResp(
                target=target,
                text="Permission denied for this command.",
            )
        except Exception as exc:
            logger.exception(f"Feishu cli command failed: [{invocation.spec.path}]")
            return FeishuFailedResp(
                target=target,
                text=f"{invocation.spec.path} failed.\n{type(exc).__name__}: {exc}",
            )
        if result.presentation.kind == "table":
            return FeishuCardResp(
                target=target,
                card=build_table_card(
                    title=f"{invocation.spec.path} ({len(result.presentation.rows)} rows)",
                    columns=result.presentation.columns,
                    rows=result.presentation.rows,
                ),
            )
        return FeishuTextResp(
            target=target,
            text=_render_command_result_for_chat(result),
        )

    def close(self) -> None:
        if self._owns_executor:
            logger.info("Shutting down Feishu bot executor.")
            self.executor.shutdown(wait=False)
        if self._owns_client and hasattr(self.client, "close"):
            logger.info("Closing Feishu bot client.")
            self.client.close()

    def _add_reaction_best_effort(self, *, message_id: str, emoji_type: str) -> None:
        try:
            self.client.add_reaction(message_id=message_id, emoji_type=emoji_type)
        except Exception:
            logger.exception(
                f"Failed to add Feishu reaction: message_id={message_id} emoji_type={emoji_type}"
            )

    def _send_failure_best_effort(self, event: FeishuMessageEvent, text: str) -> None:
        try:
            self.client.send(FeishuFailedResp(target=event.target, text=text))
        except Exception:
            logger.exception(
                f"Failed to send Feishu failure reply: event_id={event.event_id}"
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


def _consume_leading_placeholder_mentions(
    raw_text: str,
) -> _LeadingPlaceholderMentions | None:
    match = _LEADING_PLACEHOLDER_MENTION_PATTERN.match(raw_text)
    if match is None:
        return None
    keys = tuple(re.findall(r"@_user_\d+", match.group(0)))
    remainder = raw_text[match.end() :].strip()
    return _LeadingPlaceholderMentions(keys=keys, remainder=remainder)


def _extract_mentions(raw_mentions: object) -> dict[str, FeishuMention]:
    if not isinstance(raw_mentions, list):
        return {}
    mentions: dict[str, FeishuMention] = {}
    for item in raw_mentions:
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        target_id = item.get("id")
        id_type = item.get("id_type")
        if isinstance(target_id, dict):
            open_id = target_id.get("open_id")
            if isinstance(open_id, str) and open_id:
                target_id = open_id
                id_type = "open_id"
        if not all(
            isinstance(value, str) and value for value in (key, target_id, id_type)
        ):
            continue
        mentions[key] = FeishuMention(
            key=key,
            target_id=target_id,
            id_type=id_type,
        )
    return mentions


def _strip_leading_placeholder_mentions(raw_text: str) -> str:
    without_tags = _AT_TAG_PATTERN.sub(" ", raw_text)
    return _LEADING_PLACEHOLDER_MENTION_PATTERN.sub("", without_tags, count=1).strip()


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
