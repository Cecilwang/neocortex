"""Shared DTOs for Feishu bot parsing, actions, and persistence."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


FEISHU_HELP_TEXT = """Available commands:
help
job <job-id>
cli <full-cli-command>"""


@dataclass(frozen=True, slots=True)
class FeishuMessageTarget:
    """Resolved Feishu transport target for one outbound message."""

    chat_id: str
    reply_to_message_id: str | None = None
    reply_in_thread: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class FeishuResp:
    """Base outbound Feishu response."""

    target: FeishuMessageTarget
    ok: bool = True
    job_id: int | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class FeishuTextResp(FeishuResp):
    """Plain text Feishu response."""

    text: str


@dataclass(frozen=True, slots=True, kw_only=True)
class FeishuHelpResp(FeishuTextResp):
    """Help text response."""


@dataclass(frozen=True, slots=True, kw_only=True)
class FeishuDefaultHelpResp(FeishuHelpResp):
    """Default bot help text response."""

    text: str = FEISHU_HELP_TEXT


@dataclass(frozen=True, slots=True, kw_only=True)
class FeishuFailedResp(FeishuTextResp):
    """Failure text response."""

    ok: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class FeishuFailedWithDefaultHelpResp(FeishuFailedResp):
    """Failure text response with appended default help message."""


@dataclass(frozen=True, slots=True, kw_only=True)
class FeishuCardResp(FeishuResp):
    """Interactive card response."""

    card: dict[str, Any]


class JobStatus(StrEnum):
    """Supported async job lifecycle states."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class FeishuMention:
    """One structured mention extracted from a Feishu message payload."""

    key: str
    target_id: str
    id_type: str


@dataclass(frozen=True, slots=True)
class FeishuMessageEvent:
    """Extracted text message event emitted by Feishu."""

    event_id: str
    message_id: str
    thread_id: str
    parent_id: str
    root_id: str
    chat_id: str
    chat_type: str
    sender_id: str
    text: str
    mentions: dict[str, FeishuMention] | None = None

    @property
    def target(self) -> FeishuMessageTarget:
        if self.thread_id:
            return FeishuMessageTarget(
                chat_id=self.chat_id,
                reply_to_message_id=self.message_id,
                reply_in_thread=True,
            )
        return FeishuMessageTarget(chat_id=self.chat_id)


@dataclass(frozen=True, slots=True)
class BotRequest:
    """Routed bot request after activation and root-command parsing."""

    kind: str
    text: str


@dataclass(frozen=True, slots=True)
class FeishuJobRecord:
    """One persisted async job."""

    id: int
    command_name: str
    command_text: str
    chat_id: str
    user_open_id: str
    reply_to_message_id: str | None
    reply_in_thread: bool
    status: JobStatus
    submitted_at: str
    started_at: str | None = None
    finished_at: str | None = None

    @property
    def target(self) -> FeishuMessageTarget:
        return FeishuMessageTarget(
            chat_id=self.chat_id,
            reply_to_message_id=self.reply_to_message_id,
            reply_in_thread=self.reply_in_thread,
        )
