"""Shared DTOs for Feishu bot parsing, actions, and persistence."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class JobStatus(StrEnum):
    """Supported async job lifecycle states."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class FeishuMessageEvent:
    """Extracted text message event emitted by Feishu."""

    event_id: str
    message_id: str
    chat_id: str
    chat_type: str
    sender_id: str
    text: str


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
    status: JobStatus
    submitted_at: str
    started_at: str | None = None
    finished_at: str | None = None
    result_text: str | None = None
    error_text: str | None = None
