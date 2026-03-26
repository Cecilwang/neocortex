"""Shared fakes for Feishu transport tests."""

from __future__ import annotations

import json

from neocortex.feishu.models import (
    FeishuCardResp,
    FeishuDefaultHelpResp,
    FeishuResp,
    FEISHU_HELP_TEXT,
    FeishuFailedWithDefaultHelpResp,
    FeishuTextResp,
)


class FakeClient:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []
        self.reactions: list[dict[str, str]] = []
        self.closed = False
        self.bot_open_id = "ou_bot"
        self.fail_next_send: Exception | None = None

    def send(self, response: FeishuResp) -> None:
        if self.fail_next_send is not None:
            exc = self.fail_next_send
            self.fail_next_send = None
            raise exc
        msg_type = "interactive" if isinstance(response, FeishuCardResp) else "text"
        text = _render_fake_text(response)
        card = (
            _render_fake_card(response)
            if isinstance(response, FeishuCardResp)
            else None
        )
        self.messages.append(
            {
                "chat_id": response.target.chat_id,
                "msg_type": msg_type,
                "text": text,
                "card": card,
                "reply_to_message_id": response.target.reply_to_message_id,
                "reply_in_thread": response.target.reply_in_thread,
            }
        )

    def add_reaction(self, *, message_id: str, emoji_type: str) -> None:
        self.reactions.append({"message_id": message_id, "emoji_type": emoji_type})

    def get_bot_open_id(self) -> str:
        return self.bot_open_id

    def close(self) -> None:
        self.closed = True


class ImmediateExecutor:
    def submit(self, fn, *args, **kwargs) -> None:
        fn(*args, **kwargs)
        return None


class FakeExecutor:
    def __init__(self) -> None:
        self.shutdown_called = False

    def submit(self, fn, *args, **kwargs) -> None:
        fn(*args, **kwargs)
        return None

    def shutdown(self, *, wait: bool) -> None:
        self.shutdown_called = True


class FakeService:
    def __init__(self) -> None:
        self.payloads: list[dict[str, object]] = []
        self.closed = False

    def handle_event_payload(self, payload: dict[str, object]) -> None:
        self.payloads.append(payload)

    def close(self) -> None:
        self.closed = True


def _render_fake_text(response: FeishuResp) -> str | None:
    if isinstance(response, FeishuDefaultHelpResp):
        text = response.text
    elif isinstance(response, FeishuTextResp):
        text = response.text
    else:
        return None
    if isinstance(response, FeishuFailedWithDefaultHelpResp):
        text = f"{text}\n\n{FEISHU_HELP_TEXT}"
    if response.job_id is not None:
        status = "succeeded" if response.ok else "failed"
        text = f"Job {response.job_id} {status}.\n{text}"
    return text


def _render_fake_card(response: FeishuCardResp) -> dict[str, object]:
    if response.job_id is None:
        return response.card

    status = "succeeded" if response.ok else "failed"
    card = json.loads(json.dumps(response.card, ensure_ascii=False))
    header = card.get("header")
    if not isinstance(header, dict):
        return card
    title = header.get("title")
    if not isinstance(title, dict):
        return card
    content = title.get("content")
    if not isinstance(content, str):
        return card
    title["content"] = f"Job {response.job_id} {status}: {content}"
    return card
