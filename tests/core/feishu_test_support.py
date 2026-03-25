"""Shared fakes for Feishu transport tests."""

from __future__ import annotations


class FakeClient:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []
        self.closed = False

    def send_text(self, *, chat_id: str, text: str) -> None:
        self.messages.append((chat_id, text))

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
