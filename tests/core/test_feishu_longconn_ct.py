from types import SimpleNamespace

from neocortex.feishu.longconn import FeishuLongConnectionRunner
from neocortex.feishu.settings import FeishuSettings


class FakeService:
    def __init__(self) -> None:
        self.payloads: list[dict[str, object]] = []

    def handle_event_payload(self, payload: dict[str, object]) -> None:
        self.payloads.append(payload)


class FakeWSClient:
    last_init: dict[str, object] | None = None
    started: bool = False

    def __init__(
        self,
        app_id,
        app_secret,
        *,
        log_level,
        event_handler,
        domain,
    ) -> None:
        type(self).last_init = {
            "app_id": app_id,
            "app_secret": app_secret,
            "log_level": log_level,
            "event_handler": event_handler,
            "domain": domain,
        }

    def start(self) -> None:
        type(self).started = True


def test_long_connection_runner_forwards_message_events(tmp_path) -> None:
    settings = FeishuSettings(
        app_id="cli_app",
        app_secret="cli_secret",
        db_path=tmp_path / "bot.sqlite3",
    )
    service = FakeService()
    runner = FeishuLongConnectionRunner(settings, service=service)
    event = SimpleNamespace(
        header=SimpleNamespace(
            event_id="evt-1",
            event_type="im.message.receive_v1",
        ),
        event={
            "sender": {"sender_id": {"open_id": "ou_user"}},
            "message": {
                "message_id": "msg-1",
                "chat_id": "oc_chat",
                "chat_type": "p2p",
                "message_type": "text",
                "content": '{"text":"/neo help"}',
            },
        },
    )

    runner._handle_message_receive_event(event)

    assert service.payloads == [
        {
            "schema": "2.0",
            "header": {
                "event_id": "evt-1",
                "event_type": "im.message.receive_v1",
            },
            "event": event.event,
        }
    ]


def test_long_connection_runner_starts_sdk_client(tmp_path) -> None:
    settings = FeishuSettings(
        app_id="cli_app",
        app_secret="cli_secret",
        base_url="https://open.feishu.cn",
        db_path=tmp_path / "bot.sqlite3",
    )
    runner = FeishuLongConnectionRunner(
        settings,
        service=FakeService(),
        ws_client_factory=FakeWSClient,
    )

    runner.start()

    assert FakeWSClient.started is True
    assert FakeWSClient.last_init is not None
    assert FakeWSClient.last_init["app_id"] == "cli_app"
    assert FakeWSClient.last_init["app_secret"] == "cli_secret"
    assert FakeWSClient.last_init["domain"] == "https://open.feishu.cn"
