from pathlib import Path

from neocortex.feishu.actions import BotActionRunner
from neocortex.feishu.models import BotCommand
from neocortex.feishu.service import FeishuBotService
from neocortex.feishu.settings import FeishuSettings
from neocortex.feishu.storage import FeishuBotStore


class FakeClient:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def send_text(self, *, chat_id: str, text: str) -> None:
        self.messages.append((chat_id, text))


class RecordingRunner(BotActionRunner):
    def __init__(self, *, store: FeishuBotStore) -> None:
        super().__init__(store=store, db_path=store.engine.url.database)
        self.commands: list[str] = []

    def run(self, command: BotCommand) -> str:
        self.commands.append(command.name)
        if command.name == "pipeline_run":
            return "Pipeline run placeholder completed."
        return super().run(command)


class ImmediateExecutor:
    def submit(self, fn, *args, **kwargs) -> None:
        fn(*args, **kwargs)
        return None


def _settings(
    tmp_path: Path, *, admins: frozenset[str] = frozenset()
) -> FeishuSettings:
    return FeishuSettings(
        app_id="app_id",
        app_secret="app_secret",
        db_path=tmp_path / "feishu.sqlite3",
        admin_open_ids=admins,
    )


def _message_event(
    *,
    text: str,
    event_id: str = "evt-1",
    message_id: str = "msg-1",
    sender_open_id: str = "ou_user",
) -> dict[str, object]:
    return {
        "schema": "2.0",
        "header": {
            "event_id": event_id,
            "event_type": "im.message.receive_v1",
        },
        "event": {
            "sender": {"sender_id": {"open_id": sender_open_id}},
            "message": {
                "message_id": message_id,
                "chat_id": "oc_test_chat",
                "chat_type": "group",
                "message_type": "text",
                "content": f'{{"text":"{text}"}}',
            },
        },
    }


def test_help_command_sends_help_message(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(_message_event(text="/neo help"))

    assert len(client.messages) == 1
    assert client.messages[0][0] == "oc_test_chat"
    assert "/neo help" in client.messages[0][1]


def test_group_mention_prefix_is_normalized_into_command(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(_message_event(text="@_user_1 /neo help"))

    assert len(client.messages) == 1
    assert "/neo help" in client.messages[0][1]


def test_non_admin_async_command_is_rejected(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(
        _message_event(text="/neo pipeline run 600519 XSHG 2026-03-19")
    )

    assert client.messages == [("oc_test_chat", "Permission denied for this command.")]


def test_async_job_is_persisted_and_notified(tmp_path) -> None:
    client = FakeClient()
    store = FeishuBotStore(tmp_path / "jobs.sqlite3")
    runner = RecordingRunner(store=store)
    service = FeishuBotService(
        _settings(tmp_path, admins=frozenset({"ou_admin"})),
        client=client,
        store=store,
        action_runner=runner,
        executor=ImmediateExecutor(),
    )

    service.handle_event_payload(
        _message_event(
            text="/neo pipeline run 600519 XSHG 2026-03-19",
            sender_open_id="ou_admin",
        )
    )

    job = store.get_job(1)
    assert job is not None
    assert job.status.value == "succeeded"
    assert runner.commands == ["pipeline_run"]
    assert client.messages[0] == (
        "oc_test_chat",
        "Accepted job 1: pipeline_run. Use `/neo job 1` to query status.",
    )
    assert client.messages[1] == (
        "oc_test_chat",
        "Job 1 succeeded.\nPipeline run placeholder completed.",
    )


def test_duplicate_event_is_ignored(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)
    payload = _message_event(text="/neo help")

    service.handle_event_payload(payload)
    service.handle_event_payload(payload)

    assert len(client.messages) == 1
