import argparse
import json
import sqlite3
from pathlib import Path

from neocortex.commands import (
    AuthPolicy,
    CommandRegistry,
    CommandResult,
    CommandSpec,
    ExecutionMode,
    Exposure,
)
from neocortex.feishu.service import FeishuBotService
from neocortex.feishu.settings import FeishuSettings
from neocortex.feishu.storage import FeishuBotStore


class FakeClient:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def send_text(self, *, chat_id: str, text: str) -> None:
        self.messages.append((chat_id, text))

class ImmediateExecutor:
    def submit(self, fn, *args, **kwargs) -> None:
        fn(*args, **kwargs)
        return None


def _build_async_cli_registry() -> CommandRegistry:
    registry = CommandRegistry()

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--value", required=True)

    def handler(args: argparse.Namespace, context) -> CommandResult:
        _ = context
        return CommandResult.text(f"async:{args.value}")

    registry.register(
        CommandSpec(
            id=("demo", "async"),
            summary="Run one async demo command.",
            description="Run one async demo command.",
            exposure=Exposure.SHARED,
            auth=AuthPolicy.PUBLIC,
            execution_mode=ExecutionMode.ASYNC,
            configure_parser=configure_parser,
            handler=handler,
        )
    )
    return registry


def _build_policy_cli_registry() -> CommandRegistry:
    registry = CommandRegistry()

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--all", action="store_true")

    def handler(args: argparse.Namespace, context) -> CommandResult:
        _ = context
        return CommandResult.text(f"policy:{args.all}")

    registry.register(
        CommandSpec(
            id=("demo", "policy"),
            summary="Run one policy demo command.",
            description="Run one policy demo command.",
            exposure=Exposure.SHARED,
            auth=AuthPolicy.PUBLIC,
            execution_mode=ExecutionMode.SYNC,
            configure_parser=configure_parser,
            handler=handler,
            execution_policy=lambda args: (
                ExecutionMode.ASYNC if args.all else ExecutionMode.SYNC
            ),
        )
    )
    return registry


def _settings(
    tmp_path: Path,
    *,
    admins: frozenset[str] = frozenset(),
) -> FeishuSettings:
    return FeishuSettings(
        app_id="app_id",
        app_secret="app_secret",
        bot_open_id="ou_bot",
        db_path=tmp_path / "feishu.sqlite3",
        admin_open_ids=admins,
    )


def _message_event(
    *,
    text: str,
    chat_type: str = "group",
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
                "chat_type": chat_type,
                "message_type": "text",
                "content": json.dumps({"text": text}),
            },
        },
    }


def _activated_group_message_event(
    *,
    text: str,
    event_id: str = "evt-1",
    message_id: str = "msg-1",
    sender_open_id: str = "ou_user",
) -> dict[str, object]:
    return _message_event(
        text=f'<at user_id="ou_bot"></at> {text}',
        event_id=event_id,
        message_id=message_id,
        sender_open_id=sender_open_id,
    )


def test_group_message_without_activation_is_ignored(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(_message_event(text="/neo help"))

    assert client.messages == []


def test_p2p_help_message_sends_help_message(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(_message_event(text="help", chat_type="p2p"))

    assert len(client.messages) == 1
    assert client.messages[0][0] == "oc_test_chat"
    assert "Available commands:" in client.messages[0][1]
    assert "cli <full-cli-command>" in client.messages[0][1]


def test_group_placeholder_mention_is_ignored_and_warns(tmp_path, caplog) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(_message_event(text="@_user_1 help"))

    assert client.messages == []
    assert "placeholder mention without at-tag" in caplog.text


def test_group_non_bot_mention_does_not_activate_command(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(_message_event(text="@someone help"))

    assert client.messages == []


def test_group_at_tag_matching_bot_open_id_activates_command(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(
        _message_event(text='<at user_id="ou_bot"></at> help')
    )

    assert len(client.messages) == 1
    assert "Available commands:" in client.messages[0][1]
    assert "cli <full-cli-command>" in client.messages[0][1]


def test_cli_route_executes_registry_command(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)
    db_path = tmp_path / "market.sqlite3"
    with sqlite3.connect(db_path) as connection:
        connection.execute("create table sample_rows (name text)")
        connection.execute("insert into sample_rows (name) values ('alpha')")
        connection.commit()

    service.handle_event_payload(
        _activated_group_message_event(
            text=f"cli db query --db-path {db_path} --table sample_rows"
        )
    )

    assert len(client.messages) == 1
    assert "name" in client.messages[0][1]
    assert "alpha" in client.messages[0][1]


def test_p2p_cli_route_executes_registry_command(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)
    db_path = tmp_path / "market.sqlite3"
    with sqlite3.connect(db_path) as connection:
        connection.execute("create table sample_rows (name text)")
        connection.execute("insert into sample_rows (name) values ('alpha')")
        connection.commit()

    service.handle_event_payload(
        _message_event(
            text=f"cli db query --db-path {db_path} --table sample_rows",
            chat_type="p2p",
        )
    )

    assert len(client.messages) == 1
    assert "name" in client.messages[0][1]
    assert "alpha" in client.messages[0][1]


def test_cli_route_rejects_cli_only_command(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(_activated_group_message_event(text="cli feishu longconn"))

    assert client.messages == [
        ("oc_test_chat", "feishu longconn is only available from the CLI.")
    ]


def test_cli_route_reports_usage_error(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(_activated_group_message_event(text="cli db query"))

    assert len(client.messages) == 1
    assert "one of the arguments --sql --table is required" in client.messages[0][1]
    assert "db query" in client.messages[0][1]


def test_cli_async_route_persists_job_and_notifies(tmp_path, monkeypatch) -> None:
    from neocortex.feishu import service as feishu_service

    client = FakeClient()
    store = FeishuBotStore(tmp_path / "jobs.sqlite3")
    service = FeishuBotService(
        _settings(tmp_path),
        client=client,
        store=store,
        executor=ImmediateExecutor(),
    )
    monkeypatch.setattr(
        feishu_service,
        "build_command_registry",
        _build_async_cli_registry,
    )

    service.handle_event_payload(
        _activated_group_message_event(text="cli demo async --value alpha")
    )

    job = store.get_job(1)
    assert job is not None
    assert job.status.value == "succeeded"
    assert client.messages[0] == (
        "oc_test_chat",
        "Accepted job 1: demo async. Use `job 1` to query status.",
    )
    assert client.messages[1] == (
        "oc_test_chat",
        "Job 1 succeeded.\nasync:alpha",
    )


def test_cli_execution_policy_switches_between_sync_and_async(tmp_path, monkeypatch) -> None:
    from neocortex.feishu import service as feishu_service

    client = FakeClient()
    store = FeishuBotStore(tmp_path / "jobs.sqlite3")
    service = FeishuBotService(
        _settings(tmp_path),
        client=client,
        store=store,
        executor=ImmediateExecutor(),
    )
    monkeypatch.setattr(
        feishu_service,
        "build_command_registry",
        _build_policy_cli_registry,
    )

    service.handle_event_payload(
        _activated_group_message_event(text="cli demo policy")
    )
    service.handle_event_payload(
        _activated_group_message_event(
            text="cli demo policy --all",
            event_id="evt-2",
            message_id="msg-2",
        )
    )

    assert client.messages[0] == ("oc_test_chat", "policy:False")
    assert client.messages[1] == (
        "oc_test_chat",
        "Accepted job 1: demo policy. Use `job 1` to query status.",
    )
    assert client.messages[2] == (
        "oc_test_chat",
        "Job 1 succeeded.\npolicy:True",
    )


def test_job_route_is_handled_directly_by_service(tmp_path, monkeypatch) -> None:
    client = FakeClient()
    store = FeishuBotStore(tmp_path / "jobs.sqlite3")
    store.create_job(
        command_name="demo async",
        command_text="cli demo async --value alpha",
        chat_id="oc_test_chat",
        user_open_id="ou_user",
    )
    service = FeishuBotService(_settings(tmp_path), client=client, store=store)

    service.handle_event_payload(_activated_group_message_event(text="job 1"))

    assert len(client.messages) == 1
    assert "job_id=1" in client.messages[0][1]
    assert "command=demo async" in client.messages[0][1]


def test_job_route_reports_usage_for_invalid_job_id(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(_activated_group_message_event(text="job abc"))

    assert client.messages == [("oc_test_chat", "Usage: job <job-id>")]


def test_legacy_command_is_rejected_with_help(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(
        _activated_group_message_event(text="db table sample_rows")
    )

    assert len(client.messages) == 1
    assert client.messages[0][0] == "oc_test_chat"
    assert "Invalid command." in client.messages[0][1]
    assert "cli <full-cli-command>" in client.messages[0][1]


def test_slash_neo_prefix_is_rejected(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(_activated_group_message_event(text="/neo help"))

    assert len(client.messages) == 1
    assert "Invalid command." in client.messages[0][1]
    assert "cli <full-cli-command>" in client.messages[0][1]


def test_duplicate_event_is_ignored(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)
    payload = _activated_group_message_event(text="help")

    service.handle_event_payload(payload)
    service.handle_event_payload(payload)

    assert len(client.messages) == 1
