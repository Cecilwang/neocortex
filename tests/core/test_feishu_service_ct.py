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
from tests.core.feishu_test_support import FakeClient, FakeExecutor, ImmediateExecutor


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


def _build_async_table_cli_registry() -> CommandRegistry:
    registry = CommandRegistry()

    def handler(args: argparse.Namespace, context) -> CommandResult:
        _ = args
        _ = context
        return CommandResult.table(
            columns=("symbol", "close"),
            rows=(("600519", 123.45),),
        )

    registry.register(
        CommandSpec(
            id=("demo", "async-table"),
            summary="Run one async demo table command.",
            description="Run one async demo table command.",
            exposure=Exposure.SHARED,
            auth=AuthPolicy.PUBLIC,
            execution_mode=ExecutionMode.ASYNC,
            configure_parser=lambda parser: None,
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
        db_path=tmp_path / "feishu.sqlite3",
        admin_open_ids=admins,
    )


def _message_event(
    *,
    text: str,
    chat_type: str = "group",
    event_id: str = "evt-1",
    message_id: str = "msg-1",
    thread_id: str = "",
    parent_id: str = "",
    root_id: str = "",
    sender_open_id: str = "ou_user",
    mentions: tuple[dict[str, str], ...] = (),
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
                "thread_id": thread_id,
                "parent_id": parent_id,
                "root_id": root_id,
                "chat_id": "oc_test_chat",
                "chat_type": chat_type,
                "message_type": "text",
                "content": json.dumps({"text": text}),
                "mentions": list(mentions),
            },
        },
    }


def _activated_group_message_event(
    *,
    text: str,
    event_id: str = "evt-1",
    message_id: str = "msg-1",
    thread_id: str = "",
    parent_id: str = "",
    root_id: str = "",
    sender_open_id: str = "ou_user",
) -> dict[str, object]:
    return _message_event(
        text=f'<at user_id="ou_bot"></at> {text}',
        event_id=event_id,
        message_id=message_id,
        thread_id=thread_id,
        parent_id=parent_id,
        root_id=root_id,
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
    assert client.messages[0]["chat_id"] == "oc_test_chat"
    assert "Available commands:" in client.messages[0]["text"]
    assert "cli <full-cli-command>" in client.messages[0]["text"]


def test_group_placeholder_mention_matching_mentions_metadata_activates_command(
    tmp_path,
) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(
        _message_event(
            text="@_user_1 help",
            mentions=(
                {
                    "key": "@_user_1",
                    "id": "ou_bot",
                    "id_type": "open_id",
                },
            ),
        )
    )

    assert len(client.messages) == 1
    assert "Available commands:" in client.messages[0]["text"]


def test_group_placeholder_mention_matching_nested_mentions_metadata_activates_command(
    tmp_path,
) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(
        _message_event(
            text="@_user_1 help",
            mentions=(
                {
                    "key": "@_user_1",
                    "id": {
                        "open_id": "ou_bot",
                        "union_id": "on_bot",
                        "user_id": None,
                    },
                },
            ),
        )
    )

    assert len(client.messages) == 1
    assert "Available commands:" in client.messages[0]["text"]


def test_group_placeholder_mention_without_matching_bot_target_is_ignored(
    tmp_path, caplog
) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(
        _message_event(
            text="@_user_1 help",
            mentions=(
                {
                    "key": "@_user_1",
                    "id": "ou_other_bot",
                    "id_type": "open_id",
                },
            ),
        )
    )

    assert client.messages == []
    assert "placeholder mention without matching bot target" in caplog.text


def test_group_non_bot_mention_does_not_activate_command(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(_message_event(text="@someone help"))

    assert client.messages == []


def test_group_at_tag_matching_bot_open_id_activates_command(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(_message_event(text='<at user_id="ou_bot"></at> help'))

    assert len(client.messages) == 1
    assert "Available commands:" in client.messages[0]["text"]
    assert "cli <full-cli-command>" in client.messages[0]["text"]


def test_service_fetches_bot_open_id_when_missing_from_settings(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(
        FeishuSettings(
            app_id="app_id",
            app_secret="app_secret",
            db_path=tmp_path / "feishu.sqlite3",
        ),
        client=client,
    )

    service.handle_event_payload(
        _message_event(
            text="@_user_1 help",
            mentions=(
                {
                    "key": "@_user_1",
                    "id": "ou_bot",
                    "id_type": "open_id",
                },
            ),
        )
    )

    assert len(client.messages) == 1
    assert "Available commands:" in client.messages[0]["text"]


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
    assert client.messages[0]["msg_type"] == "interactive"
    assert client.messages[0]["card"] is not None
    table = client.messages[0]["card"]["body"]["elements"][0]
    assert table["tag"] == "table"
    assert table["columns"][0]["display_name"] == "name"
    assert table["rows"][0]["col_0"] == "alpha"


def test_cli_route_replies_in_thread_for_group_thread_message(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)
    db_path = tmp_path / "market.sqlite3"
    with sqlite3.connect(db_path) as connection:
        connection.execute("create table sample_rows (name text)")
        connection.execute("insert into sample_rows (name) values ('alpha')")
        connection.commit()

    service.handle_event_payload(
        _activated_group_message_event(
            text=f"cli db query --db-path {db_path} --table sample_rows",
            message_id="om_thread_message",
            thread_id="omt_thread",
            parent_id="om_parent",
            root_id="om_root",
        )
    )

    assert len(client.messages) == 1
    assert client.messages[0]["chat_id"] == "oc_test_chat"
    assert client.messages[0]["msg_type"] == "interactive"
    assert client.messages[0]["card"] is not None
    table = client.messages[0]["card"]["body"]["elements"][0]
    assert table["rows"][0]["col_0"] == "alpha"
    assert client.messages[0]["reply_to_message_id"] == "om_thread_message"
    assert client.messages[0]["reply_in_thread"] is True


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
    assert client.messages[0]["msg_type"] == "interactive"
    assert client.messages[0]["card"] is not None
    table = client.messages[0]["card"]["body"]["elements"][0]
    assert table["rows"][0]["col_0"] == "alpha"


def test_cli_route_rejects_cli_only_command(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(
        _activated_group_message_event(text="cli feishu longconn")
    )

    assert client.messages == [
        {
            "chat_id": "oc_test_chat",
            "msg_type": "text",
            "text": "feishu longconn is only available from the CLI.",
            "card": None,
            "reply_to_message_id": None,
            "reply_in_thread": False,
        }
    ]


def test_cli_route_reports_usage_error(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(_activated_group_message_event(text="cli db query"))

    assert len(client.messages) == 1
    assert (
        "one of the arguments --sql --table is required" in client.messages[0]["text"]
    )
    assert "db query" in client.messages[0]["text"]


def test_cli_route_reports_handler_stage_usage_error(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)
    db_path = tmp_path / "market.sqlite3"
    with sqlite3.connect(db_path) as connection:
        connection.execute("create table sample_rows (name text)")
        connection.commit()

    service.handle_event_payload(
        _activated_group_message_event(
            text=f'cli db query --db-path {db_path} --sql "DELETE FROM sample_rows"'
        )
    )

    assert len(client.messages) == 1
    assert "Only read-only SELECT queries are allowed." in client.messages[0]["text"]


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
    assert job.reply_to_message_id is None
    assert job.reply_in_thread is False
    assert client.messages[0]["chat_id"] == "oc_test_chat"
    assert client.messages[0]["msg_type"] == "text"
    assert (
        client.messages[0]["text"]
        == "Accepted job 1: demo async. Use `job 1` to query status."
    )
    assert client.messages[1]["chat_id"] == "oc_test_chat"
    assert client.messages[1]["msg_type"] == "text"
    assert client.messages[1]["text"] == "Job 1 succeeded.\nasync:alpha"


def test_cli_async_table_route_prefixes_job_status_in_card_title(
    tmp_path, monkeypatch
) -> None:
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
        _build_async_table_cli_registry,
    )

    service.handle_event_payload(
        _activated_group_message_event(text="cli demo async-table")
    )

    assert client.messages[0]["msg_type"] == "text"
    assert client.messages[1]["msg_type"] == "interactive"
    assert (
        client.messages[1]["card"]["header"]["title"]["content"]
        == "Job 1 succeeded: demo async-table (1 rows)"
    )


def test_cli_execution_policy_switches_between_sync_and_async(
    tmp_path, monkeypatch
) -> None:
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

    service.handle_event_payload(_activated_group_message_event(text="cli demo policy"))
    service.handle_event_payload(
        _activated_group_message_event(
            text="cli demo policy --all",
            event_id="evt-2",
            message_id="msg-2",
        )
    )

    assert client.messages[0]["chat_id"] == "oc_test_chat"
    assert client.messages[0]["msg_type"] == "text"
    assert client.messages[0]["text"] == "policy:False"
    assert client.messages[1]["chat_id"] == "oc_test_chat"
    assert client.messages[1]["msg_type"] == "text"
    assert (
        client.messages[1]["text"]
        == "Accepted job 1: demo policy. Use `job 1` to query status."
    )
    assert client.messages[2]["chat_id"] == "oc_test_chat"
    assert client.messages[2]["msg_type"] == "text"
    assert client.messages[2]["text"] == "Job 1 succeeded.\npolicy:True"


def test_cli_async_route_persists_thread_reply_context_and_notifies_in_thread(
    tmp_path, monkeypatch
) -> None:
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
        _activated_group_message_event(
            text="cli demo async --value alpha",
            message_id="om_thread_message",
            thread_id="omt_thread",
            parent_id="om_parent",
            root_id="om_root",
        )
    )

    job = store.get_job(1)
    assert job is not None
    assert job.reply_to_message_id == "om_thread_message"
    assert job.reply_in_thread is True
    assert client.messages[0]["reply_to_message_id"] == "om_thread_message"
    assert client.messages[0]["reply_in_thread"] is True
    assert client.messages[1]["reply_to_message_id"] == "om_thread_message"
    assert client.messages[1]["reply_in_thread"] is True


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
    assert "job_id=1" in client.messages[0]["text"]
    assert "command=demo async" in client.messages[0]["text"]


def test_normalizer_rejects_message_without_sender_open_id(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)
    payload = _message_event(text="help")
    payload["event"]["sender"]["sender_id"] = {"user_id": "u_user"}  # type: ignore[index]

    service.handle_event_payload(payload)

    assert client.messages == []


def test_service_close_closes_owned_resources(tmp_path) -> None:
    client = FakeClient()
    executor = FakeExecutor()
    service = FeishuBotService(
        _settings(tmp_path),
        client=client,
        executor=executor,
    )
    service._owns_client = True
    service._owns_executor = True
    client.closed = False

    service.close()

    assert client.closed is True
    assert executor.shutdown_called is True


def test_job_route_reports_usage_for_invalid_job_id(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(_activated_group_message_event(text="job abc"))

    assert client.messages == [
        {
            "chat_id": "oc_test_chat",
            "msg_type": "text",
            "text": "Usage: job <job-id>",
            "card": None,
            "reply_to_message_id": None,
            "reply_in_thread": False,
        }
    ]


def test_legacy_command_is_rejected_with_help(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(
        _activated_group_message_event(text="db table sample_rows")
    )

    assert len(client.messages) == 1
    assert client.messages[0]["chat_id"] == "oc_test_chat"
    assert "Invalid command." in client.messages[0]["text"]
    assert "cli <full-cli-command>" in client.messages[0]["text"]


def test_slash_neo_prefix_is_rejected(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(_activated_group_message_event(text="/neo help"))

    assert len(client.messages) == 1
    assert "Invalid command." in client.messages[0]["text"]
    assert "cli <full-cli-command>" in client.messages[0]["text"]


def test_duplicate_event_is_ignored(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)
    payload = _activated_group_message_event(text="help")

    service.handle_event_payload(payload)
    service.handle_event_payload(payload)

    assert len(client.messages) == 1


def test_group_thread_message_replies_in_thread(tmp_path) -> None:
    client = FakeClient()
    service = FeishuBotService(_settings(tmp_path), client=client)

    service.handle_event_payload(
        _activated_group_message_event(
            text="help",
            thread_id="omt_123",
            parent_id="om_parent",
            root_id="om_root",
            message_id="om_thread_message",
        )
    )

    assert len(client.messages) == 1
    assert client.messages[0]["chat_id"] == "oc_test_chat"
    assert "Available commands:" in client.messages[0]["text"]
    assert client.messages[0]["reply_to_message_id"] == "om_thread_message"
    assert client.messages[0]["reply_in_thread"] is True
