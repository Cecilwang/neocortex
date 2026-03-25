import sqlite3
from pathlib import Path

from neocortex.feishu.storage import FeishuBotStore


def seed_cleanup_test_data(
    db_path: Path,
) -> tuple[FeishuBotStore, int, int, int]:
    store = FeishuBotStore(db_path)
    store.record_event(event_id="evt-old", message_id="msg-old")
    store.record_event(event_id="evt-new", message_id="msg-new")
    old_job = store.create_job(
        command_name="demo old",
        command_text="cli demo old",
        chat_id="oc_chat",
        user_open_id="ou_user",
    )
    recent_job = store.create_job(
        command_name="demo recent",
        command_text="cli demo recent",
        chat_id="oc_chat",
        user_open_id="ou_user",
    )
    running_job = store.create_job(
        command_name="demo running",
        command_text="cli demo running",
        chat_id="oc_chat",
        user_open_id="ou_user",
    )
    store.mark_job_succeeded(old_job.id)
    store.mark_job_failed(recent_job.id)
    store.mark_job_running(running_job.id)

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "update feishu_event_receipts set received_at = ? where event_id = ?",
            ("2025-01-01T00:00:00Z", "evt-old"),
        )
        connection.execute(
            "update feishu_event_receipts set received_at = ? where event_id = ?",
            ("2026-03-24T00:00:00Z", "evt-new"),
        )
        connection.execute(
            "update feishu_jobs set finished_at = ? where id = ?",
            ("2025-01-01T00:00:00Z", old_job.id),
        )
        connection.execute(
            "update feishu_jobs set finished_at = ? where id = ?",
            ("2026-03-24T00:00:00Z", recent_job.id),
        )
        connection.commit()

    return store, old_job.id, recent_job.id, running_job.id
