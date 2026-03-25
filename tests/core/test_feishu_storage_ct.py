import sqlite3

import pytest

from neocortex.feishu.storage import FeishuBotStore
from neocortex.feishu.models import JobStatus
from tests.core.feishu_storage_test_support import seed_cleanup_test_data


def test_record_event_is_idempotent(tmp_path) -> None:
    store = FeishuBotStore(tmp_path / "feishu.sqlite3")

    first = store.record_event(event_id="evt-1", message_id="msg-1")
    second = store.record_event(event_id="evt-1", message_id="msg-1")

    assert first is True
    assert second is False


def test_job_state_transitions_round_trip_through_store(tmp_path) -> None:
    store = FeishuBotStore(tmp_path / "feishu.sqlite3")

    queued_job = store.create_job(
        command_name="demo run",
        command_text="cli demo run",
        chat_id="oc_chat",
        user_open_id="ou_user",
    )
    assert queued_job.status is JobStatus.QUEUED

    store.mark_job_running(queued_job.id)
    running_job = store.get_job(queued_job.id)
    assert running_job is not None
    assert running_job.status is JobStatus.RUNNING
    assert running_job.started_at is not None

    store.mark_job_succeeded(queued_job.id, result_text="done")
    succeeded_job = store.get_job(queued_job.id)
    assert succeeded_job is not None
    assert succeeded_job.status is JobStatus.SUCCEEDED
    assert succeeded_job.result_text == "done"
    assert succeeded_job.finished_at is not None

    failed_job = store.create_job(
        command_name="demo fail",
        command_text="cli demo fail",
        chat_id="oc_chat",
        user_open_id="ou_user",
    )
    store.mark_job_failed(failed_job.id, error_text="boom")
    loaded_failed_job = store.get_job(failed_job.id)
    assert loaded_failed_job is not None
    assert loaded_failed_job.status is JobStatus.FAILED
    assert loaded_failed_job.error_text == "boom"
    assert loaded_failed_job.finished_at is not None


def test_cleanup_older_than_deletes_old_receipts_and_terminal_jobs(tmp_path) -> None:
    db_path = tmp_path / "feishu.sqlite3"
    store, old_job_id, recent_job_id, running_job_id = seed_cleanup_test_data(db_path)

    receipts_deleted, jobs_deleted = store.cleanup_older_than(older_than_days=30)

    assert receipts_deleted == 1
    assert jobs_deleted == 1
    assert store.get_job(old_job_id) is None
    assert store.get_job(recent_job_id) is not None
    assert store.get_job(running_job_id) is not None
    with sqlite3.connect(db_path) as connection:
        event_ids = tuple(
            row[0]
            for row in connection.execute(
                "select event_id from feishu_event_receipts order by event_id"
            ).fetchall()
        )
    assert event_ids == ("evt-new",)


def test_cleanup_older_than_rejects_non_positive_retention(tmp_path) -> None:
    store = FeishuBotStore(tmp_path / "feishu.sqlite3")

    with pytest.raises(ValueError) as exc_info:
        store.cleanup_older_than(older_than_days=0)

    assert "--older-than-days must be a positive integer." in str(exc_info.value)
