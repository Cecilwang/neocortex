import sqlite3

import pytest

from neocortex.feishu.storage import FeishuBotStore
from tests.core.feishu_storage_test_support import seed_cleanup_test_data


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
