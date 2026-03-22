"""SQLite-backed persistence helpers for Feishu events and jobs."""

from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy.exc import IntegrityError

from neocortex.feishu.models import FeishuJobRecord, JobStatus
from neocortex.storage.bot_models import (
    BotBase,
    FeishuEventReceiptRow,
    FeishuJobRow,
)
from neocortex.storage.sqlite import SessionFactory, create_sqlite_engine
from neocortex.storage.utils import utc_now_iso

logger = logging.getLogger(__name__)


class FeishuBotStore:
    """Persist processed event receipts and async job state."""

    def __init__(self, db_path: str | Path) -> None:
        self.engine = create_sqlite_engine(db_path)
        self.session_factory = SessionFactory(bind=self.engine, expire_on_commit=False)
        BotBase.metadata.create_all(self.engine)
        logger.info("Initialized FeishuBotStore: db_path=%s", db_path)

    def record_event(self, *, event_id: str, message_id: str) -> bool:
        """Store one event receipt and return whether it was new."""

        logger.info("Recording Feishu event receipt: event_id=%s", event_id)
        with self.session_factory() as session:
            session.add(
                FeishuEventReceiptRow(
                    event_id=event_id,
                    message_id=message_id,
                    received_at=utc_now_iso(),
                )
            )
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                logger.info(
                    "Feishu event receipt already exists: event_id=%s", event_id
                )
                return False
        logger.info("Recorded Feishu event receipt: event_id=%s", event_id)
        return True

    def create_job(
        self,
        *,
        command_name: str,
        command_text: str,
        chat_id: str,
        user_open_id: str,
    ) -> FeishuJobRecord:
        """Create one queued async job."""

        logger.info(
            "Creating Feishu job: command_name=%s chat_id=%s",
            command_name,
            chat_id,
        )
        row = FeishuJobRow(
            command_name=command_name,
            command_text=command_text,
            chat_id=chat_id,
            user_open_id=user_open_id,
            status=JobStatus.QUEUED.value,
            submitted_at=utc_now_iso(),
        )
        with self.session_factory() as session:
            session.add(row)
            session.commit()
            session.refresh(row)
            logger.info("Created Feishu job: job_id=%s status=%s", row.id, row.status)
            return _to_job_record(row)

    def get_job(self, job_id: int) -> FeishuJobRecord | None:
        """Load one job by id."""

        logger.info("Loading Feishu job: job_id=%s", job_id)
        with self.session_factory() as session:
            row = session.get(FeishuJobRow, job_id)
            if row is None:
                logger.info("Feishu job not found: job_id=%s", job_id)
                return None
            logger.info("Loaded Feishu job: job_id=%s status=%s", job_id, row.status)
            return _to_job_record(row)

    def mark_job_running(self, job_id: int) -> None:
        """Mark one job as running."""

        logger.info("Marking Feishu job running: job_id=%s", job_id)
        with self.session_factory() as session:
            row = session.get(FeishuJobRow, job_id)
            if row is None:
                logger.info(
                    "Feishu job missing when marking running: job_id=%s", job_id
                )
                return
            row.status = JobStatus.RUNNING.value
            row.started_at = utc_now_iso()
            session.commit()
            logger.info("Marked Feishu job running: job_id=%s", job_id)

    def mark_job_succeeded(
        self, job_id: int, *, result_text: str
    ) -> FeishuJobRecord | None:
        """Mark one job as succeeded and persist the rendered output."""

        logger.info("Marking Feishu job succeeded: job_id=%s", job_id)
        with self.session_factory() as session:
            row = session.get(FeishuJobRow, job_id)
            if row is None:
                logger.info(
                    "Feishu job missing when marking succeeded: job_id=%s", job_id
                )
                return None
            row.status = JobStatus.SUCCEEDED.value
            row.result_text = result_text
            row.finished_at = utc_now_iso()
            session.commit()
            session.refresh(row)
            logger.info("Marked Feishu job succeeded: job_id=%s", job_id)
            return _to_job_record(row)

    def mark_job_failed(
        self, job_id: int, *, error_text: str
    ) -> FeishuJobRecord | None:
        """Mark one job as failed and persist the failure reason."""

        logger.info("Marking Feishu job failed: job_id=%s", job_id)
        with self.session_factory() as session:
            row = session.get(FeishuJobRow, job_id)
            if row is None:
                logger.info("Feishu job missing when marking failed: job_id=%s", job_id)
                return None
            row.status = JobStatus.FAILED.value
            row.error_text = error_text
            row.finished_at = utc_now_iso()
            session.commit()
            session.refresh(row)
            logger.info("Marked Feishu job failed: job_id=%s", job_id)
            return _to_job_record(row)


def _to_job_record(row: FeishuJobRow) -> FeishuJobRecord:
    return FeishuJobRecord(
        id=row.id,
        command_name=row.command_name,
        command_text=row.command_text,
        chat_id=row.chat_id,
        user_open_id=row.user_open_id,
        status=JobStatus(row.status),
        submitted_at=row.submitted_at,
        started_at=row.started_at,
        finished_at=row.finished_at,
        result_text=row.result_text,
        error_text=row.error_text,
    )
