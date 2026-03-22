"""SQLAlchemy models for the Feishu bot database."""

from __future__ import annotations

from sqlalchemy import Index, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class BotBase(DeclarativeBase):
    """Base class for Feishu bot ORM models."""


class FeishuEventReceiptRow(BotBase):
    """One processed Feishu event used for idempotency."""

    __tablename__ = "feishu_event_receipts"
    __table_args__ = (Index("idx_feishu_event_receipts_message_id", "message_id"),)

    event_id: Mapped[str] = mapped_column(String, primary_key=True)
    message_id: Mapped[str] = mapped_column(String, nullable=False)
    received_at: Mapped[str] = mapped_column(String, nullable=False)


class FeishuJobRow(BotBase):
    """Async bot jobs persisted so operators can inspect state."""

    __tablename__ = "feishu_jobs"
    __table_args__ = (Index("idx_feishu_jobs_status", "status"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    command_name: Mapped[str] = mapped_column(String, nullable=False)
    command_text: Mapped[str] = mapped_column(String, nullable=False)
    chat_id: Mapped[str] = mapped_column(String, nullable=False)
    user_open_id: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    submitted_at: Mapped[str] = mapped_column(String, nullable=False)
    started_at: Mapped[str | None] = mapped_column(String, nullable=True)
    finished_at: Mapped[str | None] = mapped_column(String, nullable=True)
    result_text: Mapped[str | None] = mapped_column(String, nullable=True)
    error_text: Mapped[str | None] = mapped_column(String, nullable=True)
