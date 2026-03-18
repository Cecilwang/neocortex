"""SQLAlchemy models for the shared Neocortex storage database."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import (
    ForeignKeyConstraint,
    Index,
    String,
    UniqueConstraint,
    create_engine,
    event,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


class Base(DeclarativeBase):
    """Base class for all storage ORM models."""


class SecurityRow(Base):
    """Canonical securities keyed by stable internal identity."""

    __tablename__ = "securities"

    market: Mapped[str] = mapped_column(String, primary_key=True)
    exchange: Mapped[str] = mapped_column(String, primary_key=True)
    symbol: Mapped[str] = mapped_column(String, primary_key=True)
    source: Mapped[str] = mapped_column(String, nullable=False)
    last_seen_at: Mapped[str] = mapped_column(String, nullable=False)


class CompanyProfileRow(Base):
    """Latest normalized company profile snapshot."""

    __tablename__ = "company_profiles"
    __table_args__ = (
        ForeignKeyConstraint(
            ["market", "exchange", "symbol"],
            ["securities.market", "securities.exchange", "securities.symbol"],
        ),
    )

    market: Mapped[str] = mapped_column(String, primary_key=True)
    exchange: Mapped[str] = mapped_column(String, primary_key=True)
    symbol: Mapped[str] = mapped_column(String, primary_key=True)
    company_name: Mapped[str] = mapped_column(String, nullable=False)
    sector: Mapped[str] = mapped_column(String, nullable=False)
    industry: Mapped[str] = mapped_column(String, nullable=False)
    country: Mapped[str] = mapped_column(String, nullable=False)
    currency: Mapped[str] = mapped_column(String, nullable=False)
    fetched_at: Mapped[str] = mapped_column(String, nullable=False)


class SecurityAliasRow(Base):
    """Search aliases kept separate so names can evolve independently."""

    __tablename__ = "security_aliases"
    __table_args__ = (
        ForeignKeyConstraint(
            ["market", "exchange", "symbol"],
            ["securities.market", "securities.exchange", "securities.symbol"],
        ),
        UniqueConstraint(
            "market",
            "exchange",
            "symbol",
            "alias",
            "language",
        ),
        Index("idx_security_aliases_alias_norm", "alias_norm"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    market: Mapped[str] = mapped_column(String, nullable=False)
    exchange: Mapped[str] = mapped_column(String, nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    alias: Mapped[str] = mapped_column(String, nullable=False)
    alias_norm: Mapped[str] = mapped_column(String, nullable=False)
    language: Mapped[str] = mapped_column(String, nullable=False)
    source: Mapped[str] = mapped_column(String, nullable=False)
    updated_at: Mapped[str] = mapped_column(String, nullable=False)


class FeishuEventReceiptRow(Base):
    """One processed Feishu event used for idempotency."""

    __tablename__ = "feishu_event_receipts"
    __table_args__ = (Index("idx_feishu_event_receipts_message_id", "message_id"),)

    event_id: Mapped[str] = mapped_column(String, primary_key=True)
    message_id: Mapped[str] = mapped_column(String, nullable=False)
    received_at: Mapped[str] = mapped_column(String, nullable=False)


class FeishuJobRow(Base):
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


def create_sqlite_engine(db_path: str | Path) -> Engine:
    """Create a SQLite engine with foreign-key enforcement enabled."""

    resolved_path = Path(db_path)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(
        f"sqlite:///{resolved_path}",
        future=True,
        connect_args={"timeout": 30},
    )

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.execute("PRAGMA busy_timeout = 30000")
        cursor.execute("PRAGMA journal_mode = WAL")
        cursor.close()

    return engine


SessionFactory = sessionmaker
