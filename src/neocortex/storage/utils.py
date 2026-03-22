"""Storage utility helpers shared by bot and market data layers."""

from __future__ import annotations

from datetime import UTC, datetime


def utc_now_iso() -> str:
    """Return a stable UTC timestamp string for SQLite audit columns."""

    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_alias(value: str) -> str:
    """Normalize aliases for case-insensitive and whitespace-stable lookup."""

    return " ".join(value.split()).lower()
