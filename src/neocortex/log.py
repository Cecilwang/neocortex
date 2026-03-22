"""Shared logging helpers for Neocortex entrypoints."""

from __future__ import annotations

import logging


DEFAULT_LOG_FORMAT = (
    "%(asctime)s %(levelname)s %(name)s "
    "[%(filename)s:%(funcName)s:%(lineno)d] %(message)s"
)


def configure_logging(
    level: str | int = logging.INFO,
    *,
    format: str = DEFAULT_LOG_FORMAT,
) -> None:
    """Configure process-wide logging for command entrypoints."""

    resolved_level = getattr(logging, level) if isinstance(level, str) else level
    logging.basicConfig(level=resolved_level, format=format)
