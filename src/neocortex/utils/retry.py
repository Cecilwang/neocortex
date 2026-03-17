"""Retry helpers for unstable upstream integrations."""

from __future__ import annotations

import logging
import time
from typing import Callable, TypeVar


T = TypeVar("T")


def call_with_retries(
    operation: Callable[[], T],
    *,
    retry_count: int = 0,
    sleep_seconds: float = 0.0,
    logger: logging.Logger | None = None,
    description: str = "Operation failed",
    exc_info: bool = False,
) -> T:
    """Call one operation with bounded retries and optional logging."""

    total_attempts = retry_count + 1
    for attempt in range(1, total_attempts + 1):
        try:
            return operation()
        except Exception:
            if attempt >= total_attempts:
                raise
            if logger is not None:
                logger.warning(
                    "%s on attempt %s/%s, retrying.",
                    description,
                    attempt,
                    total_attempts,
                    exc_info=exc_info,
                )
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    raise AssertionError("Retry loop exited without returning or raising.")
