"""Shared retry helpers for network-facing operations."""

from __future__ import annotations

import functools
import logging
import time
from collections.abc import Callable
from typing import Any, TypeVar, overload

from neocortex.config import get_config

logger = logging.getLogger(__name__)

T = TypeVar("T")


@overload
def connector_retry(func: Callable[..., T], /) -> Callable[..., T]: ...


@overload
def connector_retry(
    func: None = None,
    /,
    *,
    source_name: str | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]: ...


def connector_retry(
    func: Callable[..., T] | None = None,
    /,
    *,
    source_name: str | None = None,
) -> Callable[..., Any]:
    """Retry one callable according to configured connector or default retry config."""

    def decorator(inner: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(inner)
        def wrapper(*args, **kwargs) -> T:
            retry_config = get_config().connectors.retry_for(source_name or "")
            attempts = retry_config.max_attempts
            for attempt in range(1, attempts + 1):
                try:
                    return inner(*args, **kwargs)
                except retry_config.retryable_exceptions as exc:
                    if attempt >= attempts:
                        raise
                    logger.warning(
                        f"Retrying after attempt {attempt}/{attempts} due to "
                        f"{type(exc).__name__}: {exc}",
                        exc_info=retry_config.exc_info,
                    )
                    if retry_config.backoff_seconds > 0:
                        time.sleep(retry_config.backoff_seconds)

            raise AssertionError("Retry wrapper exited without returning or raising.")

        return wrapper

    if func is not None:
        return decorator(func)
    return decorator
