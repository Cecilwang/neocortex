import logging

import pytest

from neocortex.utils.retry import call_with_retries


def test_call_with_retries_returns_after_transient_failures() -> None:
    attempts = {"count": 0}

    def flaky() -> str:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("try again")
        return "ok"

    result = call_with_retries(
        flaky,
        retry_count=2,
        logger=logging.getLogger(__name__),
        description="transient failure",
    )

    assert result == "ok"
    assert attempts["count"] == 3


def test_call_with_retries_raises_after_exhausting_attempts() -> None:
    attempts = {"count": 0}

    def always_fail() -> None:
        attempts["count"] += 1
        raise RuntimeError("still broken")

    with pytest.raises(RuntimeError, match="still broken"):
        call_with_retries(
            always_fail,
            retry_count=2,
            logger=logging.getLogger(__name__),
            description="persistent failure",
        )

    assert attempts["count"] == 3
