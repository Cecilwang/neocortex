import logging

from neocortex.utils.retry import connector_retry


def test_connector_retry_uses_configured_attempt_budget(monkeypatch) -> None:
    class DummyWorker:
        def __init__(self) -> None:
            self.calls = 0

        @connector_retry(source_name="baostock")
        def flaky(self) -> str:
            self.calls += 1
            if self.calls < 3:
                raise RuntimeError("try again")
            return "ok"

    slept: list[float] = []
    monkeypatch.setattr("neocortex.utils.retry.time.sleep", slept.append)
    connector = DummyWorker()

    assert connector.flaky() == "ok"
    assert connector.calls == 3
    assert slept == [1.0, 1.0]


def test_connector_retry_logs_compact_message(caplog, monkeypatch) -> None:
    class DummyWorker:
        def __init__(self) -> None:
            self.calls = 0

        @connector_retry(source_name="baostock")
        def flaky(self) -> str:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("temporary")
            return "ok"

    monkeypatch.setattr("neocortex.utils.retry.time.sleep", lambda _: None)
    connector = DummyWorker()

    with caplog.at_level(logging.WARNING):
        assert connector.flaky() == "ok"

    assert "Retrying after attempt 1/3 due to RuntimeError: temporary" in caplog.text
    assert caplog.records[0].exc_info is False


def test_connector_retry_without_source_name_uses_default_config(monkeypatch) -> None:
    class PlainWorker:
        def __init__(self) -> None:
            self.calls = 0

        @connector_retry
        def flaky(self) -> str:
            self.calls += 1
            if self.calls < 3:
                raise RuntimeError("try again")
            return "ok"

    slept: list[float] = []
    monkeypatch.setattr("neocortex.utils.retry.time.sleep", slept.append)
    worker = PlainWorker()

    assert worker.flaky() == "ok"
    assert worker.calls == 3
    assert slept == [1.0, 1.0]
