from dataclasses import dataclass
from datetime import date, datetime
import json
import logging

import pytest

from neocortex.log import configure_logging
from neocortex.serialization import parse_json_object, to_json_ready
from neocortex.storage.sqlite import create_sqlite_engine


@dataclass(frozen=True, slots=True)
class _SamplePayload:
    created_at: datetime
    as_of_date: date


def test_parse_json_object_requires_top_level_mapping() -> None:
    with pytest.raises(ValueError) as exc_info:
        parse_json_object('["bullish", 0.72]')

    assert "Expected one JSON object at top level." in str(exc_info.value)


def test_parse_json_object_rejects_invalid_json() -> None:
    with pytest.raises(json.JSONDecodeError):
        parse_json_object("ASdgb")


def test_to_json_ready_normalizes_dataclasses_and_dates() -> None:
    payload = _SamplePayload(
        created_at=datetime(2026, 3, 25, 10, 30, 0),
        as_of_date=date(2026, 3, 25),
    )

    assert to_json_ready(payload) == {
        "created_at": "2026-03-25T10:30:00",
        "as_of_date": "2026-03-25",
    }


def test_configure_logging_reconfigures_root_logger() -> None:
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level

    try:
        configure_logging("ERROR")
        assert root_logger.level == logging.ERROR
        configure_logging("DEBUG")
        assert root_logger.level == logging.DEBUG
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
        for handler in original_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(original_level)


def test_create_sqlite_engine_enables_foreign_keys_and_creates_parent_dir(
    tmp_path,
) -> None:
    db_path = tmp_path / "nested" / "data.sqlite3"

    engine = create_sqlite_engine(db_path)

    assert db_path.parent.exists()
    with engine.connect() as connection:
        foreign_keys = connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one()
        busy_timeout = connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one()

    assert foreign_keys == 1
    assert busy_timeout == 30000
