"""Shared JSON-friendly serialization helpers."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from typing import Any


def to_json_ready(value: Any) -> Any:
    """Convert dataclasses and datetimes into JSON-friendly primitives."""

    if is_dataclass(value):
        return to_json_ready(asdict(value))
    if isinstance(value, dict):
        return {key: to_json_ready(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [to_json_ready(item) for item in value]
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


def to_pretty_json(value: Any) -> str:
    """Render one value as indented JSON after normalizing Python objects."""

    return json.dumps(to_json_ready(value), ensure_ascii=False, indent=2)


def parse_json_object(raw_output: str | dict[str, Any]) -> dict[str, Any]:
    """Normalize one raw model output into a JSON object."""

    if isinstance(raw_output, dict):
        return raw_output
    parsed = json.loads(raw_output)
    if not isinstance(parsed, dict):
        raise ValueError("Expected one JSON object at top level.")
    return parsed
