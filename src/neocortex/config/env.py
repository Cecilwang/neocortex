"""Minimal dotenv loading for local development."""

from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(path: str | Path = ".env", *, override: bool = False) -> Path | None:
    """Load environment variables from one dotenv file if it exists."""

    dotenv_path = Path(path)
    if not dotenv_path.is_absolute():
        dotenv_path = Path.cwd() / dotenv_path
    if not dotenv_path.exists():
        return None

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        value = _parse_dotenv_value(raw_value.strip())
        if override or key not in os.environ:
            os.environ[key] = value
    return dotenv_path


def _parse_dotenv_value(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value
