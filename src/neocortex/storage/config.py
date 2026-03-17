"""Storage configuration defaults."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


_REPO_ROOT = Path(__file__).resolve().parents[3]
_CONFIG_PATH = _REPO_ROOT / "config" / "storage.yaml"


@dataclass(frozen=True, slots=True)
class StorageConfig:
    """Resolved storage configuration rooted at the repository."""

    db_path: Path


def _load_storage_config() -> StorageConfig:
    raw_config = yaml.safe_load(_CONFIG_PATH.read_text(encoding="utf-8"))
    db_path = Path(raw_config["db_path"])
    resolved_db_path = db_path if db_path.is_absolute() else _REPO_ROOT / db_path
    return StorageConfig(db_path=resolved_db_path)


DEFAULT_STORAGE_CONFIG = _load_storage_config()
DEFAULT_DB_PATH = DEFAULT_STORAGE_CONFIG.db_path
