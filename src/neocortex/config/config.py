"""Application configuration loaded from one repository YAML file."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import builtins
import os
from pathlib import Path
from typing import Any

import yaml

from neocortex.models import Market


_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_CONFIG_PATH = _REPO_ROOT / "config" / "config.yaml"
_CONFIG_ENV_VAR = "NEOCORTEX_CONFIG_PATH"


@dataclass(frozen=True, slots=True)
class StorageConfig:
    bot_db_path: Path
    market_data_db_path: Path


@dataclass(frozen=True, slots=True)
class ConnectorRetryConfig:
    max_attempts: int
    backoff_seconds: float
    exc_info: bool
    retryable_exceptions: tuple[type[Exception], ...]


@dataclass(frozen=True, slots=True)
class ConnectorConfig:
    retry: ConnectorRetryConfig | None = None


@dataclass(frozen=True, slots=True)
class ConnectorsConfig:
    defaults: ConnectorConfig
    sources: dict[str, ConnectorConfig]

    def retry_for(self, source_name: str) -> ConnectorRetryConfig:
        source_config = self.sources.get(source_name)
        if source_config is not None and source_config.retry is not None:
            return source_config.retry
        if self.defaults.retry is None:
            raise ValueError("Missing default connector retry config.")
        return self.defaults.retry


@dataclass(frozen=True, slots=True)
class MarketDataProviderConfig:
    source_priority: dict[Market, dict[str, tuple[str, ...]]]


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    agents: dict[str, dict[str, object]]


@dataclass(frozen=True, slots=True)
class AppConfig:
    path: Path
    storage: StorageConfig
    connectors: ConnectorsConfig
    market_data_provider: MarketDataProviderConfig
    pipeline: PipelineConfig


def default_config_path() -> Path:
    raw_path = os.environ.get(_CONFIG_ENV_VAR)
    if raw_path:
        path = Path(raw_path)
        return path if path.is_absolute() else Path.cwd() / path
    return _DEFAULT_CONFIG_PATH


def get_config(config_path: str | Path | None = None) -> AppConfig:
    resolved_path = (
        Path(config_path) if config_path is not None else default_config_path()
    )
    if not resolved_path.is_absolute():
        resolved_path = Path.cwd() / resolved_path
    return _load_cached_config(str(resolved_path.resolve()))


def reset_config_cache() -> None:
    _load_cached_config.cache_clear()


@lru_cache(maxsize=None)
def _load_cached_config(config_path: str) -> AppConfig:
    path = Path(config_path)
    raw_document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw_document, dict):
        raise ValueError("Config file must be one YAML mapping.")

    storage_document = _mapping_section(raw_document, "storage")
    connectors_document = _mapping_section(raw_document, "connectors")
    connector_defaults_document = _mapping_section(connectors_document, "defaults")
    market_data_document = _mapping_section(raw_document, "market_data_provider")
    pipeline_document = _mapping_section(raw_document, "pipeline")
    agents_document = _mapping_section(pipeline_document, "agents")
    source_priority_document = _mapping_section(market_data_document, "source_priority")

    storage = StorageConfig(
        bot_db_path=_resolve_repo_path(path, storage_document["bot_db_path"]),
        market_data_db_path=_resolve_repo_path(
            path, storage_document["market_data_db_path"]
        ),
    )
    connectors = ConnectorsConfig(
        defaults=ConnectorConfig(
            retry=_retry_config(_mapping_section(connector_defaults_document, "retry")),
        ),
        sources={
            source_name: ConnectorConfig(
                retry=(
                    _retry_config(_mapping_section(source_document, "retry"))
                    if "retry" in source_document
                    else None
                ),
            )
            for source_name, source_document in connectors_document.items()
            if source_name != "defaults" and isinstance(source_document, dict)
        },
    )
    source_priority: dict[Market, dict[str, tuple[str, ...]]] = {}
    for market_name, resource_priorities in source_priority_document.items():
        if not isinstance(resource_priorities, dict):
            raise ValueError(
                f"Source priority for market={market_name} must be one mapping."
            )
        source_priority[Market(market_name)] = {
            resource_type: _string_sequence(resource_type, source_names)
            for resource_type, source_names in resource_priorities.items()
        }

    agents = {
        agent_name: dict(agent_config)
        for agent_name, agent_config in agents_document.items()
        if isinstance(agent_config, dict)
    }
    if len(agents) != len(agents_document):
        raise ValueError("Pipeline agents config must map agent names to mappings.")

    return AppConfig(
        path=path,
        storage=storage,
        connectors=connectors,
        market_data_provider=MarketDataProviderConfig(source_priority=source_priority),
        pipeline=PipelineConfig(agents=agents),
    )


def _mapping_section(document: dict[str, Any], key: str) -> dict[str, Any]:
    value = document.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"Missing config mapping for {key}.")
    return value


def _resolve_repo_path(config_path: Path, raw_value: object) -> Path:
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise ValueError("Configured path values must be non-empty strings.")
    path = Path(raw_value)
    if path.is_absolute():
        return path
    base_dir = (
        config_path.parent.parent
        if config_path.parent.name == "config"
        else config_path.parent
    )
    return base_dir / path


def _retry_config(document: dict[str, Any]) -> ConnectorRetryConfig:
    max_attempts = document.get("max_attempts")
    backoff_seconds = document.get("backoff_seconds")
    exc_info = document.get("exc_info", False)
    retryable_exception_names = document.get("retryable_exceptions")
    if not isinstance(max_attempts, int) or max_attempts < 1:
        raise ValueError("Connector retry max_attempts must be an integer >= 1.")
    if not isinstance(backoff_seconds, (int, float)) or backoff_seconds < 0:
        raise ValueError("Connector retry backoff_seconds must be >= 0.")
    if not isinstance(exc_info, bool):
        raise ValueError("Connector retry exc_info must be a boolean.")
    if not isinstance(retryable_exception_names, list) or not all(
        isinstance(item, str) for item in retryable_exception_names
    ):
        raise ValueError(
            "Connector retry retryable_exceptions must be a list of strings."
        )
    return ConnectorRetryConfig(
        max_attempts=max_attempts,
        backoff_seconds=float(backoff_seconds),
        exc_info=exc_info,
        retryable_exceptions=tuple(
            _resolve_exception_class(name) for name in retryable_exception_names
        ),
    )


def _resolve_exception_class(name: str) -> type[Exception]:
    candidate = getattr(builtins, name, None)
    if not isinstance(candidate, type) or not issubclass(candidate, Exception):
        raise ValueError(f"Unsupported retryable exception class: {name}")
    return candidate


def _string_sequence(resource_type: str, value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(
            f"Source priority for {resource_type} must be a list of strings."
        )
    return tuple(value)
