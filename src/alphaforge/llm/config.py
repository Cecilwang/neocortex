"""LLM service and inference configuration models."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


JsonDict = dict[str, Any]


class LLMService(StrEnum):
    """Supported LLM service families."""

    OPENAI = "openai"
    AZURE_OPENAI = "azure_openai"
    ANTHROPIC = "anthropic"
    GOOGLE = "google"
    OPENROUTER = "openrouter"
    CUSTOM = "custom"


@dataclass(frozen=True, slots=True)
class LLMEndpoint:
    """Static endpoint configuration for one inference service and model."""

    service: LLMService
    model: str
    base_url: str
    auth_env_var: str


@dataclass(frozen=True, slots=True)
class LLMRequestConfig:
    """Request-level inference settings for a single model call."""

    temperature: float | None = None
    max_tokens: int | None = None
    top_p: float | None = None
    extra_params: JsonDict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LLMInferenceConfig:
    """Complete inference configuration used for one runtime execution."""

    endpoint: LLMEndpoint
    request: LLMRequestConfig = field(default_factory=LLMRequestConfig)
