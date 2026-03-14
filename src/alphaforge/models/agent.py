"""Shared agent request, response, and trace models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import Any

from alphaforge.llm import LLMInferenceConfig
from alphaforge.models.core import SecurityId


JsonDict = dict[str, Any]


class AgentRole(StrEnum):
    """Canonical agent identifiers used across prompts, traces, and UI."""

    TECHNICAL = "technical_agent"
    QUANT_FUNDAMENTAL = "quant_fundamental_agent"
    QUALITATIVE_FUNDAMENTAL = "qualitative_fundamental_agent"
    NEWS = "news_agent"
    SECTOR = "sector_agent"
    MACRO = "macro_agent"
    PM = "pm_agent"


class ResponseValidationStatus(StrEnum):
    """Validation state for an agent response after parsing."""

    PENDING = "pending"
    PASSED = "passed"
    FAILED = "failed"
    REPAIRED = "repaired"


@dataclass(frozen=True, slots=True)
class AgentRequest:
    """Structured input passed into an agent-specific prompt builder."""

    request_id: str
    agent: AgentRole
    security_id: SecurityId
    as_of_date: date
    schema_version: str
    payload: JsonDict = field(default_factory=dict)
    dependencies: tuple[AgentRole, ...] = ()


@dataclass(frozen=True, slots=True)
class AgentResponse:
    """Minimal response fields shared by all agent outputs."""

    request_id: str
    agent: AgentRole
    security_id: SecurityId
    as_of_date: date
    schema_version: str
    reasoning: str

    score: float | None
    confidence: float | None
    raw_model_output: JsonDict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class AgentExecutionTrace:
    """Audit log for a single agent invocation."""

    request: AgentRequest
    response: AgentResponse | None
    prompt_version: str
    inference_config: LLMInferenceConfig
    started_at: datetime
    finished_at: datetime | None = None
    response_validation_status: ResponseValidationStatus = (
        ResponseValidationStatus.PENDING
    )
    response_validation_errors: tuple[str, ...] = ()
