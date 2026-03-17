"""Shared agent abstractions and reusable prompt input DTOs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime
from datetime import date
from typing import Any, Mapping

from neocortex.connectors.base import MarketDataConnector
from neocortex.llm import LLMInferenceConfig, LLMTransport
from neocortex.models import (
    AgentExecutionTrace,
    AgentRequest,
    AgentResponse,
    AgentRole,
    ResponseValidationStatus,
    SecurityId,
)
from neocortex.prompts import render_prompt_text
from neocortex.serialization import parse_json_object


class Agent(ABC):
    """Common behavior contract shared by all agent implementations."""

    role: AgentRole
    system_prompt: str
    user_prompt: str
    dependencies: tuple[AgentRole, ...]
    market_data: MarketDataConnector

    def __init__(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        dependencies: tuple[AgentRole, ...],
        market_data: MarketDataConnector,
    ) -> None:
        self.system_prompt = system_prompt
        self.user_prompt = user_prompt
        self.dependencies = dependencies
        self.market_data = market_data

    @abstractmethod
    def build_request(
        self,
        *,
        request_id: str,
        security_id: SecurityId,
        as_of_date: date,
        trace_by_role: Mapping[AgentRole, AgentExecutionTrace] | None = None,
    ) -> AgentRequest:
        """Build the normalized request payload for one agent invocation."""

    @abstractmethod
    def build_response(
        self,
        request: AgentRequest,
        parsed_output: dict[str, Any],
    ) -> AgentResponse:
        """Build one normalized agent response from parsed model output."""

    def get_system_prompt(self, request: AgentRequest) -> str:
        """Render the system prompt for one request."""

        if request.agent is not self.role:
            raise ValueError(f"{self.__class__.__name__} requires a {self.role} request.")
        return render_prompt_text(self.system_prompt, **request.payload)

    def get_user_prompt(self, request: AgentRequest) -> str:
        """Render the user prompt for one request."""

        if request.agent is not self.role:
            raise ValueError(f"{self.__class__.__name__} requires a {self.role} request.")
        return render_prompt_text(self.user_prompt, **request.payload)

    def send(
        self,
        request: AgentRequest,
        inference_config: LLMInferenceConfig,
        *,
        transport: LLMTransport,
    ) -> AgentResponse:
        """Render the prompt, send it through the configured transport, and parse JSON."""

        raw_output = transport.complete(
            agent=self.role,
            system_prompt=self.get_system_prompt(request),
            user_prompt=self.get_user_prompt(request),
            inference_config=inference_config,
        )
        parsed_output = parse_json_object(raw_output)
        return self.build_response(request, parsed_output)

    def run(
        self,
        *,
        request_id: str,
        security_id: SecurityId,
        as_of_date: date,
        inference_config: LLMInferenceConfig,
        transport: LLMTransport,
        trace_by_role: Mapping[AgentRole, AgentExecutionTrace] | None = None,
    ) -> AgentExecutionTrace:
        """Build, render, send, and wrap one agent invocation into a trace."""

        request = self.build_request(
            request_id=request_id,
            security_id=security_id,
            as_of_date=as_of_date,
            trace_by_role=trace_by_role,
        )
        started_at = datetime.now(UTC)
        try:
            response = self.send(request, inference_config, transport=transport)
        except Exception as exc:
            return AgentExecutionTrace(
                request=request,
                response=None,
                inference_config=inference_config,
                started_at=started_at,
                finished_at=datetime.now(UTC),
                response_validation_status=ResponseValidationStatus.FAILED,
                response_validation_errors=(str(exc),),
            )
        return AgentExecutionTrace(
            request=request,
            response=response,
            inference_config=inference_config,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            response_validation_status=ResponseValidationStatus.PASSED,
        )
