"""Shared agent abstractions and reusable prompt input DTOs."""

from __future__ import annotations

from abc import ABC
from datetime import UTC, datetime
from datetime import date
import logging
from typing import Any, Mapping

from neocortex.llm import LLMInferenceConfig, LLMTransport
from neocortex.market_data_provider import MarketDataProvider
from neocortex.models import (
    AgentExecutionTrace,
    AgentRequest,
    AgentResponse,
    AgentRole,
    ResponseValidationStatus,
    SecurityId,
)
from neocortex.prompts import load_prompt_template, render_prompt_text
from neocortex.serialization import parse_json_object

logger = logging.getLogger(__name__)


class Agent(ABC):
    """Common behavior contract shared by all agent implementations."""

    role: AgentRole
    system_prompt: str
    user_prompt: str
    dependencies: tuple[AgentRole, ...]
    market_data: MarketDataProvider
    config: dict[str, object]

    def __init__(
        self,
        *,
        market_data: MarketDataProvider,
        config: Mapping[str, object],
    ) -> None:
        self.market_data = market_data
        self.config = dict(config)
        template_name = self.config.get("template")
        if not isinstance(template_name, str):
            raise ValueError("Agent config must contain a string 'template' field.")
        template = load_prompt_template(template_name)
        self.system_prompt = template.system
        self.user_prompt = template.user
        self.dependencies = template.dependencies
        logger.debug(
            "Initialized agent role=%s template=%s dependencies=%s",
            getattr(self, "role", "unknown"),
            template_name,
            [dependency.value for dependency in self.dependencies],
        )

    def build_request(
        self,
        *,
        request_id: str,
        security_id: SecurityId,
        as_of_date: date,
        trace_by_role: Mapping[AgentRole, AgentExecutionTrace] | None = None,
    ) -> AgentRequest:
        """Build the normalized request payload for one agent invocation."""

        _ = trace_by_role
        return AgentRequest(
            request_id=request_id,
            agent=self.role,
            security_id=security_id,
            as_of_date=as_of_date,
        )

    def build_response(
        self,
        request: AgentRequest,
        parsed_output: dict[str, Any],
    ) -> AgentResponse:
        """Build one normalized agent response from parsed model output."""

        return AgentResponse(
            request_id=request.request_id,
            agent=request.agent,
            security_id=request.security_id,
            as_of_date=request.as_of_date,
            reasoning=str(parsed_output["reason"]),
            score=float(parsed_output["score"]),
            raw_model_output=parsed_output,
        )

    def get_system_prompt(self, request: AgentRequest) -> str:
        """Render the system prompt for one request."""

        if request.agent is not self.role:
            raise ValueError(
                f"{self.__class__.__name__} requires a {self.role} request."
            )
        return render_prompt_text(
            self.system_prompt,
            **self.build_render_context(request),
        )

    def get_user_prompt(self, request: AgentRequest) -> str:
        """Render the user prompt for one request."""

        if request.agent is not self.role:
            raise ValueError(
                f"{self.__class__.__name__} requires a {self.role} request."
            )
        return render_prompt_text(
            self.user_prompt,
            **self.build_render_context(request),
        )

    def build_render_context(self, request: AgentRequest) -> dict[str, object]:
        """Build one template context from the stored request payload."""

        return dict(request.payload)

    def send(
        self,
        request: AgentRequest,
        inference_config: LLMInferenceConfig,
        *,
        transport: LLMTransport,
    ) -> AgentResponse:
        """Render the prompt, send it through the configured transport, and parse JSON."""

        logger.info(
            "Sending agent request: role=%s request_id=%s security=%s",
            self.role.value,
            request.request_id,
            request.security_id.ticker,
        )
        raw_output = transport.complete(
            agent=self.role,
            system_prompt=self.get_system_prompt(request),
            user_prompt=self.get_user_prompt(request),
            inference_config=inference_config,
        )
        parsed_output = parse_json_object(raw_output)
        logger.info(
            "Received agent response: role=%s request_id=%s score=%s",
            self.role.value,
            request.request_id,
            parsed_output.get("score"),
        )
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

        logger.info(
            "Agent run started: role=%s request_id=%s security=%s as_of_date=%s",
            self.role.value,
            request_id,
            security_id.ticker,
            as_of_date,
        )
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
            logger.exception(
                "Agent run failed: role=%s request_id=%s",
                self.role.value,
                request_id,
            )
            return AgentExecutionTrace(
                request=request,
                response=None,
                inference_config=inference_config,
                started_at=started_at,
                finished_at=datetime.now(UTC),
                response_validation_status=ResponseValidationStatus.FAILED,
                response_validation_errors=(str(exc),),
            )
        logger.info(
            "Agent run finished: role=%s request_id=%s",
            self.role.value,
            request_id,
        )
        return AgentExecutionTrace(
            request=request,
            response=response,
            inference_config=inference_config,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            response_validation_status=ResponseValidationStatus.PASSED,
        )
