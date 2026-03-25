"""Shared agent abstractions and reusable prompt input DTOs."""

from __future__ import annotations

from abc import ABC
from datetime import UTC, datetime
from datetime import date
import logging
from typing import Any, Mapping

from neocortex.llm import LLMInferenceConfig
from neocortex.llm.transport import LLMTransport
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
            f"Initialized agent role={getattr(self, 'role', 'unknown')} "
            f"template={template_name} "
            f"dependencies={[dependency.value for dependency in self.dependencies]}"
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

    def render_prompts(self, request: AgentRequest) -> tuple[str, str]:
        """Render both prompts while building the template context only once."""

        if request.agent is not self.role:
            raise ValueError(
                f"{self.__class__.__name__} requires a {self.role} request."
            )
        render_context = self.build_render_context(request)
        return (
            render_prompt_text(self.system_prompt, **render_context),
            render_prompt_text(self.user_prompt, **render_context),
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
            f"Sending agent request: role={self.role.value} "
            f"request_id={request.request_id} security={request.security_id.ticker}"
        )
        system_prompt, user_prompt = self.render_prompts(request)
        raw_output = transport.complete(
            agent=self.role,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            inference_config=inference_config,
        )
        parsed_output = parse_json_object(raw_output)
        logger.info(
            f"Received agent response: role={self.role.value} "
            f"request_id={request.request_id} score={parsed_output.get('score')}"
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
            f"Agent run started: role={self.role.value} request_id={request_id} "
            f"security={security_id.ticker} as_of_date={as_of_date}"
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
                f"Agent run failed: role={self.role.value} request_id={request_id}"
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
            f"Agent run finished: role={self.role.value} request_id={request_id}"
        )
        return AgentExecutionTrace(
            request=request,
            response=response,
            inference_config=inference_config,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            response_validation_status=ResponseValidationStatus.PASSED,
        )
