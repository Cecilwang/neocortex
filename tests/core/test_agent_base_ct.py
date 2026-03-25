from datetime import date

import pytest

from neocortex.agents.base import Agent
from neocortex.llm import LLMEndpoint, LLMInferenceConfig, LLMRequestConfig, LLMService
from neocortex.models import (
    AgentRole,
    Exchange,
    Market,
    SecurityId,
    ResponseValidationStatus,
)


class _DummyTransport:
    def __init__(self, raw_output=None, error: Exception | None = None) -> None:
        self.raw_output = raw_output
        self.error = error

    def complete(self, *, agent, system_prompt, user_prompt, inference_config):
        _ = agent, system_prompt, user_prompt, inference_config
        if self.error is not None:
            raise self.error
        return self.raw_output


class _DummyAgent(Agent):
    role = AgentRole.TECHNICAL

    def __init__(self) -> None:
        self.market_data = None
        self.config = {}
        self.system_prompt = "system"
        self.user_prompt = "user"
        self.dependencies = ()


def _security_id() -> SecurityId:
    return SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)


def _inference_config() -> LLMInferenceConfig:
    return LLMInferenceConfig(
        endpoint=LLMEndpoint(
            service=LLMService.OPENAI,
            model="gpt-test",
            base_url="https://api.openai.com/v1",
            auth_env_var="OPENAI_API_KEY",
        ),
        request=LLMRequestConfig(),
    )


def test_agent_run_returns_failed_trace_for_invalid_model_output() -> None:
    agent = _DummyAgent()

    trace = agent.run(
        request_id="req-1",
        security_id=_security_id(),
        as_of_date=date(2026, 3, 25),
        inference_config=_inference_config(),
        transport=_DummyTransport(raw_output='{"reason": "ok"}'),
    )

    assert trace.response is None
    assert trace.response_validation_status is ResponseValidationStatus.FAILED
    assert trace.response_validation_errors


def test_agent_run_propagates_transport_errors() -> None:
    agent = _DummyAgent()

    with pytest.raises(RuntimeError, match="transport down"):
        agent.run(
            request_id="req-2",
            security_id=_security_id(),
            as_of_date=date(2026, 3, 25),
            inference_config=_inference_config(),
            transport=_DummyTransport(error=RuntimeError("transport down")),
        )
