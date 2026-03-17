from datetime import date, datetime

import pytest

from neocortex.llm import LLMEndpoint, LLMInferenceConfig, LLMRequestConfig, LLMService
from neocortex.models.agent import (
    AgentExecutionTrace,
    AgentRequest,
    AgentResponse,
    AgentRole,
    ResponseValidationStatus,
)
from neocortex.models.core import Exchange, Market, SecurityId


@pytest.fixture
def security_id() -> SecurityId:
    return SecurityId(symbol="AAPL", market=Market.US, exchange=Exchange.XNAS)


def test_agent_trace_captures_request_and_response_contract(
    security_id: SecurityId,
) -> None:
    request = AgentRequest(
        request_id="req-20260315-001",
        agent=AgentRole.TECHNICAL,
        security_id=security_id,
        as_of_date=date(2026, 3, 15),
        payload={"roc_20d": 0.12},
    )
    response = AgentResponse(
        request_id="req-20260315-001",
        agent=AgentRole.TECHNICAL,
        security_id=security_id,
        as_of_date=date(2026, 3, 15),
        reasoning="Momentum remains constructive.",
        score=74.0,
        confidence=0.81,
    )
    trace = AgentExecutionTrace(
        request=request,
        response=response,
        inference_config=LLMInferenceConfig(
            endpoint=LLMEndpoint(
                service=LLMService.OPENAI,
                model="gpt-test",
                base_url="https://api.openai.com/v1",
                auth_env_var="OPENAI_API_KEY",
            ),
            request=LLMRequestConfig(
                temperature=0.2,
                max_tokens=800,
            ),
        ),
        started_at=datetime(2026, 3, 15, 10, 0),
        response_validation_status=ResponseValidationStatus.PASSED,
    )

    assert trace.request.agent is AgentRole.TECHNICAL
    assert trace.request.request_id == "req-20260315-001"
    assert trace.request.security_id.market is Market.US
    assert trace.response is not None
    assert trace.response.request_id == "req-20260315-001"
    assert trace.inference_config.endpoint.model == "gpt-test"
    assert trace.inference_config.request.temperature == 0.2
    assert trace.response.security_id.exchange is Exchange.XNAS
    assert trace.response.reasoning == "Momentum remains constructive."
    assert trace.response_validation_status is ResponseValidationStatus.PASSED
