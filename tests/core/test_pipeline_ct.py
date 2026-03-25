from datetime import date

import pytest

from neocortex.llm import LLMEndpoint, LLMInferenceConfig, LLMRequestConfig, LLMService
from neocortex.models import (
    AgentRequest,
    AgentResponse,
    AgentRole,
    AgentExecutionTrace,
    Exchange,
    Market,
    ResponseValidationStatus,
    SecurityId,
)
from neocortex.pipeline import Pipeline


_DEPENDENCY_MAP = {
    AgentRole.TECHNICAL: (),
    AgentRole.QUANT_FUNDAMENTAL: (),
    AgentRole.QUALITATIVE_FUNDAMENTAL: (),
    AgentRole.NEWS: (),
    AgentRole.MACRO: (),
    AgentRole.SECTOR: (
        AgentRole.TECHNICAL,
        AgentRole.QUANT_FUNDAMENTAL,
        AgentRole.QUALITATIVE_FUNDAMENTAL,
        AgentRole.NEWS,
    ),
    AgentRole.PM: (AgentRole.MACRO, AgentRole.SECTOR),
}


class RecordingTransport:
    def complete(self, *, agent, system_prompt, user_prompt, inference_config):
        raise AssertionError(
            "Transport should not be called in this pipeline wiring test."
        )


class RecordingAgent:
    role: AgentRole

    def __init__(
        self,
        *,
        market_data=None,
        config,
    ) -> None:
        self.market_data = market_data
        self.config = config
        self.system_prompt = f"{config['template']} system"
        self.user_prompt = f"{config['template']} user"
        self.dependencies = _DEPENDENCY_MAP[
            AgentRole(config["template"].removesuffix(".yaml"))
        ]
        self.calls: list[dict[str, object]] = []

    def run(
        self,
        *,
        request_id,
        security_id,
        as_of_date,
        inference_config,
        transport,
        trace_by_role=None,
    ):
        build_kwargs = {
            "request_id": request_id,
            "security_id": security_id,
            "as_of_date": as_of_date,
        }
        if trace_by_role is not None:
            if self.role is AgentRole.SECTOR:
                build_kwargs["analyst_reports"] = tuple(
                    trace_by_role[dependency].response
                    for dependency in self.dependencies
                )
            if self.role is AgentRole.PM:
                build_kwargs["macro_report"] = trace_by_role[AgentRole.MACRO].response
                build_kwargs["sector_report"] = trace_by_role[AgentRole.SECTOR].response
        self.calls.append(build_kwargs)
        assert transport is not None
        response = AgentResponse(
            request_id=str(build_kwargs["request_id"]),
            agent=self.role,
            security_id=build_kwargs["security_id"],
            as_of_date=build_kwargs["as_of_date"],
            reasoning=f"{self.role.value} reasoning",
            score=60.0,
        )
        request = AgentRequest(
            request_id=str(build_kwargs["request_id"]),
            agent=self.role,
            security_id=build_kwargs["security_id"],
            as_of_date=build_kwargs["as_of_date"],
        )
        return AgentExecutionTrace(
            request=request,
            response=response,
            inference_config=inference_config,
            started_at=date(2026, 3, 13),
            response_validation_status=ResponseValidationStatus.PASSED,
        )


def _recording_agent_class(role: AgentRole) -> type[RecordingAgent]:
    class _RoleRecordingAgent(RecordingAgent):
        def __init__(
            self,
            *,
            market_data=None,
            config,
        ) -> None:
            self.role = role
            super().__init__(
                market_data=market_data,
                config=config,
            )

    return _RoleRecordingAgent


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


def test_pipeline_injects_upstream_reports_into_sector_and_pm(monkeypatch) -> None:
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    created: dict[AgentRole, RecordingAgent] = {}

    def fake_load_pipeline_document(self):
        _ = self
        return {
            role.value: {
                "template": f"{role.value}.yaml",
                **(
                    {"price_series_lookback_days": 123}
                    if role is AgentRole.TECHNICAL
                    else {}
                ),
            }
            for role in AgentRole
        }

    agent_classes = {role: _recording_agent_class(role) for role in AgentRole}

    monkeypatch.setattr(
        "neocortex.pipeline.pipeline.Pipeline._load_pipeline_document",
        fake_load_pipeline_document,
    )
    monkeypatch.setattr("neocortex.pipeline.pipeline._AGENT_CLASSES", agent_classes)
    pipeline = Pipeline(transport=RecordingTransport())
    created.update({role: pipeline.get_agent(role) for role in AgentRole})

    traces = pipeline.run(
        security_id=security_id,
        as_of_date=date(2026, 3, 13),
        request_id="req-pipeline-001",
        inference_config=_inference_config(),
    )

    assert tuple(traces) == (
        AgentRole.MACRO,
        AgentRole.TECHNICAL,
        AgentRole.QUANT_FUNDAMENTAL,
        AgentRole.QUALITATIVE_FUNDAMENTAL,
        AgentRole.NEWS,
        AgentRole.SECTOR,
        AgentRole.PM,
    )
    assert created[AgentRole.TECHNICAL].config["price_series_lookback_days"] == 123
    assert len(created[AgentRole.SECTOR].calls[0]["analyst_reports"]) == 4
    assert created[AgentRole.PM].calls[0]["macro_report"].agent is AgentRole.MACRO
    assert created[AgentRole.PM].calls[0]["sector_report"].agent is AgentRole.SECTOR


def test_pipeline_stops_when_dependency_trace_failed(monkeypatch) -> None:
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)

    def fake_load_pipeline_document(self):
        _ = self
        return {
            role.value: {"template": f"{role.value}.yaml"}
            for role in AgentRole
        }

    class FailingTechnicalAgent(_recording_agent_class(AgentRole.TECHNICAL)):
        def run(
            self,
            *,
            request_id,
            security_id,
            as_of_date,
            inference_config,
            transport,
            trace_by_role=None,
        ):
            request = AgentRequest(
                request_id=str(request_id),
                agent=self.role,
                security_id=security_id,
                as_of_date=as_of_date,
            )
            return AgentExecutionTrace(
                request=request,
                response=None,
                inference_config=inference_config,
                started_at=date(2026, 3, 13),
                response_validation_status=ResponseValidationStatus.FAILED,
                response_validation_errors=("bad output",),
            )

    agent_classes = {role: _recording_agent_class(role) for role in AgentRole}
    agent_classes[AgentRole.TECHNICAL] = FailingTechnicalAgent

    monkeypatch.setattr(
        "neocortex.pipeline.pipeline.Pipeline._load_pipeline_document",
        fake_load_pipeline_document,
    )
    monkeypatch.setattr("neocortex.pipeline.pipeline._AGENT_CLASSES", agent_classes)
    pipeline = Pipeline(transport=RecordingTransport())

    with pytest.raises(
        RuntimeError,
        match="Cannot run agent sector: dependency technical failed.",
    ):
        pipeline.run_agent(
            AgentRole.SECTOR,
            security_id=security_id,
            as_of_date=date(2026, 3, 13),
            request_id="req-pipeline-002",
            inference_config=_inference_config(),
            trace_by_role={},
        )

    assert pipeline.get_agent(AgentRole.SECTOR).calls == []
