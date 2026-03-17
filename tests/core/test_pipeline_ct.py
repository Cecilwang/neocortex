from datetime import date
from types import SimpleNamespace

from neocortex.llm import LLMEndpoint, LLMInferenceConfig, LLMRequestConfig, LLMService
from neocortex.models import AgentRequest, AgentResponse, AgentRole, AgentExecutionTrace, Exchange, Market, SecurityId
from neocortex.pipeline import Pipeline


class RecordingTransport:
    def complete(self, *, agent, system_prompt, user_prompt, inference_config):
        raise AssertionError("Transport should not be called in this pipeline wiring test.")


class RecordingAgent:
    role: AgentRole

    def __init__(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        dependencies: tuple[AgentRole, ...] = (),
        market_data=None,
    ) -> None:
        self.system_prompt = system_prompt
        self.user_prompt = user_prompt
        self.dependencies = dependencies
        self.market_data = market_data
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
                    trace_by_role[dependency].response for dependency in self.dependencies
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
            confidence=None,
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
        )


def _recording_agent_class(role: AgentRole) -> type[RecordingAgent]:
    class _RoleRecordingAgent(RecordingAgent):
        def __init__(
            self,
            *,
            system_prompt: str,
            user_prompt: str,
            dependencies: tuple[AgentRole, ...] = (),
            market_data=None,
        ) -> None:
            self.role = role
            super().__init__(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                dependencies=dependencies,
                market_data=market_data,
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

    dependency_map = {
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

    def fake_load_pipeline_document(self):
        _ = self
        return {role.value: f"{role.value}.yaml" for role in AgentRole}

    def fake_load_prompt_template(template_name: str):
        role = AgentRole(template_name.removesuffix(".yaml"))
        return SimpleNamespace(
            system=f"{role.value} system",
            user=f"{role.value} user",
            dependencies=dependency_map[role],
        )

    agent_classes = {
        role: _recording_agent_class(role)
        for role in AgentRole
    }

    monkeypatch.setattr(
        "neocortex.pipeline.pipeline.Pipeline._load_pipeline_document",
        fake_load_pipeline_document,
    )
    monkeypatch.setattr(
        "neocortex.pipeline.pipeline.load_prompt_template",
        fake_load_prompt_template,
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
    assert len(created[AgentRole.SECTOR].calls[0]["analyst_reports"]) == 4
    assert created[AgentRole.PM].calls[0]["macro_report"].agent is AgentRole.MACRO
    assert created[AgentRole.PM].calls[0]["sector_report"].agent is AgentRole.SECTOR
