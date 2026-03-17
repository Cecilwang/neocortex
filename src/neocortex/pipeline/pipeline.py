"""Recursive multi-agent pipeline execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from importlib import resources

import yaml

from neocortex.agents.base import Agent
from neocortex.agents.macro import MacroAgent
from neocortex.agents.news import NewsAgent
from neocortex.agents.pm import PMAgent
from neocortex.agents.qualitative_fundamental import QualitativeFundamentalAgent
from neocortex.agents.quant_fundamental import QuantFundamentalAgent
from neocortex.agents.sector import SectorAgent
from neocortex.agents.technical import TechnicalAgent
from neocortex.connectors.base import MarketDataConnector
from neocortex.llm import LLMInferenceConfig, LLMTransport
from neocortex.models import AgentExecutionTrace, AgentRole, SecurityId
from neocortex.prompts import load_prompt_template

_AGENT_CLASSES: dict[AgentRole, type[Agent]] = {
    AgentRole.TECHNICAL: TechnicalAgent,
    AgentRole.QUANT_FUNDAMENTAL: QuantFundamentalAgent,
    AgentRole.QUALITATIVE_FUNDAMENTAL: QualitativeFundamentalAgent,
    AgentRole.NEWS: NewsAgent,
    AgentRole.SECTOR: SectorAgent,
    AgentRole.MACRO: MacroAgent,
    AgentRole.PM: PMAgent,
}


@dataclass(slots=True)
class Pipeline:
    """Recursive multi-agent pipeline built from one packaged YAML config."""

    transport: LLMTransport | None = None
    pipeline_config_name: str = "pipeline.yaml"
    market_data: MarketDataConnector | None = None
    agents: dict[AgentRole, Agent] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.agents = self._load_agents()

    def get_agent(self, role: AgentRole) -> Agent:
        """Return one already-constructed agent from the pipeline."""

        return self.agents[role]

    def run(
        self,
        *,
        security_id: SecurityId,
        as_of_date: date,
        request_id: str,
        inference_config: LLMInferenceConfig,
    ) -> dict[AgentRole, AgentExecutionTrace]:
        if self.transport is None:
            raise RuntimeError("Pipeline requires a transport to run agents.")

        trace_by_role: dict[AgentRole, AgentExecutionTrace] = {}
        self.run_agent(
            AgentRole.PM,
            security_id=security_id,
            as_of_date=as_of_date,
            request_id=request_id,
            inference_config=inference_config,
            trace_by_role=trace_by_role,
        )
        return trace_by_role

    def run_agent(
        self,
        role: AgentRole,
        *,
        security_id: SecurityId,
        as_of_date: date,
        request_id: str,
        inference_config: LLMInferenceConfig,
        trace_by_role: dict[AgentRole, AgentExecutionTrace],
    ) -> AgentExecutionTrace:
        if self.transport is None:
            raise RuntimeError("Pipeline requires a transport to run agents.")
        if role in trace_by_role:
            return trace_by_role[role]

        agent = self.get_agent(role)
        for dependency in agent.dependencies:
            self.run_agent(
                dependency,
                security_id=security_id,
                as_of_date=as_of_date,
                request_id=request_id,
                inference_config=inference_config,
                trace_by_role=trace_by_role,
            )

        trace = agent.run(
            request_id=f"{request_id}-{role.value}",
            security_id=security_id,
            as_of_date=as_of_date,
            inference_config=inference_config,
            transport=self.transport,
            trace_by_role=trace_by_role,
        )
        trace_by_role[role] = trace
        return trace

    def _load_agents(self) -> dict[AgentRole, Agent]:
        document = self._load_pipeline_document()
        return {
            role: self._build_agent(role, template_name=document[role.value])
            for role in AgentRole
        }

    def _load_pipeline_document(self) -> dict[str, str]:
        source = (
            resources.files("neocortex.agents")
            .joinpath(self.pipeline_config_name)
            .read_text(encoding="utf-8")
        )
        document = yaml.safe_load(source)
        if not isinstance(document, dict):
            raise ValueError("Pipeline config must be one YAML mapping.")
        for role in AgentRole:
            template_name = document.get(role.value)
            if not isinstance(template_name, str):
                raise ValueError(f"Missing template name for {role.value}.")
        return document

    def _build_agent(self, role: AgentRole, *, template_name: str) -> Agent:
        template = load_prompt_template(template_name)
        agent_class = _AGENT_CLASSES[role]
        return agent_class(
            system_prompt=template.system,
            user_prompt=template.user,
            dependencies=template.dependencies,
            market_data=self.market_data,
        )
