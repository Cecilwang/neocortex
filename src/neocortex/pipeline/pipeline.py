"""Recursive multi-agent pipeline execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import logging

from neocortex.agents.base import Agent
from neocortex.agents.macro import MacroAgent
from neocortex.agents.news import NewsAgent
from neocortex.agents.pm import PMAgent
from neocortex.agents.qualitative_fundamental import QualitativeFundamentalAgent
from neocortex.agents.quant_fundamental import QuantFundamentalAgent
from neocortex.agents.sector import SectorAgent
from neocortex.agents.technical import TechnicalAgent
from neocortex.config import get_config
from neocortex.llm import LLMInferenceConfig
from neocortex.llm.transport import LLMTransport
from neocortex.market_data_provider import MarketDataProvider
from neocortex.models import AgentExecutionTrace, AgentRole, SecurityId

_AGENT_CLASSES: dict[AgentRole, type[Agent]] = {
    AgentRole.TECHNICAL: TechnicalAgent,
    AgentRole.QUANT_FUNDAMENTAL: QuantFundamentalAgent,
    AgentRole.QUALITATIVE_FUNDAMENTAL: QualitativeFundamentalAgent,
    AgentRole.NEWS: NewsAgent,
    AgentRole.SECTOR: SectorAgent,
    AgentRole.MACRO: MacroAgent,
    AgentRole.PM: PMAgent,
}

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class Pipeline:
    """Recursive multi-agent pipeline built from one packaged YAML config."""

    transport: LLMTransport | None = None
    market_data: MarketDataProvider | None = None
    agents: dict[AgentRole, Agent] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.agents = self._load_agents()
        logger.info(f"Pipeline initialized with {len(self.agents)} agents.")

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

        logger.info(
            f"Pipeline run started: security={security_id.ticker} "
            f"as_of_date={as_of_date} request_id={request_id}"
        )
        trace_by_role: dict[AgentRole, AgentExecutionTrace] = {}
        self.run_agent(
            AgentRole.PM,
            security_id=security_id,
            as_of_date=as_of_date,
            request_id=request_id,
            inference_config=inference_config,
            trace_by_role=trace_by_role,
        )
        logger.info(
            f"Pipeline run finished: security={security_id.ticker} "
            f"request_id={request_id} traces={len(trace_by_role)}"
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
            logger.debug(f"Pipeline cache hit for agent role={role.value}.")
            return trace_by_role[role]

        agent = self.get_agent(role)
        logger.info(
            f"Pipeline running agent role={role.value} "
            f"dependencies={[dependency.value for dependency in agent.dependencies]}"
        )
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
        logger.info(
            f"Pipeline completed agent role={role.value} "
            f"status={trace.response_validation_status.value}"
        )
        return trace

    def _load_agents(self) -> dict[AgentRole, Agent]:
        document = self._load_pipeline_document()
        agents = {
            role: self._build_agent(role, config=document[role.value])
            for role in AgentRole
        }
        logger.debug(
            f"Loaded pipeline agent configs for roles={[r.value for r in agents]}"
        )
        return agents

    def _load_pipeline_document(self) -> dict[str, dict[str, object]]:
        document = get_config().pipeline.agents
        if not isinstance(document, dict):
            raise ValueError("Pipeline config must be one YAML mapping.")
        for role in AgentRole:
            config = document.get(role.value)
            if not isinstance(config, dict):
                raise ValueError(f"Missing config mapping for {role.value}.")
        return document

    def _build_agent(self, role: AgentRole, *, config: dict[str, object]) -> Agent:
        agent_class = _AGENT_CLASSES[role]
        return agent_class(
            market_data=self.market_data,
            config=config,
        )
