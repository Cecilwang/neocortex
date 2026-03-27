"""Portfolio manager agent implementation."""

from __future__ import annotations

from datetime import date
import logging
from typing import Mapping

from neocortex.agents.base import Agent
from neocortex.market_data_provider import MarketDataProvider
from neocortex.models import (
    AgentExecutionTrace,
    AgentRequest,
    AgentResponse,
    AgentRole,
    SecurityId,
)
from neocortex.serialization import to_pretty_json

logger = logging.getLogger(__name__)


class PMAgent(Agent):
    role = AgentRole.PM

    def __init__(
        self,
        *,
        market_data: MarketDataProvider,
        config: Mapping[str, object],
    ) -> None:
        super().__init__(
            market_data=market_data,
            config=config,
        )

    def build_request(
        self,
        *,
        request_id: str,
        security_id: SecurityId,
        as_of_date: date,
        macro_report: AgentResponse | None = None,
        sector_report: AgentResponse | None = None,
        trace_by_role: Mapping[AgentRole, AgentExecutionTrace] | None = None,
    ) -> AgentRequest:
        logger.info(
            f"Building PM request: security={security_id.ticker} as_of_date={as_of_date}"
        )
        if macro_report is None:
            if trace_by_role is None:
                raise RuntimeError("PMAgent requires macro_report or trace_by_role.")
            macro_report = _require_trace_response(trace_by_role, AgentRole.MACRO)
        if sector_report is None:
            if trace_by_role is None:
                raise RuntimeError("PMAgent requires sector_report or trace_by_role.")
            sector_report = _require_trace_response(trace_by_role, AgentRole.SECTOR)
        logger.info(
            f"PM request dependencies ready: security={security_id.ticker} "
            f"macro={macro_report.agent.value} sector={sector_report.agent.value}"
        )
        payload = {
            "macro_report_json": to_pretty_json(
                macro_report.raw_model_output
                or {"score": macro_report.score, "reason": macro_report.reasoning}
            ),
            "sector_report_json": to_pretty_json(
                sector_report.raw_model_output
                or {"score": sector_report.score, "reason": sector_report.reasoning}
            ),
        }
        return AgentRequest(
            request_id=request_id,
            agent=self.role,
            security_id=security_id,
            as_of_date=as_of_date,
            payload=payload,
            dependencies=self.dependencies,
        )

    def build_response(
        self,
        request: AgentRequest,
        parsed_output: dict[str, object],
    ) -> AgentResponse:
        return AgentResponse(
            request_id=request.request_id,
            agent=request.agent,
            security_id=request.security_id,
            as_of_date=request.as_of_date,
            reasoning=str(parsed_output["reason"]),
            score=float(parsed_output["final_score"]),
            raw_model_output=parsed_output,
        )


def _require_trace_response(
    trace_by_role: Mapping[AgentRole, AgentExecutionTrace],
    role: AgentRole,
) -> AgentResponse:
    trace = trace_by_role.get(role)
    if trace is None:
        raise RuntimeError(f"Missing trace for dependency {role.value}.")
    if trace.response is None:
        raise RuntimeError(f"{role.value} did not produce a response.")
    return trace.response
