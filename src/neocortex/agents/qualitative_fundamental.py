"""Qualitative strategic agent implementation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import logging
from typing import Callable, Mapping

from neocortex.agents.base import Agent
from neocortex.market_data_provider import MarketDataProvider
from neocortex.models import (
    AgentExecutionTrace,
    AgentRequest,
    AgentResponse,
    AgentRole,
    CompanyProfile,
    SecurityId,
)
from neocortex.serialization import to_json_ready

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class QualitativeFundamentalInput:
    """Explicit appendix-shaped input for the qualitative template."""

    info_update: str = "n/a"
    overview: str = "n/a"
    risks: str = "n/a"
    mda: str = "n/a"
    governance: str = "n/a"


class QualitativeFundamentalAgent(Agent):
    role = AgentRole.QUALITATIVE_FUNDAMENTAL

    def __init__(
        self,
        *,
        market_data: MarketDataProvider | None = None,
        config: Mapping[str, object],
        disclosures_loader: Callable[[SecurityId, date], QualitativeFundamentalInput]
        | None = None,
    ) -> None:
        super().__init__(
            market_data=market_data,
            config=config,
        )
        self.disclosures_loader = disclosures_loader

    def build_request(
        self,
        *,
        request_id: str,
        security_id: SecurityId,
        as_of_date: date,
        company_profile: CompanyProfile | None = None,
        disclosures: QualitativeFundamentalInput | None = None,
        trace_by_role: Mapping[AgentRole, AgentExecutionTrace] | None = None,
    ) -> AgentRequest:
        logger.info(
            "Building qualitative fundamental request: "
            f"security={security_id.ticker} as_of_date={as_of_date}"
        )
        _ = trace_by_role
        if company_profile is None:
            company_profile = self.market_data.get_company_profile(security_id)
        if disclosures is None:
            if self.disclosures_loader is None:
                raise RuntimeError(
                    "QualitativeFundamentalAgent requires a disclosures_loader."
                )
            disclosures = self.disclosures_loader(security_id, as_of_date)
        if company_profile.security_id != security_id:
            raise ValueError(
                "company_profile.security_id must match the request security_id."
            )
        payload = to_json_ready(disclosures)
        logger.info(
            f"Qualitative fundamental inputs ready: security={security_id.ticker} "
            f"sections={len(payload)}"
        )
        return AgentRequest(
            request_id=request_id,
            agent=self.role,
            security_id=security_id,
            as_of_date=as_of_date,
            payload=payload,
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
            reasoning=str(parsed_output["insight"]),
            score=None,
            raw_model_output=parsed_output,
        )
