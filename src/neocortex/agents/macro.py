"""Macro agent implementation."""

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
    SecurityId,
)
from neocortex.serialization import to_json_ready

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MacroMetric:
    """One explicit macro metric cell in the appendix template."""

    value: str = "n/a"
    roc: str = "n/a"


@dataclass(frozen=True, slots=True)
class MacroInput:
    """Explicit appendix-shaped input for the macro template."""

    us_fed_rate: MacroMetric = MacroMetric()
    us_10y_yield: MacroMetric = MacroMetric()
    jp_policy_rate: MacroMetric = MacroMetric()
    jp_10y_yield: MacroMetric = MacroMetric()
    cn_policy_rate: MacroMetric = MacroMetric()
    cn_10y_yield: MacroMetric = MacroMetric()
    hk_base_rate: MacroMetric = MacroMetric()
    hk_10y_yield: MacroMetric = MacroMetric()

    us_cpi: MacroMetric = MacroMetric()
    jp_cpi: MacroMetric = MacroMetric()
    cn_cpi: MacroMetric = MacroMetric()
    hk_cpi: MacroMetric = MacroMetric()
    gold: MacroMetric = MacroMetric()
    crude_oil: MacroMetric = MacroMetric()

    us_payrolls: MacroMetric = MacroMetric()
    ind_prod: MacroMetric = MacroMetric()
    housing_starts: MacroMetric = MacroMetric()
    unemp_rate: MacroMetric = MacroMetric()
    jp_business_index: MacroMetric = MacroMetric()
    cn_pmi: MacroMetric = MacroMetric()
    hk_retail_sales: MacroMetric = MacroMetric()
    hk_unemp_rate: MacroMetric = MacroMetric()

    usd_jpy: MacroMetric = MacroMetric()
    usd_cnh: MacroMetric = MacroMetric()
    usd_hkd: MacroMetric = MacroMetric()
    nikkei_225: MacroMetric = MacroMetric()
    sp_500: MacroMetric = MacroMetric()
    csi_300: MacroMetric = MacroMetric()
    hang_seng_index: MacroMetric = MacroMetric()
    us_vix: MacroMetric = MacroMetric()
    nikkei_vi: MacroMetric = MacroMetric()
    vhsi: MacroMetric = MacroMetric()


class MacroAgent(Agent):
    role = AgentRole.MACRO

    def __init__(
        self,
        *,
        market_data: MarketDataProvider,
        config: Mapping[str, object],
        macro_data_loader: Callable[[SecurityId, date], MacroInput] | None = None,
    ) -> None:
        super().__init__(
            market_data=market_data,
            config=config,
        )
        self.macro_data_loader = macro_data_loader

    def build_request(
        self,
        *,
        request_id: str,
        security_id: SecurityId,
        as_of_date: date,
        macro_data: MacroInput | None = None,
        trace_by_role: Mapping[AgentRole, AgentExecutionTrace] | None = None,
    ) -> AgentRequest:
        logger.info(
            f"Building macro request: security={security_id.ticker} as_of_date={as_of_date}"
        )
        _ = trace_by_role
        if macro_data is None:
            if self.macro_data_loader is None:
                raise RuntimeError("MacroAgent requires a macro_data_loader.")
            macro_data = self.macro_data_loader(security_id, as_of_date)
        payload = {
            "market": security_id.market,
            **to_json_ready(macro_data),
        }
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
            reasoning=str(parsed_output["summary"]),
            score=None,
            raw_model_output=parsed_output,
        )
