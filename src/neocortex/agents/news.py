"""News agent implementation."""

from __future__ import annotations

import logging
from datetime import date
from collections.abc import Sequence
from typing import Callable, Mapping

from neocortex.agents.base import Agent
from neocortex.market_data_provider import MarketDataProvider
from neocortex.models import (
    AgentExecutionTrace,
    AgentRequest,
    AgentResponse,
    AgentRole,
    CompanyProfile,
    NewsItem,
    SecurityId,
)
from neocortex.serialization import to_json_ready

logger = logging.getLogger(__name__)


class NewsAgent(Agent):
    role = AgentRole.NEWS

    def __init__(
        self,
        *,
        market_data: MarketDataProvider | None = None,
        config: Mapping[str, object],
        news_items_loader: Callable[[SecurityId, date], Sequence[NewsItem]]
        | None = None,
    ) -> None:
        super().__init__(
            market_data=market_data,
            config=config,
        )
        self.news_items_loader = news_items_loader

    def build_request(
        self,
        *,
        request_id: str,
        security_id: SecurityId,
        as_of_date: date,
        company_profile: CompanyProfile | None = None,
        news_items: Sequence[NewsItem] | None = None,
        trace_by_role: Mapping[AgentRole, AgentExecutionTrace] | None = None,
    ) -> AgentRequest:
        logger.info(
            f"Building news request: security={security_id.ticker} as_of_date={as_of_date}"
        )
        _ = trace_by_role
        if company_profile is None:
            company_profile = self.market_data.get_company_profile(security_id)
        if news_items is None:
            if self.news_items_loader is None:
                raise RuntimeError("NewsAgent requires a news_items_loader.")
            news_items = self.news_items_loader(security_id, as_of_date)
        if company_profile.security_id != security_id:
            raise ValueError(
                "company_profile.security_id must match the request security_id."
            )
        logger.info(
            f"News request inputs ready: security={security_id.ticker} "
            f"news_items={len(news_items)}"
        )
        payload = {
            "news_items": to_json_ready(news_items),
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
            reasoning=str(parsed_output["reason"]),
            score=None,
            raw_model_output=parsed_output,
        )
