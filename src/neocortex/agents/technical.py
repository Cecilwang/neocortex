"""Technical agent implementation backed by the appendix fine-grained template."""

from __future__ import annotations

from datetime import timedelta
import logging
from typing import Mapping

from neocortex.agents.base import Agent
from neocortex.indicators import (
    calculate_indicator,
    get_indicator_spec,
    list_indicator_specs,
)
from neocortex.market_data_provider import MarketDataProvider
from neocortex.models import (
    AgentRequest,
    AgentRole,
    PriceSeries,
)

_PRICE_SERIES_LOOKBACK_DAYS = 400
logger = logging.getLogger(__name__)


class TechnicalAgent(Agent):
    role = AgentRole.TECHNICAL

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
        self.price_series_lookback_days = _coerce_lookback_days(self.config)

    def build_render_context(self, request: AgentRequest) -> dict[str, object]:
        logger.info(
            "Building technical render context: security=%s as_of_date=%s lookback_days=%s",
            request.security_id.ticker,
            request.as_of_date,
            self.price_series_lookback_days,
        )
        price_series = self.market_data.get_price_bars(
            request.security_id,
            start_date=request.as_of_date
            - timedelta(days=self.price_series_lookback_days),
            end_date=request.as_of_date,
        )
        if price_series.security_id != request.security_id:
            raise ValueError(
                "price_series.security_id must match the request security_id."
            )
        if not price_series:
            raise ValueError("TechnicalAgent requires at least one price bar.")
        context = {
            "market": request.security_id.market,
        }
        indicator_namespace = _IndicatorTemplateNamespace(price_series)
        for indicator in list_indicator_specs():
            context[indicator.key] = getattr(indicator_namespace, indicator.key)
        logger.info(
            "Technical render context ready: security=%s indicators=%s bars=%s",
            request.security_id.ticker,
            len(list_indicator_specs()),
            len(price_series.bars),
        )
        return context


class _IndicatorTemplateNamespace:
    def __init__(self, price_series: PriceSeries) -> None:
        self.price_series = price_series

    def __getattr__(self, key: str) -> object:
        get_indicator_spec(key)

        def call(**parameters: object) -> object:
            return calculate_indicator(
                key,
                self.price_series,
                parameters=parameters or None,
            )

        return call


def _coerce_lookback_days(config: Mapping[str, object]) -> int:
    raw_value = config.get("price_series_lookback_days", _PRICE_SERIES_LOOKBACK_DAYS)
    if not isinstance(raw_value, int) or raw_value <= 0:
        raise ValueError("price_series_lookback_days must be a positive integer.")
    return raw_value
