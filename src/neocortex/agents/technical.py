"""Technical agent implementation backed by the appendix fine-grained template."""

from __future__ import annotations

from datetime import timedelta
from typing import Mapping

from neocortex.agents.base import Agent
from neocortex.connectors.base import MarketDataConnector
from neocortex.indicators import calculate_indicator, get_indicator_spec, list_indicator_specs
from neocortex.models import (
    AgentRequest,
    AgentRole,
    PriceSeries,
)

_PRICE_SERIES_LOOKBACK_DAYS = 400


class TechnicalAgent(Agent):
    role = AgentRole.TECHNICAL

    def __init__(
        self,
        *,
        market_data: MarketDataConnector,
        config: Mapping[str, object],
    ) -> None:
        super().__init__(
            market_data=market_data,
            config=config,
        )
        self.price_series_lookback_days = _coerce_lookback_days(self.config)

    def build_render_context(self, request: AgentRequest) -> dict[str, object]:
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
