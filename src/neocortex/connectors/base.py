"""Connector interface definitions for normalized market data access."""

from __future__ import annotations

from datetime import date
from typing import Protocol

from neocortex.models.core import (
    CompanyProfile,
    Market,
    MarketContext,
    PriceBar,
    SecurityId,
)


class MarketDataConnector(Protocol):
    """Runtime interface for provider adapters that return normalized models."""

    def get_company_profile(self, security_id: SecurityId) -> CompanyProfile:
        """Return normalized company metadata for one security."""

    def get_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        interval: str = "1d",
    ) -> tuple[PriceBar, ...]:
        """Return normalized OHLCV bars for one security."""

    def get_market_context(self, market: Market) -> MarketContext:
        """Return market-level context used by downstream components."""
