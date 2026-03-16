"""Connector interface definitions for normalized market data access."""

from __future__ import annotations

from datetime import date
from typing import Protocol

from neocortex.models.core import (
    CompanyProfile,
    PriceSeries,
    SecurityId,
)

DAILY_BAR_INTERVAL = "1d"


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
        interval: str = DAILY_BAR_INTERVAL,
        adjust: str | None = None,
    ) -> PriceSeries:
        """Return a normalized price series for one security."""
