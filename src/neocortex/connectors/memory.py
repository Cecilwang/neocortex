"""In-memory connector for tests, fixtures, and local development."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from neocortex.connectors.base import DAILY_BAR_INTERVAL
from neocortex.models.core import (
    CompanyProfile,
    PriceSeries,
    SecurityId,
)


@dataclass(slots=True)
class InMemoryConnector:
    """Serve normalized models from in-memory collections."""

    company_profiles: dict[SecurityId, CompanyProfile] = field(default_factory=dict)
    price_bars: dict[SecurityId, PriceSeries] = field(default_factory=dict)

    def get_company_profile(self, security_id: SecurityId) -> CompanyProfile:
        """Return the stored company profile for one security."""

        return self.company_profiles[security_id]

    def get_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        interval: str = DAILY_BAR_INTERVAL,
        adjust: str | None = None,
    ) -> PriceSeries:
        """Return stored bars within the requested date range."""

        if interval != DAILY_BAR_INTERVAL:
            raise ValueError(
                f"InMemoryConnector currently supports only the {DAILY_BAR_INTERVAL} interval."
            )
        if adjust:
            raise ValueError(
                "InMemoryConnector does not support adjusted price series."
            )

        series = self.price_bars[security_id]

        return PriceSeries(
            security_id=security_id,
            bars=tuple(
                bar
                for bar in series
                if start_date <= bar.timestamp.date() <= end_date
            ),
        )
