"""In-memory connector for tests, fixtures, and local development."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from alphaforge.models.core import (
    CompanyProfile,
    Market,
    MarketContext,
    PriceBar,
    SecurityId,
)


@dataclass(slots=True)
class InMemoryConnector:
    """Serve normalized models from in-memory collections."""

    company_profiles: dict[SecurityId, CompanyProfile] = field(default_factory=dict)
    market_contexts: dict[Market, MarketContext] = field(default_factory=dict)
    price_bars: dict[SecurityId, tuple[PriceBar, ...]] = field(default_factory=dict)

    def get_company_profile(self, security_id: SecurityId) -> CompanyProfile:
        """Return the stored company profile for one security."""

        try:
            return self.company_profiles[security_id]
        except KeyError as exc:
            raise KeyError(
                f"Missing company profile for {security_id.ticker}."
            ) from exc

    def get_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        interval: str = "1d",
    ) -> tuple[PriceBar, ...]:
        """Return stored bars within the requested date range."""

        if interval != "1d":
            raise ValueError(
                "InMemoryConnector currently supports only the 1d interval."
            )

        try:
            bars = self.price_bars[security_id]
        except KeyError as exc:
            raise KeyError(f"Missing price bars for {security_id.ticker}.") from exc

        return tuple(
            bar for bar in bars if start_date <= bar.timestamp.date() <= end_date
        )

    def get_market_context(self, market: Market) -> MarketContext:
        """Return the stored market context for one market."""

        try:
            return self.market_contexts[market]
        except KeyError as exc:
            raise KeyError(f"Missing market context for {market}.") from exc
