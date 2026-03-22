"""Read-through runtime market-data provider."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
import logging
from pathlib import Path

from neocortex.config import get_config
from neocortex.connectors.akshare import AkShareConnector
from neocortex.connectors.baostock import BaoStockConnector
from neocortex.connectors.base import BaseSourceConnector, DAILY_BAR_INTERVAL
from neocortex.connectors.efinance import EFinanceConnector
from neocortex.market_data_provider.base import MarketDataProvider
from neocortex.market_data_provider.db_reader import DBRouteReader
from neocortex.market_data_provider.routing import route_read_through
from neocortex.market_data_provider.source_fetcher import SourceRouteFetcher
from neocortex.models import (
    CompanyProfile,
    DisclosureSection,
    FundamentalSnapshot,
    MacroSeriesPoint,
    Market,
    PriceSeries,
    SecurityId,
)
from neocortex.storage.market_store import MarketDataStore

logger = logging.getLogger(__name__)


class ReadThroughMarketDataProvider(MarketDataProvider):
    """Runtime provider that reads DB first and falls back to network source connectors."""

    def __init__(
        self,
        *,
        store: MarketDataStore,
        source_connectors: Mapping[str, BaseSourceConnector],
        source_priority: Mapping[Market, Mapping[str, Sequence[str]]] | None = None,
        today: callable = date.today,
    ) -> None:
        self.store = store
        self.store.ensure_schema()
        self.source_connectors = dict(source_connectors)
        configured_priority = (
            source_priority or get_config().market_data_provider.source_priority
        )
        self.source_priority = {
            market: {
                resource_type: tuple(source_names)
                for resource_type, source_names in priorities.items()
            }
            for market, priorities in configured_priority.items()
        }
        self.today = today
        self._validate_source_priority()
        self.db_reader = DBRouteReader(
            store=self.store,
            source_connectors=self.source_connectors,
            source_priority=self.source_priority,
        )
        self.source_fetcher = SourceRouteFetcher(
            store=self.store,
            source_connectors=self.source_connectors,
            source_priority=self.source_priority,
            today=self.today,
        )

    @classmethod
    def from_defaults(cls, db_path: str | Path) -> "ReadThroughMarketDataProvider":
        store = MarketDataStore(db_path)
        return cls(
            store=store,
            source_connectors={
                "baostock": BaoStockConnector(store=store),
                "efinance": EFinanceConnector(store=store),
                "akshare": AkShareConnector(store=store),
            },
        )

    def list_securities(self, *, market: Market) -> tuple[SecurityId, ...]:
        security_ids = self.store.securities.list_security_ids(market=market)
        if security_ids:
            logger.info(
                "Securities DB hit: market=%s count=%s",
                market.value,
                len(security_ids),
            )
            return security_ids
        logger.info("Securities DB miss: market=%s", market.value)
        return self.source_fetcher.list_securities(market=market)

    @route_read_through(
        db_method="get_company_profile",
        fetch_method="get_company_profile",
        resource_label="Company profile",
    )
    def get_company_profile(self, security_id: SecurityId) -> CompanyProfile:
        raise AssertionError("route_read_through should intercept get_company_profile")

    def get_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        interval: str = DAILY_BAR_INTERVAL,
        adjust: str | None = None,
    ) -> PriceSeries:
        if interval != DAILY_BAR_INTERVAL:
            raise ValueError(
                "ReadThroughMarketDataProvider currently supports only daily bars."
            )
        if adjust is None:
            return self._get_raw_daily_price_bars(
                security_id=security_id,
                start_date=start_date,
                end_date=end_date,
            )
        if adjust not in {"qfq", "hfq"}:
            raise ValueError("Adjusted daily price bars support only qfq or hfq.")
        logger.info(
            "Adjusted daily bars are not DB-backed; routing directly to source fetcher: security=%s adjust=%s",
            security_id.ticker,
            adjust,
        )
        return self.source_fetcher.get_adjusted_daily_price_bars(
            security_id=security_id,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
        )

    @route_read_through(
        db_method="get_fundamental_snapshots",
        fetch_method="get_fundamental_snapshots",
        resource_label="Fundamentals",
    )
    def get_fundamental_snapshots(
        self,
        security_id: SecurityId,
        *,
        as_of_date: date,
    ) -> tuple[FundamentalSnapshot, ...]:
        raise AssertionError(
            "route_read_through should intercept get_fundamental_snapshots"
        )

    @route_read_through(
        db_method="get_disclosure_sections",
        fetch_method="get_disclosure_sections",
        resource_label="Disclosures",
    )
    def get_disclosure_sections(
        self,
        security_id: SecurityId,
        *,
        as_of_date: date,
    ) -> tuple[DisclosureSection, ...]:
        raise AssertionError(
            "route_read_through should intercept get_disclosure_sections"
        )

    @route_read_through(
        db_method="get_macro_points",
        fetch_method="get_macro_points",
        resource_label="Macro",
    )
    def get_macro_points(
        self,
        *,
        market: Market,
        as_of_date: date,
    ) -> tuple[MacroSeriesPoint, ...]:
        raise AssertionError("route_read_through should intercept get_macro_points")

    @route_read_through(
        db_method="get_raw_daily_price_bars",
        fetch_method="get_raw_daily_price_bars",
        resource_label="Raw daily",
    )
    def _get_raw_daily_price_bars(
        self,
        *,
        security_id: SecurityId,
        start_date: date,
        end_date: date,
    ) -> PriceSeries:
        raise AssertionError(
            "route_read_through should intercept _get_raw_daily_price_bars"
        )

    def _validate_source_priority(self) -> None:
        for market, resources in self.source_priority.items():
            for resource_type, source_names in resources.items():
                if not source_names:
                    raise ValueError(
                        f"Source priority for market={market.value} resource={resource_type} cannot be empty."
                    )
                for source_name in source_names:
                    if source_name not in self.source_connectors:
                        raise ValueError(
                            f"Missing source connector implementation for {source_name}."
                        )
