"""Read-through runtime market-data provider."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, timedelta
import logging
from pathlib import Path

from neocortex.config import get_config
from neocortex.connectors.akshare import AkShareConnector
from neocortex.connectors.baostock import BaoStockConnector
from neocortex.connectors.base import BaseSourceConnector, DAILY_BAR_INTERVAL
from neocortex.connectors.efinance import EFinanceConnector
from neocortex.connectors.types import DailyPriceBarRecord
from neocortex.connectors.types import TradingDateRecord
from neocortex.markets import get_market_context
from neocortex.market_data_provider.base import (
    MarketDataProvider,
    RESOURCE_DAILY_PRICE_BARS,
    RESOURCE_TRADING_DATES,
    price_series_from_daily_records,
)
from neocortex.market_data_provider.db_reader import DBRouteReader
from neocortex.market_data_provider.routing import (
    route_by_source,
    route_read_through,
)
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
        )

    @classmethod
    def from_defaults(cls, db_path: str | Path) -> "ReadThroughMarketDataProvider":
        store = MarketDataStore(db_path)
        return cls(
            store=store,
            source_connectors={
                "baostock": BaoStockConnector(),
                "efinance": EFinanceConnector(),
                "akshare": AkShareConnector(),
            },
        )

    def _priority(self, market: Market, resource_type: str) -> tuple[str, ...]:
        market_priority = self.source_priority.get(market)
        if market_priority is None or resource_type not in market_priority:
            raise ValueError(
                f"Missing source priority config for market={market.value} resource={resource_type}."
            )
        return market_priority[resource_type]

    def list_securities(self, *, market: Market) -> tuple[SecurityId, ...]:
        security_ids = self.store.securities.list_security_ids(market=market)
        if security_ids:
            logger.info(
                f"Securities DB hit: market={market.value} count={len(security_ids)}"
            )
            return security_ids
        logger.info(f"Securities DB miss: market={market.value}")
        return self.source_fetcher.list_securities(market=market)

    @route_read_through(
        db_method="get_company_profile",
        fetch_method="get_company_profile",
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
            return self.get_raw_daily_price_bars(
                security_id=security_id,
                start_date=start_date,
                end_date=end_date,
            )
        if adjust not in {"qfq", "hfq"}:
            raise ValueError("Adjusted daily price bars support only qfq or hfq.")
        logger.info(
            "Adjusted daily bars are not DB-backed; routing directly to source "
            f"fetcher: security={security_id.ticker} adjust={adjust}"
        )
        return self.get_adjusted_daily_price_bars(
            security_id=security_id,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
        )

    def get_next_trading_date(
        self,
        *,
        market: Market,
        trade_date: date,
    ) -> date:
        calendar = get_market_context(market).trading_calendar.value
        lookup_date = trade_date + timedelta(days=1)
        for source_name in self._priority(market, RESOURCE_TRADING_DATES):
            next_date = self.store.trading_dates.next_trading_date(
                source=source_name,
                market=market,
                calendar=calendar,
                trade_date=lookup_date,
            )
            if next_date is not None:
                return next_date
        raise KeyError((market, trade_date))

    def get_previous_trading_date(
        self,
        *,
        market: Market,
        trade_date: date,
    ) -> date:
        calendar = get_market_context(market).trading_calendar.value
        lookup_date = trade_date - timedelta(days=1)
        for source_name in self._priority(market, RESOURCE_TRADING_DATES):
            previous_date = self.store.trading_dates.previous_trading_date(
                source=source_name,
                market=market,
                calendar=calendar,
                trade_date=lookup_date,
            )
            if previous_date is not None:
                return previous_date
        raise KeyError((market, trade_date))

    @route_read_through(
        db_method="get_raw_daily_price_bars",
        fetch_method="get_raw_daily_price_bars",
    )
    def get_raw_daily_price_bars(
        self,
        *,
        security_id: SecurityId,
        start_date: date,
        end_date: date,
    ) -> PriceSeries:
        raise AssertionError(
            "route_read_through should intercept get_raw_daily_price_bars"
        )

    @route_by_source(RESOURCE_DAILY_PRICE_BARS)
    def get_adjusted_daily_price_bars(
        self,
        *,
        security_id: SecurityId,
        start_date: date,
        end_date: date,
        adjust: str,
        source_name: str,
    ) -> PriceSeries:
        adjusted_records = self._get_adjusted_daily_records_for_source(
            source_name=source_name,
            security_id=security_id,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
        )
        return price_series_from_daily_records(security_id, adjusted_records)

    def _get_adjusted_daily_records_for_source(
        self,
        *,
        source_name: str,
        security_id: SecurityId,
        start_date: date,
        end_date: date,
        adjust: str,
    ) -> tuple[DailyPriceBarRecord, ...]:
        connector = self.source_connectors[source_name]
        if connector.supports_adjustment_factors:
            try:
                try:
                    raw_daily_records = self.db_reader.get_raw_daily_records_for_source(
                        source_name=source_name,
                        security_id=security_id,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    logger.info(
                        f"Adjusted-path raw daily DB hit: source={source_name} "
                        f"security={security_id.ticker} start={start_date} end={end_date} "
                        f"count={len(raw_daily_records)}"
                    )
                except KeyError:
                    logger.info(
                        f"Adjusted-path raw daily DB miss: source={source_name} "
                        f"security={security_id.ticker} start={start_date} end={end_date}"
                    )
                    raw_daily_records = (
                        self.source_fetcher.get_raw_daily_records_for_source(
                            source_name=source_name,
                            security_id=security_id,
                            start_date=start_date,
                            end_date=end_date,
                        )
                    )
                return connector.apply_adjustment(
                    security_id,
                    adjustment_type=adjust,
                    raw_daily_records=raw_daily_records,
                )
            except Exception:
                if not connector.supports_adjusted_daily_bars:
                    raise
                logger.info(
                    "Adjustment-factor path failed; falling back to direct adjusted "
                    f"daily fetch: source={source_name} security={security_id.ticker} "
                    f"adjust={adjust}",
                    exc_info=True,
                )
        if connector.supports_adjusted_daily_bars:
            return connector.get_adjusted_daily_price_bars(
                security_id,
                start_date=start_date,
                end_date=end_date,
                adjustment_type=adjust,
            )
        raise NotImplementedError(
            f"{source_name} does not support adjusted daily bars."
        )

    @route_read_through(
        db_method="get_fundamental_snapshots",
        fetch_method="get_fundamental_snapshots",
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
    )
    def get_macro_points(
        self,
        *,
        market: Market,
        as_of_date: date,
    ) -> tuple[MacroSeriesPoint, ...]:
        raise AssertionError("route_read_through should intercept get_macro_points")

    @route_read_through(
        db_method="get_trading_dates",
        fetch_method="get_trading_dates",
    )
    def get_trading_dates(
        self,
        *,
        market: Market,
        start_date: date,
        end_date: date,
    ) -> tuple[TradingDateRecord, ...]:
        raise AssertionError("route_read_through should intercept get_trading_dates")

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
