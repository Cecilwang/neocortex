"""Per-source database readers for runtime market-data lookups."""

from __future__ import annotations

from datetime import date
import logging

from neocortex.connectors.common import daily_records_cover_requested_range
from neocortex.connectors.types import DailyPriceBarRecord, TradingDateRecord
from neocortex.markets import get_market_context
from neocortex.market_data_provider.base import (
    RESOURCE_COMPANY_PROFILE,
    RESOURCE_DAILY_PRICE_BARS,
    RESOURCE_DISCLOSURES,
    RESOURCE_FUNDAMENTALS,
    RESOURCE_MACRO,
    RESOURCE_TRADING_DATES,
    company_profile_from_snapshot,
    disclosure_from_record,
    fundamental_snapshot_from_record,
    macro_point_from_record,
    price_series_from_daily_records,
    resolve_effective_daily_range,
)
from neocortex.market_data_provider.routing import (
    SourceRoutedComponent,
    route_by_source,
)
from neocortex.models import (
    CompanyProfile,
    DisclosureSection,
    FundamentalSnapshot,
    MacroSeriesPoint,
    Market,
    PriceSeries,
    SecurityId,
)

logger = logging.getLogger(__name__)


class DBRouteReader(SourceRoutedComponent):
    """Read DB-backed per-source market data without network side effects."""

    @route_by_source(RESOURCE_COMPANY_PROFILE)
    def get_company_profile(
        self,
        *,
        security_id: SecurityId,
        source_name: str,
    ) -> CompanyProfile:
        snapshot = self.store.security_profiles.get(
            source=source_name,
            security_id=security_id,
        )
        if snapshot is None:
            raise KeyError(security_id)
        return company_profile_from_snapshot(snapshot)

    @route_by_source(RESOURCE_FUNDAMENTALS)
    def get_fundamental_snapshots(
        self,
        *,
        security_id: SecurityId,
        as_of_date: date,
        source_name: str,
    ) -> tuple[FundamentalSnapshot, ...]:
        records = self.store.fundamental_snapshots.get_as_of(
            source=source_name,
            security_id=security_id,
            as_of_date=as_of_date,
        )
        if not records:
            raise KeyError(security_id)
        return tuple(fundamental_snapshot_from_record(record) for record in records)

    @route_by_source(RESOURCE_DISCLOSURES)
    def get_disclosure_sections(
        self,
        *,
        security_id: SecurityId,
        as_of_date: date,
        source_name: str,
    ) -> tuple[DisclosureSection, ...]:
        records = self.store.disclosure_sections.get_as_of(
            source=source_name,
            security_id=security_id,
            as_of_date=as_of_date,
        )
        if not records:
            raise KeyError(security_id)
        return tuple(disclosure_from_record(record) for record in records)

    @route_by_source(RESOURCE_MACRO)
    def get_macro_points(
        self,
        *,
        market: Market,
        as_of_date: date,
        source_name: str,
    ) -> tuple[MacroSeriesPoint, ...]:
        records = self.store.macro_points.get_as_of(
            source=source_name,
            market=market,
            as_of_date=as_of_date,
        )
        if not records:
            raise KeyError(market)
        return tuple(macro_point_from_record(record) for record in records)

    @route_by_source(RESOURCE_DAILY_PRICE_BARS)
    def get_raw_daily_records(
        self,
        *,
        security_id: SecurityId,
        start_date: date,
        end_date: date,
        source_name: str,
    ) -> tuple[DailyPriceBarRecord, ...]:
        return self.get_raw_daily_records_for_source(
            source_name=source_name,
            security_id=security_id,
            start_date=start_date,
            end_date=end_date,
        )

    def get_raw_daily_price_bars(
        self,
        *,
        security_id: SecurityId,
        start_date: date,
        end_date: date,
    ) -> PriceSeries:
        records = self.get_raw_daily_records(
            security_id=security_id,
            start_date=start_date,
            end_date=end_date,
        )
        return price_series_from_daily_records(security_id, records)

    def get_raw_daily_records_for_source(
        self,
        *,
        source_name: str,
        security_id: SecurityId,
        start_date: date,
        end_date: date,
    ) -> tuple[DailyPriceBarRecord, ...]:
        effective_start_date, effective_end_date = resolve_effective_daily_range(
            store=self.store,
            source_name=source_name,
            market=security_id.market,
            start_date=start_date,
            end_date=end_date,
        )
        records = self.store.daily_price_bars.get_range(
            source=source_name,
            security_id=security_id,
            start_date=effective_start_date,
            end_date=effective_end_date,
        )
        if not daily_records_cover_requested_range(
            records=records,
            start_date=effective_start_date,
            end_date=effective_end_date,
        ):
            raise KeyError(security_id)
        logger.info(
            f"Raw daily DB hit: source={source_name} security={security_id.ticker} "
            f"start={start_date} end={end_date} count={len(records)}"
        )
        return records

    @route_by_source(RESOURCE_TRADING_DATES)
    def get_trading_dates(
        self,
        *,
        market: Market,
        start_date: date,
        end_date: date,
        source_name: str,
    ) -> tuple[TradingDateRecord, ...]:
        calendar = get_market_context(market).trading_calendar.value
        if not self.store.trading_dates.covers_range(
            source=source_name,
            market=market,
            calendar=calendar,
            start_date=start_date,
            end_date=end_date,
        ):
            raise KeyError(market)
        records = self.store.trading_dates.get_range(
            source=source_name,
            market=market,
            calendar=calendar,
            start_date=start_date,
            end_date=end_date,
        )
        logger.info(
            f"Trading dates DB hit: source={source_name} market={market.value} "
            f"calendar={calendar} start={start_date} end={end_date} "
            f"count={len(records)}"
        )
        return records
