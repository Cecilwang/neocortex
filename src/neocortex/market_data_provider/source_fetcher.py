"""Per-source connector fetchers for runtime market-data lookups."""

from __future__ import annotations

from datetime import date
import logging

from neocortex.connectors.types import (
    DailyPriceBarRecord,
    SecurityListing,
    SecurityProfileSnapshot,
)
from neocortex.market_data_provider.base import (
    RESOURCE_COMPANY_PROFILE,
    RESOURCE_DAILY_PRICE_BARS,
    RESOURCE_DISCLOSURES,
    RESOURCE_FUNDAMENTALS,
    RESOURCE_MACRO,
    RESOURCE_SECURITIES,
    company_profile_from_snapshot,
    disclosure_from_record,
    fundamental_snapshot_from_record,
    macro_point_from_record,
    price_series_from_daily_records,
)
from neocortex.market_data_provider.routing import (
    SourceRoutedComponent,
    route_fetch_by_source,
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
from neocortex.storage.utils import utc_now_iso

logger = logging.getLogger(__name__)


class SourceRouteFetcher(SourceRoutedComponent):
    """Fetch per-source market data and persist write-through state."""

    def __init__(self, *, today=date.today, **kwargs) -> None:
        super().__init__(**kwargs)
        self.today = today

    @route_fetch_by_source(resource_type=RESOURCE_SECURITIES)
    def list_securities(
        self,
        *,
        market: Market,
        source_name: str,
    ) -> tuple[SecurityId, ...]:
        connector = self._source_connector(source_name)
        listings = connector.list_securities(market=market)
        if not listings:
            raise KeyError(market)
        for listing in listings:
            self.store.seed_security_listing(listing, source=source_name)
        return tuple(listing.security_id for listing in listings)

    @route_fetch_by_source(resource_type=RESOURCE_COMPANY_PROFILE)
    def get_company_profile(
        self,
        *,
        security_id: SecurityId,
        source_name: str,
    ) -> CompanyProfile:
        connector = self._source_connector(source_name)
        snapshot = connector.get_security_profile_snapshot(security_id)
        self._ensure_security_exists(security_id)
        self._store_profile_snapshot(snapshot)
        return company_profile_from_snapshot(snapshot)

    @route_fetch_by_source(resource_type=RESOURCE_FUNDAMENTALS)
    def get_fundamental_snapshots(
        self,
        *,
        security_id: SecurityId,
        as_of_date: date,
        source_name: str,
    ) -> tuple[FundamentalSnapshot, ...]:
        connector = self._source_connector(source_name)
        fetched = connector.get_fundamental_snapshots(
            security_id,
            as_of_date=as_of_date,
        )
        if not fetched:
            raise KeyError(security_id)
        self._ensure_security_exists(security_id)
        self.store.fundamental_snapshots.upsert_many(
            fetched,
            fetched_at=utc_now_iso(),
        )
        return tuple(fundamental_snapshot_from_record(record) for record in fetched)

    @route_fetch_by_source(resource_type=RESOURCE_DISCLOSURES)
    def get_disclosure_sections(
        self,
        *,
        security_id: SecurityId,
        as_of_date: date,
        source_name: str,
    ) -> tuple[DisclosureSection, ...]:
        connector = self._source_connector(source_name)
        fetched = connector.get_disclosure_sections(
            security_id,
            as_of_date=as_of_date,
        )
        if not fetched:
            raise KeyError(security_id)
        self._ensure_security_exists(security_id)
        self.store.disclosure_sections.upsert_many(
            fetched,
            fetched_at=utc_now_iso(),
        )
        return tuple(disclosure_from_record(record) for record in fetched)

    @route_fetch_by_source(resource_type=RESOURCE_MACRO)
    def get_macro_points(
        self,
        *,
        market: Market,
        as_of_date: date,
        source_name: str,
    ) -> tuple[MacroSeriesPoint, ...]:
        connector = self._source_connector(source_name)
        fetched = connector.get_macro_points(market=market, as_of_date=as_of_date)
        if not fetched:
            raise KeyError(market)
        self.store.macro_points.upsert_many(fetched, fetched_at=utc_now_iso())
        return tuple(macro_point_from_record(record, market) for record in fetched)

    @route_fetch_by_source(resource_type=RESOURCE_DAILY_PRICE_BARS)
    def get_raw_daily_price_bars(
        self,
        *,
        security_id: SecurityId,
        start_date: date,
        end_date: date,
        source_name: str,
    ) -> PriceSeries:
        records = self._fetch_raw_daily_records_for_source(
            source_name=source_name,
            security_id=security_id,
            start_date=start_date,
            end_date=end_date,
        )
        return price_series_from_daily_records(security_id, records)

    @route_fetch_by_source(resource_type=RESOURCE_DAILY_PRICE_BARS)
    def get_adjusted_daily_price_bars(
        self,
        *,
        security_id: SecurityId,
        start_date: date,
        end_date: date,
        adjust: str,
        source_name: str,
    ) -> PriceSeries:
        connector = self._source_connector(source_name)
        if connector.supports_adjustment_factors:
            raw_daily_records = self._fetch_raw_daily_records_for_source(
                source_name=source_name,
                security_id=security_id,
                start_date=start_date,
                end_date=end_date,
            )
            try:
                adjusted_records = connector.apply_adjustment(
                    security_id,
                    adjustment_type=adjust,
                    raw_daily_records=raw_daily_records,
                )
                return price_series_from_daily_records(security_id, adjusted_records)
            except Exception:
                if not connector.supports_adjusted_daily_bars:
                    raise
                logger.info(
                    "Adjustment-factor path failed; falling back to direct adjusted daily fetch: source=%s security=%s adjust=%s",
                    source_name,
                    security_id.ticker,
                    adjust,
                    exc_info=True,
                )

        if connector.supports_adjusted_daily_bars:
            adjusted_records = connector.get_adjusted_daily_price_bars(
                security_id,
                start_date=start_date,
                end_date=end_date,
                adjustment_type=adjust,
            )
            return price_series_from_daily_records(security_id, adjusted_records)

        raise NotImplementedError(
            f"{source_name} does not support adjusted daily bars."
        )

    def _fetch_raw_daily_records_for_source(
        self,
        *,
        source_name: str,
        security_id: SecurityId,
        start_date: date,
        end_date: date,
    ) -> tuple[DailyPriceBarRecord, ...]:
        connector = self._source_connector(source_name)
        fetched = connector.get_daily_price_bars(
            security_id,
            start_date=start_date,
            end_date=end_date,
        )
        if not fetched:
            raise KeyError(security_id)
        # 避免把未收盘数据写入DB
        if self._should_write_back_daily(end_date=end_date):
            self._ensure_security_exists(security_id)
            self.store.daily_price_bars.upsert_many(
                fetched,
                fetched_at=utc_now_iso(),
            )
            logger.info(
                "Raw daily write-back complete: source=%s security=%s count=%s",
                source_name,
                security_id.ticker,
                len(fetched),
            )
        return fetched

    def _store_profile_snapshot(self, snapshot: SecurityProfileSnapshot) -> None:
        fetched_at = utc_now_iso()
        self.store.security_profiles.upsert(snapshot, fetched_at=fetched_at)
        if snapshot.provider_company_name:
            self.store.aliases.upsert(
                snapshot.security_id,
                alias=snapshot.provider_company_name,
                language="zh",
                source=snapshot.source,
                updated_at=fetched_at,
            )

    def _ensure_security_exists(self, security_id: SecurityId) -> None:
        self.store.securities.upsert(
            SecurityListing(security_id=security_id, name=None),
            observed_at=utc_now_iso(),
        )

    def _should_write_back_daily(self, *, end_date: date) -> bool:
        return end_date < self.today()
