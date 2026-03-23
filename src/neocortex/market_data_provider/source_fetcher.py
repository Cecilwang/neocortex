"""Per-source connector fetchers for runtime market-data lookups."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
import logging
from zoneinfo import ZoneInfo

from neocortex.connectors.types import (
    DailyPriceBarRecord,
    SecurityListing,
    SecurityProfileSnapshot,
    TradingDateRecord,
)
from neocortex.markets import get_market_context
from neocortex.market_data_provider.base import (
    RESOURCE_COMPANY_PROFILE,
    RESOURCE_DAILY_PRICE_BARS,
    RESOURCE_DISCLOSURES,
    RESOURCE_FUNDAMENTALS,
    RESOURCE_MACRO,
    RESOURCE_SECURITIES,
    RESOURCE_TRADING_DATES,
    company_profile_from_snapshot,
    disclosure_from_record,
    fundamental_snapshot_from_record,
    macro_point_from_record,
    price_series_from_daily_records,
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
from neocortex.storage.utils import utc_now_iso

logger = logging.getLogger(__name__)

_MARKET_WRITE_BACK_DELAY = timedelta(minutes=30)
_MARKET_CLOSE_TIMES = {
    Market.US: time(16, 0),
    Market.JP: time(15, 0),
    Market.HK: time(16, 0),
    Market.CN: time(15, 0),
}


class SourceRouteFetcher(SourceRoutedComponent):
    """Fetch per-source market data and persist write-through state."""

    @route_by_source(RESOURCE_SECURITIES)
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

    @route_by_source(RESOURCE_COMPANY_PROFILE)
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

    @route_by_source(RESOURCE_FUNDAMENTALS)
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

    @route_by_source(RESOURCE_DISCLOSURES)
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

    @route_by_source(RESOURCE_MACRO)
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
        connector = self._source_connector(source_name)
        fetched = connector.get_daily_price_bars(
            security_id,
            start_date=start_date,
            end_date=end_date,
        )
        if not fetched:
            raise KeyError(security_id)
        # 避免把未收盘数据写入DB
        if self._should_write_back_daily(
            market=security_id.market,
            end_date=end_date,
        ):
            self._ensure_security_exists(security_id)
            self.store.daily_price_bars.upsert_many(
                fetched,
                fetched_at=utc_now_iso(),
            )
            logger.info(
                f"Raw daily write-back complete: source={source_name} "
                f"security={security_id.ticker} count={len(fetched)}"
            )
        return fetched

    @route_by_source(RESOURCE_TRADING_DATES)
    def get_trading_dates(
        self,
        *,
        market: Market,
        start_date: date,
        end_date: date,
        source_name: str,
    ) -> tuple[TradingDateRecord, ...]:
        connector = self._source_connector(source_name)
        calendar = get_market_context(market).trading_calendar.value
        bounds = self.store.trading_dates.get_bounds(
            source=source_name,
            market=market,
            calendar=calendar,
        )
        logger.info(
            f"Preparing trading-date fetch: source={source_name} market={market.value} "
            f"calendar={calendar} start={start_date} end={end_date} bounds={bounds}"
        )
        fetch_ranges: list[tuple[date, date]] = []
        if bounds is None:
            fetch_ranges.append((start_date, end_date))
        else:
            existing_start, existing_end = bounds
            if start_date < existing_start:
                fetch_ranges.append((start_date, existing_start - timedelta(days=1)))
            if end_date > existing_end:
                fetch_ranges.append((existing_end + timedelta(days=1), end_date))
        for fetch_start_date, fetch_end_date in fetch_ranges:
            if fetch_start_date > fetch_end_date:
                continue
            fetched = connector.get_trading_dates(
                market=market,
                start_date=fetch_start_date,
                end_date=fetch_end_date,
            )
            if not fetched:
                raise KeyError(market)
            self.store.trading_dates.upsert_many(fetched, fetched_at=utc_now_iso())
        if not self.store.trading_dates.covers_range(
            source=source_name,
            market=market,
            calendar=calendar,
            start_date=start_date,
            end_date=end_date,
        ):
            raise KeyError(market)
        trading_date_records = self.store.trading_dates.get_range(
            source=source_name,
            market=market,
            calendar=calendar,
            start_date=start_date,
            end_date=end_date,
        )
        logger.info(
            f"Trading dates fetch complete: source={source_name} market={market.value} "
            f"calendar={calendar} "
            f"start={start_date} end={end_date} count={len(trading_date_records)}"
        )
        return trading_date_records

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

    def _should_write_back_daily(self, *, market: Market, end_date: date) -> bool:
        market_now = datetime.now(ZoneInfo(get_market_context(market).timezone))
        market_today = market_now.date()
        if end_date < market_today:
            return True
        if end_date > market_today:
            return False
        market_close_at = datetime.combine(
            market_today,
            _MARKET_CLOSE_TIMES[market],
            tzinfo=market_now.tzinfo,
        )
        return market_now >= market_close_at + _MARKET_WRITE_BACK_DELAY
