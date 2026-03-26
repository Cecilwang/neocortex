"""Shared types and helpers for runtime market-data providers."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, time, timedelta
import logging
from typing import Protocol

import pandas as pd
from neocortex.connectors.types import (
    DailyPriceBarRecord,
    DisclosureSectionRecord,
    FundamentalSnapshotRecord,
    MacroPointRecord,
    SecurityProfileSnapshot,
    TradingDateRecord,
)
from neocortex.markets import get_market_context
from neocortex.models import (
    CompanyProfile,
    DisclosureSection,
    FundamentalStatement,
    FundamentalSnapshot,
    FundamentalValueOrigin,
    MacroSeriesPoint,
    Market,
    PRICE_BAR_CLOSE,
    PRICE_BAR_HIGH,
    PRICE_BAR_LOW,
    PRICE_BAR_OPEN,
    PRICE_BAR_TIMESTAMP,
    PRICE_BAR_VOLUME,
    PriceSeries,
    SecurityId,
)
from neocortex.storage.market_store import MarketDataStore

logger = logging.getLogger(__name__)

RESOURCE_SECURITIES = "securities"
RESOURCE_COMPANY_PROFILE = "company_profile"
RESOURCE_DAILY_PRICE_BARS = "daily_price_bars"
RESOURCE_FUNDAMENTALS = "fundamentals"
RESOURCE_DISCLOSURES = "disclosures"
RESOURCE_MACRO = "macro"
RESOURCE_TRADING_DATES = "trading_dates"


class MarketDataProvider(Protocol):
    """Runtime market-data interface consumed by agents and product flows."""

    def list_securities(self, *, market: Market) -> tuple[SecurityId, ...]:
        """Return canonical securities for one market."""

    def get_company_profile(self, security_id: SecurityId) -> CompanyProfile:
        """Return one normalized company profile."""

    def get_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        interval: str = "1d",
        adjust: str | None = None,
    ) -> PriceSeries:
        """Return one normalized price series."""

    def get_fundamental_snapshots(
        self,
        security_id: SecurityId,
        *,
        as_of_date: date,
    ) -> tuple[FundamentalSnapshot, ...]:
        """Return normalized fundamentals visible as of one date."""

    def get_disclosure_sections(
        self,
        security_id: SecurityId,
        *,
        as_of_date: date,
    ) -> tuple[DisclosureSection, ...]:
        """Return qualitative disclosure sections visible as of one date."""

    def get_macro_points(
        self,
        *,
        market: Market,
        as_of_date: date,
    ) -> tuple[MacroSeriesPoint, ...]:
        """Return macro or market series points visible as of one date."""

    def get_trading_dates(
        self,
        *,
        market: Market,
        start_date: date,
        end_date: date,
    ) -> tuple[TradingDateRecord, ...]:
        """Return one market's calendar records within one range."""

    def is_trading_day(
        self,
        *,
        market: Market,
        trade_date: date,
    ) -> bool:
        records = self.get_trading_dates(
            market=market,
            start_date=trade_date,
            end_date=trade_date,
        )
        if len(records) != 1:
            raise KeyError((market, trade_date))
        return records[0].is_trading_day

    def get_next_trading_date(
        self,
        *,
        market: Market,
        trade_date: date,
    ) -> date:
        for offset in range(1, 31):
            candidate_date = trade_date + timedelta(days=offset)
            records = self.get_trading_dates(
                market=market,
                start_date=candidate_date,
                end_date=candidate_date,
            )
            if len(records) == 1 and records[0].is_trading_day:
                return candidate_date
        raise KeyError((market, trade_date))

    def get_previous_trading_date(
        self,
        *,
        market: Market,
        trade_date: date,
    ) -> date:
        for offset in range(1, 31):
            candidate_date = trade_date - timedelta(days=offset)
            records = self.get_trading_dates(
                market=market,
                start_date=candidate_date,
                end_date=candidate_date,
            )
            if len(records) == 1 and records[0].is_trading_day:
                return candidate_date
        raise KeyError((market, trade_date))


def company_profile_from_snapshot(snapshot: SecurityProfileSnapshot) -> CompanyProfile:
    company_name = snapshot.provider_company_name or snapshot.security_id.symbol
    sector = snapshot.sector or ""
    industry = snapshot.industry or sector
    country = snapshot.country or snapshot.security_id.market.value
    currency = snapshot.currency or ""
    return CompanyProfile(
        security_id=snapshot.security_id,
        company_name=company_name,
        sector=sector,
        industry=industry,
        country=country,
        currency=currency,
        primary_listing=snapshot.primary_listing,
    )


def price_series_from_daily_records(
    security_id: SecurityId,
    records: Sequence[DailyPriceBarRecord],
) -> PriceSeries:
    frame = pd.DataFrame.from_records(
        [
            {
                PRICE_BAR_TIMESTAMP: datetime.combine(
                    date.fromisoformat(record.trade_date),
                    time(15, 0),
                ),
                PRICE_BAR_OPEN: record.open,
                PRICE_BAR_HIGH: record.high,
                PRICE_BAR_LOW: record.low,
                PRICE_BAR_CLOSE: record.close,
                PRICE_BAR_VOLUME: record.volume,
            }
            for record in records
        ]
    )
    return PriceSeries(security_id=security_id, data=frame)


def fundamental_snapshot_from_record(
    record: FundamentalSnapshotRecord,
) -> FundamentalSnapshot:
    return FundamentalSnapshot(
        security_id=record.security_id,
        report_date=date.fromisoformat(record.report_date),
        ann_date=date.fromisoformat(record.ann_date),
        fetch_at=datetime.fromisoformat(record.fetch_at.replace("Z", "+00:00")),
        statement=FundamentalStatement(record.statement),
        value=record.value,
        value_origin=FundamentalValueOrigin(record.value_origin),
        source=record.source,
    )


def disclosure_from_record(record: DisclosureSectionRecord) -> DisclosureSection:
    return DisclosureSection(
        security_id=record.security_id,
        report_date=date.fromisoformat(record.report_date),
        section_kind=record.section_kind,
        content=record.content,
        source=record.source,
    )


def macro_point_from_record(
    record: MacroPointRecord,
) -> MacroSeriesPoint:
    return MacroSeriesPoint(
        market=record.market,
        series_name=record.series_name,
        observed_at=date.fromisoformat(record.observed_at),
        value=record.value,
        unit=record.unit,
        frequency=record.frequency,
        change_pct=record.change_pct,
        yoy_change_pct=record.yoy_change_pct,
        source=record.source,
    )


def resolve_effective_daily_range(
    *,
    store: MarketDataStore,
    source_name: str,
    market: Market,
    start_date: date,
    end_date: date,
) -> tuple[date, date]:
    calendar = get_market_context(market).trading_calendar.value
    effective_start_date = store.trading_dates.next_trading_date(
        source=source_name,
        market=market,
        calendar=calendar,
        trade_date=start_date,
    )
    effective_end_date = store.trading_dates.previous_trading_date(
        source=source_name,
        market=market,
        calendar=calendar,
        trade_date=end_date,
    )
    if effective_start_date is None or effective_start_date > end_date:
        effective_start_date = start_date
    if effective_end_date is None or effective_end_date < start_date:
        effective_end_date = end_date
    if effective_start_date > effective_end_date:
        logger.info(
            "Effective daily range fallback to requested bounds because trading-date "
            f"coverage is missing: source={source_name} market={market.value} "
            f"requested_start={start_date} requested_end={end_date} "
            f"effective_start={effective_start_date} effective_end={effective_end_date}"
        )
        return start_date, end_date
    logger.info(
        f"Resolved daily range from [{start_date} {end_date}] to [{effective_start_date} {effective_end_date}]"
    )
    return effective_start_date, effective_end_date
