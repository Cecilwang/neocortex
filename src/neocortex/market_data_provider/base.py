"""Shared types and helpers for runtime market-data providers."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, time
import json
from typing import Protocol

import pandas as pd
from neocortex.connectors.types import (
    DailyPriceBarRecord,
    DisclosureSectionRecord,
    FundamentalSnapshotRecord,
    MacroPointRecord,
    SecurityProfileSnapshot,
)
from neocortex.models import (
    CompanyProfile,
    DisclosureSection,
    FundamentalSnapshot,
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

RESOURCE_SECURITIES = "securities"
RESOURCE_COMPANY_PROFILE = "company_profile"
RESOURCE_DAILY_PRICE_BARS = "daily_price_bars"
RESOURCE_FUNDAMENTALS = "fundamentals"
RESOURCE_DISCLOSURES = "disclosures"
RESOURCE_MACRO = "macro"


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
        as_of_date=date.fromisoformat(record.period_end_date),
        period_label=record.canonical_period_label,
        raw_items=json.loads(record.raw_items_json),
        derived_metrics=json.loads(record.derived_metrics_json),
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
    market: Market,
) -> MacroSeriesPoint:
    return MacroSeriesPoint(
        market=market,
        series_name=record.series_name,
        observed_at=date.fromisoformat(record.observed_at),
        value=record.value,
        unit=record.unit,
        frequency=record.frequency,
        change_pct=record.change_pct,
        yoy_change_pct=record.yoy_change_pct,
        source=record.source,
    )
