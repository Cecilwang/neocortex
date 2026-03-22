from collections.abc import Mapping
from datetime import date

from neocortex.connectors.base import BaseSourceConnector, DAILY_BAR_INTERVAL
from neocortex.connectors.types import (
    AdjustmentFactorRecord,
    DailyPriceBarRecord,
    DisclosureSectionRecord,
    FundamentalSnapshotRecord,
    MacroPointRecord,
    SecurityProfileSnapshot,
)
from neocortex.market_data_provider import MarketDataProvider
from neocortex.models import (
    CompanyProfile,
    DisclosureSection,
    FundamentalSnapshot,
    MacroSeriesPoint,
    Market,
    PRICE_BAR_TIMESTAMP,
    PriceSeries,
    SecurityId,
)
from neocortex.storage.market_store import MarketDataStore


class InMemoryMarketDataProvider(MarketDataProvider):
    def __init__(
        self,
        *,
        company_profiles: Mapping[SecurityId, CompanyProfile] | None = None,
        price_bars: Mapping[SecurityId, PriceSeries] | None = None,
        fundamentals: Mapping[SecurityId, tuple[FundamentalSnapshot, ...]]
        | None = None,
        disclosures: Mapping[SecurityId, tuple[DisclosureSection, ...]] | None = None,
        macro_points: Mapping[Market, tuple[MacroSeriesPoint, ...]] | None = None,
    ) -> None:
        self.company_profiles = dict(company_profiles or {})
        self.price_bars = dict(price_bars or {})
        self.fundamentals = dict(fundamentals or {})
        self.disclosures = dict(disclosures or {})
        self.macro_points = dict(macro_points or {})

    def list_securities(self, *, market: Market) -> tuple[SecurityId, ...]:
        return tuple(
            security_id
            for security_id in self.company_profiles
            if security_id.market is market
        )

    def get_company_profile(self, security_id: SecurityId) -> CompanyProfile:
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
        if interval != DAILY_BAR_INTERVAL:
            raise ValueError(
                f"InMemoryMarketDataProvider currently supports only the {DAILY_BAR_INTERVAL} interval."
            )
        if adjust:
            raise ValueError(
                "InMemoryMarketDataProvider does not support adjusted price series."
            )

        series = self.price_bars[security_id]
        mask = series.bars[PRICE_BAR_TIMESTAMP].dt.date.between(start_date, end_date)
        return PriceSeries(security_id=security_id, data=series.bars.loc[mask])

    def get_fundamental_snapshots(
        self,
        security_id: SecurityId,
        *,
        as_of_date: date,
    ) -> tuple[FundamentalSnapshot, ...]:
        _ = as_of_date
        return self.fundamentals.get(security_id, ())

    def get_disclosure_sections(
        self,
        security_id: SecurityId,
        *,
        as_of_date: date,
    ) -> tuple[DisclosureSection, ...]:
        _ = as_of_date
        return self.disclosures.get(security_id, ())

    def get_macro_points(
        self,
        *,
        market: Market,
        as_of_date: date,
    ) -> tuple[MacroSeriesPoint, ...]:
        _ = as_of_date
        return self.macro_points.get(market, ())


class FakeSourceConnector(BaseSourceConnector):
    source_name = "fake"

    def __init__(
        self,
        *,
        store: MarketDataStore | None = None,
        profile: SecurityProfileSnapshot | None = None,
        daily_records: tuple[DailyPriceBarRecord, ...] = (),
        factor_records: tuple[AdjustmentFactorRecord, ...] = (),
        adjusted_daily_records: tuple[DailyPriceBarRecord, ...] = (),
        fundamental_records: tuple[FundamentalSnapshotRecord, ...] = (),
        disclosure_records: tuple[DisclosureSectionRecord, ...] = (),
        macro_records: tuple[MacroPointRecord, ...] = (),
        profile_error: Exception | None = None,
        factor_error: Exception | None = None,
        adjusted_daily_error: Exception | None = None,
        supports_adjustment_factors: bool = True,
        supports_adjusted_daily_bars: bool = True,
    ) -> None:
        super().__init__(store=store)
        self.store = store
        self.profile = profile
        self.daily_records = daily_records
        self.factor_records = factor_records
        self.adjusted_daily_records = adjusted_daily_records
        self.fundamental_records = fundamental_records
        self.disclosure_records = disclosure_records
        self.macro_records = macro_records
        self.profile_error = profile_error
        self.factor_error = factor_error
        self.adjusted_daily_error = adjusted_daily_error
        self.supports_adjustment_factors = supports_adjustment_factors
        self.supports_adjusted_daily_bars = supports_adjusted_daily_bars
        self.profile_calls = 0
        self.daily_calls = 0
        self.factor_calls = 0
        self.adjusted_daily_calls = 0
        self.apply_adjustment_calls = 0
        self.last_adjusted_raw_daily_records: tuple[DailyPriceBarRecord, ...] | None = (
            None
        )

    def get_security_profile_snapshot(
        self,
        security_id: SecurityId,
    ) -> SecurityProfileSnapshot:
        _ = security_id
        self.profile_calls += 1
        if self.profile_error is not None:
            raise self.profile_error
        if self.profile is None:
            raise NotImplementedError
        return self.profile

    def get_daily_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
    ) -> tuple[DailyPriceBarRecord, ...]:
        _ = security_id, start_date, end_date
        self.daily_calls += 1
        if not self.daily_records:
            raise KeyError(security_id)
        return self.daily_records

    def get_adjustment_factors(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
    ) -> tuple[AdjustmentFactorRecord, ...]:
        _ = security_id, start_date, end_date
        self.factor_calls += 1
        if self.factor_error is not None:
            raise self.factor_error
        if not self.factor_records:
            raise KeyError(security_id)
        return self.factor_records

    def get_adjusted_daily_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        adjustment_type: str,
    ) -> tuple[DailyPriceBarRecord, ...]:
        _ = security_id, start_date, end_date, adjustment_type
        self.adjusted_daily_calls += 1
        if self.adjusted_daily_error is not None:
            raise self.adjusted_daily_error
        if not self.adjusted_daily_records:
            raise KeyError(security_id)
        return self.adjusted_daily_records

    def apply_adjustment(
        self,
        security_id: SecurityId,
        *,
        adjustment_type: str,
        raw_daily_records: tuple[DailyPriceBarRecord, ...],
    ) -> tuple[DailyPriceBarRecord, ...]:
        _ = security_id, adjustment_type
        self.apply_adjustment_calls += 1
        self.last_adjusted_raw_daily_records = raw_daily_records
        if self.factor_error is not None:
            raise self.factor_error
        if not raw_daily_records or not self.factor_records:
            raise KeyError(security_id)
        self.factor_calls += 1
        filtered = tuple(
            record
            for record in self.factor_records
            if record.adjustment_type == adjustment_type
        )
        if not filtered:
            raise KeyError(security_id)
        factor_map = {record.trade_date: record.factor for record in filtered}
        return tuple(
            DailyPriceBarRecord(
                source=record.source,
                security_id=record.security_id,
                trade_date=record.trade_date,
                open=record.open * factor_map[record.trade_date],
                high=record.high * factor_map[record.trade_date],
                low=record.low * factor_map[record.trade_date],
                close=record.close * factor_map[record.trade_date],
                volume=record.volume,
                amount=record.amount,
            )
            for record in raw_daily_records
        )

    def get_fundamental_snapshots(
        self,
        security_id: SecurityId,
        *,
        as_of_date: date,
    ) -> tuple[FundamentalSnapshotRecord, ...]:
        _ = security_id, as_of_date
        if not self.fundamental_records:
            raise KeyError(security_id)
        return self.fundamental_records

    def get_disclosure_sections(
        self,
        security_id: SecurityId,
        *,
        as_of_date: date,
    ) -> tuple[DisclosureSectionRecord, ...]:
        _ = security_id, as_of_date
        if not self.disclosure_records:
            raise KeyError(security_id)
        return self.disclosure_records

    def get_macro_points(
        self,
        *,
        market: Market,
        as_of_date: date,
    ) -> tuple[MacroPointRecord, ...]:
        _ = market, as_of_date
        if not self.macro_records:
            raise KeyError(market)
        return self.macro_records
