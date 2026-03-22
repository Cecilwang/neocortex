"""SQLite-backed repositories for market data."""

from __future__ import annotations

import json
from datetime import date
import logging
from pathlib import Path

from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from neocortex.connectors.types import (
    DailyPriceBarRecord,
    DisclosureSectionRecord,
    FundamentalSnapshotRecord,
    IntradayPriceBarRecord,
    MacroPointRecord,
    SecurityListing,
    SecurityProfileSnapshot,
)
from neocortex.models import Exchange, Market, SecurityId
from neocortex.storage.market_models import (
    DailyPriceBarRow,
    DisclosureSectionRow,
    FundamentalSnapshotRow,
    IntradayPriceBarRow,
    MacroPointRow,
    MarketDataBase,
    SecurityAliasRow,
    SecurityProfileRow,
    SecurityRow,
)
from neocortex.storage.sqlite import SessionFactory, create_sqlite_engine
from neocortex.storage.utils import normalize_alias, utc_now_iso

logger = logging.getLogger(__name__)


class SecurityRepository:
    """Persist and query canonical securities."""

    def __init__(self, session_factory) -> None:
        self.session_factory = session_factory

    def upsert(self, listing: SecurityListing, *, observed_at: str) -> None:
        logger.info(
            "Upserting security: market=%s exchange=%s symbol=%s",
            listing.security_id.market.value,
            listing.security_id.exchange.value,
            listing.security_id.symbol,
        )
        with self.session_factory() as session:
            statement = sqlite_insert(SecurityRow).values(
                market=listing.security_id.market.value,
                exchange=listing.security_id.exchange.value,
                symbol=listing.security_id.symbol,
                last_seen_at=observed_at,
            )
            session.execute(
                statement.on_conflict_do_update(
                    index_elements=["market", "exchange", "symbol"],
                    set_={"last_seen_at": statement.excluded.last_seen_at},
                )
            )
            session.commit()

    def list_security_ids(
        self, *, market: Market | None = None
    ) -> tuple[SecurityId, ...]:
        logger.debug("Listing securities: market=%s", market.value if market else None)
        with self.session_factory() as session:
            query = session.query(SecurityRow)
            if market is not None:
                query = query.filter(SecurityRow.market == market.value)
            rows = query.order_by(
                SecurityRow.market, SecurityRow.exchange, SecurityRow.symbol
            ).all()
            return tuple(
                SecurityId(
                    symbol=row.symbol,
                    market=Market(row.market),
                    exchange=Exchange(row.exchange),
                )
                for row in rows
            )


class AliasRepository:
    """Persist canonical aliases with provider provenance."""

    def __init__(self, session_factory) -> None:
        self.session_factory = session_factory

    def upsert(
        self,
        security_id: SecurityId,
        *,
        alias: str,
        language: str,
        source: str,
        updated_at: str,
    ) -> None:
        logger.info(
            "Upserting alias: source=%s security=%s alias=%s",
            source,
            security_id.ticker,
            alias,
        )
        with self.session_factory() as session:
            statement = sqlite_insert(SecurityAliasRow).values(
                market=security_id.market.value,
                exchange=security_id.exchange.value,
                symbol=security_id.symbol,
                alias=alias,
                alias_norm=normalize_alias(alias),
                language=language,
                source=source,
                updated_at=updated_at,
            )
            session.execute(
                statement.on_conflict_do_update(
                    index_elements=[
                        "market",
                        "exchange",
                        "symbol",
                        "alias",
                        "language",
                        "source",
                    ],
                    set_={
                        "alias_norm": statement.excluded.alias_norm,
                        "updated_at": statement.excluded.updated_at,
                    },
                )
            )
            session.commit()


class SecurityProfileRepository:
    """Persist source-specific company profiles."""

    def __init__(self, session_factory) -> None:
        self.session_factory = session_factory

    def upsert(self, snapshot: SecurityProfileSnapshot, *, fetched_at: str) -> None:
        logger.info(
            "Upserting security profile: source=%s security=%s",
            snapshot.source,
            snapshot.security_id.ticker,
        )
        with self.session_factory() as session:
            statement = sqlite_insert(SecurityProfileRow).values(
                source=snapshot.source,
                market=snapshot.security_id.market.value,
                exchange=snapshot.security_id.exchange.value,
                symbol=snapshot.security_id.symbol,
                provider_company_name=snapshot.provider_company_name,
                sector=snapshot.sector,
                industry=snapshot.industry,
                country=snapshot.country,
                currency=snapshot.currency,
                primary_listing=1 if snapshot.primary_listing else 0,
                fetched_at=fetched_at,
            )
            session.execute(
                statement.on_conflict_do_update(
                    index_elements=["source", "market", "exchange", "symbol"],
                    set_={
                        "provider_company_name": statement.excluded.provider_company_name,
                        "sector": statement.excluded.sector,
                        "industry": statement.excluded.industry,
                        "country": statement.excluded.country,
                        "currency": statement.excluded.currency,
                        "primary_listing": statement.excluded.primary_listing,
                        "fetched_at": statement.excluded.fetched_at,
                    },
                )
            )
            session.commit()

    def get(
        self,
        *,
        source: str,
        security_id: SecurityId,
    ) -> SecurityProfileSnapshot | None:
        logger.debug(
            "Loading security profile: source=%s security=%s",
            source,
            security_id.ticker,
        )
        with self.session_factory() as session:
            row = session.get(
                SecurityProfileRow,
                {
                    "source": source,
                    "market": security_id.market.value,
                    "exchange": security_id.exchange.value,
                    "symbol": security_id.symbol,
                },
            )
            if row is None:
                return None
            return SecurityProfileSnapshot(
                source=row.source,
                security_id=security_id,
                provider_company_name=row.provider_company_name,
                sector=row.sector,
                industry=row.industry,
                country=row.country,
                currency=row.currency,
                primary_listing=bool(row.primary_listing),
            )


class DailyPriceBarRepository:
    """Persist source-specific daily bars."""

    def __init__(self, session_factory) -> None:
        self.session_factory = session_factory

    def upsert_many(
        self, records: tuple[DailyPriceBarRecord, ...], *, fetched_at: str
    ) -> int:
        if not records:
            return 0
        logger.info(
            "Upserting daily bars: source=%s security=%s count=%s",
            records[0].source,
            records[0].security_id.ticker,
            len(records),
        )
        with self.session_factory() as session:
            for record in records:
                statement = sqlite_insert(DailyPriceBarRow).values(
                    source=record.source,
                    market=record.security_id.market.value,
                    exchange=record.security_id.exchange.value,
                    symbol=record.security_id.symbol,
                    trade_date=record.trade_date,
                    open=record.open,
                    high=record.high,
                    low=record.low,
                    close=record.close,
                    volume=record.volume,
                    amount=record.amount,
                    fetched_at=fetched_at,
                )
                session.execute(
                    statement.on_conflict_do_update(
                        index_elements=[
                            "source",
                            "market",
                            "exchange",
                            "symbol",
                            "trade_date",
                        ],
                        set_={
                            "open": statement.excluded.open,
                            "high": statement.excluded.high,
                            "low": statement.excluded.low,
                            "close": statement.excluded.close,
                            "volume": statement.excluded.volume,
                            "amount": statement.excluded.amount,
                            "fetched_at": statement.excluded.fetched_at,
                        },
                    )
                )
            session.commit()
        return len(records)

    def get_range(
        self,
        *,
        source: str,
        security_id: SecurityId,
        start_date: date,
        end_date: date,
    ) -> tuple[DailyPriceBarRecord, ...]:
        logger.debug(
            "Loading daily bars: source=%s security=%s start=%s end=%s",
            source,
            security_id.ticker,
            start_date,
            end_date,
        )
        with self.session_factory() as session:
            rows = (
                session.query(DailyPriceBarRow)
                .filter(
                    DailyPriceBarRow.source == source,
                    DailyPriceBarRow.market == security_id.market.value,
                    DailyPriceBarRow.exchange == security_id.exchange.value,
                    DailyPriceBarRow.symbol == security_id.symbol,
                    DailyPriceBarRow.trade_date >= start_date.isoformat(),
                    DailyPriceBarRow.trade_date <= end_date.isoformat(),
                )
                .order_by(DailyPriceBarRow.trade_date)
                .all()
            )
        return tuple(
            DailyPriceBarRecord(
                source=row.source,
                security_id=security_id,
                trade_date=row.trade_date,
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume,
                amount=row.amount,
            )
            for row in rows
        )

    def aggregate_bars(
        self,
        *,
        source: str,
        security_id: SecurityId,
        interval: str,
    ) -> tuple[DailyPriceBarRecord, ...]:
        if interval not in {"1w", "1mo"}:
            raise ValueError("interval must be '1w' or '1mo'.")
        with self.session_factory() as session:
            rows = (
                session.query(DailyPriceBarRow)
                .filter(
                    DailyPriceBarRow.source == source,
                    DailyPriceBarRow.market == security_id.market.value,
                    DailyPriceBarRow.exchange == security_id.exchange.value,
                    DailyPriceBarRow.symbol == security_id.symbol,
                )
                .order_by(DailyPriceBarRow.trade_date)
                .all()
            )
        return self._aggregate_rows(rows, security_id=security_id, interval=interval)

    @staticmethod
    def _aggregate_rows(
        rows: list[DailyPriceBarRow],
        *,
        security_id: SecurityId,
        interval: str,
    ) -> tuple[DailyPriceBarRecord, ...]:
        if not rows:
            return ()

        aggregated: list[DailyPriceBarRecord] = []
        current_key: tuple[int, int] | None = None
        current_source = rows[0].source
        current_open = 0.0
        current_high = 0.0
        current_low = 0.0
        current_close = 0.0
        current_trade_date = ""
        current_volume: float | None = None
        current_amount: float | None = None

        def group_key(trade_date: str) -> tuple[int, int]:
            parsed = date.fromisoformat(trade_date)
            if interval == "1w":
                iso = parsed.isocalendar()
                return (iso.year, iso.week)
            return (parsed.year, parsed.month)

        def flush() -> None:
            aggregated.append(
                DailyPriceBarRecord(
                    source=current_source,
                    security_id=security_id,
                    trade_date=current_trade_date,
                    open=current_open,
                    high=current_high,
                    low=current_low,
                    close=current_close,
                    volume=current_volume,
                    amount=current_amount,
                )
            )

        for row in rows:
            row_key = group_key(row.trade_date)
            if current_key != row_key:
                if current_key is not None:
                    flush()
                current_key = row_key
                current_source = row.source
                current_open = row.open
                current_high = row.high
                current_low = row.low
                current_close = row.close
                current_trade_date = row.trade_date
                current_volume = row.volume
                current_amount = row.amount
                continue
            current_high = max(current_high, row.high)
            current_low = min(current_low, row.low)
            current_close = row.close
            current_trade_date = row.trade_date
            if row.volume is not None:
                current_volume = (current_volume or 0.0) + row.volume
            if row.amount is not None:
                current_amount = (current_amount or 0.0) + row.amount

        flush()
        return tuple(aggregated)


class IntradayPriceBarRepository:
    """Persist source-specific intraday bars."""

    def __init__(self, session_factory) -> None:
        self.session_factory = session_factory

    def upsert_many(
        self,
        records: tuple[IntradayPriceBarRecord, ...],
        *,
        fetched_at: str,
    ) -> int:
        if not records:
            return 0
        logger.info(
            "Upserting intraday bars: source=%s security=%s interval=%s count=%s",
            records[0].source,
            records[0].security_id.ticker,
            records[0].interval,
            len(records),
        )
        with self.session_factory() as session:
            for record in records:
                statement = sqlite_insert(IntradayPriceBarRow).values(
                    source=record.source,
                    market=record.security_id.market.value,
                    exchange=record.security_id.exchange.value,
                    symbol=record.security_id.symbol,
                    interval=record.interval,
                    bar_time=record.bar_time,
                    open=record.open,
                    high=record.high,
                    low=record.low,
                    close=record.close,
                    volume=record.volume,
                    amount=record.amount,
                    fetched_at=fetched_at,
                )
                session.execute(
                    statement.on_conflict_do_update(
                        index_elements=[
                            "source",
                            "market",
                            "exchange",
                            "symbol",
                            "interval",
                            "bar_time",
                        ],
                        set_={
                            "open": statement.excluded.open,
                            "high": statement.excluded.high,
                            "low": statement.excluded.low,
                            "close": statement.excluded.close,
                            "volume": statement.excluded.volume,
                            "amount": statement.excluded.amount,
                            "fetched_at": statement.excluded.fetched_at,
                        },
                    )
                )
            session.commit()
        return len(records)


class FundamentalSnapshotRepository:
    """Persist source-specific fundamental snapshots."""

    def __init__(self, session_factory) -> None:
        self.session_factory = session_factory

    def upsert_many(
        self,
        records: tuple[FundamentalSnapshotRecord, ...],
        *,
        fetched_at: str,
    ) -> int:
        if not records:
            return 0
        logger.info(
            "Upserting fundamental snapshots: source=%s security=%s count=%s",
            records[0].source,
            records[0].security_id.ticker,
            len(records),
        )
        with self.session_factory() as session:
            for record in records:
                statement = sqlite_insert(FundamentalSnapshotRow).values(
                    source=record.source,
                    market=record.security_id.market.value,
                    exchange=record.security_id.exchange.value,
                    symbol=record.security_id.symbol,
                    period_end_date=record.period_end_date,
                    canonical_period_label=record.canonical_period_label,
                    statement_kind=record.statement_kind,
                    provider_period_label=record.provider_period_label,
                    report_date=record.report_date,
                    currency=record.currency,
                    raw_items_json=record.raw_items_json,
                    derived_metrics_json=record.derived_metrics_json,
                    fetched_at=fetched_at,
                )
                session.execute(
                    statement.on_conflict_do_update(
                        index_elements=[
                            "source",
                            "market",
                            "exchange",
                            "symbol",
                            "period_end_date",
                            "canonical_period_label",
                            "statement_kind",
                        ],
                        set_={
                            "provider_period_label": statement.excluded.provider_period_label,
                            "report_date": statement.excluded.report_date,
                            "currency": statement.excluded.currency,
                            "raw_items_json": statement.excluded.raw_items_json,
                            "derived_metrics_json": statement.excluded.derived_metrics_json,
                            "fetched_at": statement.excluded.fetched_at,
                        },
                    )
                )
            session.commit()
        return len(records)

    def get_as_of(
        self,
        *,
        source: str,
        security_id: SecurityId,
        as_of_date: date,
    ) -> tuple[FundamentalSnapshotRecord, ...]:
        logger.debug(
            "Loading fundamentals as of: source=%s security=%s as_of_date=%s",
            source,
            security_id.ticker,
            as_of_date,
        )
        with self.session_factory() as session:
            rows = (
                session.query(FundamentalSnapshotRow)
                .filter(
                    FundamentalSnapshotRow.source == source,
                    FundamentalSnapshotRow.market == security_id.market.value,
                    FundamentalSnapshotRow.exchange == security_id.exchange.value,
                    FundamentalSnapshotRow.symbol == security_id.symbol,
                    FundamentalSnapshotRow.period_end_date <= as_of_date.isoformat(),
                )
                .order_by(FundamentalSnapshotRow.period_end_date.desc())
                .all()
            )
        if not rows:
            return ()
        latest_period_end_date = rows[0].period_end_date
        latest_rows = [
            row for row in rows if row.period_end_date == latest_period_end_date
        ]
        return tuple(
            FundamentalSnapshotRecord(
                source=row.source,
                security_id=security_id,
                period_end_date=row.period_end_date,
                canonical_period_label=row.canonical_period_label,
                statement_kind=row.statement_kind,
                provider_period_label=row.provider_period_label,
                report_date=row.report_date,
                currency=row.currency,
                raw_items_json=row.raw_items_json,
                derived_metrics_json=row.derived_metrics_json,
            )
            for row in latest_rows
        )


class DisclosureSectionRepository:
    """Persist qualitative disclosure sections."""

    def __init__(self, session_factory) -> None:
        self.session_factory = session_factory

    def upsert_many(
        self,
        records: tuple[DisclosureSectionRecord, ...],
        *,
        fetched_at: str,
    ) -> int:
        if not records:
            return 0
        logger.info(
            "Upserting disclosure sections: source=%s security=%s count=%s",
            records[0].source,
            records[0].security_id.ticker,
            len(records),
        )
        with self.session_factory() as session:
            for record in records:
                statement = sqlite_insert(DisclosureSectionRow).values(
                    source=record.source,
                    market=record.security_id.market.value,
                    exchange=record.security_id.exchange.value,
                    symbol=record.security_id.symbol,
                    report_date=record.report_date,
                    section_kind=record.section_kind,
                    content=record.content,
                    fetched_at=fetched_at,
                )
                session.execute(
                    statement.on_conflict_do_update(
                        index_elements=[
                            "source",
                            "market",
                            "exchange",
                            "symbol",
                            "report_date",
                            "section_kind",
                        ],
                        set_={
                            "content": statement.excluded.content,
                            "fetched_at": statement.excluded.fetched_at,
                        },
                    )
                )
            session.commit()
        return len(records)

    def get_as_of(
        self,
        *,
        source: str,
        security_id: SecurityId,
        as_of_date: date,
    ) -> tuple[DisclosureSectionRecord, ...]:
        logger.debug(
            "Loading disclosures as of: source=%s security=%s as_of_date=%s",
            source,
            security_id.ticker,
            as_of_date,
        )
        with self.session_factory() as session:
            rows = (
                session.query(DisclosureSectionRow)
                .filter(
                    DisclosureSectionRow.source == source,
                    DisclosureSectionRow.market == security_id.market.value,
                    DisclosureSectionRow.exchange == security_id.exchange.value,
                    DisclosureSectionRow.symbol == security_id.symbol,
                    DisclosureSectionRow.report_date <= as_of_date.isoformat(),
                )
                .order_by(DisclosureSectionRow.report_date.desc())
                .all()
            )
        if not rows:
            return ()
        latest_report_date = rows[0].report_date
        latest_rows = [row for row in rows if row.report_date == latest_report_date]
        return tuple(
            DisclosureSectionRecord(
                source=row.source,
                security_id=security_id,
                report_date=row.report_date,
                section_kind=row.section_kind,
                content=row.content,
            )
            for row in latest_rows
        )


class MacroPointRepository:
    """Persist macro and market points."""

    def __init__(self, session_factory) -> None:
        self.session_factory = session_factory

    def upsert_many(
        self, records: tuple[MacroPointRecord, ...], *, fetched_at: str
    ) -> int:
        if not records:
            return 0
        logger.info(
            "Upserting macro points: source=%s market=%s count=%s",
            records[0].source,
            records[0].market,
            len(records),
        )
        with self.session_factory() as session:
            for record in records:
                statement = sqlite_insert(MacroPointRow).values(
                    source=record.source,
                    market=record.market,
                    series_key=record.series_key,
                    observed_at=record.observed_at,
                    series_name=record.series_name,
                    value=record.value,
                    unit=record.unit,
                    frequency=record.frequency,
                    category=record.category,
                    change_pct=record.change_pct,
                    yoy_change_pct=record.yoy_change_pct,
                    fetched_at=fetched_at,
                )
                session.execute(
                    statement.on_conflict_do_update(
                        index_elements=[
                            "source",
                            "market",
                            "series_key",
                            "observed_at",
                        ],
                        set_={
                            "series_name": statement.excluded.series_name,
                            "value": statement.excluded.value,
                            "unit": statement.excluded.unit,
                            "frequency": statement.excluded.frequency,
                            "category": statement.excluded.category,
                            "change_pct": statement.excluded.change_pct,
                            "yoy_change_pct": statement.excluded.yoy_change_pct,
                            "fetched_at": statement.excluded.fetched_at,
                        },
                    )
                )
            session.commit()
        return len(records)

    def get_as_of(
        self,
        *,
        source: str,
        market: Market,
        as_of_date: date,
    ) -> tuple[MacroPointRecord, ...]:
        logger.debug(
            "Loading macro points as of: source=%s market=%s as_of_date=%s",
            source,
            market.value,
            as_of_date,
        )
        with self.session_factory() as session:
            rows = (
                session.query(MacroPointRow)
                .filter(
                    MacroPointRow.source == source,
                    MacroPointRow.market == market.value,
                    MacroPointRow.observed_at <= as_of_date.isoformat(),
                )
                .order_by(
                    MacroPointRow.series_key,
                    MacroPointRow.observed_at.desc(),
                )
                .all()
            )
        latest_by_series: dict[str, MacroPointRow] = {}
        for row in rows:
            latest_by_series.setdefault(row.series_key, row)
        return tuple(
            MacroPointRecord(
                source=row.source,
                market=row.market,
                series_key=row.series_key,
                observed_at=row.observed_at,
                series_name=row.series_name,
                unit=row.unit,
                frequency=row.frequency,
                category=row.category,
                value=row.value,
                change_pct=row.change_pct,
                yoy_change_pct=row.yoy_change_pct,
            )
            for row in latest_by_series.values()
        )


class MarketDataStore:
    """Bundle all market-data repositories around one SQLite database."""

    def __init__(self, db_path: str | Path) -> None:
        logger.info("Initializing MarketDataStore: db_path=%s", db_path)
        self.engine = create_sqlite_engine(db_path)
        self.session_factory = SessionFactory(bind=self.engine, expire_on_commit=False)
        self.securities = SecurityRepository(self.session_factory)
        self.aliases = AliasRepository(self.session_factory)
        self.security_profiles = SecurityProfileRepository(self.session_factory)
        self.daily_price_bars = DailyPriceBarRepository(self.session_factory)
        self.intraday_price_bars = IntradayPriceBarRepository(self.session_factory)
        self.fundamental_snapshots = FundamentalSnapshotRepository(self.session_factory)
        self.disclosure_sections = DisclosureSectionRepository(self.session_factory)
        self.macro_points = MacroPointRepository(self.session_factory)

    def ensure_schema(self) -> None:
        logger.info("Ensuring market-data schema exists.")
        MarketDataBase.metadata.create_all(self.engine)

    def seed_security_listing(self, listing: SecurityListing, *, source: str) -> None:
        logger.info(
            "Seeding security listing: source=%s security=%s name=%s",
            source,
            listing.security_id.ticker,
            listing.name,
        )
        observed_at = utc_now_iso()
        self.securities.upsert(listing, observed_at=observed_at)
        if listing.name:
            self.aliases.upsert(
                listing.security_id,
                alias=listing.name,
                language="zh",
                source=source,
                updated_at=observed_at,
            )

    def dump_table(self, table_name: str) -> list[dict[str, object]]:
        logger.debug("Dumping table contents: table=%s", table_name)
        table_lookup = {
            "securities": SecurityRow,
            "security_aliases": SecurityAliasRow,
            "security_profiles": SecurityProfileRow,
            "daily_price_bars": DailyPriceBarRow,
            "intraday_price_bars": IntradayPriceBarRow,
            "fundamental_snapshots": FundamentalSnapshotRow,
            "disclosure_sections": DisclosureSectionRow,
            "macro_points": MacroPointRow,
        }
        row_model = table_lookup[table_name]
        with self.session_factory() as session:
            rows = session.query(row_model).all()
            return [
                {
                    key: value
                    for key, value in row.__dict__.items()
                    if key != "_sa_instance_state"
                }
                for row in rows
            ]


def json_dumps(payload: object) -> str:
    """Return stable JSON text for persistence."""

    return json.dumps(payload, ensure_ascii=False, sort_keys=True)
