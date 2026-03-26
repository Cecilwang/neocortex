"""SQLite-backed repositories for market data."""

from __future__ import annotations

import json
from datetime import date, timedelta
import logging
from pathlib import Path

from sqlalchemy import func
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from neocortex.connectors.types import (
    DailyPriceBarRecord,
    DisclosureSectionRecord,
    FundamentalSnapshotRecord,
    IntradayPriceBarRecord,
    MacroPointRecord,
    SecurityListing,
    SecurityProfileSnapshot,
    TradingDateRecord,
)
from neocortex.models import Exchange, Market, SecurityId
from neocortex.models import FundamentalStatement, FundamentalValueOrigin
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
    TradingDateRow,
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
            f"Upserting security: market={listing.security_id.market.value} "
            f"exchange={listing.security_id.exchange.value} "
            f"symbol={listing.security_id.symbol}"
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
        logger.debug(f"Listing securities: market={market.value if market else None}")
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
            f"Upserting alias: source={source} security={security_id.ticker} alias={alias}"
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

    def search_security_ids(
        self,
        *,
        market: Market,
        query: str,
        limit: int = 10,
    ) -> tuple[tuple[SecurityId, str], ...]:
        query_norm = normalize_alias(query)
        logger.debug(
            f"Searching security aliases: market={market.value} query={query} limit={limit}"
        )
        with self.session_factory() as session:
            rows = (
                session.query(SecurityAliasRow)
                .filter(SecurityAliasRow.market == market.value)
                .filter(SecurityAliasRow.alias_norm.like(f"%{query_norm}%"))
                .order_by(
                    func.length(SecurityAliasRow.alias_norm), SecurityAliasRow.alias
                )
                .all()
            )

        results: list[tuple[SecurityId, str]] = []
        seen: set[tuple[str, str, str]] = set()
        for row in rows:
            key = (row.market, row.exchange, row.symbol)
            if key in seen:
                continue
            seen.add(key)
            results.append(
                (
                    SecurityId(
                        symbol=row.symbol,
                        market=Market(row.market),
                        exchange=Exchange(row.exchange),
                    ),
                    row.alias,
                )
            )
            if len(results) >= limit:
                break
        return tuple(results)


class SecurityProfileRepository:
    """Persist source-specific company profiles."""

    def __init__(self, session_factory) -> None:
        self.session_factory = session_factory

    def upsert(self, snapshot: SecurityProfileSnapshot, *, fetched_at: str) -> None:
        logger.info(
            f"Upserting security profile: source={snapshot.source} "
            f"security={snapshot.security_id.ticker}"
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
            f"Loading security profile: source={source} security={security_id.ticker}"
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
            f"Upserting daily bars: source={records[0].source} "
            f"security={records[0].security_id.ticker} count={len(records)}"
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
            f"Loading daily bars: source={source} security={security_id.ticker} "
            f"start={start_date} end={end_date}"
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
            f"Upserting intraday bars: source={records[0].source} "
            f"security={records[0].security_id.ticker} interval={records[0].interval} "
            f"count={len(records)}"
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
    """Persist source-specific normalized quantitative fundamentals."""

    def __init__(self, session_factory) -> None:
        self.session_factory = session_factory

    def upsert_many(
        self,
        records: tuple[FundamentalSnapshotRecord, ...],
    ) -> int:
        if not records:
            return 0
        logger.info(
            f"Upserting fundamental snapshots: source={records[0].source} "
            f"security={records[0].security_id.ticker} count={len(records)}"
        )
        with self.session_factory() as session:
            for record in records:
                statement = sqlite_insert(FundamentalSnapshotRow).values(
                    source=record.source,
                    market=record.security_id.market.value,
                    exchange=record.security_id.exchange.value,
                    symbol=record.security_id.symbol,
                    report_date=record.report_date,
                    ann_date=record.ann_date,
                    statement=record.statement.value,
                    value=record.value,
                    value_origin=record.value_origin.value,
                    fetched_at=record.fetch_at,
                )
                session.execute(
                    statement.on_conflict_do_update(
                        index_elements=[
                            "source",
                            "market",
                            "exchange",
                            "symbol",
                            "report_date",
                            "ann_date",
                            "statement",
                        ],
                        set_={
                            "value": statement.excluded.value,
                            "value_origin": statement.excluded.value_origin,
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
            f"Loading fundamentals as of: source={source} security={security_id.ticker} "
            f"as_of_date={as_of_date}"
        )
        with self.session_factory() as session:
            rows = (
                session.query(FundamentalSnapshotRow)
                .filter(
                    FundamentalSnapshotRow.source == source,
                    FundamentalSnapshotRow.market == security_id.market.value,
                    FundamentalSnapshotRow.exchange == security_id.exchange.value,
                    FundamentalSnapshotRow.symbol == security_id.symbol,
                    FundamentalSnapshotRow.ann_date <= as_of_date.isoformat(),
                )
                .order_by(
                    FundamentalSnapshotRow.report_date.desc(),
                    FundamentalSnapshotRow.ann_date.desc(),
                    FundamentalSnapshotRow.statement.asc(),
                )
                .all()
            )
        latest_rows: list[FundamentalSnapshotRow] = []
        seen: set[tuple[str, str]] = set()
        for row in rows:
            key = (row.report_date, row.statement)
            if key in seen:
                continue
            seen.add(key)
            latest_rows.append(row)
        return tuple(
            FundamentalSnapshotRecord(
                source=row.source,
                security_id=security_id,
                report_date=row.report_date,
                ann_date=row.ann_date,
                fetch_at=row.fetched_at,
                statement=FundamentalStatement(row.statement),
                value=row.value,
                value_origin=FundamentalValueOrigin(row.value_origin),
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
            f"Upserting disclosure sections: source={records[0].source} "
            f"security={records[0].security_id.ticker} count={len(records)}"
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
            f"Loading disclosures as of: source={source} security={security_id.ticker} "
            f"as_of_date={as_of_date}"
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
            f"Upserting macro points: source={records[0].source} "
            f"market={records[0].market} count={len(records)}"
        )
        with self.session_factory() as session:
            for record in records:
                statement = sqlite_insert(MacroPointRow).values(
                    source=record.source,
                    market=record.market.value,
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
            f"Loading macro points as of: source={source} market={market.value} "
            f"as_of_date={as_of_date}"
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
                market=Market(row.market),
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


class TradingDateRepository:
    """Persist source-specific market trading-date rows."""

    def __init__(self, session_factory) -> None:
        self.session_factory = session_factory

    def upsert_many(
        self,
        records: tuple[TradingDateRecord, ...],
        *,
        fetched_at: str,
    ) -> int:
        if not records:
            return 0
        self._validate_upsert_batch(records)
        source = records[0].source
        market = records[0].market
        calendar = records[0].calendar
        new_start = min(date.fromisoformat(record.trade_date) for record in records)
        new_end = max(date.fromisoformat(record.trade_date) for record in records)
        existing_bounds = self.get_bounds(
            source=source,
            market=market,
            calendar=calendar,
        )
        if existing_bounds is not None:
            existing_start, existing_end = existing_bounds
            if new_start > existing_end + timedelta(
                days=1
            ) or new_end < existing_start - timedelta(days=1):
                raise ValueError(
                    "Trading-date upsert must remain contiguous with existing stored range."
                )
        logger.info(
            f"Upserting trading dates: source={records[0].source} "
            f"market={records[0].market.value} calendar={records[0].calendar} "
            f"count={len(records)}"
        )
        with self.session_factory() as session:
            for record in records:
                statement = sqlite_insert(TradingDateRow).values(
                    source=record.source,
                    market=record.market.value,
                    calendar=record.calendar,
                    trade_date=record.trade_date,
                    is_trading_day=1 if record.is_trading_day else 0,
                    fetched_at=fetched_at,
                )
                session.execute(
                    statement.on_conflict_do_update(
                        index_elements=["source", "market", "calendar", "trade_date"],
                        set_={
                            "is_trading_day": statement.excluded.is_trading_day,
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
        market: Market,
        calendar: str,
        start_date: date,
        end_date: date,
    ) -> tuple[TradingDateRecord, ...]:
        logger.debug(
            f"Loading trading dates: source={source} market={market.value} "
            f"calendar={calendar} start={start_date} end={end_date}"
        )
        with self.session_factory() as session:
            rows = (
                session.query(TradingDateRow)
                .filter(
                    TradingDateRow.source == source,
                    TradingDateRow.market == market.value,
                    TradingDateRow.calendar == calendar,
                    TradingDateRow.trade_date >= start_date.isoformat(),
                    TradingDateRow.trade_date <= end_date.isoformat(),
                )
                .order_by(TradingDateRow.trade_date)
                .all()
            )
        return tuple(
            TradingDateRecord(
                source=row.source,
                market=Market(row.market),
                calendar=row.calendar,
                trade_date=row.trade_date,
                is_trading_day=bool(row.is_trading_day),
            )
            for row in rows
        )

    def get_bounds(
        self,
        *,
        source: str,
        market: Market,
        calendar: str,
    ) -> tuple[date, date] | None:
        with self.session_factory() as session:
            min_trade_date, max_trade_date, row_count = (
                session.query(
                    func.min(TradingDateRow.trade_date),
                    func.max(TradingDateRow.trade_date),
                    func.count(),
                )
                .filter(
                    TradingDateRow.source == source,
                    TradingDateRow.market == market.value,
                    TradingDateRow.calendar == calendar,
                )
                .one()
            )
        if row_count == 0 or min_trade_date is None or max_trade_date is None:
            return None
        start_date = date.fromisoformat(min_trade_date)
        end_date = date.fromisoformat(max_trade_date)
        expected_count = (end_date - start_date).days + 1
        if row_count != expected_count:
            raise ValueError(
                "Stored trading dates must remain contiguous in the database."
            )
        return start_date, end_date

    def covers_range(
        self,
        *,
        source: str,
        market: Market,
        calendar: str,
        start_date: date,
        end_date: date,
    ) -> bool:
        bounds = self.get_bounds(
            source=source,
            market=market,
            calendar=calendar,
        )
        if bounds is None:
            return False
        existing_start, existing_end = bounds
        return existing_start <= start_date and end_date <= existing_end

    def next_trading_date(
        self,
        *,
        source: str,
        market: Market,
        calendar: str,
        trade_date: date,
    ) -> date | None:
        with self.session_factory() as session:
            next_trade_date = (
                session.query(func.min(TradingDateRow.trade_date))
                .filter(
                    TradingDateRow.source == source,
                    TradingDateRow.market == market.value,
                    TradingDateRow.calendar == calendar,
                    TradingDateRow.trade_date >= trade_date.isoformat(),
                    TradingDateRow.is_trading_day == 1,
                )
                .scalar()
            )
        return None if next_trade_date is None else date.fromisoformat(next_trade_date)

    def previous_trading_date(
        self,
        *,
        source: str,
        market: Market,
        calendar: str,
        trade_date: date,
    ) -> date | None:
        with self.session_factory() as session:
            previous_trade_date = (
                session.query(func.max(TradingDateRow.trade_date))
                .filter(
                    TradingDateRow.source == source,
                    TradingDateRow.market == market.value,
                    TradingDateRow.calendar == calendar,
                    TradingDateRow.trade_date <= trade_date.isoformat(),
                    TradingDateRow.is_trading_day == 1,
                )
                .scalar()
            )
        return (
            None
            if previous_trade_date is None
            else date.fromisoformat(previous_trade_date)
        )

    def _validate_upsert_batch(
        self,
        records: tuple[TradingDateRecord, ...],
    ) -> None:
        source = records[0].source
        market = records[0].market
        calendar = records[0].calendar
        parsed_dates = sorted(
            date.fromisoformat(record.trade_date) for record in records
        )
        if any(
            record.source != source
            or record.market is not market
            or record.calendar != calendar
            for record in records
        ):
            raise ValueError(
                "Trading-date upsert batch must have one source, market, and calendar."
            )
        unique_dates = tuple(dict.fromkeys(parsed_dates))
        if len(unique_dates) != len(parsed_dates):
            raise ValueError(
                "Trading-date upsert batch contains duplicate trade dates."
            )
        for previous, current in zip(unique_dates, unique_dates[1:]):
            if current != previous + timedelta(days=1):
                raise ValueError(
                    "Trading-date upsert batch must be continuous day by day."
                )


class MarketDataStore:
    """Bundle all market-data repositories around one SQLite database."""

    def __init__(self, db_path: str | Path) -> None:
        logger.debug(f"Initializing MarketDataStore: db_path={db_path}")
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
        self.trading_dates = TradingDateRepository(self.session_factory)

    def ensure_schema(self) -> None:
        logger.debug("Ensuring market-data schema exists.")
        MarketDataBase.metadata.create_all(self.engine)

    def seed_security_listing(self, listing: SecurityListing, *, source: str) -> None:
        logger.info(
            f"Seeding security listing: source={source} security={listing.security_id.ticker} "
            f"name={listing.name}"
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
        logger.debug(f"Dumping table contents: table={table_name}")
        table_lookup = {
            "securities": SecurityRow,
            "security_aliases": SecurityAliasRow,
            "security_profiles": SecurityProfileRow,
            "daily_price_bars": DailyPriceBarRow,
            "intraday_price_bars": IntradayPriceBarRow,
            "fundamental_snapshots": FundamentalSnapshotRow,
            "disclosure_sections": DisclosureSectionRow,
            "macro_points": MacroPointRow,
            "trading_dates": TradingDateRow,
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
