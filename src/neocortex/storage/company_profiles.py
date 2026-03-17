"""SQLAlchemy-backed company profile backfill utilities."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from neocortex.connectors import AkShareConnector
from neocortex.models import CompanyProfile, Exchange, Market, SecurityId
from neocortex.storage.models import (
    Base,
    CompanyProfileRow,
    SecurityAliasRow,
    SecurityRow,
    SessionFactory,
    create_sqlite_engine,
)
from neocortex.utils.retry import call_with_retries

if TYPE_CHECKING:
    import pandas as pd


logger = logging.getLogger(__name__)
_AKSHARE_SOURCE = "akshare"
_ZH_ALIAS_LANGUAGE = "zh"


@dataclass(frozen=True, slots=True)
class BackfillStats:
    """Counters emitted by one backfill run."""

    processed: int = 0
    fetched: int = 0
    skipped_unsupported: int = 0
    failed: int = 0


@dataclass(frozen=True, slots=True)
class ProfileFetchResult:
    """Result of one concurrent profile fetch."""

    security_id: SecurityId
    listed_name: str
    profile: CompanyProfile | None


class SQLiteCompanyProfileStore:
    """Persist normalized company profiles and search aliases in SQLite."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.engine = create_sqlite_engine(self.db_path)
        self.session_factory = SessionFactory(bind=self.engine, expire_on_commit=False)

    def session(self) -> Session:
        return self.session_factory()

    def ensure_schema(self) -> None:
        Base.metadata.create_all(self.engine)

    def upsert_security(
        self,
        session: Session,
        security_id: SecurityId,
        *,
        source: str,
        observed_at: str,
    ) -> None:
        statement = sqlite_insert(SecurityRow).values(
            market=security_id.market.value,
            exchange=security_id.exchange.value,
            symbol=security_id.symbol,
            source=source,
            last_seen_at=observed_at,
        )
        session.execute(
            statement.on_conflict_do_update(
                index_elements=["market", "exchange", "symbol"],
                set_={
                    "source": statement.excluded.source,
                    "last_seen_at": statement.excluded.last_seen_at,
                },
            )
        )

    def upsert_company_profile(
        self,
        session: Session,
        profile: CompanyProfile,
        *,
        fetched_at: str,
    ) -> None:
        statement = sqlite_insert(CompanyProfileRow).values(
            market=profile.security_id.market.value,
            exchange=profile.security_id.exchange.value,
            symbol=profile.security_id.symbol,
            company_name=profile.company_name,
            sector=profile.sector,
            industry=profile.industry,
            country=profile.country,
            currency=profile.currency,
            fetched_at=fetched_at,
        )
        session.execute(
            statement.on_conflict_do_update(
                index_elements=["market", "exchange", "symbol"],
                set_={
                    "company_name": statement.excluded.company_name,
                    "sector": statement.excluded.sector,
                    "industry": statement.excluded.industry,
                    "country": statement.excluded.country,
                    "currency": statement.excluded.currency,
                    "fetched_at": statement.excluded.fetched_at,
                },
            )
        )

    def upsert_alias(
        self,
        session: Session,
        security_id: SecurityId,
        *,
        alias: str,
        language: str,
        source: str,
        updated_at: str,
    ) -> None:
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
                ],
                set_={
                    "alias_norm": statement.excluded.alias_norm,
                    "source": statement.excluded.source,
                    "updated_at": statement.excluded.updated_at,
                },
            )
        )


def normalize_alias(value: str) -> str:
    """Normalize aliases for case-insensitive and whitespace-stable lookup."""

    return " ".join(value.split()).lower()


def utc_now_iso() -> str:
    """Return a stable UTC timestamp string for SQLite audit columns."""

    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def infer_cn_exchange(symbol: str) -> Exchange | None:
    """Infer the listing exchange from the six-digit mainland stock code."""

    if symbol.startswith("6"):
        return Exchange.XSHG
    if symbol.startswith(("0", "3")):
        return Exchange.XSHE
    return None


def prepare_cn_security_candidates(
    frame: pd.DataFrame,
    *,
    limit: int | None = None,
) -> tuple[tuple[tuple[SecurityId, str], ...], int]:
    """Convert the AkShare code list into normalized security ids."""

    supported: list[tuple[SecurityId, str]] = []
    skipped_unsupported = 0
    for _, row in frame.iterrows():
        if limit is not None and len(supported) >= limit:
            break
        symbol = str(row["code"]).zfill(6).strip()
        exchange = infer_cn_exchange(symbol)
        if exchange is None:
            skipped_unsupported += 1
            continue
        security_id = SecurityId(symbol=symbol, market=Market.CN, exchange=exchange)
        supported.append((security_id, str(row["name"]).strip()))
    return tuple(supported), skipped_unsupported


def _alias_values(profile: CompanyProfile, listed_name: str) -> tuple[str, ...]:
    values: list[str] = []
    for candidate in (profile.company_name, listed_name):
        alias = candidate.strip()
        if alias and alias not in values:
            values.append(alias)
    return tuple(values)


def _fetch_profile_result(
    security_id: SecurityId,
    listed_name: str,
    *,
    timeout: float | None,
    retry_count: int,
    sleep_seconds: float,
    connector_factory: Callable[..., Any],
) -> ProfileFetchResult:
    connector = connector_factory(timeout=timeout)
    try:
        profile = call_with_retries(
            lambda: connector.get_company_profile(security_id),
            retry_count=retry_count,
            sleep_seconds=sleep_seconds,
        )
    except Exception:
        logger.warning(
            "Failed to fetch company profile for %s after %s attempt(s).",
            security_id.ticker,
            retry_count + 1,
        )
        return ProfileFetchResult(
            security_id=security_id,
            listed_name=listed_name,
            profile=None,
        )
    return ProfileFetchResult(
        security_id=security_id,
        listed_name=listed_name,
        profile=profile,
    )


def _fetch_cn_security_list(
    timeout: float | None,
    retry_count: int,
    sleep_seconds: float,
    connector_factory: Callable[..., Any],
) -> pd.DataFrame:
    connector = connector_factory(timeout=timeout)
    try:
        return call_with_retries(
            connector.get_cn_security_list,
            retry_count=retry_count,
            sleep_seconds=sleep_seconds,
        )
    except Exception:
        logger.error(
            "Failed to fetch cn code list after %s attempt(s).",
            retry_count + 1,
        )
        raise


def backfill_company_profiles(
    db_path: str | Path,
    *,
    timeout: float | None = None,
    limit: int | None = None,
    retry_count: int = 0,
    sleep_seconds: float = 0.0,
    workers: int = 8,
    code_name_frame: pd.DataFrame | None = None,
    connector_factory: Callable[..., Any] = AkShareConnector,
) -> BackfillStats:
    """Backfill normalized company profiles for supported China A-shares."""

    frame = (
        code_name_frame
        if code_name_frame is not None
        else _fetch_cn_security_list(
            timeout=timeout,
            retry_count=retry_count,
            sleep_seconds=sleep_seconds,
            connector_factory=connector_factory,
        )
    )
    supported_candidates, unsupported_count = prepare_cn_security_candidates(
        frame,
        limit=limit,
    )
    store = SQLiteCompanyProfileStore(db_path)
    processed = 0
    fetched = 0
    failed = 0

    store.ensure_schema()
    with store.session() as session:
        for security_id, _ in supported_candidates:
            store.upsert_security(
                session,
                security_id,
                source=_AKSHARE_SOURCE,
                observed_at=utc_now_iso(),
            )
        session.commit()
    processed = len(supported_candidates)

    with ThreadPoolExecutor(max_workers=max(workers, 1)) as executor:
        futures = [
            executor.submit(
                _fetch_profile_result,
                security_id,
                listed_name,
                timeout=timeout,
                retry_count=retry_count,
                sleep_seconds=sleep_seconds,
                connector_factory=connector_factory,
            )
            for security_id, listed_name in supported_candidates
        ]

        for future in as_completed(futures):
            result = future.result()
            if result.profile is None:
                failed += 1
                continue

            fetched_at = utc_now_iso()
            with store.session() as session:
                store.upsert_company_profile(
                    session,
                    result.profile,
                    fetched_at=fetched_at,
                )
                for alias in _alias_values(result.profile, result.listed_name):
                    store.upsert_alias(
                        session,
                        result.security_id,
                        alias=alias,
                        language=_ZH_ALIAS_LANGUAGE,
                        source=_AKSHARE_SOURCE,
                        updated_at=fetched_at,
                    )
                session.commit()
            fetched += 1

    return BackfillStats(
        processed=processed,
        fetched=fetched,
        skipped_unsupported=unsupported_count,
        failed=failed,
    )
