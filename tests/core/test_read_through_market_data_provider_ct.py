from datetime import date

import pytest

from tests.core._market_data_provider_fakes import FakeSourceConnector

from neocortex.connectors.types import (
    AdjustmentFactorRecord,
    DailyPriceBarRecord,
    DisclosureSectionRecord,
    FundamentalSnapshotRecord,
    MacroPointRecord,
    SecurityListing,
    SecurityProfileSnapshot,
)
from neocortex.market_data_provider import (
    RESOURCE_COMPANY_PROFILE,
    RESOURCE_DAILY_PRICE_BARS,
    RESOURCE_DISCLOSURES,
    RESOURCE_FUNDAMENTALS,
    RESOURCE_MACRO,
    ReadThroughMarketDataProvider,
    SourceRoutingError,
)
from neocortex.models import Exchange, Market, SecurityId
from neocortex.storage.market_store import MarketDataStore


@pytest.fixture
def cn_security_id() -> SecurityId:
    return SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)


def test_read_through_provider_prefers_db_profile_without_network_call(
    tmp_path,
    cn_security_id: SecurityId,
) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    store.seed_security_listing(
        SecurityListing(security_id=cn_security_id, name="贵州茅台"),
        source="baostock",
    )
    store.security_profiles.upsert(
        SecurityProfileSnapshot(
            source="baostock",
            security_id=cn_security_id,
            provider_company_name="贵州茅台股份有限公司",
            sector="白酒",
            industry="白酒",
            country="CN",
            currency="CNY",
        ),
        fetched_at="2026-03-19T00:00:00Z",
    )
    source_connector = FakeSourceConnector(
        profile=SecurityProfileSnapshot(
            source="baostock",
            security_id=cn_security_id,
            provider_company_name="should-not-be-used",
        )
    )
    provider = ReadThroughMarketDataProvider(
        store=store,
        source_connectors={"baostock": source_connector},
        source_priority={
            Market.CN: {RESOURCE_COMPANY_PROFILE: ("baostock",)},
        },
    )

    profile = provider.get_company_profile(cn_security_id)

    assert profile.company_name == "贵州茅台股份有限公司"
    assert source_connector.profile_calls == 0


def test_read_through_provider_falls_back_by_priority_and_writes_profile(
    tmp_path,
    cn_security_id: SecurityId,
) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    store.seed_security_listing(
        SecurityListing(security_id=cn_security_id, name="贵州茅台"),
        source="seed",
    )
    provider = ReadThroughMarketDataProvider(
        store=store,
        source_connectors={
            "baostock": FakeSourceConnector(profile_error=NotImplementedError()),
            "efinance": FakeSourceConnector(
                profile=SecurityProfileSnapshot(
                    source="efinance",
                    security_id=cn_security_id,
                    provider_company_name="贵州茅台股份有限公司",
                    sector="白酒",
                    industry="白酒",
                    country="CN",
                    currency="CNY",
                )
            ),
        },
        source_priority={
            Market.CN: {RESOURCE_COMPANY_PROFILE: ("baostock", "efinance")},
        },
    )

    profile = provider.get_company_profile(cn_security_id)
    stored_rows = store.dump_table("security_profiles")
    assert profile.company_name == "贵州茅台股份有限公司"
    assert stored_rows[0]["source"] == "efinance"


def test_read_through_provider_fetches_factors_then_keeps_adjusted_bars_single_source(
    tmp_path,
    cn_security_id: SecurityId,
) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    store.seed_security_listing(
        SecurityListing(security_id=cn_security_id, name="贵州茅台"),
        source="seed",
    )
    store.daily_price_bars.upsert_many(
        (
            DailyPriceBarRecord(
                source="baostock",
                security_id=cn_security_id,
                trade_date="2026-01-05",
                open=10.0,
                high=11.0,
                low=9.0,
                close=10.0,
                volume=100.0,
            ),
        ),
        fetched_at="2026-03-19T00:00:00Z",
    )
    baostock = FakeSourceConnector(
        store=store,
        daily_records=(
            DailyPriceBarRecord(
                source="baostock",
                security_id=cn_security_id,
                trade_date="2026-01-05",
                open=10.0,
                high=11.0,
                low=9.0,
                close=10.0,
                volume=100.0,
            ),
        ),
        factor_records=(
            AdjustmentFactorRecord(
                source="baostock",
                security_id=cn_security_id,
                trade_date="2026-01-05",
                adjustment_type="qfq",
                factor=3.0,
            ),
        ),
    )
    provider = ReadThroughMarketDataProvider(
        store=store,
        source_connectors={
            "baostock": baostock,
            "efinance": FakeSourceConnector(),
        },
        source_priority={
            Market.CN: {RESOURCE_DAILY_PRICE_BARS: ("baostock", "efinance")},
        },
    )

    bars = provider.get_price_bars(
        cn_security_id,
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 5),
        adjust="qfq",
    )

    assert bars.closes.tolist() == [30.0]
    assert baostock.factor_calls == 1
    assert baostock.adjusted_daily_calls == 0
    assert baostock.apply_adjustment_calls == 1
    assert baostock.last_adjusted_raw_daily_records == (
        DailyPriceBarRecord(
            source="baostock",
            security_id=cn_security_id,
            trade_date="2026-01-05",
            open=10.0,
            high=11.0,
            low=9.0,
            close=10.0,
            volume=100.0,
        ),
    )
    assert len(store.dump_table("daily_price_bars")) == 1


def test_read_through_provider_uses_direct_adjusted_daily_without_writing_db(
    tmp_path,
    cn_security_id: SecurityId,
) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    direct_adjusted = FakeSourceConnector(
        factor_error=NotImplementedError(),
        supports_adjustment_factors=False,
        adjusted_daily_records=(
            DailyPriceBarRecord(
                source="efinance",
                security_id=cn_security_id,
                trade_date="2026-01-05",
                open=20.0,
                high=21.0,
                low=19.0,
                close=20.5,
                volume=100.0,
            ),
        ),
    )
    provider = ReadThroughMarketDataProvider(
        store=store,
        source_connectors={"efinance": direct_adjusted},
        source_priority={
            Market.CN: {RESOURCE_DAILY_PRICE_BARS: ("efinance",)},
        },
    )

    bars = provider.get_price_bars(
        cn_security_id,
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 5),
        adjust="qfq",
    )

    assert bars.closes.tolist() == [20.5]
    assert direct_adjusted.factor_calls == 0
    assert direct_adjusted.adjusted_daily_calls == 1
    assert store.dump_table("daily_price_bars") == []


def test_read_through_provider_falls_back_to_direct_adjusted_after_factor_path_failure(
    tmp_path,
    cn_security_id: SecurityId,
) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    fallback_connector = FakeSourceConnector(
        daily_records=(
            DailyPriceBarRecord(
                source="baostock",
                security_id=cn_security_id,
                trade_date="2026-01-05",
                open=10.0,
                high=11.0,
                low=9.0,
                close=10.0,
                volume=100.0,
            ),
        ),
        factor_error=ValueError("bad factor payload"),
        adjusted_daily_records=(
            DailyPriceBarRecord(
                source="baostock",
                security_id=cn_security_id,
                trade_date="2026-01-05",
                open=20.0,
                high=21.0,
                low=19.0,
                close=20.5,
                volume=100.0,
            ),
        ),
    )
    provider = ReadThroughMarketDataProvider(
        store=store,
        source_connectors={"baostock": fallback_connector},
        source_priority={
            Market.CN: {RESOURCE_DAILY_PRICE_BARS: ("baostock",)},
        },
    )

    bars = provider.get_price_bars(
        cn_security_id,
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 5),
        adjust="qfq",
    )

    assert bars.closes.tolist() == [20.5]
    assert fallback_connector.apply_adjustment_calls == 1
    assert fallback_connector.factor_calls == 0
    assert fallback_connector.adjusted_daily_calls == 1


def test_read_through_provider_raises_when_no_adjusted_path_is_available(
    tmp_path,
    cn_security_id: SecurityId,
) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    provider = ReadThroughMarketDataProvider(
        store=store,
        source_connectors={
            "efinance": FakeSourceConnector(
                factor_error=NotImplementedError(),
                adjusted_daily_error=NotImplementedError(),
                supports_adjustment_factors=False,
            ),
        },
        source_priority={
            Market.CN: {RESOURCE_DAILY_PRICE_BARS: ("efinance",)},
        },
    )

    with pytest.raises(SourceRoutingError) as exc_info:
        provider.get_price_bars(
            cn_security_id,
            start_date=date(2026, 1, 5),
            end_date=date(2026, 1, 5),
            adjust="qfq",
        )

    assert exc_info.value.resource_type == RESOURCE_DAILY_PRICE_BARS
    assert exc_info.value.target == cn_security_id
    assert len(exc_info.value.failures) == 1
    assert exc_info.value.failures[0][0] == "efinance"
    assert isinstance(exc_info.value.failures[0][1], NotImplementedError)


def test_read_through_provider_aggregate_error_contains_multiple_source_failures(
    tmp_path,
    cn_security_id: SecurityId,
) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    provider = ReadThroughMarketDataProvider(
        store=store,
        source_connectors={
            "baostock": FakeSourceConnector(profile_error=KeyError(cn_security_id)),
            "efinance": FakeSourceConnector(
                profile_error=ValueError("bad profile payload")
            ),
        },
        source_priority={
            Market.CN: {RESOURCE_COMPANY_PROFILE: ("baostock", "efinance")},
        },
    )

    with pytest.raises(SourceRoutingError) as exc_info:
        provider.get_company_profile(cn_security_id)

    assert exc_info.value.resource_type == RESOURCE_COMPANY_PROFILE
    assert exc_info.value.target == cn_security_id
    assert [source for source, _ in exc_info.value.failures] == [
        "baostock",
        "efinance",
    ]
    assert isinstance(exc_info.value.failures[0][1], KeyError)
    assert isinstance(exc_info.value.failures[1][1], ValueError)


def test_read_through_provider_does_not_write_back_current_day_daily(
    tmp_path,
    cn_security_id: SecurityId,
) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    today = date(2026, 3, 19)
    source_connector = FakeSourceConnector(
        daily_records=(
            DailyPriceBarRecord(
                source="baostock",
                security_id=cn_security_id,
                trade_date=today.isoformat(),
                open=10.0,
                high=11.0,
                low=9.0,
                close=10.5,
                volume=100.0,
            ),
        )
    )
    provider = ReadThroughMarketDataProvider(
        store=store,
        source_connectors={"baostock": source_connector},
        source_priority={
            Market.CN: {RESOURCE_DAILY_PRICE_BARS: ("baostock",)},
        },
        today=lambda: today,
    )

    first = provider.get_price_bars(
        cn_security_id,
        start_date=today,
        end_date=today,
    )
    second = provider.get_price_bars(
        cn_security_id,
        start_date=today,
        end_date=today,
    )

    assert first.closes.tolist() == [10.5]
    assert second.closes.tolist() == [10.5]
    assert source_connector.daily_calls == 2
    assert store.dump_table("daily_price_bars") == []


def test_read_through_provider_writes_historical_daily_without_provider_cache_side_effects(
    tmp_path,
    cn_security_id: SecurityId,
) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    source_connector = FakeSourceConnector(
        daily_records=(
            DailyPriceBarRecord(
                source="baostock",
                security_id=cn_security_id,
                trade_date="2026-03-18",
                open=10.0,
                high=11.0,
                low=9.0,
                close=10.5,
                volume=100.0,
            ),
        )
    )
    provider = ReadThroughMarketDataProvider(
        store=store,
        source_connectors={"baostock": source_connector},
        source_priority={
            Market.CN: {RESOURCE_DAILY_PRICE_BARS: ("baostock",)},
        },
        today=lambda: date(2026, 3, 19),
    )

    first = provider.get_price_bars(
        cn_security_id,
        start_date=date(2026, 3, 18),
        end_date=date(2026, 3, 18),
    )
    second = provider.get_price_bars(
        cn_security_id,
        start_date=date(2026, 3, 18),
        end_date=date(2026, 3, 18),
    )

    assert first.closes.tolist() == [10.5]
    assert second.closes.tolist() == [10.5]
    assert source_connector.daily_calls == 1
    assert store.dump_table("daily_price_bars")[0]["source"] == "baostock"


def test_read_through_provider_returns_other_runtime_resources(
    tmp_path,
    cn_security_id: SecurityId,
) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    connector = FakeSourceConnector(
        fundamental_records=(
            FundamentalSnapshotRecord(
                source="baostock",
                security_id=cn_security_id,
                period_end_date="2025-12-31",
                canonical_period_label="2025FY",
                statement_kind="valuation",
                raw_items_json='{"pe": 20.0}',
                derived_metrics_json='{"roe": 0.18}',
            ),
        ),
        disclosure_records=(
            DisclosureSectionRecord(
                source="baostock",
                security_id=cn_security_id,
                report_date="2026-03-10",
                section_kind="overview",
                content="渠道和产品都在扩张。",
            ),
        ),
        macro_records=(
            MacroPointRecord(
                source="baostock",
                market="CN",
                series_key="cn_rrr.latest",
                observed_at="2026-03-01",
                series_name="CN RRR",
                unit="%",
                frequency="monthly",
                category="rates",
                value=7.0,
            ),
        ),
    )
    provider = ReadThroughMarketDataProvider(
        store=store,
        source_connectors={"baostock": connector},
        source_priority={
            Market.CN: {
                RESOURCE_FUNDAMENTALS: ("baostock",),
                RESOURCE_DISCLOSURES: ("baostock",),
                RESOURCE_MACRO: ("baostock",),
            },
        },
    )

    fundamentals = provider.get_fundamental_snapshots(
        cn_security_id,
        as_of_date=date(2026, 3, 19),
    )
    disclosures = provider.get_disclosure_sections(
        cn_security_id,
        as_of_date=date(2026, 3, 19),
    )
    macro_points = provider.get_macro_points(
        market=Market.CN,
        as_of_date=date(2026, 3, 19),
    )

    assert fundamentals[0].period_label == "2025FY"
    assert fundamentals[0].source == "baostock"
    assert disclosures[0].section_kind == "overview"
    assert disclosures[0].content == "渠道和产品都在扩张。"
    assert macro_points[0].series_name == "CN RRR"
    assert macro_points[0].source == "baostock"
