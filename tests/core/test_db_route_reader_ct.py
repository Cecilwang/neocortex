from tests.core._market_data_provider_fakes import FakeSourceConnector

from neocortex.connectors.types import (
    SecurityListing,
    SecurityProfileSnapshot,
)
from neocortex.market_data_provider import (
    DBRouteReader,
    RESOURCE_COMPANY_PROFILE,
)
from neocortex.models import Exchange, Market, SecurityId
from neocortex.storage.market_store import MarketDataStore


def test_db_route_reader_prefers_later_source_profile_hit_without_network_call(
    tmp_path,
) -> None:
    store = MarketDataStore(tmp_path / "market.sqlite3")
    store.ensure_schema()
    security_id = SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)
    store.seed_security_listing(
        SecurityListing(security_id=security_id, name="贵州茅台"),
        source="seed",
    )
    store.security_profiles.upsert(
        SecurityProfileSnapshot(
            source="efinance",
            security_id=security_id,
            provider_company_name="贵州茅台股份有限公司",
            sector="白酒",
            industry="白酒",
            country="CN",
            currency="CNY",
        ),
        fetched_at="2026-03-19T00:00:00Z",
    )
    baostock = FakeSourceConnector(
        profile=SecurityProfileSnapshot(
            source="baostock",
            security_id=security_id,
            provider_company_name="should-not-be-used",
        )
    )
    efinance = FakeSourceConnector(
        profile=SecurityProfileSnapshot(
            source="efinance",
            security_id=security_id,
            provider_company_name="should-not-be-called",
        )
    )
    reader = DBRouteReader(
        store=store,
        source_connectors={"baostock": baostock, "efinance": efinance},
        source_priority={
            Market.CN: {RESOURCE_COMPANY_PROFILE: ("baostock", "efinance")},
        },
    )

    profile = reader.get_company_profile(security_id=security_id)

    assert profile.company_name == "贵州茅台股份有限公司"
    assert baostock.profile_calls == 0
    assert efinance.profile_calls == 0
