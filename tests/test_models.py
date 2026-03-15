from datetime import date, datetime

import pytest

from neocortex.llm import LLMEndpoint, LLMInferenceConfig, LLMRequestConfig, LLMService
from neocortex.models.agent import (
    AgentExecutionTrace,
    AgentRequest,
    AgentResponse,
    AgentRole,
    ResponseValidationStatus,
)
from neocortex.models.core import (
    CompanyProfile,
    DataProvider,
    FundamentalSnapshot,
    Market,
    MarketContext,
    PriceBar,
    SecurityId,
    SectorBenchmark,
)


@pytest.fixture
def security_id() -> SecurityId:
    return SecurityId(symbol="AAPL", market=Market.US, exchange="NASDAQ")


def test_core_models_store_normalized_entities(security_id: SecurityId) -> None:
    profile = CompanyProfile(
        security_id=security_id,
        company_name="Apple Inc.",
        sector="Technology",
        industry="Consumer Electronics",
        country="US",
        currency="USD",
    )
    market_context = MarketContext(
        market=Market.US,
        region="North America",
        timezone="America/New_York",
        trading_currency="USD",
        benchmark_symbol="SPY",
        trading_calendar="XNYS",
    )
    bar = PriceBar(
        security_id=security_id,
        timestamp=datetime(2026, 3, 15, 9, 30),
        open=210.0,
        high=212.0,
        low=208.5,
        close=211.4,
        volume=10_000_000,
    )
    benchmark = SectorBenchmark(
        market=Market.US,
        sector="Technology",
        as_of_date=date(2026, 3, 15),
        metric_averages={"roe": 0.18},
        constituents=("AAPL", "MSFT"),
    )

    assert profile.security_id.ticker == "US:AAPL"
    assert market_context.trading_calendar == "XNYS"
    assert bar.close == 211.4
    assert benchmark.metric_averages["roe"] == 0.18


def test_fundamental_snapshot_tracks_source_provider_without_symbol_mapping(
    security_id: SecurityId,
) -> None:
    snapshot = FundamentalSnapshot(
        security_id=security_id,
        as_of_date=date(2026, 3, 15),
        period_label="TTM",
        raw_items={"revenue": 100.0},
        derived_metrics={"roe": 0.18},
        source=DataProvider.YAHOO_FINANCE,
    )

    assert snapshot.source is DataProvider.YAHOO_FINANCE
    assert snapshot.security_id.ticker == "US:AAPL"


def test_agent_trace_captures_request_and_response_contract(
    security_id: SecurityId,
) -> None:
    request = AgentRequest(
        request_id="req-20260315-001",
        agent=AgentRole.TECHNICAL,
        security_id=security_id,
        as_of_date=date(2026, 3, 15),
        schema_version="v1",
        payload={"roc_20d": 0.12},
    )
    response = AgentResponse(
        request_id="req-20260315-001",
        agent=AgentRole.TECHNICAL,
        security_id=security_id,
        as_of_date=date(2026, 3, 15),
        schema_version="v1",
        reasoning="Momentum remains constructive.",
        score=74.0,
        confidence=0.81,
    )
    trace = AgentExecutionTrace(
        request=request,
        response=response,
        prompt_version="technical-v1",
        inference_config=LLMInferenceConfig(
            endpoint=LLMEndpoint(
                service=LLMService.OPENAI,
                model="gpt-test",
                base_url="https://api.openai.com/v1",
                auth_env_var="OPENAI_API_KEY",
            ),
            request=LLMRequestConfig(
                temperature=0.2,
                max_tokens=800,
            ),
        ),
        started_at=datetime(2026, 3, 15, 10, 0),
        response_validation_status=ResponseValidationStatus.PASSED,
    )

    assert trace.request.agent is AgentRole.TECHNICAL
    assert trace.request.request_id == "req-20260315-001"
    assert trace.request.security_id.market is Market.US
    assert trace.response is not None
    assert trace.response.request_id == "req-20260315-001"
    assert trace.inference_config.endpoint.model == "gpt-test"
    assert trace.inference_config.request.temperature == 0.2
    assert trace.response.security_id.exchange == "NASDAQ"
    assert trace.response.reasoning == "Momentum remains constructive."
    assert trace.response_validation_status is ResponseValidationStatus.PASSED


@pytest.mark.parametrize(
    ("market", "symbol", "expected_ticker"),
    [
        (Market.US, "AAPL", "US:AAPL"),
        (Market.JP, "7203", "JP:7203"),
        (Market.HK, "0700", "HK:0700"),
    ],
)
def test_security_id_builds_market_scoped_ticker(
    market: Market,
    symbol: str,
    expected_ticker: str,
) -> None:
    assert (
        SecurityId(symbol=symbol, market=market, exchange="TEST").ticker
        == expected_ticker
    )
