from datetime import date, datetime
from typing import Any

import pytest
from jinja2 import UndefinedError

from neocortex.agents import (
    MacroInput,
    MacroMetric,
    MetricRow,
    QualitativeFundamentalInput,
    QuantFundamentalInput,
    QuantMetric,
)
from neocortex.llm import LLMEndpoint, LLMInferenceConfig, LLMRequestConfig, LLMService
from tests.core._market_data_provider_fakes import InMemoryMarketDataProvider
from neocortex.models import (
    AgentResponse,
    AgentRole,
    CompanyProfile,
    Exchange,
    FundamentalSnapshot,
    FundamentalStatement,
    FundamentalValueOrigin,
    Market,
    NewsItem,
    PriceBar,
    PriceSeries,
    SecurityId,
)
from neocortex.prompts import load_prompt_template, render_prompt_text
from neocortex.pipeline import Pipeline


class FakeTransport:
    def __init__(self, payloads: dict[AgentRole, dict[str, Any]]) -> None:
        self.payloads = payloads
        self.prompts: list[tuple[AgentRole, str, str]] = []

    def complete(
        self,
        *,
        agent: AgentRole,
        system_prompt: str,
        user_prompt: str,
        inference_config: LLMInferenceConfig,
    ) -> dict[str, Any]:
        self.prompts.append((agent, system_prompt, user_prompt))
        assert inference_config.endpoint.model == "gpt-test"
        return self.payloads[agent]


def _inference_config() -> LLMInferenceConfig:
    return LLMInferenceConfig(
        endpoint=LLMEndpoint(
            service=LLMService.OPENAI,
            model="gpt-test",
            base_url="https://api.openai.com/v1",
            auth_env_var="OPENAI_API_KEY",
        ),
        request=LLMRequestConfig(temperature=0.1, max_tokens=800),
    )


def _security_id() -> SecurityId:
    return SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG)


def _company_profile(security_id: SecurityId) -> CompanyProfile:
    return CompanyProfile(
        security_id=security_id,
        company_name="贵州茅台",
        sector="白酒",
        industry="白酒",
        country="CN",
        currency="CNY",
    )


def _price_series(security_id: SecurityId) -> PriceSeries:
    closes = (100.0, 102.0, 101.0, 105.0, 110.0)
    bars = tuple(
        PriceBar(
            security_id=security_id,
            timestamp=datetime(2026, 3, 9 + index, 15, 0),
            open=close - 1.0,
            high=close + 2.0,
            low=close - 2.0,
            close=close,
            volume=1_000_000.0 + (index * 10_000.0),
        )
        for index, close in enumerate(closes)
    )
    return PriceSeries(security_id=security_id, bars=bars)


def _fundamental_snapshots(security_id: SecurityId) -> tuple[FundamentalSnapshot, ...]:
    fetched_at = datetime(2026, 3, 13, 12, 0)
    return (
        FundamentalSnapshot(
            security_id=security_id,
            report_date=date(2025, 12, 31),
            ann_date=date(2026, 3, 10),
            fetch_at=fetched_at,
            statement=FundamentalStatement.NET_MARGIN,
            value=0.21,
            value_origin=FundamentalValueOrigin.FETCHED,
            source="baostock",
        ),
        FundamentalSnapshot(
            security_id=security_id,
            report_date=date(2025, 12, 31),
            ann_date=date(2026, 3, 10),
            fetch_at=fetched_at,
            statement=FundamentalStatement.ROE,
            value=0.18,
            value_origin=FundamentalValueOrigin.FETCHED,
            source="baostock",
        ),
        FundamentalSnapshot(
            security_id=security_id,
            report_date=date(2025, 12, 31),
            ann_date=date(2026, 3, 10),
            fetch_at=fetched_at,
            statement=FundamentalStatement.ASSET_TURN,
            value=0.8,
            value_origin=FundamentalValueOrigin.FETCHED,
            source="baostock",
        ),
        FundamentalSnapshot(
            security_id=security_id,
            report_date=date(2025, 12, 31),
            ann_date=date(2026, 3, 10),
            fetch_at=fetched_at,
            statement=FundamentalStatement.INV_TURN_DAYS,
            value=35.0,
            value_origin=FundamentalValueOrigin.FETCHED,
            source="baostock",
        ),
        FundamentalSnapshot(
            security_id=security_id,
            report_date=date(2025, 12, 31),
            ann_date=date(2026, 3, 10),
            fetch_at=fetched_at,
            statement=FundamentalStatement.EQUITY_RATIO,
            value=0.55,
            value_origin=FundamentalValueOrigin.DERIVED,
            source="baostock",
        ),
        FundamentalSnapshot(
            security_id=security_id,
            report_date=date(2025, 12, 31),
            ann_date=date(2026, 3, 10),
            fetch_at=fetched_at,
            statement=FundamentalStatement.QUICK_RATIO,
            value=1.4,
            value_origin=FundamentalValueOrigin.FETCHED,
            source="baostock",
        ),
        FundamentalSnapshot(
            security_id=security_id,
            report_date=date(2025, 12, 31),
            ann_date=date(2026, 3, 10),
            fetch_at=fetched_at,
            statement=FundamentalStatement.DE_RATIO,
            value=0.3,
            value_origin=FundamentalValueOrigin.DERIVED,
            source="baostock",
        ),
        FundamentalSnapshot(
            security_id=security_id,
            report_date=date(2025, 12, 31),
            ann_date=date(2026, 3, 10),
            fetch_at=fetched_at,
            statement=FundamentalStatement.SALES_YOY,
            value=0.10,
            value_origin=FundamentalValueOrigin.FETCHED,
            source="baostock",
        ),
        FundamentalSnapshot(
            security_id=security_id,
            report_date=date(2025, 12, 31),
            ann_date=date(2026, 3, 10),
            fetch_at=fetched_at,
            statement=FundamentalStatement.EPS_TTM,
            value=4.5,
            value_origin=FundamentalValueOrigin.FETCHED,
            source="baostock",
        ),
        FundamentalSnapshot(
            security_id=security_id,
            report_date=date(2025, 12, 31),
            ann_date=date(2026, 3, 10),
            fetch_at=fetched_at,
            statement=FundamentalStatement.EPS_GROWTH,
            value=0.12,
            value_origin=FundamentalValueOrigin.FETCHED,
            source="baostock",
        ),
        FundamentalSnapshot(
            security_id=security_id,
            report_date=date(2025, 9, 30),
            ann_date=date(2025, 10, 25),
            fetch_at=datetime(2025, 10, 25, 12, 0),
            statement=FundamentalStatement.NET_MARGIN,
            value=0.19,
            value_origin=FundamentalValueOrigin.FETCHED,
            source="baostock",
        ),
        FundamentalSnapshot(
            security_id=security_id,
            report_date=date(2025, 9, 30),
            ann_date=date(2025, 10, 25),
            fetch_at=datetime(2025, 10, 25, 12, 0),
            statement=FundamentalStatement.ROE,
            value=0.16,
            value_origin=FundamentalValueOrigin.FETCHED,
            source="baostock",
        ),
        FundamentalSnapshot(
            security_id=security_id,
            report_date=date(2025, 9, 30),
            ann_date=date(2025, 10, 25),
            fetch_at=datetime(2025, 10, 25, 12, 0),
            statement=FundamentalStatement.EPS_TTM,
            value=4.0,
            value_origin=FundamentalValueOrigin.FETCHED,
            source="baostock",
        ),
    )


def _agent_reports(security_id: SecurityId) -> tuple[AgentResponse, ...]:
    return (
        AgentResponse(
            request_id="req-tech",
            agent=AgentRole.TECHNICAL,
            security_id=security_id,
            as_of_date=date(2026, 3, 13),
            reasoning="Momentum remains constructive.",
            score=72.0,
        ),
        AgentResponse(
            request_id="req-quant",
            agent=AgentRole.QUANT_FUNDAMENTAL,
            security_id=security_id,
            as_of_date=date(2026, 3, 13),
            reasoning="Margins and cash flow are stable.",
            score=68.0,
        ),
        AgentResponse(
            request_id="req-qual",
            agent=AgentRole.QUALITATIVE_FUNDAMENTAL,
            security_id=security_id,
            as_of_date=date(2026, 3, 13),
            reasoning="Management execution is improving.",
            score=None,
            raw_model_output={
                "business_momentum": 4,
                "immediate_risk": 3,
                "management_trust": 4,
                "insight": "Management execution is improving.",
            },
        ),
        AgentResponse(
            request_id="req-news",
            agent=AgentRole.NEWS,
            security_id=security_id,
            as_of_date=date(2026, 3, 13),
            reasoning="Product and channel news remain supportive.",
            score=None,
            raw_model_output={
                "return_outlook": 4,
                "risk_outlook": 2,
                "reason": "Product and channel news remain supportive.",
            },
        ),
    )


def _pipeline() -> Pipeline:
    return Pipeline(
        market_data=InMemoryMarketDataProvider(
            company_profiles={_security_id(): _company_profile(_security_id())},
            price_bars={_security_id(): _price_series(_security_id())},
            fundamentals={_security_id(): _fundamental_snapshots(_security_id())},
        ),
    )


def test_registry_exposes_all_agent_roles() -> None:
    pipeline = _pipeline()
    agents = tuple(pipeline.get_agent(role) for role in AgentRole)
    assert tuple(agent.role for agent in agents) == tuple(AgentRole)
    assert pipeline.get_agent(AgentRole.TECHNICAL).role is AgentRole.TECHNICAL
    assert pipeline.get_agent(AgentRole.PM).role is AgentRole.PM


def test_render_prompt_raises_when_required_context_is_missing() -> None:
    template = load_prompt_template("macro_fine.yaml")
    with pytest.raises(UndefinedError):
        render_prompt_text(template.system, indicator_rows=[])


def test_render_prompt_decimal_filter_formats_numeric_values() -> None:
    rendered = render_prompt_text(
        "{{ value|decimal(4) }}|{{ none_value|decimal(4) }}",
        value=1.2,
        none_value=None,
    )

    assert rendered == "1.2000|None"


def test_technical_agent_builds_request_and_uses_market() -> None:
    security_id = _security_id()
    pipeline = _pipeline()
    agent = pipeline.get_agent(AgentRole.TECHNICAL)

    request = agent.build_request(
        request_id="req-tech-001",
        security_id=security_id,
        as_of_date=date(2026, 3, 13),
    )
    system_prompt, user_prompt = agent.render_prompts(request)
    prompt = f"System Prompt:\n{system_prompt}\n\nUser Prompt:\n{user_prompt}"

    assert request.agent is AgentRole.TECHNICAL
    assert request.payload == {}
    assert "You are a technical analyst on the trading team." in prompt
    assert "Technical indicators used:" in prompt
    assert "RoC 5day: None%" in prompt
    assert "MACD: None" in prompt
    assert "CN" in prompt
    assert '"score": <integer 0-100>' in prompt
    assert system_prompt.startswith("Role:")
    assert user_prompt.startswith("Instruction:")
    assert pipeline.market_data.last_bars_call == (
        security_id,
        date(2025, 2, 6),
        date(2026, 3, 13),
        "qfq",
    )


def test_quant_and_macro_agents_build_appendix_style_prompts() -> None:
    security_id = _security_id()
    pipeline = _pipeline()
    quant_agent = pipeline.get_agent(AgentRole.QUANT_FUNDAMENTAL)
    macro_agent = pipeline.get_agent(AgentRole.MACRO)

    quant_request = quant_agent.build_request(
        request_id="req-quant-001",
        security_id=security_id,
        as_of_date=date(2026, 3, 13),
        company_profile=_company_profile(security_id),
        fundamentals=QuantFundamentalInput(
            current_date="2026-03-13",
            net_margin=QuantMetric(value="21%", diff="+1%"),
            roa=QuantMetric(value="12%", diff="+1%"),
            roe=QuantMetric(value="18%", diff="+2%"),
            asset_turn=QuantMetric(value="0.8", diff="+0.1"),
            inv_turn_days=QuantMetric(value="35", diff="-2"),
            per=QuantMetric(value="28", diff="+1"),
            fcf=QuantMetric(value="45", diff="+3%"),
            fcf_margin=QuantMetric(value="15%", diff="+1%"),
            ebitda=QuantMetric(value="60", diff="+4%"),
            equity_ratio=QuantMetric(value="55%", diff="+1%"),
            quick_ratio=QuantMetric(value="1.4", diff="+0.1"),
            de_ratio=QuantMetric(value="0.3", diff="-0.1"),
            sales_yoy=QuantMetric(value="10%", diff="+2%"),
            cagr_3y=QuantMetric(value="8%", diff="+1%"),
            eps_growth=QuantMetric(value="12%", diff="+3%"),
            dps=QuantMetric(value="4.2", diff="+0.2"),
        ),
    )
    macro_request = macro_agent.build_request(
        request_id="req-macro-001",
        security_id=SecurityId(symbol="AAPL", market=Market.US, exchange=Exchange.XNAS),
        as_of_date=date(2026, 3, 13),
        macro_data=MacroInput(
            us_fed_rate=MacroMetric(value="5.25%", roc="0.0%"),
            us_10y_yield=MacroMetric(value="4.2%", roc="+0.1%"),
            jp_policy_rate=MacroMetric(value="0.5%", roc="0.0%"),
            jp_10y_yield=MacroMetric(value="1.1%", roc="+0.1%"),
            cn_policy_rate=MacroMetric(value="1.5%", roc="0.0%"),
            cn_10y_yield=MacroMetric(value="2.3%", roc="+0.1%"),
            hk_base_rate=MacroMetric(value="5.75%", roc="0.0%"),
            hk_10y_yield=MacroMetric(value="3.1%", roc="+0.1%"),
        ),
    )

    quant_prompt = (
        f"System Prompt:\n{quant_agent.render_prompts(quant_request)[0]}\n\n"
        f"User Prompt:\n{quant_agent.render_prompts(quant_request)[1]}"
    )
    macro_prompt = (
        f"System Prompt:\n{macro_agent.render_prompts(macro_request)[0]}\n\n"
        f"User Prompt:\n{macro_agent.render_prompts(macro_request)[1]}"
    )

    assert "Role: You are a Quantitative Fundamental Analyst." in quant_prompt
    assert "Current Date: 2026-03-13" in quant_prompt
    assert "Net Margin: 21% (diff: +1%)" in quant_prompt
    assert "latest visible reporting period" in quant_prompt
    assert "US" in macro_prompt
    assert "macro indicators relevant to US" in macro_prompt
    assert "CN Policy Rate: 1.5% (RoC: 0.0%)" in macro_prompt
    assert "HK Base Rate: 5.75% (RoC: 0.0%)" in macro_prompt


def test_agent_send_and_run_wrap_transport_output_into_response_and_trace() -> None:
    security_id = _security_id()
    transport = FakeTransport(
        {
            AgentRole.TECHNICAL: {
                "score": 74,
                "reason": "Trend and momentum are aligned.",
            }
        }
    )
    agent = _pipeline().get_agent(AgentRole.TECHNICAL)

    trace = agent.run(
        request_id="req-tech-run-001",
        security_id=security_id,
        as_of_date=date(2026, 3, 13),
        inference_config=_inference_config(),
        transport=transport,
    )

    assert trace.response is not None
    assert trace.response.score == 74.0
    assert trace.response.reasoning == "Trend and momentum are aligned."
    assert transport.prompts[0][0] is AgentRole.TECHNICAL
    assert transport.prompts[0][1].startswith("Role:")
    assert transport.prompts[0][2].startswith("Instruction:")


def test_quant_agent_builds_render_context_from_market_data() -> None:
    security_id = _security_id()
    agent = _pipeline().get_agent(AgentRole.QUANT_FUNDAMENTAL)

    request = agent.build_request(
        request_id="req-quant-runtime-001",
        security_id=security_id,
        as_of_date=date(2026, 3, 13),
    )
    system_prompt, user_prompt = agent.render_prompts(request)
    prompt = f"{system_prompt}\n\n{user_prompt}"

    assert "Current Date: 2026-03-13" in prompt
    assert "Net Margin: 21.00% (diff: +2.00%)" in prompt
    assert "ROE: 18.00% (diff: +2.00%)" in prompt
    assert "PER: 24.44 (diff: -3.06)" in prompt
    assert "FCF: n/a (diff: n/a)" in prompt


def test_quant_agent_sets_per_to_na_when_ttm_eps_is_missing() -> None:
    security_id = _security_id()
    provider = InMemoryMarketDataProvider(
        company_profiles={security_id: _company_profile(security_id)},
        price_bars={security_id: _price_series(security_id)},
        fundamentals={
            security_id: tuple(
                snapshot
                for snapshot in _fundamental_snapshots(security_id)
                if snapshot.statement is not FundamentalStatement.EPS_TTM
            )
        },
    )
    agent = Pipeline(market_data=provider).get_agent(AgentRole.QUANT_FUNDAMENTAL)

    request = agent.build_request(
        request_id="req-quant-no-ttm-eps-001",
        security_id=security_id,
        as_of_date=date(2026, 3, 13),
    )
    prompt = "\n\n".join(agent.render_prompts(request))

    assert "PER: n/a (diff: n/a)" in prompt


def test_quant_agent_rejects_non_cn_market() -> None:
    security_id = SecurityId(symbol="AAPL", market=Market.US, exchange=Exchange.XNAS)
    provider = InMemoryMarketDataProvider(
        company_profiles={security_id: _company_profile(security_id)},
        price_bars={security_id: _price_series(security_id)},
    )
    agent = Pipeline(market_data=provider).get_agent(AgentRole.QUANT_FUNDAMENTAL)
    request = agent.build_request(
        request_id="req-quant-us-001",
        security_id=security_id,
        as_of_date=date(2026, 3, 13),
    )

    with pytest.raises(ValueError, match="supports only CN"):
        agent.render_prompts(request)


def test_other_agents_build_expected_request_shapes() -> None:
    security_id = _security_id()

    pipeline = _pipeline()
    qualitative_request = pipeline.get_agent(
        AgentRole.QUALITATIVE_FUNDAMENTAL
    ).build_request(
        request_id="req-qual-001",
        security_id=security_id,
        as_of_date=date(2026, 3, 13),
        company_profile=_company_profile(security_id),
        disclosures=QualitativeFundamentalInput(
            info_update="Yes",
            overview="Channel expansion remains on track.",
            risks="Input-cost pressure persists.",
            mda="Sell-through remains healthy.",
            governance="Board oversight remains stable.",
        ),
    )
    news_request = pipeline.get_agent(AgentRole.NEWS).build_request(
        request_id="req-news-001",
        security_id=security_id,
        as_of_date=date(2026, 3, 13),
        company_profile=_company_profile(security_id),
        news_items=(
            NewsItem(
                security_id=security_id,
                published_at=datetime(2026, 3, 12, 9, 0),
                source="ExampleWire",
                title="Premium product launch",
                summary="The company launched a premium line.",
                url="https://example.com/news/1",
            ),
        ),
    )
    sector_request = pipeline.get_agent(AgentRole.SECTOR).build_request(
        request_id="req-sector-001",
        security_id=security_id,
        as_of_date=date(2026, 3, 13),
        company_profile=_company_profile(security_id),
        comparison_rows=(
            MetricRow(
                section="Growth",
                label="Sales YoY",
                value="+12%",
                peer_value="+8%",
                change="+2%",
            ),
        ),
        analyst_reports=_agent_reports(security_id),
    )
    pm_request = pipeline.get_agent(AgentRole.PM).build_request(
        request_id="req-pm-001",
        security_id=security_id,
        as_of_date=date(2026, 3, 13),
        macro_report=AgentResponse(
            request_id="req-macro",
            agent=AgentRole.MACRO,
            security_id=security_id,
            as_of_date=date(2026, 3, 13),
            reasoning="Liquidity conditions remain supportive.",
            score=63.0,
        ),
        sector_report=AgentResponse(
            request_id="req-sector",
            agent=AgentRole.SECTOR,
            security_id=security_id,
            as_of_date=date(2026, 3, 13),
            reasoning="The stock is still leading its sector.",
            score=71.0,
        ),
    )

    assert qualitative_request.dependencies == ()
    assert news_request.dependencies == ()
    assert sector_request.dependencies == (
        AgentRole.TECHNICAL,
        AgentRole.QUANT_FUNDAMENTAL,
        AgentRole.QUALITATIVE_FUNDAMENTAL,
        AgentRole.NEWS,
    )
    assert pm_request.dependencies == (AgentRole.MACRO, AgentRole.SECTOR)
