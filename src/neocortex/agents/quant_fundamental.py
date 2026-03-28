"""Quantitative fundamental agent implementation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import logging
from typing import Mapping

from neocortex.agents.base import Agent
from neocortex.models import (
    AgentRequest,
    AgentResponse,
    AgentRole,
    CompanyProfile,
    FundamentalSnapshot,
    FundamentalStatement,
    Market,
    PRICE_BAR_CLOSE,
    SecurityId,
)
from neocortex.serialization import to_json_ready

logger = logging.getLogger(__name__)

_PER_LOOKBACK_DAYS = 30
_PERCENT_STATEMENTS = frozenset(
    {
        FundamentalStatement.NET_MARGIN,
        FundamentalStatement.ROA,
        FundamentalStatement.ROE,
        FundamentalStatement.FCF_MARGIN,
        FundamentalStatement.EQUITY_RATIO,
        FundamentalStatement.SALES_YOY,
        FundamentalStatement.CAGR_3Y,
        FundamentalStatement.EPS_GROWTH,
    }
)
_RATIO_STATEMENTS = frozenset(
    {
        FundamentalStatement.ASSET_TURN,
        FundamentalStatement.QUICK_RATIO,
        FundamentalStatement.DE_RATIO,
    }
)
_VALUE_STATEMENTS = frozenset(
    {
        FundamentalStatement.FCF,
        FundamentalStatement.EBITDA,
        FundamentalStatement.DPS,
    }
)
_PROMPT_STATEMENTS = {
    "net_margin": FundamentalStatement.NET_MARGIN,
    "roa": FundamentalStatement.ROA,
    "roe": FundamentalStatement.ROE,
    "asset_turn": FundamentalStatement.ASSET_TURN,
    "inv_turn_days": FundamentalStatement.INV_TURN_DAYS,
    "fcf": FundamentalStatement.FCF,
    "fcf_margin": FundamentalStatement.FCF_MARGIN,
    "ebitda": FundamentalStatement.EBITDA,
    "equity_ratio": FundamentalStatement.EQUITY_RATIO,
    "quick_ratio": FundamentalStatement.QUICK_RATIO,
    "de_ratio": FundamentalStatement.DE_RATIO,
    "sales_yoy": FundamentalStatement.SALES_YOY,
    "cagr_3y": FundamentalStatement.CAGR_3Y,
    "eps_growth": FundamentalStatement.EPS_GROWTH,
    "dps": FundamentalStatement.DPS,
}


@dataclass(frozen=True, slots=True)
class QuantMetric:
    """One explicit metric cell in the appendix quant template."""

    value: str = "n/a"
    diff: str = "n/a"


@dataclass(frozen=True, slots=True)
class QuantFundamentalInput:
    """Explicit appendix-shaped input for the quant agent template."""

    current_date: str = "n/a"
    net_margin: QuantMetric = QuantMetric()
    roa: QuantMetric = QuantMetric()
    roe: QuantMetric = QuantMetric()
    asset_turn: QuantMetric = QuantMetric()
    inv_turn_days: QuantMetric = QuantMetric()
    per: QuantMetric = QuantMetric()
    fcf: QuantMetric = QuantMetric()
    fcf_margin: QuantMetric = QuantMetric()
    ebitda: QuantMetric = QuantMetric()
    equity_ratio: QuantMetric = QuantMetric()
    quick_ratio: QuantMetric = QuantMetric()
    de_ratio: QuantMetric = QuantMetric()
    sales_yoy: QuantMetric = QuantMetric()
    cagr_3y: QuantMetric = QuantMetric()
    eps_growth: QuantMetric = QuantMetric()
    dps: QuantMetric = QuantMetric()


class QuantFundamentalAgent(Agent):
    role = AgentRole.QUANT_FUNDAMENTAL

    def build_request(
        self,
        *,
        request_id: str,
        security_id: SecurityId,
        as_of_date: date,
        company_profile: CompanyProfile | None = None,
        fundamentals: QuantFundamentalInput | None = None,
        trace_by_role: Mapping[AgentRole, object] | None = None,
    ) -> AgentRequest:
        _ = trace_by_role
        if company_profile is not None and company_profile.security_id != security_id:
            raise ValueError(
                "company_profile.security_id must match the request security_id."
            )
        return AgentRequest(
            request_id=request_id,
            agent=self.role,
            security_id=security_id,
            as_of_date=as_of_date,
            payload={} if fundamentals is None else to_json_ready(fundamentals),
        )

    def build_render_context(self, request: AgentRequest) -> dict[str, object]:
        if request.security_id.market is not Market.CN:
            raise ValueError("QuantFundamentalAgent currently supports only CN.")
        if request.payload:
            return dict(request.payload)
        logger.info(
            f"Building quant render context: security={request.security_id.ticker} "
            f"as_of_date={request.as_of_date}"
        )
        fundamentals = self.market_data.get_fundamental_snapshots(
            request.security_id,
            as_of_date=request.as_of_date,
        )
        if not fundamentals:
            raise ValueError("QuantFundamentalAgent requires visible fundamentals.")
        quant_input = _build_quant_input(
            security_id=request.security_id,
            as_of_date=request.as_of_date,
            fundamentals=fundamentals,
            latest_close=_latest_close(
                market_data=self.market_data,
                security_id=request.security_id,
                as_of_date=request.as_of_date,
            ),
        )
        return to_json_ready(quant_input)

    def build_response(
        self,
        request: AgentRequest,
        parsed_output: dict[str, object],
    ) -> AgentResponse:
        return AgentResponse(
            request_id=request.request_id,
            agent=request.agent,
            security_id=request.security_id,
            as_of_date=request.as_of_date,
            reasoning=str(parsed_output["reason"]),
            score=float(parsed_output["score"]),
            raw_model_output=parsed_output,
        )


def _build_quant_input(
    *,
    security_id: SecurityId,
    as_of_date: date,
    fundamentals: tuple[FundamentalSnapshot, ...],
    latest_close: float | None,
) -> QuantFundamentalInput:
    rows_by_report_date: dict[
        date, dict[FundamentalStatement, FundamentalSnapshot]
    ] = {}
    for snapshot in fundamentals:
        rows_by_report_date.setdefault(snapshot.report_date, {})[snapshot.statement] = (
            snapshot
        )
    report_dates = sorted(rows_by_report_date, reverse=True)
    if not report_dates:
        raise ValueError("QuantFundamentalAgent requires visible fundamentals.")
    latest_report_date = report_dates[0]
    previous_report_date = report_dates[1] if len(report_dates) > 1 else None
    latest_rows = rows_by_report_date[latest_report_date]
    previous_rows = (
        rows_by_report_date[previous_report_date] if previous_report_date else {}
    )
    metrics = {
        name: _build_metric(
            statement=statement,
            latest=latest_rows.get(statement),
            previous=previous_rows.get(statement),
        )
        for name, statement in _PROMPT_STATEMENTS.items()
    }
    latest_eps = latest_rows.get(FundamentalStatement.EPS_TTM)
    previous_eps = previous_rows.get(FundamentalStatement.EPS_TTM)
    per = _build_ratio_metric(
        latest_value=None
        if latest_close is None or latest_eps is None
        else latest_close / latest_eps.value,
        previous_value=(
            None
            if latest_close is None or previous_eps is None
            else latest_close / previous_eps.value
        ),
    )
    return QuantFundamentalInput(
        current_date=as_of_date.isoformat(),
        per=per,
        **metrics,
    )


def _build_metric(
    *,
    statement: FundamentalStatement,
    latest: FundamentalSnapshot | None,
    previous: FundamentalSnapshot | None,
) -> QuantMetric:
    if latest is None:
        return QuantMetric()
    diff = None if previous is None else latest.value - previous.value
    return QuantMetric(
        value=_format_metric(statement, latest.value, signed=False),
        diff="n/a" if diff is None else _format_metric(statement, diff, signed=True),
    )


def _build_ratio_metric(
    *,
    latest_value: float | None,
    previous_value: float | None,
) -> QuantMetric:
    if latest_value is None:
        return QuantMetric()
    diff = None if previous_value is None else latest_value - previous_value
    return QuantMetric(
        value=_format_decimal(latest_value),
        diff="n/a" if diff is None else _format_decimal(diff, signed=True),
    )


def _latest_close(
    *,
    market_data,
    security_id: SecurityId,
    as_of_date: date,
) -> float | None:
    series = market_data.get_price_bars(
        security_id,
        start_date=as_of_date - timedelta(days=_PER_LOOKBACK_DAYS),
        end_date=as_of_date,
        adjust=None,
    )
    if not series:
        return None
    return float(series.bars.iloc[-1][PRICE_BAR_CLOSE])


def _format_metric(
    statement: FundamentalStatement,
    value: float,
    *,
    signed: bool,
) -> str:
    if statement in _PERCENT_STATEMENTS:
        return _format_percent(value, signed=signed)
    if statement in _RATIO_STATEMENTS:
        return _format_decimal(value, signed=signed)
    if statement is FundamentalStatement.INV_TURN_DAYS:
        return _format_days(value, signed=signed)
    if statement in _VALUE_STATEMENTS:
        return _format_decimal(value, signed=signed)
    return _format_decimal(value, signed=signed)


def _format_percent(value: float, *, signed: bool) -> str:
    scaled = value * 100.0
    return f"{scaled:+.2f}%" if signed else f"{scaled:.2f}%"


def _format_decimal(value: float, *, signed: bool = False) -> str:
    rendered = f"{value:+.2f}" if signed else f"{value:.2f}"
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def _format_days(value: float, *, signed: bool) -> str:
    rendered = f"{value:+.1f}" if signed else f"{value:.1f}"
    if rendered.endswith(".0"):
        rendered = rendered[:-2]
    return rendered
