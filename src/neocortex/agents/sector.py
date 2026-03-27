"""Sector synthesis agent implementation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import logging
from collections.abc import Sequence
from typing import Callable, Mapping

from neocortex.agents.base import Agent
from neocortex.market_data_provider import MarketDataProvider
from neocortex.models import (
    AgentExecutionTrace,
    AgentRequest,
    AgentResponse,
    AgentRole,
    CompanyProfile,
    SecurityId,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MetricRow:
    """One sector comparison row used by the appendix-style sector prompt."""

    section: str
    label: str
    value: str | int | float | None
    change: str | int | float | None = None
    peer_value: str | int | float | None = None


class SectorAgent(Agent):
    role = AgentRole.SECTOR

    def __init__(
        self,
        *,
        market_data: MarketDataProvider | None = None,
        config: Mapping[str, object],
        comparison_rows_loader: Callable[[SecurityId, date], Sequence[MetricRow]]
        | None = None,
    ) -> None:
        super().__init__(
            market_data=market_data,
            config=config,
        )
        self.comparison_rows_loader = comparison_rows_loader

    def build_request(
        self,
        *,
        request_id: str,
        security_id: SecurityId,
        as_of_date: date,
        company_profile: CompanyProfile | None = None,
        comparison_rows: Sequence[MetricRow] | None = None,
        analyst_reports: Sequence[AgentResponse] | None = None,
        trace_by_role: Mapping[AgentRole, AgentExecutionTrace] | None = None,
    ) -> AgentRequest:
        logger.info(
            f"Building sector request: security={security_id.ticker} as_of_date={as_of_date}"
        )
        if company_profile is None:
            company_profile = self.market_data.get_company_profile(security_id)
        if comparison_rows is None:
            if self.comparison_rows_loader is None:
                raise RuntimeError("SectorAgent requires a comparison_rows_loader.")
            comparison_rows = self.comparison_rows_loader(security_id, as_of_date)
        if analyst_reports is None:
            if trace_by_role is None:
                raise RuntimeError(
                    "SectorAgent requires analyst_reports or trace_by_role."
                )
            analyst_reports = tuple(
                _require_trace_response(trace_by_role, dependency)
                for dependency in self.dependencies
            )
        if company_profile.security_id != security_id:
            raise ValueError(
                "company_profile.security_id must match the request security_id."
            )
        if not comparison_rows:
            raise ValueError("SectorAgent requires at least one comparison row.")
        if not analyst_reports:
            raise ValueError("SectorAgent requires upstream analyst reports.")
        logger.info(
            f"Sector request inputs ready: security={security_id.ticker} "
            f"comparison_rows={len(comparison_rows)} analyst_reports={len(analyst_reports)}"
        )
        report_by_role = {report.agent: report for report in analyst_reports}
        payload = {
            "technical_report": _format_report(report_by_role, AgentRole.TECHNICAL),
            "quant_report": _format_report(report_by_role, AgentRole.QUANT_FUNDAMENTAL),
            "qualitative_report": _format_report(
                report_by_role, AgentRole.QUALITATIVE_FUNDAMENTAL
            ),
            "target": _build_sector_side(comparison_rows, use_peer=False),
            "sector": _build_sector_side(comparison_rows, use_peer=True),
        }
        return AgentRequest(
            request_id=request_id,
            agent=self.role,
            security_id=security_id,
            as_of_date=as_of_date,
            payload=payload,
            dependencies=self.dependencies,
        )

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
            reasoning=str(parsed_output["investment_thesis"]),
            score=float(parsed_output["score"]),
            raw_model_output=parsed_output,
        )


def _format_report(
    report_by_role: dict[AgentRole, AgentResponse],
    role: AgentRole,
) -> str:
    report = report_by_role.get(role)
    if report is None:
        raise ValueError(f"SectorAgent requires an upstream {role.value} report.")
    score = report.score if report.score is not None else "n/a"
    return f"<Score={score}, Comment={report.reasoning}>"


def _require_trace_response(
    trace_by_role: Mapping[AgentRole, AgentExecutionTrace],
    role: AgentRole,
) -> AgentResponse:
    trace = trace_by_role.get(role)
    if trace is None:
        raise RuntimeError(f"Missing trace for dependency {role.value}.")
    if trace.response is None:
        raise RuntimeError(f"{role.value} did not produce a response.")
    return trace.response


def _build_sector_side(
    comparison_rows: Sequence[MetricRow],
    *,
    use_peer: bool,
) -> dict[str, dict[str, str]]:
    side = {field: {"roc": "n/a"} for field in _SECTOR_ROW_FIELDS.values()}
    for row in comparison_rows:
        field = _SECTOR_ROW_FIELDS.get(row.label)
        if field is None:
            continue
        value = row.peer_value if use_peer else row.value
        side[field]["roc"] = "n/a" if value is None else str(value).removesuffix("%")
    return side


_SECTOR_ROW_FIELDS = {
    "Sales": "sales",
    "Sales YoY": "sales",
    "Op Profit": "op_profit",
    "Operating Profit": "op_profit",
    "Net Income": "net_income",
    "Cost of Sales": "cost_of_sales",
    "Depreciation": "depreciation",
    "Total Assets": "total_assets",
    "Equity": "equity",
    "Cash": "cash",
    "Receivables": "receivables",
    "Inventory": "inventory",
    "Financial Assets": "financial_assets",
    "Interest Bearing Debt": "interest_bearing_debt",
    "Cur. Liabilities": "cur_liabilities",
    "Issued Shares": "issued_shares",
    "Op CF": "op_cf",
    "Operating CF": "op_cf",
    "Investing CF": "investing_cf",
    "Dividends": "dividends",
    "Monthly Close": "monthly_close",
}
