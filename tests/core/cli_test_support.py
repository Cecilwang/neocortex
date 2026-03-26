"""Shared fakes and helpers for CLI command tests."""

from __future__ import annotations

from datetime import date, datetime

from neocortex.connectors.types import TradingDateRecord
from neocortex.models import (
    AgentRequest,
    AgentRole,
    Exchange,
    Market,
    PriceBar,
    PriceSeries,
    SecurityId,
)


class FakeAkShareConnector:
    last_timeout: float | None = None
    last_profile_security_id: SecurityId | None = None
    last_daily_call: tuple[SecurityId, date, date] | None = None
    last_adjusted_daily_call: tuple[SecurityId, date, date, str] | None = None

    def __init__(self, *, timeout: float | None = None) -> None:
        self.last_timeout = timeout

    def list_securities(self, *, market: Market):
        return (
            {
                "market": market.value,
                "symbol": "600519",
            },
        )

    def get_security_profile_snapshot(self, security_id: SecurityId):
        self.last_profile_security_id = security_id
        return {
            "source": "akshare",
            "security_id": {
                "symbol": security_id.symbol,
                "market": security_id.market.value,
                "exchange": security_id.exchange.value,
            },
            "provider_company_name": "贵州茅台",
        }

    def get_daily_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
    ):
        self.last_daily_call = (security_id, start_date, end_date)
        return (
            {
                "source": "akshare",
                "security_id": {
                    "symbol": security_id.symbol,
                    "market": security_id.market.value,
                    "exchange": security_id.exchange.value,
                },
                "trade_date": "2026-03-14",
                "close": 1515.0,
            },
        )

    def get_adjusted_daily_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        adjustment_type: str,
    ):
        self.last_adjusted_daily_call = (
            security_id,
            start_date,
            end_date,
            adjustment_type,
        )
        return (
            {
                "source": "akshare",
                "security_id": {
                    "symbol": security_id.symbol,
                    "market": security_id.market.value,
                    "exchange": security_id.exchange.value,
                },
                "trade_date": "2026-03-14",
                "close": 1515.0,
                "adjustment_type": adjustment_type,
            },
        )


class FakeProvider:
    def __init__(self) -> None:
        self.last_profile_security_id: SecurityId | None = None
        self.last_bars_call: tuple[SecurityId, date, date, str | None] | None = None
        self.last_disclosures_call: tuple[SecurityId, date] | None = None
        self.last_macro_call: tuple[Market, date] | None = None
        self.list_securities_calls: list[Market] = []
        self.bar_calls: list[tuple[SecurityId, date, date, str | None]] = []
        self.trading_dates_calls: list[tuple[Market, date, date]] = []

    def list_securities(self, *, market: Market):
        self.list_securities_calls.append(market)
        return (SecurityId(symbol="600519", market=market, exchange=Exchange.XSHG),)

    def get_company_profile(self, security_id: SecurityId):
        self.last_profile_security_id = security_id
        return {
            "security_id": {
                "symbol": security_id.symbol,
                "market": security_id.market.value,
                "exchange": security_id.exchange.value,
            },
            "company_name": "贵州茅台",
        }

    def get_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        interval: str = "1d",
        adjust: str | None = None,
    ) -> PriceSeries:
        assert interval == "1d"
        self.last_bars_call = (security_id, start_date, end_date, adjust)
        self.bar_calls.append((security_id, start_date, end_date, adjust))
        return PriceSeries(
            security_id=security_id,
            bars=(
                PriceBar(
                    security_id=security_id,
                    timestamp=datetime(2026, 3, 14, 15, 0),
                    open=1500.0,
                    high=1520.0,
                    low=1498.0,
                    close=1515.0,
                    volume=120000.0,
                ),
                PriceBar(
                    security_id=security_id,
                    timestamp=datetime(2026, 3, 15, 15, 0),
                    open=1510.0,
                    high=1533.0,
                    low=1505.0,
                    close=1528.0,
                    volume=110000.0,
                ),
            ),
        )

    def get_fundamental_snapshots(self, security_id: SecurityId, *, as_of_date: date):
        return [{"symbol": security_id.symbol, "as_of_date": as_of_date.isoformat()}]

    def get_disclosure_sections(self, security_id: SecurityId, *, as_of_date: date):
        self.last_disclosures_call = (security_id, as_of_date)
        return [{"symbol": security_id.symbol, "as_of_date": as_of_date.isoformat()}]

    def get_macro_points(self, *, market: Market, as_of_date: date):
        self.last_macro_call = (market, as_of_date)
        return [{"market": market.value, "as_of_date": as_of_date.isoformat()}]

    def get_trading_dates(
        self,
        *,
        market: Market,
        start_date: date,
        end_date: date,
    ) -> tuple[TradingDateRecord, ...]:
        self.trading_dates_calls.append((market, start_date, end_date))
        records: list[TradingDateRecord] = []
        current = start_date
        while current <= end_date:
            records.append(
                TradingDateRecord(
                    source="baostock",
                    market=market,
                    calendar="XSHG",
                    trade_date=current.isoformat(),
                    is_trading_day=current.weekday() < 5,
                )
            )
            current = current.fromordinal(current.toordinal() + 1)
        return tuple(records)

    def is_trading_day(self, *, market: Market, trade_date: date) -> bool:
        _ = market
        return trade_date.weekday() < 5

    def get_previous_trading_date(self, *, market: Market, trade_date: date) -> date:
        _ = market
        candidate = trade_date.fromordinal(trade_date.toordinal() - 1)
        while candidate.weekday() >= 5:
            candidate = candidate.fromordinal(candidate.toordinal() - 1)
        return candidate


class FakeProviderFactory:
    created_db_paths: list[str] = []
    provider = FakeProvider()

    @classmethod
    def from_defaults(cls, db_path: str):
        cls.created_db_paths.append(str(db_path))
        return cls.provider


class FakeAgent:
    def __init__(self, role: AgentRole) -> None:
        self.role = role

    def build_request(
        self,
        *,
        request_id: str,
        security_id: SecurityId,
        as_of_date: date,
        trace_by_role=None,
    ) -> AgentRequest:
        _ = trace_by_role
        return AgentRequest(
            request_id=request_id,
            agent=self.role,
            security_id=security_id,
            as_of_date=as_of_date,
        )

    def render_prompts(self, request: AgentRequest) -> tuple[str, str]:
        return (
            f"system:{request.security_id.ticker}",
            f"user:{request.as_of_date.isoformat()}",
        )


class FakePipeline:
    def __init__(self, *, market_data) -> None:
        self.market_data = market_data

    def get_agent(self, role: AgentRole) -> FakeAgent:
        return FakeAgent(role)


def reset_fake_provider_state() -> None:
    FakeProviderFactory.created_db_paths.clear()
    FakeProviderFactory.provider = FakeProvider()
