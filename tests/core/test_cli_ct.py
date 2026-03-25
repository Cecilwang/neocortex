import json
import importlib
import sqlite3
from datetime import date, datetime

import pytest

from tests.core.feishu_storage_test_support import seed_cleanup_test_data
from neocortex.cli.common import find_security_ids_by_name
from neocortex.connectors.types import SecurityListing, TradingDateRecord
from neocortex.models import (
    AgentRequest,
    AgentRole,
    Exchange,
    Market,
    PriceBar,
    PriceSeries,
    SecurityId,
)
from neocortex.storage import MarketDataStore


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
            agent=AgentRole.TECHNICAL,
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
        assert role is AgentRole.TECHNICAL
        return FakeAgent()


def _reset_fake_provider_state() -> None:
    FakeProviderFactory.created_db_paths.clear()
    FakeProviderFactory.provider = FakeProvider()


def test_cli_connector_profile_command_prints_source_snapshot(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import connector as connector_commands

    fake_connector = FakeAkShareConnector(timeout=3.0)
    monkeypatch.setattr(
        connector_commands,
        "AkShareConnector",
        lambda timeout=None: fake_connector,
    )

    exit_code = cli.main(
        [
            "connector",
            "akshare",
            "profile",
            "--market",
            "CN",
            "--symbol",
            "600519",
            "--exchange",
            "XSHG",
            "--timeout",
            "3",
        ]
    )

    assert exit_code == 0
    assert fake_connector.last_timeout == 3.0
    assert fake_connector.last_profile_security_id == SecurityId(
        symbol="600519",
        market=Market.CN,
        exchange=Exchange.XSHG,
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["source"] == "akshare"
    assert payload["provider_company_name"] == "贵州茅台"


def test_cli_connector_profile_infers_cn_exchange_from_symbol(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import connector as connector_commands

    fake_connector = FakeAkShareConnector(timeout=3.0)
    monkeypatch.setattr(
        connector_commands,
        "AkShareConnector",
        lambda timeout=None: fake_connector,
    )

    exit_code = cli.main(
        [
            "connector",
            "akshare",
            "profile",
            "--market",
            "CN",
            "--symbol",
            "600519",
            "--timeout",
            "3",
        ]
    )

    assert exit_code == 0
    assert fake_connector.last_profile_security_id == SecurityId(
        symbol="600519",
        market=Market.CN,
        exchange=Exchange.XSHG,
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["provider_company_name"] == "贵州茅台"


def test_find_security_ids_by_name_searches_aliases(tmp_path) -> None:
    db_path = tmp_path / "market.sqlite3"
    store = MarketDataStore(db_path)
    store.ensure_schema()
    store.seed_security_listing(
        SecurityListing(
            security_id=SecurityId(
                symbol="600519",
                market=Market.CN,
                exchange=Exchange.XSHG,
            ),
            name="贵州茅台",
        ),
        source="akshare",
    )

    matches = find_security_ids_by_name(
        name="茅台",
        market=Market.CN,
        db_path=db_path,
    )

    assert matches == (
        (
            SecurityId(
                symbol="600519",
                market=Market.CN,
                exchange=Exchange.XSHG,
            ),
            "贵州茅台",
        ),
    )


def test_cli_connector_profile_command_accepts_name_lookup(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import connector as connector_commands
    from neocortex import security_resolution

    fake_connector = FakeAkShareConnector(timeout=3.0)
    monkeypatch.setattr(
        connector_commands,
        "AkShareConnector",
        lambda timeout=None: fake_connector,
    )
    monkeypatch.setattr(
        security_resolution,
        "find_security_ids_by_name",
        lambda **kwargs: (
            (
                SecurityId(
                    symbol="600519",
                    market=Market.CN,
                    exchange=Exchange.XSHG,
                ),
                "贵州茅台",
            ),
        ),
    )

    exit_code = cli.main(
        [
            "connector",
            "akshare",
            "profile",
            "--market",
            "CN",
            "--name",
            "茅台",
            "--timeout",
            "3",
        ]
    )

    assert exit_code == 0
    assert fake_connector.last_profile_security_id == SecurityId(
        symbol="600519",
        market=Market.CN,
        exchange=Exchange.XSHG,
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["provider_company_name"] == "贵州茅台"


def test_cli_connector_baostock_securities_command_renders_security_listing_table(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import connector as connector_commands

    class FakeBaoStockConnector:
        def list_securities(self, *, market: Market):
            return (
                SecurityListing(
                    security_id=SecurityId(
                        symbol="600519",
                        market=market,
                        exchange=Exchange.XSHG,
                    ),
                    name="贵州茅台",
                ),
            )

    monkeypatch.setattr(
        connector_commands,
        "BaoStockConnector",
        lambda: FakeBaoStockConnector(),
    )

    exit_code = cli.main(["connector", "baostock", "securities", "--market", "CN"])

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "security_id.symbol" in output
    assert "security_id.market" in output
    assert "security_id.exchange" in output
    assert "贵州茅台" in output


def test_cli_connector_daily_command_prints_source_records(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import connector as connector_commands

    fake_connector = FakeAkShareConnector()
    monkeypatch.setattr(
        connector_commands,
        "AkShareConnector",
        lambda timeout=None: fake_connector,
    )

    exit_code = cli.main(
        [
            "connector",
            "akshare",
            "daily",
            "--market",
            "CN",
            "--symbol",
            "600519",
            "--exchange",
            "XSHG",
            "--start-date",
            "2026-03-14",
            "--end-date",
            "2026-03-15",
        ]
    )

    assert exit_code == 0
    assert fake_connector.last_daily_call == (
        SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG),
        date(2026, 3, 14),
        date(2026, 3, 15),
    )
    output = capsys.readouterr().out
    assert "trade_date" in output
    assert "2026-03-14" in output
    assert "1515.0" in output


def test_cli_connector_daily_defaults_to_today_and_ten_year_lookback(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import connector as connector_commands
    from neocortex import date_resolution

    fake_connector = FakeAkShareConnector()
    _reset_fake_provider_state()
    monkeypatch.setattr(
        connector_commands,
        "AkShareConnector",
        lambda timeout=None: fake_connector,
    )
    monkeypatch.setattr(
        connector_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )
    monkeypatch.setattr(
        date_resolution,
        "default_end_date",
        lambda *, market, provider=None, now=None: date(2026, 3, 20),
    )

    exit_code = cli.main(
        [
            "connector",
            "akshare",
            "daily",
            "--market",
            "CN",
            "--symbol",
            "600519",
        ]
    )

    assert exit_code == 0
    assert fake_connector.last_daily_call == (
        SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG),
        date(2016, 3, 20),
        date(2026, 3, 20),
    )
    output = capsys.readouterr().out
    assert "trade_date" in output
    assert "2026-03-14" in output


def test_default_end_date_for_cn_uses_previous_trading_day_before_close() -> None:
    from neocortex.cli import common as cli_common

    end_date = cli_common.default_end_date(
        market=Market.CN,
        provider=FakeProvider(),
        now=datetime(2026, 3, 20, 18, 29),
    )

    assert end_date == date(2026, 3, 19)


def test_default_end_date_for_cn_uses_today_after_close() -> None:
    from neocortex.cli import common as cli_common

    end_date = cli_common.default_end_date(
        market=Market.CN,
        provider=FakeProvider(),
        now=datetime(2026, 3, 20, 18, 30),
    )

    assert end_date == date(2026, 3, 20)


def test_default_end_date_for_cn_uses_previous_trading_day_on_non_trading_day() -> None:
    from neocortex.cli import common as cli_common

    end_date = cli_common.default_end_date(
        market=Market.CN,
        provider=FakeProvider(),
        now=datetime(2026, 3, 21, 12, 0),
    )

    assert end_date == date(2026, 3, 20)


def test_cli_connector_adjusted_daily_command_prints_source_records(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import connector as connector_commands

    fake_connector = FakeAkShareConnector()
    monkeypatch.setattr(
        connector_commands,
        "AkShareConnector",
        lambda timeout=None: fake_connector,
    )

    exit_code = cli.main(
        [
            "connector",
            "akshare",
            "adjusted-daily",
            "--market",
            "CN",
            "--symbol",
            "600519",
            "--exchange",
            "XSHG",
            "--start-date",
            "2026-03-14",
            "--end-date",
            "2026-03-15",
            "--adjustment-type",
            "qfq",
        ]
    )

    assert exit_code == 0
    assert fake_connector.last_adjusted_daily_call == (
        SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG),
        date(2026, 3, 14),
        date(2026, 3, 15),
        "qfq",
    )
    output = capsys.readouterr().out
    assert "adjustment_type" in output
    assert "qfq" in output


def test_cli_feishu_longconn_starts_runner(monkeypatch) -> None:
    from neocortex import cli
    from neocortex.feishu import longconn as feishu_longconn
    from neocortex.feishu import settings as feishu_settings

    parser_cli = importlib.import_module("neocortex.cli.main")

    fake_settings = object()
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        parser_cli,
        "load_dotenv",
        lambda path, override=False: captured.update(
            {"path": path, "override": override}
        ),
    )
    monkeypatch.setattr(
        feishu_settings.FeishuSettings, "from_env", lambda: fake_settings
    )
    monkeypatch.setattr(
        parser_cli,
        "configure_logging",
        lambda level: captured.update({"log_level": level}),
    )

    class FakeRunner:
        def __init__(self, settings) -> None:
            captured["settings"] = settings

        def start(self) -> None:
            captured["started"] = True

    monkeypatch.setattr(feishu_longconn, "FeishuLongConnectionRunner", FakeRunner)

    exit_code = cli.main(
        ["--env-file", ".env.local", "--log-level", "DEBUG", "feishu", "longconn"]
    )

    assert exit_code == 0
    assert captured == {
        "path": ".env.local",
        "override": True,
        "log_level": "DEBUG",
        "settings": fake_settings,
        "started": True,
    }


def test_cli_feishu_cleanup_deletes_old_terminal_jobs_and_receipts(
    tmp_path, capsys
) -> None:
    from neocortex import cli

    db_path = tmp_path / "feishu.sqlite3"
    store, old_job_id, recent_job_id, running_job_id = seed_cleanup_test_data(db_path)

    exit_code = cli.main(
        [
            "feishu",
            "cleanup",
            "--db-path",
            str(db_path),
            "--older-than-days",
            "30",
        ]
    )

    assert exit_code == 0
    assert store.get_job(old_job_id) is None
    assert store.get_job(recent_job_id) is not None
    assert store.get_job(running_job_id) is not None
    with sqlite3.connect(db_path) as connection:
        event_ids = tuple(
            row[0]
            for row in connection.execute(
                "select event_id from feishu_event_receipts order by event_id"
            ).fetchall()
        )
    assert event_ids == ("evt-new",)
    output = capsys.readouterr().out
    assert "Event receipts deleted: 1" in output
    assert "Terminal jobs deleted: 1" in output


def test_cli_loads_default_dotenv_search_by_default(monkeypatch, capsys) -> None:
    from neocortex import cli
    from neocortex.commands import connector as connector_commands

    parser_cli = importlib.import_module("neocortex.cli.main")

    fake_connector = FakeAkShareConnector()
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        connector_commands,
        "AkShareConnector",
        lambda timeout=None: fake_connector,
    )
    monkeypatch.setattr(
        parser_cli,
        "load_dotenv",
        lambda path=None, override=False: (
            captured.update({"path": path, "override": override}) or True
        ),
    )

    exit_code = cli.main(
        [
            "connector",
            "akshare",
            "profile",
            "--market",
            "CN",
            "--symbol",
            "600519",
            "--exchange",
            "XSHG",
        ]
    )

    assert exit_code == 0
    assert captured["path"] is None
    assert captured["override"] is True
    payload = json.loads(capsys.readouterr().out)
    assert payload["provider_company_name"] == "贵州茅台"


def test_cli_connector_commands_accept_shared_logging_args(monkeypatch, capsys) -> None:
    from neocortex import cli
    from neocortex.commands import connector as connector_commands

    parser_cli = importlib.import_module("neocortex.cli.main")

    fake_connector = FakeAkShareConnector()
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        connector_commands,
        "AkShareConnector",
        lambda timeout=None: fake_connector,
    )
    monkeypatch.setattr(
        parser_cli,
        "configure_logging",
        lambda level: captured.update({"log_level": level}),
    )

    exit_code = cli.main(
        [
            "--log-level",
            "WARNING",
            "connector",
            "akshare",
            "profile",
            "--market",
            "CN",
            "--symbol",
            "600519",
            "--exchange",
            "XSHG",
        ]
    )

    assert exit_code == 0
    assert captured == {"log_level": "WARNING"}
    payload = json.loads(capsys.readouterr().out)
    assert payload["provider_company_name"] == "贵州茅台"


def test_cli_indicator_list_outputs_registry_metadata(capsys) -> None:
    from neocortex import cli

    exit_code = cli.main(["indicator", "list"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert any(item["key"] == "roc" for item in payload)
    assert any(item["key"] == "macd" for item in payload)


def test_cli_indicator_help_describes_default_date_range(capsys) -> None:
    from neocortex import cli

    exit_code = cli.main(["indicator", "sma", "--help"])

    assert exit_code == 0
    help_text = capsys.readouterr().out
    assert "Defaults to 10 years before --end-date." in help_text
    assert "BaoStock data is expected to" in help_text
    assert "Beijing 18:30" in help_text
    assert "previous trading day" in help_text
    assert "Defaults to qfq." in help_text


def test_cli_indicator_subcommand_uses_provider_bars_and_parameters(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import indicator as indicator_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        indicator_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )

    exit_code = cli.main(
        [
            "indicator",
            "roc",
            "--db-path",
            "/tmp/market.sqlite3",
            "--market",
            "CN",
            "--symbol",
            "600519",
            "--start-date",
            "2026-03-14",
            "--end-date",
            "2026-03-15",
            "--param",
            "period=1",
            "--format",
            "json",
        ]
    )

    assert exit_code == 0
    assert FakeProviderFactory.created_db_paths[-1] == "/tmp/market.sqlite3"
    assert FakeProviderFactory.provider.last_bars_call == (
        SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG),
        date(2026, 3, 14),
        date(2026, 3, 15),
        "qfq",
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["indicator"] == "roc"
    assert payload["parameters"] == {"period": 1}
    assert payload["rows"][-1]["value"] == pytest.approx(
        ((1528.0 - 1515.0) / 1515.0) * 100.0
    )


def test_cli_indicator_supports_multiple_values_after_one_param_flag(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import indicator as indicator_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        indicator_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )

    exit_code = cli.main(
        [
            "indicator",
            "macd",
            "--db-path",
            "/tmp/market.sqlite3",
            "--market",
            "CN",
            "--symbol",
            "600519",
            "--start-date",
            "2026-03-14",
            "--end-date",
            "2026-03-15",
            "--param",
            "fast_window=10",
            "slow_window=20",
            "--format",
            "json",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["indicator"] == "macd"
    assert payload["parameters"] == {"fast_window": 10, "slow_window": 20}


def test_cli_db_query_command_prints_json_output_from_registry(
    tmp_path,
    capsys,
) -> None:
    from neocortex import cli

    db_path = tmp_path / "query.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE company_profiles (
                symbol TEXT NOT NULL,
                company_name TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO company_profiles (symbol, company_name)
            VALUES ('000014', '沙河股份')
            """
        )
        connection.commit()

    exit_code = cli.main(
        [
            "db",
            "query",
            "--db-path",
            str(db_path),
            "--table",
            "company_profiles",
            "--format",
            "json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["columns"] == ["symbol", "company_name"]
    assert payload["rows"] == [["000014", "沙河股份"]]


def test_cli_agent_render_defaults_as_of_date_with_market_rule(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex import date_resolution
    from neocortex.commands import agent as agent_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        agent_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )
    monkeypatch.setattr(agent_commands, "Pipeline", FakePipeline)
    monkeypatch.setattr(
        date_resolution,
        "default_end_date",
        lambda *, market, provider=None, now=None: date(2026, 3, 20),
    )

    exit_code = cli.main(
        [
            "agent",
            "render",
            "--db-path",
            "/tmp/market.sqlite3",
            "--role",
            "technical",
            "--market",
            "CN",
            "--symbol",
            "600519",
            "--format",
            "text",
        ]
    )

    assert exit_code == 0
    assert "user:2026-03-20" in capsys.readouterr().out


def test_cli_requires_exchange_for_non_cn_market() -> None:
    from neocortex import cli

    try:
        cli.main(
            [
                "agent",
                "render",
                "--role",
                "technical",
                "--market",
                "US",
                "--symbol",
                "AAPL",
                "--as-of-date",
                "2026-03-20",
            ]
        )
    except ValueError as error:
        assert error.args == ("--exchange is required for market US.",)
    else:
        raise AssertionError("cli.main() unexpectedly inferred exchange for US.")


def test_cli_market_data_provider_init_db_creates_schema(tmp_path, capsys) -> None:
    from neocortex import cli

    db_path = tmp_path / "market.sqlite3"

    exit_code = cli.main(
        [
            "market-data-provider",
            "init-db",
            "--db-path",
            str(db_path),
        ]
    )

    with sqlite3.connect(db_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }

    assert exit_code == 0
    assert "Initialized market data database" in capsys.readouterr().out
    assert "daily_price_bars" in tables
    assert "fetch_cache" not in tables


def test_cli_market_data_provider_securities_uses_read_through_provider(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import market_data_provider as provider_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        provider_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )

    exit_code = cli.main(
        [
            "market-data-provider",
            "securities",
            "--db-path",
            "/tmp/market.sqlite3",
            "--market",
            "CN",
        ]
    )

    assert exit_code == 0
    assert FakeProviderFactory.created_db_paths[-1] == "/tmp/market.sqlite3"
    assert FakeProviderFactory.provider.list_securities_calls == [Market.CN]
    output = capsys.readouterr().out
    assert "symbol" in output
    assert "market" in output
    assert "exchange" in output
    assert "600519" in output
    assert "CN" in output
    assert "XSHG" in output


def test_cli_db_query_renders_table(tmp_path, capsys) -> None:
    from neocortex import cli

    db_path = tmp_path / "query.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE company_profiles (
                symbol TEXT NOT NULL,
                company_name TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO company_profiles (symbol, company_name)
            VALUES ('688981', '中芯国际')
            """
        )
        connection.commit()

    exit_code = cli.main(
        [
            "db",
            "query",
            "--db-path",
            str(db_path),
            "--table",
            "company_profiles",
        ]
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "symbol" in output
    assert "中芯国际" in output


def test_cli_market_data_provider_profile_uses_read_through_provider(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import market_data_provider as provider_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        provider_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )

    exit_code = cli.main(
        [
            "market-data-provider",
            "profile",
            "--db-path",
            "/tmp/market.sqlite3",
            "--market",
            "CN",
            "--symbol",
            "600519",
            "--exchange",
            "XSHG",
        ]
    )

    assert exit_code == 0
    assert FakeProviderFactory.created_db_paths[-1] == "/tmp/market.sqlite3"
    assert FakeProviderFactory.provider.last_profile_security_id == SecurityId(
        symbol="600519",
        market=Market.CN,
        exchange=Exchange.XSHG,
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["company_name"] == "贵州茅台"


def test_cli_market_data_provider_fundamentals_defaults_as_of_date_with_market_rule(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex import date_resolution
    from neocortex.commands import market_data_provider as provider_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        provider_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )
    monkeypatch.setattr(
        date_resolution,
        "default_end_date",
        lambda *, market, provider=None, now=None: date(2026, 3, 20),
    )

    exit_code = cli.main(
        [
            "market-data-provider",
            "fundamentals",
            "--db-path",
            "/tmp/market.sqlite3",
            "--market",
            "CN",
            "--symbol",
            "600519",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["as_of_date"] == "2026-03-20"


def test_cli_market_data_provider_bars_prints_normalized_price_bars(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import market_data_provider as provider_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        provider_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )

    exit_code = cli.main(
        [
            "market-data-provider",
            "bars",
            "--db-path",
            "/tmp/market.sqlite3",
            "--market",
            "CN",
            "--symbol",
            "600519",
            "--exchange",
            "XSHG",
            "--start-date",
            "2026-03-14",
            "--end-date",
            "2026-03-15",
            "--adjust",
            "qfq",
        ]
    )

    assert exit_code == 0
    assert FakeProviderFactory.provider.last_bars_call == (
        SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG),
        date(2026, 3, 14),
        date(2026, 3, 15),
        "qfq",
    )
    lines = capsys.readouterr().out.strip().splitlines()
    assert "adjusted_close" not in lines[0]
    assert "1528.0" in lines[-1]


def test_cli_market_data_provider_disclosures_uses_read_through_provider(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import market_data_provider as provider_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        provider_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )

    exit_code = cli.main(
        [
            "market-data-provider",
            "disclosures",
            "--db-path",
            "/tmp/market.sqlite3",
            "--market",
            "CN",
            "--symbol",
            "600519",
            "--exchange",
            "XSHG",
            "--as-of-date",
            "2026-03-20",
        ]
    )

    assert exit_code == 0
    assert FakeProviderFactory.provider.last_disclosures_call == (
        SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG),
        date(2026, 3, 20),
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["as_of_date"] == "2026-03-20"


def test_cli_market_data_provider_macro_defaults_as_of_date_with_market_rule(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex import date_resolution
    from neocortex.commands import market_data_provider as provider_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        provider_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )
    monkeypatch.setattr(
        date_resolution,
        "default_end_date",
        lambda *, market, provider=None, now=None: date(2026, 3, 20),
    )

    exit_code = cli.main(
        [
            "market-data-provider",
            "macro",
            "--db-path",
            "/tmp/market.sqlite3",
            "--market",
            "CN",
        ]
    )

    assert exit_code == 0
    assert FakeProviderFactory.provider.last_macro_call == (
        Market.CN,
        date(2026, 3, 20),
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["as_of_date"] == "2026-03-20"


def test_cli_market_data_provider_trading_dates_supports_range_query(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import market_data_provider as provider_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        provider_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )

    exit_code = cli.main(
        [
            "market-data-provider",
            "trading-dates",
            "--db-path",
            "/tmp/market.sqlite3",
            "--market",
            "CN",
            "--start-date",
            "2026-03-19",
            "--end-date",
            "2026-03-20",
        ]
    )

    assert exit_code == 0
    assert FakeProviderFactory.provider.trading_dates_calls[-1] == (
        Market.CN,
        date(2026, 3, 19),
        date(2026, 3, 20),
    )
    lines = capsys.readouterr().out.strip().splitlines()
    assert "trade_date" in lines[0]
    assert "is_trading_day" in lines[0]
    assert "2026-03-19" in lines[1]
    assert "True" in lines[1]
    assert "2026-03-20" in lines[2]


def test_cli_market_data_provider_trading_dates_supports_point_query(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import market_data_provider as provider_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        provider_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )

    exit_code = cli.main(
        [
            "market-data-provider",
            "trading-dates",
            "--db-path",
            "/tmp/market.sqlite3",
            "--market",
            "CN",
            "--date",
            "2026-03-21",
        ]
    )

    assert exit_code == 0
    assert FakeProviderFactory.provider.trading_dates_calls[-1] == (
        Market.CN,
        date(2026, 3, 21),
        date(2026, 3, 21),
    )
    lines = capsys.readouterr().out.strip().splitlines()
    assert "trade_date" in lines[0]
    assert "2026-03-21" in lines[1]
    assert "False" in lines[1]


def test_cli_market_data_provider_trading_dates_rejects_future_date(
    monkeypatch,
) -> None:
    from neocortex import cli
    from neocortex.commands import market_data_provider as provider_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        provider_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )
    monkeypatch.setattr(
        provider_commands,
        "date",
        type("FakeDate", (), {"today": staticmethod(lambda: date(2026, 3, 20))}),
    )

    with pytest.raises(ValueError) as exc_info:
        cli.main(
            [
                "market-data-provider",
                "trading-dates",
                "--db-path",
                "/tmp/market.sqlite3",
                "--market",
                "CN",
                "--date",
                "2026-03-21",
            ]
        )

    assert exc_info.value.args == (
        "Future dates are not supported for trading-date queries.",
    )


def test_cli_sync_trading_dates_uses_fixed_cn_full_range(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import sync as sync_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        sync_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )
    monkeypatch.setattr(
        sync_commands,
        "date",
        type("FakeDate", (), {"today": staticmethod(lambda: date(2026, 3, 20))}),
    )

    exit_code = cli.main(
        [
            "sync",
            "trading-dates",
            "--db-path",
            "/tmp/market.sqlite3",
        ]
    )

    assert exit_code == 0
    assert FakeProviderFactory.provider.trading_dates_calls[-1] == (
        Market.CN,
        date(1990, 12, 19),
        date(2026, 3, 20),
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "CN"
    assert payload["start_date"] == "1990-12-19"
    assert payload["end_date"] == "2026-03-20"


def test_cli_agent_render_outputs_request_and_prompts(monkeypatch, capsys) -> None:
    from neocortex import cli
    from neocortex.commands import agent as agent_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        agent_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )
    monkeypatch.setattr(agent_commands, "Pipeline", FakePipeline)

    exit_code = cli.main(
        [
            "agent",
            "render",
            "--db-path",
            "/tmp/market.sqlite3",
            "--role",
            "technical",
            "--market",
            "CN",
            "--symbol",
            "600519",
            "--exchange",
            "XSHG",
            "--as-of-date",
            "2026-03-19",
            "--request-id",
            "req-1",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["request"]["request_id"] == "req-1"
    assert payload["request"]["agent"] == "technical"
    assert payload["system_prompt"] == "system:CN:600519"
    assert payload["user_prompt"] == "user:2026-03-19"


def test_cli_sync_securities_returns_summary(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import sync as sync_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        sync_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )

    exit_code = cli.main(
        [
            "sync",
            "securities",
            "--db-path",
            "/tmp/market.sqlite3",
            "--market",
            "CN",
        ]
    )

    assert exit_code == 0
    assert FakeProviderFactory.provider.list_securities_calls == [Market.CN]
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "CN"
    assert payload["synced_security_count"] == 1
    assert payload["tickers"] == ["CN:600519"]


def test_cli_sync_bars_supports_single_security(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import sync as sync_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        sync_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )

    exit_code = cli.main(
        [
            "sync",
            "bars",
            "--db-path",
            "/tmp/market.sqlite3",
            "--market",
            "CN",
            "--symbol",
            "600519",
            "--exchange",
            "XSHG",
            "--start-date",
            "2026-03-14",
            "--end-date",
            "2026-03-15",
        ]
    )

    assert exit_code == 0
    assert FakeProviderFactory.provider.bar_calls == [
        (
            SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG),
            date(2026, 3, 14),
            date(2026, 3, 15),
            None,
        )
    ]
    payload = json.loads(capsys.readouterr().out)
    assert payload["synced_security_count"] == 1
    assert payload["synced_bar_count"] == 2


def test_cli_sync_bars_supports_ticker_collection(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import sync as sync_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        sync_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )

    exit_code = cli.main(
        [
            "sync",
            "bars",
            "--db-path",
            "/tmp/market.sqlite3",
            "--market",
            "CN",
            "--ticker",
            "600519.XSHG",
            "--ticker",
            "000001.XSHE",
            "--start-date",
            "2026-03-14",
            "--end-date",
            "2026-03-15",
        ]
    )

    assert exit_code == 0
    assert FakeProviderFactory.provider.bar_calls == [
        (
            SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG),
            date(2026, 3, 14),
            date(2026, 3, 15),
            None,
        ),
        (
            SecurityId(symbol="000001", market=Market.CN, exchange=Exchange.XSHE),
            date(2026, 3, 14),
            date(2026, 3, 15),
            None,
        ),
    ]
    payload = json.loads(capsys.readouterr().out)
    assert payload["synced_security_count"] == 2
    assert payload["tickers"] == ["CN:600519", "CN:000001"]


def test_cli_sync_bars_supports_fuzzy_name_in_ticker_collection(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import sync as sync_commands
    from neocortex import security_resolution

    _reset_fake_provider_state()
    monkeypatch.setattr(
        sync_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )
    monkeypatch.setattr(
        security_resolution,
        "find_security_ids_by_name",
        lambda **kwargs: (
            (
                SecurityId(
                    symbol="688981",
                    market=Market.CN,
                    exchange=Exchange.XSHG,
                ),
                "中芯国际",
            ),
        ),
    )

    exit_code = cli.main(
        [
            "sync",
            "bars",
            "--db-path",
            "/tmp/market.sqlite3",
            "--market",
            "CN",
            "--ticker",
            "中芯国际",
            "--start-date",
            "2026-03-14",
            "--end-date",
            "2026-03-15",
        ]
    )

    assert exit_code == 0
    assert FakeProviderFactory.provider.bar_calls == [
        (
            SecurityId(symbol="688981", market=Market.CN, exchange=Exchange.XSHG),
            date(2026, 3, 14),
            date(2026, 3, 15),
            None,
        )
    ]
    payload = json.loads(capsys.readouterr().out)
    assert payload["synced_security_count"] == 1
    assert payload["tickers"] == ["CN:688981"]


def test_cli_sync_bars_supports_multiple_values_after_one_ticker_flag(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import sync as sync_commands
    from neocortex import security_resolution

    _reset_fake_provider_state()
    monkeypatch.setattr(
        sync_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )
    monkeypatch.setattr(
        security_resolution,
        "find_security_ids_by_name",
        lambda **kwargs: (
            (
                (
                    SecurityId(
                        symbol="002460",
                        market=Market.CN,
                        exchange=Exchange.XSHE,
                    ),
                    "赣锋",
                ),
            )
            if kwargs["name"] == "赣锋"
            else (
                (
                    SecurityId(
                        symbol="002466",
                        market=Market.CN,
                        exchange=Exchange.XSHE,
                    ),
                    "天齐",
                ),
            )
        ),
    )

    exit_code = cli.main(
        [
            "sync",
            "bars",
            "--db-path",
            "/tmp/market.sqlite3",
            "--market",
            "CN",
            "--ticker",
            "赣锋",
            "天齐",
            "--start-date",
            "2026-03-14",
            "--end-date",
            "2026-03-15",
        ]
    )

    assert exit_code == 0
    assert FakeProviderFactory.provider.bar_calls == [
        (
            SecurityId(symbol="002460", market=Market.CN, exchange=Exchange.XSHE),
            date(2026, 3, 14),
            date(2026, 3, 15),
            None,
        ),
        (
            SecurityId(symbol="002466", market=Market.CN, exchange=Exchange.XSHE),
            date(2026, 3, 14),
            date(2026, 3, 15),
            None,
        ),
    ]
    payload = json.loads(capsys.readouterr().out)
    assert payload["synced_security_count"] == 2
    assert payload["tickers"] == ["CN:002460", "CN:002466"]


def test_cli_agent_render_outputs_text_when_requested(monkeypatch, capsys) -> None:
    from neocortex import cli
    from neocortex.commands import agent as agent_commands

    _reset_fake_provider_state()
    monkeypatch.setattr(
        agent_commands,
        "ReadThroughMarketDataProvider",
        FakeProviderFactory,
    )
    monkeypatch.setattr(agent_commands, "Pipeline", FakePipeline)

    exit_code = cli.main(
        [
            "agent",
            "render",
            "--db-path",
            "/tmp/market.sqlite3",
            "--role",
            "technical",
            "--market",
            "CN",
            "--symbol",
            "600519",
            "--exchange",
            "XSHG",
            "--as-of-date",
            "2026-03-19",
            "--format",
            "text",
        ]
    )

    assert exit_code == 0
    rendered = capsys.readouterr().out
    assert "System Prompt:" in rendered
    assert "system:CN:600519" in rendered
    assert "User Prompt:" in rendered
    assert "user:2026-03-19" in rendered
