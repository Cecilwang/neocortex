import json
import sqlite3
from datetime import date, datetime

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

    def list_securities(self, *, market: Market):
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
        return [{"symbol": security_id.symbol, "as_of_date": as_of_date.isoformat()}]

    def get_macro_points(self, *, market: Market, as_of_date: date):
        return [{"market": market.value, "as_of_date": as_of_date.isoformat()}]


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

    def get_system_prompt(self, request: AgentRequest) -> str:
        return f"system:{request.security_id.ticker}"

    def get_user_prompt(self, request: AgentRequest) -> str:
        return f"user:{request.as_of_date.isoformat()}"


class FakePipeline:
    def __init__(self, *, market_data) -> None:
        self.market_data = market_data

    def get_agent(self, role: AgentRole) -> FakeAgent:
        assert role is AgentRole.TECHNICAL
        return FakeAgent()


def test_cli_connector_profile_command_prints_source_snapshot(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli

    fake_connector = FakeAkShareConnector(timeout=3.0)
    monkeypatch.setattr(cli, "AkShareConnector", lambda timeout=None: fake_connector)

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


def test_cli_connector_daily_command_prints_source_records(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli

    fake_connector = FakeAkShareConnector()
    monkeypatch.setattr(cli, "AkShareConnector", lambda timeout=None: fake_connector)

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
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["trade_date"] == "2026-03-14"
    assert payload[0]["close"] == 1515.0


def test_cli_connector_adjusted_daily_command_prints_source_records(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli

    fake_connector = FakeAkShareConnector()
    monkeypatch.setattr(cli, "AkShareConnector", lambda timeout=None: fake_connector)

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
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["adjustment_type"] == "qfq"


def test_cli_feishu_longconn_starts_runner(monkeypatch) -> None:
    from neocortex import cli

    fake_settings = object()
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        cli,
        "load_dotenv",
        lambda path, override=False: captured.update(
            {"path": path, "override": override}
        ),
    )
    monkeypatch.setattr(cli.FeishuSettings, "from_env", lambda: fake_settings)
    monkeypatch.setattr(
        cli,
        "configure_logging",
        lambda level: captured.update({"log_level": level}),
    )

    class FakeRunner:
        def __init__(self, settings) -> None:
            captured["settings"] = settings

        def start(self) -> None:
            captured["started"] = True

    monkeypatch.setattr(cli, "FeishuLongConnectionRunner", FakeRunner)

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


def test_cli_connector_commands_accept_shared_logging_args(monkeypatch, capsys) -> None:
    from neocortex import cli

    fake_connector = FakeAkShareConnector()
    captured: dict[str, object] = {}
    monkeypatch.setattr(cli, "AkShareConnector", lambda timeout=None: fake_connector)
    monkeypatch.setattr(
        cli,
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


def test_cli_market_data_provider_init_db_creates_schema(tmp_path, capsys) -> None:
    from neocortex import cli

    db_path = tmp_path / "market.sqlite3"

    exit_code = cli.main(
        [
            "market-data-provider",
            "--db-path",
            str(db_path),
            "init-db",
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


def test_cli_market_data_provider_profile_uses_read_through_provider(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli

    monkeypatch.setattr(cli, "ReadThroughMarketDataProvider", FakeProviderFactory)

    exit_code = cli.main(
        [
            "market-data-provider",
            "--db-path",
            "/tmp/market.sqlite3",
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
    assert FakeProviderFactory.created_db_paths[-1] == "/tmp/market.sqlite3"
    assert FakeProviderFactory.provider.last_profile_security_id == SecurityId(
        symbol="600519",
        market=Market.CN,
        exchange=Exchange.XSHG,
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["company_name"] == "贵州茅台"


def test_cli_market_data_provider_bars_prints_normalized_price_bars(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli

    monkeypatch.setattr(cli, "ReadThroughMarketDataProvider", FakeProviderFactory)

    exit_code = cli.main(
        [
            "market-data-provider",
            "--db-path",
            "/tmp/market.sqlite3",
            "bars",
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


def test_cli_agent_render_outputs_request_and_prompts(monkeypatch, capsys) -> None:
    from neocortex import cli

    monkeypatch.setattr(cli, "ReadThroughMarketDataProvider", FakeProviderFactory)
    monkeypatch.setattr(cli, "Pipeline", FakePipeline)

    exit_code = cli.main(
        [
            "agent",
            "--db-path",
            "/tmp/market.sqlite3",
            "render",
            "--role",
            "technical_agent",
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
    assert payload["request"]["agent"] == "technical_agent"
    assert payload["system_prompt"] == "system:CN:600519"
    assert payload["user_prompt"] == "user:2026-03-19"
