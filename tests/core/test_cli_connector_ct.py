import importlib
import json
from datetime import date, datetime

from neocortex.connectors.types import SecurityListing
from neocortex.models import Exchange, Market, SecurityId
from neocortex.security_resolution import find_security_ids_by_name
from neocortex.storage import MarketDataStore
from tests.core.cli_test_support import (
    FakeAkShareConnector,
    FakeProvider,
    FakeProviderFactory,
    reset_fake_provider_state,
)


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
    reset_fake_provider_state()
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
    from neocortex import date_resolution

    end_date = date_resolution.default_end_date(
        market=Market.CN,
        provider=FakeProvider(),
        now=datetime(2026, 3, 20, 18, 29),
    )

    assert end_date == date(2026, 3, 19)


def test_default_end_date_for_cn_uses_today_after_close() -> None:
    from neocortex import date_resolution

    end_date = date_resolution.default_end_date(
        market=Market.CN,
        provider=FakeProvider(),
        now=datetime(2026, 3, 20, 18, 30),
    )

    assert end_date == date(2026, 3, 20)


def test_default_end_date_for_cn_uses_previous_trading_day_on_non_trading_day() -> None:
    from neocortex import date_resolution

    end_date = date_resolution.default_end_date(
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
