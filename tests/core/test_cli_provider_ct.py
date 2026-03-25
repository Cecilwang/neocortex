import json
import sqlite3
from datetime import date

import pytest

from neocortex.models import Exchange, Market, SecurityId
from tests.core.cli_test_support import FakeProviderFactory, reset_fake_provider_state


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

    reset_fake_provider_state()
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


def test_cli_market_data_provider_profile_uses_read_through_provider(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import market_data_provider as provider_commands

    reset_fake_provider_state()
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

    reset_fake_provider_state()
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

    reset_fake_provider_state()
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

    reset_fake_provider_state()
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

    reset_fake_provider_state()
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

    reset_fake_provider_state()
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

    reset_fake_provider_state()
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

    reset_fake_provider_state()
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
