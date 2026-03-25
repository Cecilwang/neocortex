import json
from datetime import date

from neocortex.models import Exchange, Market, SecurityId
from tests.core.cli_test_support import FakeProviderFactory, reset_fake_provider_state


def test_cli_sync_trading_dates_uses_fixed_cn_full_range(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import sync as sync_commands

    reset_fake_provider_state()
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


def test_cli_sync_securities_returns_summary(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import sync as sync_commands

    reset_fake_provider_state()
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

    reset_fake_provider_state()
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

    reset_fake_provider_state()
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

    reset_fake_provider_state()
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

    reset_fake_provider_state()
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
