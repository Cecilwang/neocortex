import json

import pytest

from tests.core.cli_test_support import FakeProviderFactory, reset_fake_provider_state


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

    reset_fake_provider_state()
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

    reset_fake_provider_state()
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


def test_cli_indicator_rejects_duplicate_param_keys(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex.commands import indicator as indicator_commands

    reset_fake_provider_state()
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
            "fast_window=20",
        ]
    )

    assert exit_code == 2
    assert "Duplicate indicator parameter key 'fast_window'." in capsys.readouterr().err
