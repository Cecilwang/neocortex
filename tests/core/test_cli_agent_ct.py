import json
from datetime import date

from tests.core.cli_test_support import (
    FakePipeline,
    FakeProviderFactory,
    reset_fake_provider_state,
)


def test_cli_agent_render_defaults_as_of_date_with_market_rule(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli
    from neocortex import date_resolution
    from neocortex.commands import agent as agent_commands

    reset_fake_provider_state()
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


def test_cli_agent_render_outputs_request_and_prompts(monkeypatch, capsys) -> None:
    from neocortex import cli
    from neocortex.commands import agent as agent_commands

    reset_fake_provider_state()
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


def test_cli_agent_render_outputs_text_when_requested(monkeypatch, capsys) -> None:
    from neocortex import cli
    from neocortex.commands import agent as agent_commands

    reset_fake_provider_state()
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


def test_cli_agent_render_supports_quant_role(monkeypatch, capsys) -> None:
    from neocortex import cli
    from neocortex.commands import agent as agent_commands

    reset_fake_provider_state()
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
            "quant_fundamental",
            "--market",
            "CN",
            "--symbol",
            "600519",
            "--exchange",
            "XSHG",
            "--as-of-date",
            "2026-03-19",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["request"]["agent"] == "quant_fundamental"
