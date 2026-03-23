import os

from neocortex.config import get_config, load_dotenv, reset_config_cache
from neocortex.feishu.settings import FeishuSettings
from neocortex.models import Market


def test_load_dotenv_reads_values_and_ignores_comments(tmp_path, monkeypatch) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        "\n".join(
            [
                "# comment",
                "FEISHU_APP_ID=cli_123",
                "FEISHU_APP_SECRET='secret-value'",
                "export FEISHU_ADMIN_OPEN_IDS=ou_1,ou_2",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("FEISHU_APP_ID", raising=False)
    monkeypatch.delenv("FEISHU_APP_SECRET", raising=False)
    monkeypatch.delenv("FEISHU_ADMIN_OPEN_IDS", raising=False)

    loaded_path = load_dotenv(dotenv_path)

    assert loaded_path is True
    assert os.environ["FEISHU_APP_ID"] == "cli_123"
    assert os.environ["FEISHU_APP_SECRET"] == "secret-value"
    assert os.environ["FEISHU_ADMIN_OPEN_IDS"] == "ou_1,ou_2"


def test_load_dotenv_respects_existing_values_without_override(
    tmp_path, monkeypatch
) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("FEISHU_APP_ID=cli_from_file\n", encoding="utf-8")
    monkeypatch.setenv("FEISHU_APP_ID", "cli_from_env")

    load_dotenv(dotenv_path)

    assert os.environ["FEISHU_APP_ID"] == "cli_from_env"


def test_feishu_settings_use_default_storage_db_path(monkeypatch) -> None:
    monkeypatch.setenv("FEISHU_APP_ID", "cli_from_env")
    monkeypatch.setenv("FEISHU_APP_SECRET", "secret_from_env")

    reset_config_cache()
    app_config = get_config()
    settings = FeishuSettings.from_env()

    assert settings.db_path == app_config.storage.bot_db_path
    assert settings.market_data_db_path == app_config.storage.market_data_db_path


def test_get_config_reads_yaml_from_env_override(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "custom-config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "storage:",
                "  bot_db_path: custom/bot.sqlite3",
                "  market_data_db_path: custom/market.sqlite3",
                "connectors:",
                "  defaults:",
                "    retry:",
                "      max_attempts: 3",
                "      backoff_seconds: 1.0",
                "      retryable_exceptions: [RuntimeError, TimeoutError]",
                "  akshare:",
                "    retry:",
                "      max_attempts: 2",
                "      backoff_seconds: 0.5",
                "      retryable_exceptions: [RuntimeError]",
                "market_data_provider:",
                "  source_priority:",
                "    CN:",
                "      daily_price_bars: [efinance, akshare]",
                "      company_profile: [akshare]",
                "      securities: [baostock]",
                "      fundamentals: [baostock]",
                "      disclosures: [baostock]",
                "      macro: [baostock]",
                "pipeline:",
                "  agents:",
                "    technical:",
                "      template: technical_fine.yaml",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("NEOCORTEX_CONFIG_PATH", str(config_path))
    reset_config_cache()

    config = get_config()

    assert config.storage.bot_db_path == tmp_path / "custom" / "bot.sqlite3"
    assert config.storage.market_data_db_path == tmp_path / "custom" / "market.sqlite3"
    assert config.market_data_provider.source_priority[Market.CN][
        "daily_price_bars"
    ] == (
        "efinance",
        "akshare",
    )
    assert config.connectors.retry_for("akshare").max_attempts == 2
    assert config.connectors.retry_for("akshare").exc_info is False
    assert config.pipeline.agents["technical"]["template"] == "technical_fine.yaml"
    reset_config_cache()


def test_feishu_settings_do_not_load_default_dotenv_implicitly(
    tmp_path, monkeypatch
) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "FEISHU_APP_ID=cli_from_dotenv",
                "FEISHU_APP_SECRET=secret_from_dotenv",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("FEISHU_APP_ID", raising=False)
    monkeypatch.delenv("FEISHU_APP_SECRET", raising=False)

    try:
        FeishuSettings.from_env()
    except KeyError as error:
        assert error.args == ("FEISHU_APP_ID",)
    else:
        raise AssertionError("FeishuSettings.from_env() unexpectedly loaded .env")
