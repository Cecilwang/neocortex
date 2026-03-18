import os

from neocortex.config.env import load_dotenv
from neocortex.feishu.settings import FeishuSettings
from neocortex.storage.config import DEFAULT_DB_PATH


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

    assert loaded_path == dotenv_path
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

    settings = FeishuSettings.from_env()

    assert settings.db_path == DEFAULT_DB_PATH
