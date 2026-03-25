import importlib
import sqlite3

from tests.core.feishu_storage_test_support import seed_cleanup_test_data


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
