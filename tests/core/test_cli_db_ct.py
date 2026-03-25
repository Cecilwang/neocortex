import json

from tests.core.db_query_test_support import (
    seed_company_profiles_table,
    seed_sample_rows_table,
)


def test_cli_db_query_command_prints_json_output_from_registry(
    tmp_path,
    capsys,
) -> None:
    from neocortex import cli

    db_path = seed_company_profiles_table(
        tmp_path / "query.sqlite",
        rows=(("000014", "沙河股份"),),
    )

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


def test_cli_db_query_renders_table(tmp_path, capsys) -> None:
    from neocortex import cli

    db_path = seed_company_profiles_table(
        tmp_path / "query.sqlite",
        rows=(("688981", "中芯国际"),),
    )

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


def test_cli_db_query_rejects_write_sql(tmp_path, capsys) -> None:
    from neocortex import cli

    db_path = seed_sample_rows_table(
        tmp_path / "query.sqlite",
        rows=("alpha",),
    )

    exit_code = cli.main(
        [
            "db",
            "query",
            "--db-path",
            str(db_path),
            "--sql",
            "DELETE FROM sample_rows",
        ]
    )

    assert exit_code == 2
    assert "Only read-only SELECT queries are allowed." in capsys.readouterr().err
