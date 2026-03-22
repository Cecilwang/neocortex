import pytest

from neocortex.feishu.commands import HELP_TEXT, parse_command


def test_parse_help_command() -> None:
    command = parse_command("/neo help")

    assert command is not None
    assert command.name == "help"
    assert command.text == "/neo help"


def test_parse_pipeline_run_command() -> None:
    command = parse_command("/neo pipeline run 600519 XSHG 2026-03-19")

    assert command is not None
    assert command.name == "pipeline_run"
    assert command.requires_admin is True
    assert command.asynchronous is True
    assert command.args == {
        "symbol": "600519",
        "exchange": "XSHG",
        "as_of_date": "2026-03-19",
    }


def test_parse_unknown_command_raises_helpful_error() -> None:
    with pytest.raises(ValueError) as exc_info:
        parse_command("/neo frob")

    assert HELP_TEXT in str(exc_info.value)
