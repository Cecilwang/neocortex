import pytest

from neocortex.feishu.commands import HELP_TEXT, parse_command


def test_parse_help_command() -> None:
    command = parse_command("/neo help")

    assert command is not None
    assert command.name == "help"
    assert command.text == "/neo help"


def test_parse_backfill_profiles_command() -> None:
    command = parse_command(
        "/neo backfill profiles --limit 12 --workers 2 --retry-count 3 "
        "--sleep-seconds 0.5 --timeout 8"
    )

    assert command is not None
    assert command.name == "backfill_profiles"
    assert command.requires_admin is True
    assert command.asynchronous is True
    assert command.args == {
        "limit": 12,
        "workers": 2,
        "retry_count": 3,
        "sleep_seconds": 0.5,
        "timeout": 8.0,
    }


def test_parse_unknown_command_raises_helpful_error() -> None:
    with pytest.raises(ValueError) as exc_info:
        parse_command("/neo frob")

    assert HELP_TEXT in str(exc_info.value)
