"""Structured command parsing for the Feishu bot."""

from __future__ import annotations

import shlex

from neocortex.feishu.models import BotCommand


HELP_TEXT = """Available commands:
/neo help
/neo profile <symbol> <exchange> [--timeout <seconds>]
/neo bars <symbol> <exchange> <start-date> <end-date> [--adjust <value>] [--timeout <seconds>]
/neo db table <table> [--limit <n>]
/neo db sql <SELECT ...>
/neo backfill profiles [--limit <n>] [--workers <n>] [--retry-count <n>] [--sleep-seconds <seconds>] [--timeout <seconds>]
/neo job <job-id>
/neo pipeline run <symbol> <exchange> <as-of-date>"""


def parse_command(text: str) -> BotCommand | None:
    """Parse one `/neo ...` command string into a structured command."""

    stripped = text.strip()
    if not stripped.startswith("/neo"):
        return None

    tokens = shlex.split(stripped)
    if tokens == ["/neo"] or tokens == ["/neo", "help"]:
        return BotCommand(name="help", text=stripped)
    if len(tokens) < 2:
        raise ValueError(HELP_TEXT)

    root = tokens[1]
    if root == "profile":
        return _parse_profile(stripped, tokens[2:])
    if root == "bars":
        return _parse_bars(stripped, tokens[2:])
    if root == "db":
        return _parse_db(stripped, tokens[2:])
    if root == "backfill":
        return _parse_backfill(stripped, tokens[2:])
    if root == "job":
        return _parse_job(stripped, tokens[2:])
    if root == "pipeline":
        return _parse_pipeline(stripped, tokens[2:])
    raise ValueError(f"Unknown command: {root}\n\n{HELP_TEXT}")


def _split_args(tokens: list[str]) -> tuple[list[str], dict[str, str]]:
    positionals: list[str] = []
    options: dict[str, str] = {}
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.startswith("--"):
            if index + 1 >= len(tokens):
                raise ValueError(f"Missing value for option {token}.")
            options[token[2:]] = tokens[index + 1]
            index += 2
            continue
        positionals.append(token)
        index += 1
    return positionals, options


def _parse_profile(text: str, tokens: list[str]) -> BotCommand:
    positionals, options = _split_args(tokens)
    if len(positionals) != 2:
        raise ValueError(
            "Usage: /neo profile <symbol> <exchange> [--timeout <seconds>]"
        )
    args: dict[str, object] = {
        "symbol": positionals[0],
        "exchange": positionals[1],
        "timeout": _parse_optional_float(options, "timeout"),
    }
    return BotCommand(name="profile", text=text, args=args)


def _parse_bars(text: str, tokens: list[str]) -> BotCommand:
    positionals, options = _split_args(tokens)
    if len(positionals) != 4:
        raise ValueError(
            "Usage: /neo bars <symbol> <exchange> <start-date> <end-date> "
            "[--adjust <value>] [--timeout <seconds>]"
        )
    args: dict[str, object] = {
        "symbol": positionals[0],
        "exchange": positionals[1],
        "start_date": positionals[2],
        "end_date": positionals[3],
        "adjust": options.get("adjust"),
        "timeout": _parse_optional_float(options, "timeout"),
    }
    return BotCommand(name="bars", text=text, args=args)


def _parse_db(text: str, tokens: list[str]) -> BotCommand:
    if not tokens:
        raise ValueError(
            "Usage: /neo db table <table> [--limit <n>] | /neo db sql <SELECT ...>"
        )

    mode = tokens[0]
    if mode == "table":
        positionals, options = _split_args(tokens[1:])
        if len(positionals) != 1:
            raise ValueError("Usage: /neo db table <table> [--limit <n>]")
        args: dict[str, object] = {
            "table": positionals[0],
            "limit": _parse_optional_int(options, "limit", default=20),
        }
        return BotCommand(name="db_table", text=text, args=args)

    if mode == "sql":
        sql = " ".join(tokens[1:]).strip()
        if not sql:
            raise ValueError("Usage: /neo db sql <SELECT ...>")
        return BotCommand(
            name="db_sql",
            text=text,
            args={"sql": sql},
            requires_admin=True,
        )

    raise ValueError(
        "Usage: /neo db table <table> [--limit <n>] | /neo db sql <SELECT ...>"
    )


def _parse_backfill(text: str, tokens: list[str]) -> BotCommand:
    if tokens[:1] != ["profiles"]:
        raise ValueError(
            "Usage: /neo backfill profiles [--limit <n>] [--workers <n>] "
            "[--retry-count <n>] [--sleep-seconds <seconds>] [--timeout <seconds>]"
        )
    _, options = _split_args(tokens[1:])
    args: dict[str, object] = {
        "limit": _parse_optional_int(options, "limit"),
        "workers": _parse_optional_int(options, "workers", default=8),
        "retry_count": _parse_optional_int(options, "retry-count", default=0),
        "sleep_seconds": _parse_optional_float(options, "sleep-seconds", default=0.0),
        "timeout": _parse_optional_float(options, "timeout"),
    }
    return BotCommand(
        name="backfill_profiles",
        text=text,
        args=args,
        requires_admin=True,
        asynchronous=True,
    )


def _parse_job(text: str, tokens: list[str]) -> BotCommand:
    if len(tokens) != 1:
        raise ValueError("Usage: /neo job <job-id>")
    return BotCommand(name="job_status", text=text, args={"job_id": int(tokens[0])})


def _parse_pipeline(text: str, tokens: list[str]) -> BotCommand:
    positionals, _ = _split_args(tokens)
    if len(positionals) != 4 or positionals[0] != "run":
        raise ValueError("Usage: /neo pipeline run <symbol> <exchange> <as-of-date>")
    args: dict[str, object] = {
        "symbol": positionals[1],
        "exchange": positionals[2],
        "as_of_date": positionals[3],
    }
    return BotCommand(
        name="pipeline_run",
        text=text,
        args=args,
        requires_admin=True,
        asynchronous=True,
    )


def _parse_optional_int(
    options: dict[str, str],
    key: str,
    *,
    default: int | None = None,
) -> int | None:
    raw_value = options.get(key)
    if raw_value is None:
        return default
    return int(raw_value)


def _parse_optional_float(
    options: dict[str, str],
    key: str,
    *,
    default: float | None = None,
) -> float | None:
    raw_value = options.get(key)
    if raw_value is None:
        return default
    return float(raw_value)
