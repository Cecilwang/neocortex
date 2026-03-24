import importlib
import argparse

from neocortex.commands import (
    AuthPolicy,
    CommandRegistry,
    CommandResult,
    CommandSpec,
    ExecutionMode,
    Exposure,
)


def _demo_handler(args: argparse.Namespace, context) -> CommandResult:
    _ = context
    return CommandResult.text(f"registry:{args.target}")


def _legacy_show_handler(args: argparse.Namespace) -> int:
    print(f"legacy:{args.target}")
    return 0


def _build_legacy_demo_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="neocortex")
    parser.add_argument("--env-file", default=None)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    subcommands = parser.add_subparsers(dest="domain", required=True)
    demo_parser = subcommands.add_parser("demo")
    demo_commands = demo_parser.add_subparsers(dest="command", required=True)
    show_parser = demo_commands.add_parser("show")
    show_parser.add_argument("target")
    show_parser.set_defaults(handler=_legacy_show_handler)
    return parser


def test_cli_dispatches_registry_managed_root(monkeypatch, capsys) -> None:
    from neocortex import cli

    parser_cli = importlib.import_module("neocortex.cli.main")
    registry = CommandRegistry()
    registry.register(
        CommandSpec(
            id=("demo", "run"),
            summary="Run registry command.",
            description="Run registry command.",
            exposure=Exposure.SHARED,
            auth=AuthPolicy.PUBLIC,
            execution=ExecutionMode.SYNC,
            configure_parser=lambda parser: parser.add_argument("target"),
            handler=_demo_handler,
        )
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(parser_cli, "build_command_registry", lambda: registry)
    monkeypatch.setattr(
        parser_cli, "load_dotenv", lambda path=None, override=False: False
    )
    monkeypatch.setattr(
        parser_cli,
        "configure_logging",
        lambda level: captured.update({"log_level": level}),
    )

    exit_code = cli.main(["--log-level", "ERROR", "demo", "run", "alpha"])

    assert exit_code == 0
    assert captured == {"log_level": "ERROR"}
    assert capsys.readouterr().out == "registry:alpha\n"


def test_cli_registry_help_returns_zero(monkeypatch, capsys) -> None:
    from neocortex import cli

    parser_cli = importlib.import_module("neocortex.cli.main")
    registry = CommandRegistry()
    registry.register(
        CommandSpec(
            id=("demo", "run"),
            summary="Run registry command.",
            description="Run registry command.",
            exposure=Exposure.SHARED,
            auth=AuthPolicy.PUBLIC,
            execution=ExecutionMode.SYNC,
            configure_parser=lambda parser: parser.add_argument("target"),
            handler=_demo_handler,
        )
    )
    monkeypatch.setattr(parser_cli, "build_command_registry", lambda: registry)
    monkeypatch.setattr(
        parser_cli, "load_dotenv", lambda path=None, override=False: False
    )
    monkeypatch.setattr(parser_cli, "configure_logging", lambda level: None)

    exit_code = cli.main(["demo", "run", "--help"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Run registry command." in captured.out
    assert captured.err == ""


def test_cli_registry_usage_error_returns_two(monkeypatch, capsys) -> None:
    from neocortex import cli

    parser_cli = importlib.import_module("neocortex.cli.main")
    registry = CommandRegistry()

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("target")
        source_group = parser.add_mutually_exclusive_group(required=True)
        source_group.add_argument("--symbol")
        source_group.add_argument("--name")

    registry.register(
        CommandSpec(
            id=("demo", "run"),
            summary="Run registry command.",
            description="Run registry command.",
            exposure=Exposure.SHARED,
            auth=AuthPolicy.PUBLIC,
            execution=ExecutionMode.SYNC,
            configure_parser=configure_parser,
            handler=_demo_handler,
        )
    )
    monkeypatch.setattr(parser_cli, "build_command_registry", lambda: registry)
    monkeypatch.setattr(
        parser_cli, "load_dotenv", lambda path=None, override=False: False
    )
    monkeypatch.setattr(parser_cli, "configure_logging", lambda level: None)

    exit_code = cli.main(["demo", "run", "alpha"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "one of the arguments --symbol --name is required" in captured.err
    assert "usage: neocortex demo run" in captured.err


def test_cli_dispatches_unmanaged_command_path_to_legacy(monkeypatch, capsys) -> None:
    from neocortex import cli

    parser_cli = importlib.import_module("neocortex.cli.main")
    registry = CommandRegistry()
    registry.register(
        CommandSpec(
            id=("demo", "run"),
            summary="Run registry command.",
            description="Run registry command.",
            exposure=Exposure.SHARED,
            auth=AuthPolicy.PUBLIC,
            execution=ExecutionMode.SYNC,
            configure_parser=lambda parser: parser.add_argument("target"),
            handler=_demo_handler,
        )
    )
    monkeypatch.setattr(parser_cli, "build_command_registry", lambda: registry)
    monkeypatch.setattr(parser_cli, "build_legacy_parser", _build_legacy_demo_parser)
    monkeypatch.setattr(
        parser_cli, "load_dotenv", lambda path=None, override=False: False
    )
    monkeypatch.setattr(parser_cli, "configure_logging", lambda level: None)

    exit_code = cli.main(["demo", "show", "alpha"])

    assert exit_code == 0
    assert capsys.readouterr().out == "legacy:alpha\n"
