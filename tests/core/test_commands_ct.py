import argparse

import pytest

from neocortex.commands import (
    AuthPolicy,
    CommandActor,
    CommandArgumentParser,
    CommandHelpRequested,
    CommandContext,
    CommandDispatcher,
    CommandRegistry,
    CommandResult,
    CommandSpec,
    CommandUsageError,
    ExecutionMode,
    Exposure,
    InvocationSource,
    ParsedInvocation,
)


def _demo_handler(args: argparse.Namespace, context: CommandContext) -> CommandResult:
    return CommandResult.text(
        f"{context.actor.source.value}:{args.target}:{args.count}:{args.verbose}:{len(args.labels)}"
    )


def _demo_spec(
    *,
    auth: AuthPolicy = AuthPolicy.PUBLIC,
    execution: ExecutionMode = ExecutionMode.SYNC,
    require_target_source: bool = False,
) -> CommandSpec:
    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("target")
        parser.add_argument(
            "--count", type=int, default=1, help="How many times to run."
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose output.",
        )
        parser.add_argument(
            "--label",
            dest="labels",
            action="append",
            default=[],
            help="Repeatable labels.",
        )
        if require_target_source:
            source_group = parser.add_mutually_exclusive_group(required=True)
            source_group.add_argument("--symbol", default=None)
            source_group.add_argument("--name", default=None)

    return CommandSpec(
        id=("demo", "run"),
        summary="Run a demo command.",
        description="Run a demo command with typed arguments.",
        exposure=Exposure.SHARED,
        auth=auth,
        execution=execution,
        configure_parser=configure_parser,
        handler=_demo_handler,
    )


def _inspect_spec(
    command_id: tuple[str, ...],
    *,
    description: str,
) -> CommandSpec:
    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("target")

    return CommandSpec(
        id=command_id,
        summary=f"Summary for {' '.join(command_id)}",
        description=description,
        exposure=Exposure.SHARED,
        auth=AuthPolicy.PUBLIC,
        execution=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=_demo_handler,
    )


def _get_named_subparser(
    parser: argparse.ArgumentParser, name: str
) -> argparse.ArgumentParser:
    for action in parser._actions:
        if not isinstance(action, argparse._SubParsersAction):
            continue
        return action.choices[name]
    raise AssertionError(f"Subparser {name!r} not found")


def _build_registry_parser(registry: CommandRegistry) -> CommandArgumentParser:
    parser = CommandArgumentParser(prog="neocortex")
    subcommands = parser.add_subparsers(dest="_command_root", required=True)
    registry.bind_subcommands(subcommands)
    return parser


def _parse_invocation(
    registry: CommandRegistry,
    tokens: list[str] | tuple[str, ...],
) -> tuple[CommandSpec, argparse.Namespace]:
    parser = _build_registry_parser(registry)
    args = parser.parse_args(tokens)
    spec = getattr(args, "_command_spec", None)
    if not isinstance(spec, CommandSpec):
        raise RuntimeError(
            "Registry parser returned successfully without a bound command spec."
        )
    return spec, args


def test_command_registry_registers_and_lists_specs() -> None:
    registry = CommandRegistry()
    spec = _demo_spec()

    registry.register(spec)

    assert registry.list() == (spec,)


def test_registry_parser_builds_namespace_from_argparse_spec() -> None:
    registry = CommandRegistry()
    spec = _demo_spec()
    registry.register(spec)

    _, args = _parse_invocation(
        registry,
        [
            "demo",
            "run",
            "alpha",
            "--count",
            "2",
            "--verbose",
            "--label",
            "cn",
            "--label",
            "growth",
        ],
    )

    assert args.target == "alpha"
    assert args.count == 2
    assert args.verbose is True
    assert args.labels == ["cn", "growth"]


def test_registry_parser_uses_argparse_validation() -> None:
    registry = CommandRegistry()
    spec = _demo_spec(require_target_source=True)
    registry.register(spec)
    parser = _build_registry_parser(registry)

    with pytest.raises(CommandUsageError) as exc_info:
        parser.parse_args(["demo", "run", "alpha"])

    assert exc_info.value.status == 2
    assert "one of the arguments --symbol --name is required" in exc_info.value.message
    assert exc_info.value.help_text is not None


def test_registry_parser_formats_help_through_argparse() -> None:
    registry = CommandRegistry()
    spec = _demo_spec()
    registry.register(spec)

    parser = _build_registry_parser(registry)

    with pytest.raises(CommandHelpRequested) as exc_info:
        parser.parse_args(["demo", "run", "--help"])

    assert "Run a demo command with typed arguments." in exc_info.value.help_text


def test_registry_build_parser_keeps_group_parser_description_empty() -> None:
    registry = CommandRegistry()
    registry.register(
        _inspect_spec(("demo", "run"), description="Leaf description for demo run.")
    )
    registry.register(
        _inspect_spec(("demo", "show"), description="Leaf description for demo show.")
    )

    parser = _build_registry_parser(registry)
    group_parser = _get_named_subparser(parser, "demo")

    assert group_parser.description is None


def test_command_dispatcher_enforces_auth_and_logs_execution(caplog) -> None:
    registry = CommandRegistry()
    spec = _demo_spec(
        auth=AuthPolicy.ADMIN,
        execution=ExecutionMode.ASYNC,
    )
    registry.register(spec)
    matched_spec, args = _parse_invocation(registry, ["demo", "run", "alpha"])
    dispatcher = CommandDispatcher()
    user_context = CommandContext(
        actor=CommandActor(source=InvocationSource.FEISHU, is_admin=False)
    )
    admin_context = CommandContext(
        actor=CommandActor(source=InvocationSource.CLI, is_admin=True)
    )
    invocation = ParsedInvocation(spec=matched_spec, args=args)

    with pytest.raises(PermissionError):
        dispatcher.dispatch(invocation, user_context)

    with caplog.at_level("INFO"):
        result = dispatcher.dispatch(invocation, admin_context)

    assert result.presentation.text == "cli:alpha:1:False:0"
    assert "Dispatching [demo run]" in caplog.text
    assert "execution=async" in caplog.text


def test_dispatcher_can_be_used_without_cli_transport() -> None:
    registry = CommandRegistry()
    registry.register(_demo_spec())
    matched_spec, args = _parse_invocation(registry, ("demo", "run", "alpha"))
    context = CommandContext(
        actor=CommandActor(
            source=InvocationSource.FEISHU,
            user_id="ou_demo",
            chat_id="oc_demo",
            chat_type="group",
            is_admin=False,
        ),
        request_id="feishu",
    )
    dispatcher = CommandDispatcher()

    result = dispatcher.dispatch(
        ParsedInvocation(spec=matched_spec, args=args), context
    )

    assert result.presentation.text == "feishu:alpha:1:False:0"


def test_command_registry_logs_decisions_without_raw_tokens(caplog) -> None:
    registry = CommandRegistry()
    registry.register(_demo_spec())
    matched_spec, args = _parse_invocation(registry, ("demo", "run", "secret-symbol"))
    context = CommandContext(
        actor=CommandActor(source=InvocationSource.CLI, is_admin=True),
        request_id="cli",
    )
    dispatcher = CommandDispatcher()

    with caplog.at_level("INFO"):
        dispatcher.dispatch(ParsedInvocation(spec=matched_spec, args=args), context)

    assert "Dispatching [demo run]" in caplog.text
    assert "secret-symbol" not in caplog.text
