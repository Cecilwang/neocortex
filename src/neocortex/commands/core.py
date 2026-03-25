"""Command-kernel foundations shared by CLI and chat transports."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Callable

from neocortex.config.config import AppConfig


logger = logging.getLogger(__name__)


class Exposure(StrEnum):
    """Whether a command is visible to CLI, bot, or both."""

    SHARED = "shared"
    CLI_ONLY = "cli_only"
    BOT_ONLY = "bot_only"


class AuthPolicy(StrEnum):
    """High-level access policy for one command."""

    PUBLIC = "public"
    ADMIN = "admin"


class ExecutionMode(StrEnum):
    """Whether a command is expected to complete inline or as a job."""

    SYNC = "sync"
    ASYNC = "async"


class InvocationSource(StrEnum):
    """Transport that invoked the command."""

    CLI = "cli"
    FEISHU = "feishu"


@dataclass(frozen=True, slots=True)
class CommandActor:
    """Identity and privilege data for one invocation."""

    source: InvocationSource
    user_id: str | None = None
    chat_id: str | None = None
    chat_type: str | None = None
    is_admin: bool = False


@dataclass(frozen=True, slots=True)
class CommandServices:
    """Future dependency bag for command handlers."""


@dataclass(frozen=True, slots=True)
class CommandContext:
    """Runtime context shared by command handlers."""

    actor: CommandActor
    config: AppConfig | None = None
    services: CommandServices = CommandServices()
    request_id: str = "command"


@dataclass(frozen=True, slots=True)
class PresentationModel:
    """Renderer-agnostic presentation payload."""

    kind: str
    text: str | None = None
    columns: tuple[str, ...] = ()
    rows: tuple[tuple[object, ...], ...] = ()
    json_value: object | None = None

    @classmethod
    def text_output(cls, text: str) -> PresentationModel:
        return cls(kind="text", text=text)

    @classmethod
    def json_output(cls, value: object) -> PresentationModel:
        return cls(kind="json", json_value=value)

    @classmethod
    def table_output(
        cls,
        *,
        columns: tuple[str, ...],
        rows: tuple[tuple[object, ...], ...],
    ) -> PresentationModel:
        return cls(kind="table", columns=columns, rows=rows)


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Structured command output plus a renderer-friendly view."""

    payload: object | None
    presentation: PresentationModel

    @classmethod
    def text(cls, text: str, *, payload: object | None = None) -> CommandResult:
        return cls(payload=payload, presentation=PresentationModel.text_output(text))

    @classmethod
    def json(cls, value: object, *, payload: object | None = None) -> CommandResult:
        return cls(
            payload=value if payload is None else payload,
            presentation=PresentationModel.json_output(value),
        )

    @classmethod
    def table(
        cls,
        *,
        columns: tuple[str, ...],
        rows: tuple[tuple[object, ...], ...],
        payload: object | None = None,
    ) -> CommandResult:
        return cls(
            payload=payload,
            presentation=PresentationModel.table_output(columns=columns, rows=rows),
        )


CommandHandler = Callable[[argparse.Namespace, CommandContext], CommandResult]
ConfigureParser = Callable[[argparse.ArgumentParser], None]
ExecutionPolicy = Callable[[argparse.Namespace], ExecutionMode]


@dataclass(frozen=True, slots=True)
class CommandSpec:
    """One registered command definition."""

    id: tuple[str, ...]
    summary: str
    description: str
    exposure: Exposure
    auth: AuthPolicy
    execution_mode: ExecutionMode
    configure_parser: ConfigureParser
    handler: CommandHandler
    execution_policy: ExecutionPolicy | None = None

    @property
    def path(self) -> str:
        return " ".join(self.id)

    def get_execution_mode(self, args: argparse.Namespace) -> ExecutionMode:
        if self.execution_policy is None:
            return self.execution_mode
        return self.execution_policy(args)


@dataclass(frozen=True, slots=True)
class ParsedInvocation:
    """One parsed command invocation ready for dispatch."""

    spec: CommandSpec
    args: argparse.Namespace


class CommandError(ValueError):
    """Base class for command-kernel errors."""


class CommandUsageError(CommandError):
    """Raised when command arguments fail validation."""

    def __init__(
        self,
        message: str,
        *,
        help_text: str | None = None,
        status: int = 2,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.help_text = help_text
        self.status = status


class CommandHelpRequested(CommandError):
    """Raised when a caller requests command help."""

    def __init__(self, help_text: str) -> None:
        super().__init__("Command help requested.")
        self.help_text = help_text


class CommandArgumentParser(argparse.ArgumentParser):
    """Argument parser that never prints directly and reports help/usage via exceptions."""

    def _print_message(self, message: str | None, file=None) -> None:  # type: ignore[override]
        _ = message, file

    def exit(self, status: int = 0, message: str | None = None) -> None:
        if status == 0:
            raise CommandHelpRequested(self.format_help())
        raise CommandUsageError(
            (message or "Invalid command.").strip(),
            help_text=self.format_help(),
            status=status,
        )

    def error(self, message: str) -> None:
        raise CommandUsageError(
            message,
            help_text=self.format_help(),
            status=2,
        )


class CommandRegistry:
    """In-memory command registry built on top of argparse."""

    def __init__(self) -> None:
        self._specs: dict[tuple[str, ...], CommandSpec] = {}

    def register(self, spec: CommandSpec) -> None:
        if spec.id in self._specs:
            raise ValueError(f"Command {spec.path!r} is already registered.")
        logger.debug(
            f"Registering command spec: path={spec.path} auth={spec.auth.value} "
            f"execution={spec.execution_mode.value} exposure={spec.exposure.value}"
        )
        self._specs[spec.id] = spec

    def list(self, *, exposure: Exposure | None = None) -> tuple[CommandSpec, ...]:
        specs = tuple(sorted(self._specs.values(), key=lambda spec: spec.id))
        if exposure is None:
            return specs
        return tuple(spec for spec in specs if spec.exposure is exposure)

    def bind_subcommands(
        self,
        subcommands: argparse._SubParsersAction[argparse.ArgumentParser],
    ) -> None:
        logger.debug("Binding registry commands into argparse subparsers.")
        parser_nodes: dict[tuple[str, ...], argparse.ArgumentParser] = {}
        nested_subparsers: dict[
            tuple[str, ...], argparse._SubParsersAction[argparse.ArgumentParser]
        ] = {(): subcommands}
        for spec in self.list():
            prefix: tuple[str, ...] = ()
            for index, part in enumerate(spec.id):
                is_leaf = index == len(spec.id) - 1
                prefix = (*prefix, part)
                child = parser_nodes.get(prefix)
                if child is None:
                    parent = nested_subparsers[prefix[:-1]]
                    parser_kwargs = (
                        {
                            "help": spec.summary,
                            "description": spec.description,
                        }
                        if is_leaf
                        else {}
                    )
                    child = parent.add_parser(part, **parser_kwargs)
                    parser_nodes[prefix] = child
                if is_leaf:
                    logger.debug(f"Binding argparse leaf for [{spec.path}]")
                    spec.configure_parser(child)
                    child.set_defaults(_command_spec=spec)
                    continue
                next_subparsers = nested_subparsers.get(prefix)
                if next_subparsers is None:
                    next_subparsers = child.add_subparsers(
                        dest=f"_subcommand_{index}",
                        required=True,
                    )
                    nested_subparsers[prefix] = next_subparsers


class CommandDispatcher:
    """Run parsed command invocations with shared auth checks and logging."""

    def dispatch(
        self,
        invocation: ParsedInvocation,
        context: CommandContext,
    ) -> CommandResult:
        spec = invocation.spec
        execution_mode = spec.get_execution_mode(invocation.args)
        logger.info(
            f"Dispatching [{spec.path}] source={context.actor.source.value} "
            f"auth={spec.auth.value} execution={execution_mode.value}"
        )
        if spec.auth is AuthPolicy.ADMIN and not context.actor.is_admin:
            logger.warning(
                f"Rejected admin [{spec.path}] source={context.actor.source.value}"
            )
            raise PermissionError(f"Permission denied for [{spec.path}].")
        try:
            result = spec.handler(invocation.args, context)
        except Exception:
            logger.exception(f"Command handler failed: [{spec.path}]")
            raise
        logger.debug(f"Command completed: [{spec.path}]")
        return result
