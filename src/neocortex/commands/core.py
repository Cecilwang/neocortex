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


@dataclass(frozen=True, slots=True)
class CommandSpec:
    """One registered command definition."""

    id: tuple[str, ...]
    summary: str
    description: str
    exposure: Exposure
    auth: AuthPolicy
    execution: ExecutionMode
    configure_parser: ConfigureParser
    handler: CommandHandler

    @property
    def path(self) -> str:
        return " ".join(self.id)


@dataclass(frozen=True, slots=True)
class ParsedInvocation:
    """One parsed command invocation ready for dispatch."""

    spec: CommandSpec
    args: argparse.Namespace
    raw_tokens: tuple[str, ...]


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
    """Argument parser that reports help and usage as exceptions."""

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
        self._managed_roots: set[str] = set()

    def register(self, spec: CommandSpec) -> None:
        if spec.id in self._specs:
            raise ValueError(f"Command {spec.path!r} is already registered.")
        logger.info(
            f"Registering command spec: path={spec.path} auth={spec.auth.value} "
            f"execution={spec.execution.value} exposure={spec.exposure.value}"
        )
        self._specs[spec.id] = spec

    def list(self, *, exposure: Exposure | None = None) -> tuple[CommandSpec, ...]:
        specs = tuple(sorted(self._specs.values(), key=lambda spec: spec.id))
        if exposure is None:
            return specs
        return tuple(spec for spec in specs if spec.exposure is exposure)

    def root_commands(self) -> tuple[str, ...]:
        # Temporary mixed-mode CLI bridge. Remove once all roots are registry-managed.
        return tuple(sorted({command_id[0] for command_id in self._specs}))

    def mark_root_managed(self, root: str) -> None:
        # Temporary mixed-mode CLI bridge. Remove once legacy CLI dispatch is deleted.
        self._managed_roots.add(root)

    def manages_root(self, root: str | None) -> bool:
        # Temporary mixed-mode CLI bridge. Remove once legacy CLI dispatch is deleted.
        return root in self._managed_roots

    def match_command(
        self, tokens: list[str] | tuple[str, ...]
    ) -> tuple[str, ...] | None:
        """Return the longest registered command path found in the leading argv tokens.

        Temporary mixed-mode CLI bridge. Remove once legacy CLI dispatch is deleted.
        """

        matched_command: tuple[str, ...] | None = None
        path_tokens: list[str] = []
        for token in tokens:
            if token.startswith("-"):
                break
            path_tokens.append(token)
            candidate = tuple(path_tokens)
            if candidate in self._specs:
                matched_command = candidate
        return matched_command

    def build_parser(self) -> CommandArgumentParser:
        logger.debug("Building argparse parser for command registry.")
        parser = CommandArgumentParser(prog="neocortex")
        subcommands = parser.add_subparsers(dest="_command_root", required=True)
        parser_nodes: dict[tuple[str, ...], argparse.ArgumentParser] = {}
        nested_subparsers: dict[tuple[str, ...], argparse._SubParsersAction] = {
            (): subcommands
        }
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
                parent = next_subparsers
        return parser

    def parse(self, tokens: list[str] | tuple[str, ...]) -> ParsedInvocation:
        raw_tokens = tuple(tokens)
        logger.debug("Registry parser received command invocation.")
        parser = self.build_parser()
        args = parser.parse_args(raw_tokens)
        spec = getattr(args, "_command_spec", None)
        if not isinstance(spec, CommandSpec):
            raise RuntimeError(
                "Registry parser returned successfully without a bound command spec."
            )
        logger.debug(f"Registry parser matched [{spec.path}]")
        return ParsedInvocation(
            spec=spec,
            args=args,
            raw_tokens=raw_tokens,
        )

    def run(
        self,
        tokens: list[str] | tuple[str, ...],
        context: CommandContext,
    ) -> CommandResult:
        """Parse and execute one registry-managed command for any transport."""

        raw_tokens = tuple(tokens)
        logger.debug(f"Executing registry command: source={context.actor.source.value}")
        invocation = self.parse(raw_tokens)
        dispatcher = CommandDispatcher()
        return dispatcher.dispatch(invocation, context)


class CommandDispatcher:
    """Run parsed command invocations with shared auth checks and logging."""

    def dispatch(
        self,
        invocation: ParsedInvocation,
        context: CommandContext,
    ) -> CommandResult:
        spec = invocation.spec
        logger.info(
            f"Dispatching [{spec.path}] source={context.actor.source.value} "
            f"auth={spec.auth.value} execution={spec.execution.value}"
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
