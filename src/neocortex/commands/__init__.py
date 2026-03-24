"""CLI-first command-kernel primitives."""

from neocortex.commands.core import (
    AuthPolicy,
    CommandActor,
    CommandArgumentParser,
    CommandContext,
    CommandDispatcher,
    CommandError,
    CommandHelpRequested,
    CommandRegistry,
    CommandResult,
    CommandServices,
    CommandSpec,
    CommandUsageError,
    ExecutionMode,
    Exposure,
    InvocationSource,
    ParsedInvocation,
    PresentationModel,
)
from neocortex.commands.defaults import build_command_registry

__all__ = [
    "AuthPolicy",
    "CommandActor",
    "CommandArgumentParser",
    "CommandContext",
    "CommandDispatcher",
    "CommandError",
    "CommandHelpRequested",
    "CommandRegistry",
    "CommandResult",
    "CommandServices",
    "CommandSpec",
    "CommandUsageError",
    "ExecutionMode",
    "Exposure",
    "InvocationSource",
    "ParsedInvocation",
    "PresentationModel",
    "build_command_registry",
]
