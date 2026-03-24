"""Default command-registry construction."""

from __future__ import annotations

import logging

from neocortex.commands.core import CommandRegistry


logger = logging.getLogger(__name__)


def build_command_registry() -> CommandRegistry:
    """Build the default command registry.

    Iteration 1 keeps the registry empty while mixed-mode CLI wiring lands.
    """

    logger.info("Building default command registry.")
    return CommandRegistry()
