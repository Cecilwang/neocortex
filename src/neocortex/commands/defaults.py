"""Default command-registry construction."""

from __future__ import annotations

import logging

from neocortex.commands.db import build_db_query_command_spec
from neocortex.commands.core import CommandRegistry
from neocortex.config import get_config


logger = logging.getLogger(__name__)


def build_command_registry() -> CommandRegistry:
    """Build the default command registry."""

    logger.info("Building default command registry.")
    app_config = get_config()
    registry = CommandRegistry()
    registry.register(
        build_db_query_command_spec(
            default_db_path=str(app_config.storage.market_data_db_path),
        )
    )
    return registry
