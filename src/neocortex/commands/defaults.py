"""Default command-registry construction."""

from __future__ import annotations

import logging

from neocortex.commands.agent import build_agent_command_specs
from neocortex.commands.connector import build_connector_command_specs
from neocortex.commands.db import build_db_command_specs
from neocortex.commands.feishu import build_feishu_command_specs
from neocortex.commands.indicator import build_all_indicator_command_specs
from neocortex.commands.market_data_provider import (
    build_market_data_provider_command_specs,
)
from neocortex.commands.sync import build_sync_command_specs
from neocortex.commands.core import CommandRegistry
from neocortex.config import get_config


logger = logging.getLogger(__name__)


def build_command_registry() -> CommandRegistry:
    """Build the default command registry."""

    logger.debug("Building default command registry.")
    app_config = get_config()
    default_db_path = str(app_config.storage.market_data_db_path)
    default_bot_db_path = str(app_config.storage.bot_db_path)
    registry = CommandRegistry()
    spec_groups = (
        build_db_command_specs(default_db_path=default_db_path),
        build_market_data_provider_command_specs(default_db_path=default_db_path),
        build_sync_command_specs(default_db_path=default_db_path),
        build_all_indicator_command_specs(default_db_path=default_db_path),
        build_agent_command_specs(default_db_path=default_db_path),
        build_connector_command_specs(default_db_path=default_db_path),
        build_feishu_command_specs(default_db_path=default_bot_db_path),
    )
    for specs in spec_groups:
        for spec in specs:
            registry.register(spec)
    return registry
