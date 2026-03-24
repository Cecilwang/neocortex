"""Default command-registry construction."""

from __future__ import annotations

import logging

from neocortex.commands.db import build_db_query_command_spec
from neocortex.commands.market_data_provider import (
    build_market_data_provider_bars_command_spec,
    build_market_data_provider_disclosures_command_spec,
    build_market_data_provider_fundamentals_command_spec,
    build_market_data_provider_init_db_command_spec,
    build_market_data_provider_macro_command_spec,
    build_market_data_provider_profile_command_spec,
    build_market_data_provider_securities_command_spec,
    build_market_data_provider_trading_dates_command_spec,
)
from neocortex.commands.sync import (
    build_sync_bars_command_spec,
    build_sync_securities_command_spec,
    build_sync_trading_dates_command_spec,
)
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
    registry.mark_root_managed("db")
    registry.register(
        build_market_data_provider_init_db_command_spec(
            default_db_path=str(app_config.storage.market_data_db_path),
        )
    )
    registry.register(
        build_market_data_provider_securities_command_spec(
            default_db_path=str(app_config.storage.market_data_db_path),
        )
    )
    registry.register(
        build_market_data_provider_bars_command_spec(
            default_db_path=str(app_config.storage.market_data_db_path),
        )
    )
    registry.register(
        build_market_data_provider_fundamentals_command_spec(
            default_db_path=str(app_config.storage.market_data_db_path),
        )
    )
    registry.register(
        build_market_data_provider_profile_command_spec(
            default_db_path=str(app_config.storage.market_data_db_path),
        )
    )
    registry.register(
        build_market_data_provider_disclosures_command_spec(
            default_db_path=str(app_config.storage.market_data_db_path),
        )
    )
    registry.register(
        build_market_data_provider_macro_command_spec(
            default_db_path=str(app_config.storage.market_data_db_path),
        )
    )
    registry.register(
        build_market_data_provider_trading_dates_command_spec(
            default_db_path=str(app_config.storage.market_data_db_path),
        )
    )
    registry.mark_root_managed("market-data-provider")
    registry.register(
        build_sync_securities_command_spec(
            default_db_path=str(app_config.storage.market_data_db_path),
        )
    )
    registry.register(
        build_sync_bars_command_spec(
            default_db_path=str(app_config.storage.market_data_db_path),
        )
    )
    registry.register(
        build_sync_trading_dates_command_spec(
            default_db_path=str(app_config.storage.market_data_db_path),
        )
    )
    registry.mark_root_managed("sync")
    return registry
