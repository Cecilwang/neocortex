"""Registry-backed agent command definitions."""

from __future__ import annotations

import argparse
import logging

from neocortex.commands.core import (
    AuthPolicy,
    CommandContext,
    CommandResult,
    CommandSpec,
    ExecutionMode,
    Exposure,
)
from neocortex.date_resolution import add_as_of_date_argument, resolve_as_of_date
from neocortex.market_data_provider import ReadThroughMarketDataProvider
from neocortex.models import AgentRole
from neocortex.pipeline.pipeline import Pipeline
from neocortex.security_resolution import (
    add_security_identity_arguments,
    resolve_security_id,
)


logger = logging.getLogger(__name__)


def build_agent_render_command_spec(
    *,
    default_db_path: str,
) -> CommandSpec:
    """Build the registry command spec for agent render."""

    def configure_parser(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--db-path", type=str, default=default_db_path)
        parser.add_argument(
            "--role",
            required=True,
            choices=tuple(role.value for role in AgentRole),
        )
        add_security_identity_arguments(parser)
        add_as_of_date_argument(parser)
        parser.add_argument("--request-id", default="cli-agent-render")
        parser.add_argument(
            "--format",
            default="json",
            choices=("json", "text"),
        )

    def handler(args: argparse.Namespace, context: CommandContext) -> CommandResult:
        _ = context
        provider = ReadThroughMarketDataProvider.from_defaults(args.db_path)
        security_id = resolve_security_id(args, db_path=args.db_path)
        logger.info(
            f"Running agent render command: role={args.role} security={security_id.ticker} "
            f"as_of_date={args.as_of_date} request_id={args.request_id} format={args.format}"
        )
        request, system_prompt, user_prompt = _build_render_payload(
            args,
            provider=provider,
            security_id=security_id,
        )
        if args.format == "json":
            return CommandResult.json(
                {
                    "request": request,
                    "system_prompt": system_prompt,
                    "user_prompt": user_prompt,
                },
                payload=request,
            )
        return CommandResult.text(
            f"System Prompt:\n{system_prompt}\n\nUser Prompt:\n{user_prompt}",
            payload=request,
        )

    return CommandSpec(
        id=("agent", "render"),
        summary="Render one agent request and its prompts.",
        description="Render one agent request and its prompts.",
        exposure=Exposure.SHARED,
        auth=AuthPolicy.PUBLIC,
        execution=ExecutionMode.SYNC,
        configure_parser=configure_parser,
        handler=handler,
    )


def _build_render_payload(
    args: argparse.Namespace,
    *,
    provider,
    security_id,
) -> tuple[object, str, str]:
    pipeline = Pipeline(market_data=provider)
    role = AgentRole(args.role)
    agent = pipeline.get_agent(role)
    as_of_date = resolve_as_of_date(
        market=security_id.market,
        provider=provider,
        as_of_date=args.as_of_date,
    )
    request = agent.build_request(
        request_id=args.request_id,
        security_id=security_id,
        as_of_date=as_of_date,
    )
    system_prompt, user_prompt = agent.render_prompts(request)
    return request, system_prompt, user_prompt
