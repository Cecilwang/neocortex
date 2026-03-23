"""Agent CLI commands."""

from __future__ import annotations

import argparse
import logging

from neocortex.models import AgentRole
from neocortex.pipeline.pipeline import Pipeline
from neocortex.serialization import to_pretty_json

from neocortex.cli.common import (
    add_as_of_date_argument,
    add_security_identity_arguments,
    market_data_provider,
    resolve_as_of_date,
    resolve_security_id,
)

logger = logging.getLogger(__name__)


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


def run_agent_render(args: argparse.Namespace) -> int:
    provider = market_data_provider(args)
    security_id = resolve_security_id(args)
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
        print(
            to_pretty_json(
                {
                    "request": request,
                    "system_prompt": system_prompt,
                    "user_prompt": user_prompt,
                }
            )
        )
        return 0
    print(f"System Prompt:\n{system_prompt}\n\nUser Prompt:\n{user_prompt}")
    return 0


def add_agent_commands(
    subcommands: argparse._SubParsersAction[argparse.ArgumentParser],
    *,
    default_db_path: str,
) -> None:
    agent_parser = subcommands.add_parser("agent")
    agent_parser.add_argument("--db-path", type=str, default=default_db_path)
    agent_commands = agent_parser.add_subparsers(dest="command", required=True)

    agent_render = agent_commands.add_parser("render")
    agent_render.add_argument(
        "--role",
        required=True,
        choices=tuple(role.value for role in AgentRole),
    )
    add_security_identity_arguments(agent_render)
    add_as_of_date_argument(agent_render)
    agent_render.add_argument("--request-id", default="cli-agent-render")
    agent_render.add_argument(
        "--format",
        default="json",
        choices=("json", "text"),
    )
    agent_render.set_defaults(handler=run_agent_render)
