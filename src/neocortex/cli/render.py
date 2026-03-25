"""Terminal rendering for command-kernel results."""

from __future__ import annotations

from neocortex.commands import CommandResult
from neocortex.serialization import to_pretty_json
from neocortex.tabular import render_table


def render_command_result(result: CommandResult) -> int:
    """Render one command result to stdout."""

    presentation = result.presentation
    if presentation.kind == "text":
        print(presentation.text or "")
        return 0
    if presentation.kind == "json":
        print(to_pretty_json(presentation.json_value))
        return 0
    if presentation.kind == "table":
        print(render_table(list(presentation.columns), list(presentation.rows)))
        return 0
    raise ValueError(f"Unsupported presentation kind: {presentation.kind}")
