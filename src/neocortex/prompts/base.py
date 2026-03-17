"""Jinja-backed prompt template utilities."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
from typing import Any

from jinja2 import Environment, StrictUndefined
import yaml

from neocortex.models import AgentRole


@dataclass(frozen=True, slots=True)
class PromptTemplate:
    """One prompt template document with metadata and split sections."""

    dependencies: tuple[AgentRole, ...]
    system: str
    user: str


def render_prompt_text(template_text: str, **context: Any) -> str:
    """Render one already-loaded prompt template string."""

    return _render_template_text(template_text, **context).strip()


def load_prompt_template(template_name: str) -> PromptTemplate:
    """Load one prompt template YAML and return its parsed sections."""

    document = _load_prompt_document(template_name)
    raw_dependencies = document.get("dependencies", [])
    if not isinstance(raw_dependencies, list):
        raise ValueError("Prompt template YAML 'dependencies' must be a list.")
    return PromptTemplate(
        dependencies=tuple(AgentRole(value) for value in raw_dependencies),
        system=str(document["system"]),
        user=str(document["user"]),
    )


def _load_prompt_document(template_name: str) -> dict[str, Any]:
    source = (
        resources.files("neocortex.prompts")
        .joinpath("templates")
        .joinpath(template_name)
        .read_text(encoding="utf-8")
    )
    document = yaml.safe_load(source)
    if not isinstance(document, dict):
        raise ValueError("Prompt template must render to one YAML object.")
    if "system" not in document or "user" not in document:
        raise ValueError("Prompt template YAML must contain 'system' and 'user' keys.")
    return document


def _render_template_text(template_text: str, **context: Any) -> str:
    environment = Environment(
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    return environment.from_string(template_text).render(**context)
