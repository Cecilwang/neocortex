"""LLM runtime transport interface used by agents."""

from __future__ import annotations

from typing import Any, Protocol

from neocortex.llm.config import LLMInferenceConfig
from neocortex.models.agent import AgentRole


class LLMTransport(Protocol):
    """Runtime transport used by agents to send rendered prompts."""

    def complete(
        self,
        *,
        agent: AgentRole,
        system_prompt: str,
        user_prompt: str,
        inference_config: LLMInferenceConfig,
    ) -> str | dict[str, Any]:
        """Return one raw model output for the rendered prompt."""
