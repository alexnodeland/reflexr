"""Agent actions: pydantic-ai agents as adapters of reflexr's action port (ADR-0025)."""

from collections.abc import Callable
from dataclasses import KW_ONLY, dataclass

from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.usage import UsageLimits

from reflexr.agent.render import firing_text
from reflexr.workspace import Reaction

DEFAULT_PROMPT = "Respond to this firing."


@dataclass
class AgentAction[D, O]:
    """Run a pydantic-ai agent in response to a firing.

    The agent's ``deps_type`` is ``Reaction[D]``; add the :class:`~reflexr.agent.EventContext`
    capability to give it tools for the log. By default the prompt describes the firing: the
    rule, the scope and the events that made it fire. The agent's output is the run's output.
    Each run joins its causal chain's conversation, so the traces of one incident are one
    session.

    Args:
        agent: The agent.
        name: The name rules refer to the action by; defaults to the agent's name.
        prompt: The user prompt, or a function of the reaction that returns it. Defaults to a
            description of the firing.
        usage_limits: Limits on requests and tokens per attempt.
        params: The model of the params rules pass the action. Its prompt and tools read them
            with ``reaction.params_as(Model)``, naming the model again.
    """

    agent: Agent[Reaction[D], O]
    _: KW_ONLY
    name: str = ""
    prompt: str | Callable[[Reaction[D]], str] | None = None
    usage_limits: UsageLimits | None = None
    params: type[BaseModel] | None = None

    def __post_init__(self) -> None:
        self.name = self.name or self.agent.name or ""
        if not self.name:
            raise ValueError("name the action, or the agent")

    async def __call__(self, reaction: Reaction[D]) -> O:
        """Run the agent for one attempt of a run."""
        result = await self.agent.run(
            self._prompt(reaction),
            deps=reaction,
            conversation_id=reaction.run.correlation_id,
            usage_limits=self.usage_limits,
        )
        return result.output

    def _prompt(self, reaction: Reaction[D]) -> str:
        if callable(self.prompt):
            return self.prompt(reaction)
        if self.prompt is not None:
            return self.prompt
        return f"{firing_text(reaction)}\n\n{DEFAULT_PROMPT}"
