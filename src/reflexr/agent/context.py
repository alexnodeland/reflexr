"""The EventContext capability: reflexr's integration with pydantic-ai (ADR-0008).

Add it to an agent whose ``deps_type`` is :class:`~reflexr.workspace.Reaction`. It gives the
agent a tool to read back through the workspace's log and, if it may emit events, a tool to
publish them. The prompt of an :class:`~reflexr.agent.AgentAction` describes what fired.

Its ``wrap_run`` hook runs inside pydantic-ai's ``invoke_agent`` span and adds reflexr's
attribution to it.
"""

from collections.abc import Sequence
from dataclasses import KW_ONLY, dataclass, field
from typing import Any, override

from opentelemetry import trace
from pydantic_ai import AgentRunResult, FunctionToolset, RunContext
from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.capabilities.abstract import WrapRunHandler

from reflexr.agent.tools import event_tools
from reflexr.core import Event
from reflexr.telemetry import attributes as a
from reflexr.telemetry import chain_attributes, workspace_attributes
from reflexr.workspace import Reaction

type Context = RunContext[Reaction[Any]]

INTRO = (
    "You respond to events in an event log. A rule watching the log fired, and you are its "
    "response: the prompt describes the firing. Read further back in the log if you need to."
)


@dataclass
class EventContext(AbstractCapability[Reaction[Any]]):
    """Make an agent a response to a firing.

    Args:
        emit: The event types the agent may publish with the ``emit_event`` tool. Empty, the
            default, gives it no way to publish.
        max_event_chars: How much of each event ``read_events`` shows the model.
        read_limit: The most envelopes one ``read_events`` call returns.
        max_retries: How many times the model may retry a refused tool call.
    """

    emit: Sequence[type[Event]] = ()
    _: KW_ONLY
    max_event_chars: int = 2000
    read_limit: int = 50
    max_retries: int = 3
    _toolset: FunctionToolset[Reaction[Any]] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._toolset = event_tools(
            self.emit,
            read_limit=self.read_limit,
            max_event_chars=self.max_event_chars,
            max_retries=self.max_retries,
        )

    @override
    def get_toolset(self) -> FunctionToolset[Reaction[Any]]:
        """Return the tools for reading the log and emitting events."""
        return self._toolset

    @override
    def get_instructions(self) -> str:
        """Return the instructions: how to respond, and what the agent may emit."""
        if not self.emit:
            return INTRO
        kinds = ", ".join(t.event_type for t in self.emit)
        return f"{INTRO}\n\nYou may publish these kinds of event with emit_event: {kinds}."

    @override
    async def wrap_run(self, ctx: Context, *, handler: WrapRunHandler) -> AgentRunResult[Any]:
        """Attribute the agent's span to its workspace, rule, run and causal chain."""
        reaction = ctx.deps
        run = reaction.run
        workspace = reaction.workspace
        trace.get_current_span().set_attributes(
            {
                **workspace_attributes(workspace.tenant_id, workspace.workspace_id),
                **chain_attributes(run.correlation_id),
                a.RULE: run.rule,
                a.SCOPE: run.scope_key,
                a.RUN_ID: run.id,
                a.ATTEMPT: run.attempts,
            }
        )
        return await handler()
