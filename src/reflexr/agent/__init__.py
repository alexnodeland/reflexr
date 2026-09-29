"""Agent and graph actions: pydantic-ai and pydantic-graph as adapters of the action port.

An :class:`AgentAction` runs a pydantic-ai agent whose ``deps_type`` is
:class:`~reflexr.workspace.Reaction`; the :class:`EventContext` capability tells it what fired
and gives it tools to read the log and emit events.
"""

from reflexr.agent.actions import AgentAction
from reflexr.agent.context import EventContext
from reflexr.agent.render import envelope_text, firing_text

__all__ = ["AgentAction", "EventContext", "envelope_text", "firing_text"]
