"""Agent and graph actions: pydantic-ai and pydantic-graph as adapters of the action port.

An :class:`AgentAction` runs a pydantic-ai agent whose ``deps_type`` is
:class:`~reflexr.workspace.Reaction`; the :class:`EventContext` capability gives it tools to
read the log and emit events. A :class:`GraphAction` runs a pydantic-graph graph, checkpointed
at its step boundaries so a retry resumes after the last completed step.
"""

from reflexr.agent.actions import AgentAction
from reflexr.agent.context import EventContext
from reflexr.agent.graphs import GraphAction
from reflexr.agent.render import envelope_text, firing_text

__all__ = ["AgentAction", "EventContext", "GraphAction", "envelope_text", "firing_text"]
