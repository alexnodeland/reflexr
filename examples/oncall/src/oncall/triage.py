"""The triage agent: a pydantic-ai agent that decides whether a burst of alerts is an incident.

It is a plain pydantic-ai ``Agent`` whose deps are reflexr's ``Reaction``. The ``EventContext``
capability gives it ``read_events``, to look back through the log (for a recent deploy, say),
and ``emit_event``, allowed to publish ``oncall:incident.opened`` only. Its prompt, written by
``AgentAction``, describes the firing: the rule, the service and the alerts that fired it.

With ``ONCALL_LITELLM_URL`` set, it calls a LiteLLM proxy instead of a provider, through
reflexr's ``[litellm]`` extra.
"""

import os
from collections.abc import Sequence
from typing import Any

from pydantic import BaseModel, Field
from pydantic_ai import Agent
from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.models import Model
from pydantic_ai.usage import UsageLimits

from oncall.events import IncidentOpened
from oncall.services import OncallDeps
from reflexr.agent import AgentAction, EventContext
from reflexr.litellm import LiteLLMGateway, litellm_model
from reflexr.workspace import Reaction

DEFAULT_MODEL = "anthropic:claude-sonnet-5-5"

INSTRUCTIONS = """\
You triage alerts for an on-call team. A burst of severe alerts for one service fired you.

- Read back through the log if it helps: a recent deploy of the service is the usual cause.
- If the alerts describe a real problem, open an incident for the service with emit_event,
  with a severity from 1 to 10 and a one-line summary a person can act on.
- Do not open an incident for noise, such as one flapping check.
- Answer with your verdict: the severity you assessed and your summary.
"""


class TriageVerdict(BaseModel):
    """The agent's assessment: the run's output."""

    severity: int = Field(ge=1, le=10)
    summary: str


def build_triage_agent(
    model: Model | str | None = None,
    *,
    capabilities: Sequence[AbstractCapability[Any]] = (),
) -> Agent[Reaction[OncallDeps], TriageVerdict]:
    """Build the triage agent.

    With ``ONCALL_LITELLM_URL`` set, the agent calls that LiteLLM proxy's model group
    ``ONCALL_LITELLM_MODEL`` (``claude-sonnet`` by default) with the key ``ONCALL_LITELLM_KEY``,
    and every request carries the tenant, the causal chain and the trace.

    Args:
        model: A pydantic-ai model or model name. Defaults to the LiteLLM proxy, when one is
            configured; otherwise to the ``ONCALL_MODEL`` environment variable, then to a
            current Claude model.
        capabilities: More capabilities, such as pydantic-ai's instrumentation.
    """
    extra = list(capabilities)
    if model is None and (proxy := os.environ.get("ONCALL_LITELLM_URL")):
        group = os.environ.get("ONCALL_LITELLM_MODEL", "claude-sonnet")
        model = litellm_model(group, api_base=proxy, api_key=os.environ.get("ONCALL_LITELLM_KEY"))
        extra.append(LiteLLMGateway(tags=["oncall"]))
    return Agent(
        model or os.environ.get("ONCALL_MODEL", DEFAULT_MODEL),
        name="triage",
        deps_type=Reaction[OncallDeps],
        output_type=TriageVerdict,
        instructions=INSTRUCTIONS,
        capabilities=[EventContext(emit=[IncidentOpened]), *extra],
        defer_model_check=True,
    )


def triage_action(
    model: Model | str | None = None,
    *,
    capabilities: Sequence[AbstractCapability[Any]] = (),
) -> AgentAction[OncallDeps, TriageVerdict]:
    """The triage agent as an action, with a bound on the model requests one attempt makes."""
    agent = build_triage_agent(model, capabilities=capabilities)
    return AgentAction(agent, usage_limits=UsageLimits(request_limit=10))
