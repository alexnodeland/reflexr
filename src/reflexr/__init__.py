"""reflexr: rules over event streams that run LLM workflows.

Applications publish events into tenant-scoped workspaces. Rules watch each workspace's log, and
when a rule's condition holds, it runs a workflow: a pydantic-ai agent, a pydantic-graph graph,
or a plain async function::

    class OpsEvent(Event, abstract=True, event_namespace="ops"): ...


    class ServiceError(OpsEvent, name="service.error"):  # ops:service.error
        service: str
        severity: int


    error_spike = Rule(
        name="ops:error-spike",
        when=on(ServiceError).where(F.severity >= 7).count(at_least=3, within=minute),
        scope=by(F.service),
        then=run("triage"),
    )

See ``docs/architecture.md`` for the design.
"""

from importlib.metadata import version

from reflexr.core import (
    Actor,
    AgentActor,
    Envelope,
    EvaluatorActor,
    Event,
    ExternalAgentActor,
    F,
    Feedback,
    InvalidRule,
    NotFound,
    Rejection,
    RetryPolicy,
    Rule,
    SourceActor,
    SystemActor,
    UserActor,
    by,
    field,
    new_id,
    on,
    run,
    sequence,
)

__version__ = version("reflexr")

__all__ = [
    "Actor",
    "AgentActor",
    "Envelope",
    "EvaluatorActor",
    "Event",
    "ExternalAgentActor",
    "F",
    "Feedback",
    "InvalidRule",
    "NotFound",
    "Rejection",
    "RetryPolicy",
    "Rule",
    "SourceActor",
    "SystemActor",
    "UserActor",
    "__version__",
    "by",
    "field",
    "new_id",
    "on",
    "run",
    "sequence",
]
