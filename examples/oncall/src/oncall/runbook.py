"""The runbook: a pydantic-graph graph that works an opened incident to resolution.

```mermaid
graph LR
    diagnose --> decide{recent deploy?}
    decide -->|yes| roll_back
    decide -->|no| escalate
    roll_back --> verify
    escalate --> verify
    verify --> resolve
```

Each step is typed ``StepContext[RunbookState, Reaction[OncallDeps], Input]``: its deps are
reflexr's ``Reaction``, so a step reads the log through ``reaction.workspace``, acts through
``reaction.deps``, and emits through ``reaction.emit``. ``GraphAction`` saves the state and the
next step's input after each step, so a retry resumes where the last attempt stopped: a service
that fails verification is not rolled back twice.
"""

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Literal

from pydantic import BaseModel
from pydantic_graph import GraphBuilder, StepContext

from oncall.events import DeployCompleted, IncidentOpened, IncidentResolved
from oncall.services import OncallDeps
from reflexr.agent import GraphAction
from reflexr.workspace import Reaction

RECENT = timedelta(minutes=30)
"""How long before an incident a deploy counts as its likely cause."""


@dataclass
class RunbookState:
    """What the runbook has found and done so far, saved at every checkpoint."""

    notes: list[str] = field(default_factory=list[str])


class RollbackPlan(BaseModel):
    """Roll the service back to the version before a recent deploy."""

    kind: Literal["roll_back"] = "roll_back"
    service: str
    version: str


class EscalationPlan(BaseModel):
    """Nothing to roll back: page the person on call."""

    kind: Literal["escalate"] = "escalate"
    service: str
    reason: str


class Mitigation(BaseModel):
    """What the runbook did about the incident."""

    service: str
    action: str


runbook = GraphBuilder(
    name="runbook",
    state_type=RunbookState,
    deps_type=Reaction[OncallDeps],
    input_type=IncidentOpened,
    output_type=str,
)


@runbook.step
async def diagnose(
    ctx: StepContext[RunbookState, Reaction[OncallDeps], IncidentOpened],
) -> RollbackPlan | EscalationPlan:
    """Look for a recent deploy of the service in the log, and the version before it."""
    service = ctx.inputs.service
    opened = ctx.deps.events[-1]  # the incident.opened envelope that fired the runbook
    deploys = [
        (envelope.ts, envelope.event.version)
        for envelope in await ctx.deps.workspace.read()
        if envelope.seq < opened.seq
        and isinstance(envelope.event, DeployCompleted)
        and envelope.event.service == service
    ]
    recent = [version for at, version in deploys if opened.ts - at <= RECENT]
    if recent and len(deploys) > 1:
        suspect, previous = recent[-1], deploys[-2][1]
        ctx.state.notes.append(f"{service} {suspect} was deployed just before the incident")
        return RollbackPlan(service=service, version=previous)
    reason = (
        f"{service} {recent[-1]} was deployed just before the incident, with no earlier version"
        if recent
        else f"{service} was not deployed in the {RECENT.total_seconds() / 60:g} minutes before it"
    )
    ctx.state.notes.append(reason)
    return EscalationPlan(service=service, reason=reason)


@runbook.step
async def roll_back(
    ctx: StepContext[RunbookState, Reaction[OncallDeps], RollbackPlan],
) -> Mitigation:
    """Roll the service back."""
    plan = ctx.inputs
    await ctx.deps.deps.deployer.rollback(plan.service, plan.version)
    ctx.state.notes.append(f"rolled {plan.service} back to {plan.version}")
    return Mitigation(service=plan.service, action=f"rolled back to {plan.version}")


@runbook.step
async def escalate(
    ctx: StepContext[RunbookState, Reaction[OncallDeps], EscalationPlan],
) -> Mitigation:
    """Page the person on call, once per run however often the step is retried."""
    plan = ctx.inputs
    reaction = ctx.deps
    await reaction.deps.pager.page(plan.service, plan.reason, key=reaction.run_id)
    ctx.state.notes.append(f"paged the person on call about {plan.service}")
    return Mitigation(service=plan.service, action="escalated to the person on call")


@runbook.step
async def verify(ctx: StepContext[RunbookState, Reaction[OncallDeps], Mitigation]) -> Mitigation:
    """Check that the service is healthy, failing the attempt if not; its retry resumes here."""
    service = ctx.inputs.service
    if not await ctx.deps.deps.deployer.healthy(service):
        raise RuntimeError(f"{service} is still unhealthy")
    ctx.state.notes.append(f"{service} is healthy")
    return ctx.inputs


@runbook.step
async def resolve(ctx: StepContext[RunbookState, Reaction[OncallDeps], Mitigation]) -> str:
    """Resolve the incident, in the incident's causal chain."""
    resolution = "; ".join(ctx.state.notes)
    await ctx.deps.emit(IncidentResolved(service=ctx.inputs.service, resolution=resolution))
    return resolution


runbook.add(
    runbook.edge_from(runbook.start_node).to(diagnose),
    runbook.edge_from(diagnose).to(
        runbook.decision(node_id="decide", note="recent deploy?")
        .branch(runbook.match(RollbackPlan).label("yes").to(roll_back))
        .branch(runbook.match(EscalationPlan).label("no").to(escalate))
    ),
    runbook.edge_from(roll_back).to(verify),
    runbook.edge_from(escalate).to(verify),
    runbook.edge_from(verify).to(resolve),
    runbook.edge_from(resolve).to(runbook.end_node),
)
runbook_graph = runbook.build()


def incident_of(reaction: Reaction[OncallDeps]) -> IncidentOpened:
    """The graph's input: the incident that fired the runbook."""
    [incident] = [e.event for e in reaction.events if isinstance(e.event, IncidentOpened)]
    return incident


def runbook_action() -> GraphAction[OncallDeps, RunbookState, IncidentOpened, str]:
    """The runbook as an action.

    The decision's input type is ``diagnose``'s return type, which reflexr infers, so the
    boundary after ``diagnose`` is checkpointed too.
    """
    return GraphAction(runbook_graph, inputs=incident_of)
