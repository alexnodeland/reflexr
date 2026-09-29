"""Graph actions: pydantic-graph graphs, checkpointed at safe step boundaries and resumed."""

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import BaseModel, JsonValue
from pydantic_graph import GraphBuilder, StepContext
from pydantic_graph.join import reduce_list_append

from reflexr import F, RetryPolicy, Rule, SourceActor, by, on, run
from reflexr.agent import GraphAction
from reflexr.agent.graphs import CHECKPOINT_VERSION
from reflexr.core import RunProgressed
from reflexr.workspace import (
    InMemoryStorage,
    Reaction,
    Reactor,
    Storage,
    Workspace,
    WorkspaceRef,
    Workspaces,
)
from tests.event_types import Deploy
from tests.workspace.conftest import FakeClock

ACME = WorkspaceRef("acme", "prod")


@dataclass
class Flaky:
    """Deps that make chosen steps fail once, and record every step that ran."""

    fail_once: set[str] = field(default_factory=set[str])
    ran: list[str] = field(default_factory=list[str])

    def step(self, name: str) -> None:
        self.ran.append(name)
        if name in self.fail_once:
            self.fail_once.discard(name)
            raise RuntimeError(f"{name} failed")


@dataclass
class Notes:
    log: list[str] = field(default_factory=list[str])


class Finding(BaseModel):
    service: str
    cause: str


# ─── a sequential runbook ────────────────────────────────────────────────────

seq = GraphBuilder(
    name="runbook", state_type=Notes, deps_type=Reaction[Flaky], input_type=str, output_type=str
)


@seq.step
async def diagnose(ctx: StepContext[Notes, Reaction[Flaky], str]) -> Finding:
    ctx.deps.deps.step("diagnose")
    ctx.state.log.append("diagnosed")
    return Finding(service=ctx.inputs, cause="bad deploy")


@seq.step
async def mitigate(ctx: StepContext[Notes, Reaction[Flaky], Finding]) -> str:
    ctx.deps.deps.step("mitigate")
    ctx.state.log.append("mitigated")
    return f"rolled back {ctx.inputs.service}"


@seq.step
async def report(ctx: StepContext[Notes, Reaction[Flaky], str]) -> str:
    ctx.deps.deps.step("report")
    return f"{ctx.inputs} after {', '.join(ctx.state.log)}"


seq.add(
    seq.edge_from(seq.start_node).to(diagnose),
    seq.edge_from(diagnose).to(mitigate),
    seq.edge_from(mitigate).to(report),
    seq.edge_from(report).to(seq.end_node),
)
runbook = seq.build()


def service(reaction: Reaction[Flaky]) -> str:
    return str(reaction.scope["service"])


@dataclass
class Setup:
    workspace: Workspace
    reactor: Reactor[Flaky]
    storage: Storage


async def setup(
    action: GraphAction[Flaky, Any, Any, Any],
    deps: Flaky,
    clock: FakeClock,
    provider: TracerProvider | None = None,
) -> Setup:
    rule = Rule(
        name="deploys",
        when=on(Deploy),
        scope=by(F.service),
        then=run(action.name),
        retry=RetryPolicy(max_attempts=3, backoff=timedelta(seconds=1)),
    )
    storage = InMemoryStorage(clock=clock)
    workspaces = Workspaces(storage, rules=[rule], clock=clock, tracer_provider=provider)
    workspace = await workspaces.open("acme", "prod", actor=SourceActor(name="ci"))
    await workspace.publish(Deploy(service="auth"))
    return Setup(workspace, Reactor(workspaces, actions={action.name: action}, deps=deps), storage)


async def progress(workspace: Workspace) -> list[str]:
    return [e.event.step for e in await workspace.read() if isinstance(e.event, RunProgressed)]


async def test_a_graph_runs_and_checkpoints_after_each_step(clock: FakeClock) -> None:
    spans = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(spans))
    deps = Flaky()
    action = GraphAction(runbook, inputs=service)
    it = await setup(action, deps, clock, provider)
    workspace = it.workspace
    await it.reactor.settle()
    [done] = await workspace.runs()
    assert (done.status, done.output) == (
        "succeeded",
        "rolled back auth after diagnosed, mitigated",
    )
    assert await progress(workspace) == ["__start__", "diagnose", "mitigate", "report", "__end__"]
    assert done.checkpoint is not None
    names = [s.name for s in spans.get_finished_spans() if s.name.startswith("execute_step")]
    assert names == [
        "execute_step diagnose",
        "execute_step mitigate",
        "execute_step report",
        "execute_step __end__",
    ]
    provider.shutdown()


async def test_a_retry_resumes_after_the_last_completed_step(clock: FakeClock) -> None:
    deps = Flaky(fail_once={"mitigate"})
    it = await setup(GraphAction(runbook, inputs=service), deps, clock)
    workspace = it.workspace
    await it.reactor.settle()
    [waiting] = await workspace.runs()
    assert (waiting.status, waiting.step) == ("retrying", "diagnose")
    clock.advance(1)
    await it.reactor.settle()
    [done] = await workspace.runs()
    assert done.output == "rolled back auth after diagnosed, mitigated"
    assert deps.ran == ["diagnose", "mitigate", "mitigate", "report"]  # diagnose ran once


async def test_a_finished_graph_is_not_run_again(clock: FakeClock) -> None:
    deps = Flaky()
    action = GraphAction(runbook, inputs=service)
    it = await setup(action, deps, clock)
    workspace = it.workspace
    await it.reactor.evaluate()
    [pending] = await workspace.runs()
    finished = {"v": CHECKPOINT_VERSION, "nodes": sorted(runbook.nodes), "output": "done before"}
    reaction = Reaction(
        workspace=workspace,
        run=pending.model_copy(update={"checkpoint": finished}),
        rule=Rule(name="deploys", when=on(Deploy), then=run("runbook")),
        events=(),
        deps=deps,
    )
    assert await action(reaction) == "done before"
    assert deps.ran == []


@pytest.mark.parametrize(
    "checkpoint",
    ["not a checkpoint", {"v": 0}, {"v": CHECKPOINT_VERSION, "nodes": ["other"]}],
)
async def test_checkpoints_this_graph_cannot_resume_start_over(
    checkpoint: JsonValue, clock: FakeClock
) -> None:
    deps = Flaky()
    it = await setup(GraphAction(runbook, inputs=service), deps, clock)
    await it.reactor.evaluate()
    [pending] = await it.workspace.runs()
    async with it.storage.transaction(ACME) as transaction:
        await transaction.save_runs([pending.model_copy(update={"checkpoint": checkpoint})])
    await it.reactor.settle()
    assert deps.ran == ["diagnose", "mitigate", "report"]


# ─── a fork and a join ───────────────────────────────────────────────────────

fj = GraphBuilder(
    name="fanout",
    state_type=Notes,
    deps_type=Reaction[Flaky],
    input_type=list[str],
    output_type=list[str],
)


@fj.step
async def prep(ctx: StepContext[Notes, Reaction[Flaky], list[str]]) -> list[str]:
    ctx.deps.deps.step("prep")
    return ctx.inputs


@fj.step
async def check(ctx: StepContext[Notes, Reaction[Flaky], str]) -> str:
    ctx.deps.deps.step(f"check {ctx.inputs}")
    return ctx.inputs.upper()


@fj.step
async def summarize(ctx: StepContext[Notes, Reaction[Flaky], list[str]]) -> list[str]:
    ctx.deps.deps.step("summarize")
    return sorted(ctx.inputs)


gather = fj.join(reduce_list_append, initial_factory=list[str], node_id="gather")
fj.add(
    fj.edge_from(fj.start_node).to(prep),
    fj.edge_from(prep).map(fork_id="fan").to(check),
    fj.edge_from(check).to(gather),
    fj.edge_from(gather).to(summarize),
    fj.edge_from(summarize).to(fj.end_node),
)
fanout = fj.build()


def hosts(reaction: Reaction[Flaky]) -> list[str]:
    return ["a", "b", "c"]


async def test_inside_a_fork_nothing_is_saved_and_a_retry_reruns_the_branches(
    clock: FakeClock,
) -> None:
    deps = Flaky(fail_once={"check b"})
    it = await setup(GraphAction(fanout, inputs=hosts), deps, clock)
    await it.reactor.settle()
    [waiting] = await it.workspace.runs()
    assert waiting.step == "__start__"  # the last safe boundary before the fork
    clock.advance(1)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert done.output == ["A", "B", "C"]
    assert deps.ran.count("check a") == 2  # at least once: the branch ran again
    # After the join, boundaries are safe again.
    assert await progress(it.workspace) == ["__start__", "gather", "summarize", "__end__"]
    with_fork = GraphAction(fanout, inputs=hosts, input_types={"fan": list[str]})
    saved = await setup(with_fork, Flaky(fail_once={"check b"}), clock)
    await saved.reactor.settle()
    assert (await saved.workspace.runs())[0].step == "prep"  # the fork's input is saved too


# ─── decisions, overrides and inputs that cannot be saved ────────────────────

dg = GraphBuilder(
    name="routing", state_type=Notes, deps_type=Reaction[Flaky], input_type=int, output_type=str
)


@dg.step
async def measure(ctx: StepContext[Notes, Reaction[Flaky], int]) -> int:
    ctx.deps.deps.step("measure")
    return ctx.inputs


@dg.step
async def loud(ctx) -> str:  # untyped: its input type must be given
    return f"loud {ctx.inputs}"


@dg.step
async def quiet(ctx: StepContext[Notes, Reaction[Flaky], int]) -> str:
    return f"quiet {ctx.inputs}"


dg.add(
    dg.edge_from(dg.start_node).to(measure),
    dg.edge_from(measure).to(
        dg.decision(node_id="route")
        .branch(dg.match(int, matches=lambda n: n > 5).to(loud))
        .branch(dg.match(int).to(quiet))
    ),
    dg.edge_from(loud).to(dg.end_node),
    dg.edge_from(quiet).to(dg.end_node),
)
routing = dg.build()


async def test_boundaries_before_decisions_and_unknown_inputs_are_not_saved(
    clock: FakeClock,
) -> None:
    action = GraphAction(routing, name="route", inputs=lambda reaction: 9)
    it = await setup(action, Flaky(), clock)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert done.output == "loud 9"
    assert await progress(it.workspace) == ["__start__", "loud", "__end__"]  # not route
    overridden = GraphAction(
        routing, name="route", inputs=lambda reaction: 9, input_types={"loud": int}
    )
    again = await setup(overridden, Flaky(), clock)
    await again.reactor.settle()
    assert await progress(again.workspace) == ["__start__", "route", "loud", "__end__"]
    mistyped = GraphAction(
        routing, name="route", inputs=lambda reaction: 9, input_types={"loud": list[str]}
    )
    third = await setup(mistyped, Flaky(), clock)
    await third.reactor.settle()
    assert await progress(third.workspace) == ["__start__", "loud", "__end__"]  # 9: not a list


def test_an_action_needs_a_name() -> None:
    unnamed = GraphBuilder(state_type=Notes, deps_type=Reaction[Flaky], output_type=str)

    @unnamed.step
    async def only(ctx: StepContext[Notes, Reaction[Flaky], None]) -> str:
        return "x"

    unnamed.add(
        unnamed.edge_from(unnamed.start_node).to(only), unnamed.edge_from(only).to(unnamed.end_node)
    )
    graph = unnamed.build()
    with pytest.raises(ValueError, match="name the action, or the graph"):
        GraphAction(graph)
    assert GraphAction(graph, name="only").name == "only"
    assert GraphAction(runbook).name == "runbook"


jl = GraphBuilder(
    name="scatter",
    state_type=Notes,
    deps_type=Reaction[Flaky],
    input_type=list[str],
    output_type=str,
)


@jl.step
async def spread(ctx: StepContext[Notes, Reaction[Flaky], list[str]]) -> list[str]:
    return ctx.inputs


@jl.step
async def shout(ctx: StepContext[Notes, Reaction[Flaky], str]) -> str:
    return ctx.inputs.upper()


jl.add(
    jl.edge_from(jl.start_node).to(spread),
    jl.edge_from(spread).map(fork_id="spray").to(shout),
    jl.edge_from(shout).to(jl.end_node),
)
scatter = jl.build()


async def test_branches_of_a_fork_without_a_join_are_never_saved(clock: FakeClock) -> None:
    action = GraphAction(scatter, inputs=lambda reaction: ["a"])
    it = await setup(action, Flaky(), clock)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert done.output == "A"
    assert await progress(it.workspace) == ["__start__", "__end__"]  # not inside the fork
