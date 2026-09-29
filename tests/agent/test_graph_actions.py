"""Graph actions: pydantic-graph graphs, checkpointed at safe step boundaries and resumed."""

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Annotated, Any

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import BaseModel, Field, JsonValue
from pydantic_graph import Graph, GraphBuilder, StepContext
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
    workspaces: Workspaces
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
    reactor = Reactor(workspaces, actions={action.name: action}, deps=deps)
    return Setup(workspaces, workspace, reactor, storage)


async def progress(workspace: Workspace) -> list[str]:
    return [e.event.step for e in await workspace.read() if isinstance(e.event, RunProgressed)]


def frontier(checkpoint: JsonValue) -> list[tuple[str, JsonValue]]:
    """The saved next tasks: each one's node and inputs."""
    saved: Any = checkpoint
    return [(task["node_id"], task["inputs"]) for task in saved["frontier"]]


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
    [
        "not a checkpoint",
        {"v": 0},
        {"v": CHECKPOINT_VERSION, "nodes": ["other"]},
        # The same nodes, but the next one's input type is not known (as when an edge into a
        # decision has gained a transform since).
        {
            "v": CHECKPOINT_VERSION,
            "nodes": sorted(runbook.nodes),
            "frontier": [{"node_id": "__start__"}],
        },
    ],
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
    # The last safe boundary is before the fork, whose input type is prep's return type.
    assert waiting.step == "prep"
    assert frontier(waiting.checkpoint) == [("fan", ["a", "b", "c"])]
    clock.advance(1)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert done.output == ["A", "B", "C"]
    assert deps.ran.count("prep") == 1  # the retry resumed at the fork
    assert deps.ran.count("check a") == 2  # at least once: the branch ran again
    # After the join, boundaries are safe again.
    assert await progress(it.workspace) == ["__start__", "prep", "gather", "summarize", "__end__"]


# ─── a fork into parallel steps, and a join ──────────────────────────────────

pj = GraphBuilder(
    name="evidence", state_type=Notes, deps_type=Reaction[Flaky], input_type=str, output_type=str
)


@pj.step
async def locate(ctx: StepContext[Notes, Reaction[Flaky], str]) -> str:
    ctx.deps.deps.step("locate")
    return f"{ctx.inputs}.internal"


@pj.step
async def logs(ctx: StepContext[Notes, Reaction[Flaky], str]) -> str:
    ctx.deps.deps.step("logs")
    return f"logs of {ctx.inputs}"


@pj.step
async def metrics(ctx: StepContext[Notes, Reaction[Flaky], str]) -> str:
    ctx.deps.deps.step("metrics")
    return f"metrics of {ctx.inputs}"


@pj.step
async def weigh(ctx: StepContext[Notes, Reaction[Flaky], list[str]]) -> str:
    return "; ".join(sorted(ctx.inputs))


both = pj.join(reduce_list_append, initial_factory=list[str], node_id="both")
pj.add(
    pj.edge_from(pj.start_node).to(locate),
    pj.edge_from(locate).to(logs, metrics, fork_id="gather_evidence"),
    pj.edge_from(logs).to(both),
    pj.edge_from(metrics).to(both),
    pj.edge_from(both).to(weigh),
    pj.edge_from(weigh).to(pj.end_node),
)
evidence = pj.build()


async def test_a_fork_into_parallel_steps_is_saved_before_it_without_input_types(
    clock: FakeClock,
) -> None:
    deps = Flaky(fail_once={"metrics"})
    it = await setup(GraphAction(evidence, inputs=service), deps, clock)
    await it.reactor.settle()
    [waiting] = await it.workspace.runs()
    assert waiting.step == "locate"  # the fork's input is locate's return type, a str
    assert frontier(waiting.checkpoint) == [("gather_evidence", "auth.internal")]
    clock.advance(1)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert done.output == "logs of auth.internal; metrics of auth.internal"
    assert deps.ran.count("locate") == 1  # resumed at the fork, not before locate
    assert deps.ran.count("logs") == 2  # both branches ran again
    assert await progress(it.workspace) == ["__start__", "locate", "both", "weigh", "__end__"]


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
    ctx.deps.deps.step("loud")
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


def nine(reaction: Reaction[Flaky]) -> int:
    return 9


async def test_a_decision_after_an_annotated_step_is_saved_without_input_types(
    clock: FakeClock,
) -> None:
    deps = Flaky(fail_once={"loud"})
    it = await setup(GraphAction(routing, name="route", inputs=nine), deps, clock)
    await it.reactor.settle()
    [waiting] = await it.workspace.runs()
    # measure returns an int, so that is route's input type. loud is untyped, so the boundary
    # after route is not saved, and the retry resumes before route.
    assert waiting.step == "measure"
    assert frontier(waiting.checkpoint) == [("route", 9)]
    clock.advance(1)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert done.output == "loud 9"
    assert deps.ran == ["measure", "loud", "loud"]  # measure ran once
    assert await progress(it.workspace) == ["__start__", "measure", "loud", "__end__"]


async def test_a_fresh_run_resumes_from_an_inferred_checkpoint(clock: FakeClock) -> None:
    uninterrupted = await setup(GraphAction(routing, name="route", inputs=nine), Flaky(), clock)
    await uninterrupted.reactor.settle()
    [expected] = await uninterrupted.workspace.runs()
    first = await setup(
        GraphAction(routing, name="route", inputs=nine), Flaky(fail_once={"loud"}), clock
    )
    await first.reactor.settle()
    [waiting] = await first.workspace.runs()
    assert frontier(waiting.checkpoint) == [("route", 9)]
    # Another executor, with nothing of the first but its storage, picks the retry up.
    fresh = Flaky()
    action = GraphAction(routing, name="route", inputs=nine)
    reactor = Reactor(first.workspaces, actions={action.name: action}, deps=fresh)
    clock.advance(1)
    await reactor.settle()
    [done] = await first.workspace.runs()
    assert (done.status, done.output) == ("succeeded", expected.output)
    assert fresh.ran == ["loud"]  # it started at route, from the saved int


async def test_an_explicit_type_wins_over_an_inferred_one(clock: FakeClock) -> None:
    # Inferred, route's type would be int, and 9 would be saved. The explicit type says fewer
    # than 5, so 9 does not read back as it, and the boundary before route is not saved.
    small = Annotated[int, Field(lt=5)]
    action = GraphAction(routing, name="route", inputs=nine, input_types={"route": small})
    it = await setup(action, Flaky(), clock)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert done.output == "loud 9"
    assert await progress(it.workspace) == ["__start__", "loud", "__end__"]


async def test_explicit_types_fill_in_what_cannot_be_inferred(clock: FakeClock) -> None:
    typed = GraphAction(routing, name="route", inputs=nine, input_types={"loud": int})
    it = await setup(typed, Flaky(), clock)
    await it.reactor.settle()
    assert await progress(it.workspace) == ["__start__", "measure", "route", "loud", "__end__"]
    mistyped = GraphAction(routing, name="route", inputs=nine, input_types={"loud": list[str]})
    again = await setup(mistyped, Flaky(), clock)
    await again.reactor.settle()
    assert await progress(again.workspace) == [
        "__start__",
        "measure",
        "loud",
        "__end__",
    ]  # 9: not a list


# ─── decisions after decisions ───────────────────────────────────────────────

ch = GraphBuilder(
    name="grading", state_type=Notes, deps_type=Reaction[Flaky], input_type=int, output_type=str
)


@ch.step
async def level(ctx: StepContext[Notes, Reaction[Flaky], int]) -> int:
    return ctx.inputs


@ch.step
async def huge(ctx: StepContext[Notes, Reaction[Flaky], int]) -> str:
    return f"huge {ctx.inputs}"


@ch.step
async def large(ctx: StepContext[Notes, Reaction[Flaky], int]) -> str:
    return f"large {ctx.inputs}"


@ch.step
async def small(ctx: StepContext[Notes, Reaction[Flaky], int]) -> str:
    return f"small {ctx.inputs}"


fine = (
    ch.decision(node_id="fine")
    .branch(ch.match(int, matches=lambda n: n > 5).to(large))
    .branch(ch.match(int).to(small))
)
ch.add(
    ch.edge_from(ch.start_node).to(level),
    ch.edge_from(level).to(
        ch.decision(node_id="coarse")
        .branch(ch.match(int, matches=lambda n: n > 100).to(huge))
        .branch(ch.match(int).to(fine))
    ),
    ch.edge_from(huge, large, small).to(ch.end_node),
)
grading = ch.build()


async def test_a_decision_after_a_decision_takes_its_type(clock: FakeClock) -> None:
    it = await setup(GraphAction(grading, inputs=nine), Flaky(), clock)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert done.output == "large 9"
    # coarse takes level's int, and fine takes coarse's.
    assert await progress(it.workspace) == [
        "__start__",
        "level",
        "coarse",
        "fine",
        "large",
        "__end__",
    ]
    # An explicit type is what the decisions after it infer from, not what it overrides.
    wider = GraphAction(grading, inputs=nine, input_types={"coarse": int | str})
    again = await setup(wider, Flaky(), clock)
    await again.reactor.settle()
    assert await progress(again.workspace) == [
        "__start__",
        "level",
        "coarse",  # fine's type is int | str, from coarse's
        "fine",
        "large",
        "__end__",
    ]
    listed = GraphAction(grading, inputs=nine, input_types={"coarse": list[str]})
    third = await setup(listed, Flaky(), clock)
    await third.reactor.settle()
    # 9 is not a list of str, so neither boundary before a decision is saved.
    assert await progress(third.workspace) == ["__start__", "fine", "large", "__end__"]


gt = GraphBuilder(
    name="gate", state_type=Notes, deps_type=Reaction[Flaky], input_type=int, output_type=str
)


@gt.step
async def admit(ctx: StepContext[Notes, Reaction[Flaky], int]) -> str:
    return f"admitted {ctx.inputs}"


gt.add(
    gt.edge_from(gt.start_node).to(gt.decision(node_id="gate").branch(gt.match(int).to(admit))),
    gt.edge_from(admit).to(gt.end_node),
)
gate = gt.build()


async def test_a_decision_after_the_start_takes_the_graphs_input_type(clock: FakeClock) -> None:
    it = await setup(GraphAction(gate, inputs=nine), Flaky(), clock)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert done.output == "admitted 9"
    assert await progress(it.workspace) == ["__start__", "gate", "admit", "__end__"]


# ─── what inference cannot decide ────────────────────────────────────────────


def doubled(ctx: StepContext[Notes, Reaction[Flaky], int]) -> int:
    return ctx.inputs * 2


def as_text(ctx: StepContext[Notes, Reaction[Flaky], int]) -> str:
    return f"#{ctx.inputs}"


def transformed() -> Graph[Notes, Reaction[Flaky], int, str]:
    """count returns an int, and the edge transforms it: reflexr cannot read what into."""
    g = GraphBuilder(
        name="transformed",
        state_type=Notes,
        deps_type=Reaction[Flaky],
        input_type=int,
        output_type=str,
    )

    @g.step
    async def count(ctx: StepContext[Notes, Reaction[Flaky], int]) -> int:
        return ctx.inputs

    decide = g.decision(node_id="decide").branch(g.match(int).transform(as_text).to(g.end_node))
    g.add(g.edge_from(g.start_node).to(count), g.edge_from(count).transform(doubled).to(decide))
    return g.build()


def two_types() -> Graph[Notes, Reaction[Flaky], int, str]:
    """count passes an int into decide, and describe passes a str into it."""
    g = GraphBuilder(
        name="two_types",
        state_type=Notes,
        deps_type=Reaction[Flaky],
        input_type=int,
        output_type=str,
    )

    @g.step
    async def count(ctx: StepContext[Notes, Reaction[Flaky], int]) -> int:
        return ctx.inputs

    @g.step
    async def describe(ctx: StepContext[Notes, Reaction[Flaky], int]) -> str:
        return f"#{ctx.inputs}"

    decide = (
        g.decision(node_id="decide")
        .branch(g.match(int).to(describe))
        .branch(g.match(str).to(g.end_node))
    )
    g.add(
        g.edge_from(g.start_node).to(count),
        g.edge_from(count).to(decide),
        g.edge_from(describe).to(decide),
    )
    return g.build()


def returns_any() -> Graph[Notes, Reaction[Flaky], int, str]:
    """count's return annotation says nothing about what it returns."""
    g = GraphBuilder(
        name="returns_any",
        state_type=Notes,
        deps_type=Reaction[Flaky],
        input_type=int,
        output_type=str,
    )

    @g.step
    async def count(ctx: StepContext[Notes, Reaction[Flaky], int]) -> Any:
        return f"#{ctx.inputs}"

    decide = g.decision(node_id="decide").branch(g.match(str).to(g.end_node))
    g.add(g.edge_from(g.start_node).to(count), g.edge_from(count).to(decide))
    return g.build()


def unannotated() -> Graph[Notes, Reaction[Flaky], int, str]:
    """count has no return annotation."""
    g = GraphBuilder(
        name="unannotated",
        state_type=Notes,
        deps_type=Reaction[Flaky],
        input_type=int,
        output_type=str,
    )

    @g.step
    async def count(ctx: StepContext[Notes, Reaction[Flaky], int]):
        return f"#{ctx.inputs}"

    decide = g.decision(node_id="decide").branch(g.match(str).to(g.end_node))
    g.add(g.edge_from(g.start_node).to(count), g.edge_from(count).to(decide))
    return g.build()


class Opaque:
    """A class pydantic cannot write down."""

    def __init__(self, n: int) -> None:
        self.n = n


def unwritable() -> Graph[Notes, Reaction[Flaky], int, str]:
    """count returns a class pydantic has no schema for: the graph still registers."""
    g = GraphBuilder(
        name="unwritable",
        state_type=Notes,
        deps_type=Reaction[Flaky],
        input_type=int,
        output_type=str,
    )

    @g.step
    async def count(ctx: StepContext[Notes, Reaction[Flaky], int]) -> Opaque:
        return Opaque(ctx.inputs)

    @g.step
    async def show(ctx) -> str:
        return f"#{ctx.inputs.n}"

    decide = g.decision(node_id="decide").branch(g.match(Opaque).to(show))
    g.add(
        g.edge_from(g.start_node).to(count),
        g.edge_from(count).to(decide),
        g.edge_from(show).to(g.end_node),
    )
    return g.build()


def after_a_join() -> Graph[Notes, Reaction[Flaky], int, str]:
    """count is a join, whose reduced value has no annotation to read."""
    g = GraphBuilder(
        name="after_a_join",
        state_type=Notes,
        deps_type=Reaction[Flaky],
        input_type=int,
        output_type=str,
    )

    @g.step
    async def spread(ctx: StepContext[Notes, Reaction[Flaky], int]) -> list[int]:
        return [ctx.inputs, ctx.inputs]

    @g.step
    async def double(ctx: StepContext[Notes, Reaction[Flaky], int]) -> int:
        return ctx.inputs * 2

    @g.step
    async def total(ctx: StepContext[Notes, Reaction[Flaky], list[int]]) -> str:
        return f"#{sum(ctx.inputs)}"

    count = g.join(reduce_list_append, initial_factory=list[int], node_id="count")
    decide = g.decision(node_id="decide").branch(g.match(list).to(total))
    g.add(
        g.edge_from(g.start_node).to(spread),
        g.edge_from(spread).map(fork_id="each").to(double),
        g.edge_from(double).to(count),
        g.edge_from(count).to(decide),
        g.edge_from(total).to(g.end_node),
    )
    return g.build()


SAVED_AROUND_DECIDE = ["__start__", "decide", "__end__"]
"""The boundaries saved when the one before decide is not: before count, and after decide."""


@pytest.mark.parametrize(
    ("graph", "output", "saved"),
    [
        pytest.param(transformed(), "#18", SAVED_AROUND_DECIDE, id="a transform on the edge"),
        pytest.param(
            two_types(),
            "#9",
            ["__start__", "decide", "decide", "__end__"],  # not after count, nor after describe
            id="edges with different types",
        ),
        pytest.param(returns_any(), "#9", SAVED_AROUND_DECIDE, id="a step returning Any"),
        pytest.param(unannotated(), "#9", SAVED_AROUND_DECIDE, id="a step with no annotation"),
        pytest.param(
            unwritable(),
            "#9",
            ["__start__", "show", "__end__"],  # nor after decide: show is untyped
            id="a type with no schema",
        ),
        pytest.param(
            after_a_join(),
            "#36",
            ["__start__", "spread", "decide", "total", "__end__"],  # not after the join, count
            id="a join",
        ),
    ],
)
async def test_a_decision_whose_type_cannot_be_inferred_is_not_saved_before(
    graph: Graph[Notes, Reaction[Flaky], int, str],
    output: str,
    saved: list[str],
    clock: FakeClock,
) -> None:
    it = await setup(GraphAction(graph, inputs=nine), Flaky(), clock)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert done.output == output
    assert await progress(it.workspace) == saved


# ─── inputs that must not be written down ────────────────────────────────────


class Severe(Finding):
    """A finding a decision routes apart from the others."""


sv = GraphBuilder(
    name="severity", state_type=Notes, deps_type=Reaction[Flaky], input_type=int, output_type=str
)


@sv.step
async def assess(ctx: StepContext[Notes, Reaction[Flaky], int]) -> Finding:
    if ctx.inputs > 5:
        return Severe(service="auth", cause="outage")
    return Finding(service="auth", cause="blip")


@sv.step
async def page(ctx: StepContext[Notes, Reaction[Flaky], Severe]) -> str:
    return f"paged about {ctx.inputs.cause}"


@sv.step
async def note(ctx: StepContext[Notes, Reaction[Flaky], Finding]) -> str:
    return f"noted {ctx.inputs.cause}"


sv.add(
    sv.edge_from(sv.start_node).to(assess),
    sv.edge_from(assess).to(
        sv.decision(node_id="triage")
        .branch(sv.match(Severe).to(page))
        .branch(sv.match(Finding).to(note))
    ),
    sv.edge_from(page, note).to(sv.end_node),
)
severity = sv.build()


@pytest.mark.parametrize(
    ("given", "output", "saved"),
    [
        pytest.param(9, "paged about outage", False, id="a subclass"),
        pytest.param(1, "noted blip", True, id="the declared class"),
    ],
)
async def test_a_decision_is_saved_before_only_if_its_inputs_read_back_as_their_class(
    given: int, output: str, saved: bool, clock: FakeClock
) -> None:
    # assess says it returns a Finding. A Severe one would be saved as a Finding, and a
    # resumed triage would take the wrong branch, so the boundary before it is not saved.
    it = await setup(GraphAction(severity, inputs=lambda reaction: given), Flaky(), clock)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert done.output == output
    assert ("assess" in await progress(it.workspace)) is saved


lz = GraphBuilder(
    name="lazy",
    state_type=Notes,
    deps_type=Reaction[Flaky],
    input_type=list[str],
    output_type=list[str],
)


@lz.step
async def names(ctx: StepContext[Notes, Reaction[Flaky], list[str]]) -> Iterable[str]:
    return (name for name in ctx.inputs)


@lz.step
async def capitalize(ctx: StepContext[Notes, Reaction[Flaky], str]) -> str:
    return ctx.inputs.upper()


collect = lz.join(reduce_list_append, initial_factory=list[str], node_id="collect")
lz.add(
    lz.edge_from(lz.start_node).to(names),
    lz.edge_from(names).map(fork_id="each").to(capitalize),
    lz.edge_from(capitalize).to(collect),
    lz.edge_from(collect).to(lz.end_node),
)
lazy = lz.build()


async def test_a_one_shot_iterator_is_not_written_down(clock: FakeClock) -> None:
    # Saving the generator names returns would use it up, and the fork would map over nothing.
    it = await setup(GraphAction(lazy, inputs=hosts), Flaky(), clock)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert isinstance(done.output, list)
    assert sorted(map(str, done.output)) == ["A", "B", "C"]  # in the order the branches ended
    assert "names" not in await progress(it.workspace)


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
    # Before the fork is safe, and its input type is spread's return type; inside it is not.
    assert await progress(it.workspace) == ["__start__", "spread", "__end__"]
