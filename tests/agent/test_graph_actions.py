"""Graph actions: pydantic-graph graphs, checkpointed at safe step boundaries and resumed."""

from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Annotated, Any

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import BaseModel, Field, JsonValue
from pydantic_graph import BaseNode, End, Graph, GraphBuilder, GraphRunContext, StepContext
from pydantic_graph.join import reduce_list_append

from reflexr import F, RetryPolicy, Rule, SourceActor, by, on, run
from reflexr.agent import GraphAction
from reflexr.agent.graphs import CHECKPOINT_VERSION, DISCARDED
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
from tests.clock import FakeClock
from tests.event_types import Deploy

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
    ("checkpoint", "reason"),
    [
        ("not a checkpoint", "format"),
        ({"v": 0}, "format"),
        ({"v": CHECKPOINT_VERSION, "nodes": ["other"]}, "graph_changed"),
        # The same nodes, but the next one's input type is not known (as when an edge into a
        # fork has gained a transform since, or the next node is a decision).
        (
            {
                "v": CHECKPOINT_VERSION,
                "nodes": sorted(runbook.nodes),
                "frontier": [{"node_id": "__start__"}],
            },
            "untyped",
        ),
    ],
)
async def test_checkpoints_this_graph_cannot_resume_start_over(
    checkpoint: JsonValue, reason: str, clock: FakeClock
) -> None:
    spans = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(spans))
    deps = Flaky()
    it = await setup(GraphAction(runbook, inputs=service), deps, clock, provider)
    await it.reactor.evaluate()
    [pending] = await it.workspace.runs()
    async with it.storage.transaction(ACME) as transaction:
        await transaction.save_runs([pending.model_copy(update={"checkpoint": checkpoint})])
    await it.reactor.settle()
    assert deps.ran == ["diagnose", "mitigate", "report"]
    # The attempt's span says why; the first attempt of a run has no checkpoint to discard.
    [attempt] = [s for s in spans.get_finished_spans() if s.name == "invoke_workflow deploys"]
    assert [dict(e.attributes or {}) for e in attempt.events if e.name == DISCARDED] == [
        {"reflexr.checkpoint.reason": reason}
    ]
    provider.shutdown()


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


async def test_the_boundary_after_a_decision_is_saved_not_the_one_before(
    clock: FakeClock,
) -> None:
    deps = Flaky(fail_once={"loud"})
    typed = GraphAction(routing, name="route", inputs=nine, input_types={"loud": int})
    it = await setup(typed, deps, clock)
    await it.reactor.settle()
    [waiting] = await it.workspace.runs()
    # route runs no code, so the boundary after it saves what the one before would have.
    assert waiting.step == "route"
    assert frontier(waiting.checkpoint) == [("loud", 9)]
    clock.advance(1)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert done.output == "loud 9"
    assert deps.ran == ["measure", "loud", "loud"]  # measure ran once
    assert await progress(it.workspace) == ["__start__", "route", "loud", "__end__"]


async def test_inputs_that_do_not_read_back_as_the_given_type_are_not_saved(
    clock: FakeClock,
) -> None:
    # An explicit type wins over measure's annotation, and says fewer than 5; 9 is not, so the
    # boundary before measure is not saved. Nor is the one before loud, since 9 is not a list.
    small = Annotated[int, Field(lt=5)]
    action = GraphAction(
        routing, name="route", inputs=nine, input_types={"measure": small, "loud": list[str]}
    )
    it = await setup(action, Flaky(), clock)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert done.output == "loud 9"
    assert await progress(it.workspace) == ["loud", "__end__"]


@pytest.mark.parametrize(
    "node", ["route", "__end__", "mesure"], ids=["a decision", "the end", "a typo"]
)
def test_input_types_name_only_steps_and_forks(node: str) -> None:
    with pytest.raises(ValueError, match=f"not steps or forks: {node}"):
        GraphAction(routing, name="route", input_types={node: int})


# ─── what inference cannot type before a fork ────────────────────────────────


class Opaque:
    """A list pydantic cannot write down."""

    def __init__(self, items: list[str]) -> None:
        self.items = items

    def __iter__(self) -> Iterator[str]:
        return iter(self.items)


async def typed(ctx: StepContext[Notes, Reaction[Flaky], list[str]]) -> list[str]:
    return ctx.inputs


async def returns_any(ctx: StepContext[Notes, Reaction[Flaky], list[str]]) -> Any:
    return ctx.inputs


async def unannotated(ctx: StepContext[Notes, Reaction[Flaky], list[str]]):
    return ctx.inputs


async def unwritable(ctx: StepContext[Notes, Reaction[Flaky], list[str]]) -> Opaque:
    return Opaque(ctx.inputs)


def unchanged(ctx: StepContext[Notes, Reaction[Flaky], list[str]]) -> list[str]:
    return ctx.inputs


async def shout_each(ctx: StepContext[Notes, Reaction[Flaky], str]) -> str:
    return ctx.inputs.upper()


async def unchanged_each(ctx: StepContext[Notes, Reaction[Flaky], str]) -> str:
    return ctx.inputs


def fanned(
    pick: Callable[..., Awaitable[Any]] | None = None, *, transformed: bool = False
) -> Graph[Notes, Reaction[Flaky], list[str], str]:
    """A fork, fed by ``pick`` (through a transform, if asked), or by the start without it."""
    g = GraphBuilder(
        name="fanned",
        state_type=Notes,
        deps_type=Reaction[Flaky],
        input_type=list[str],
        output_type=str,
    )
    shout = g.step(shout_each)
    if pick is None:
        g.add(g.edge_from(g.start_node).map(fork_id="fan").to(shout))
    else:
        first = g.step(pick)
        edge = g.edge_from(first).transform(unchanged) if transformed else g.edge_from(first)
        g.add(g.edge_from(g.start_node).to(first), edge.map(fork_id="fan").to(shout))
    g.add(g.edge_from(shout).to(g.end_node))
    return g.build()


def after_a_decision() -> Graph[Notes, Reaction[Flaky], list[str], str]:
    g = GraphBuilder(
        name="after_a_decision",
        state_type=Notes,
        deps_type=Reaction[Flaky],
        input_type=list[str],
        output_type=str,
    )
    shout = g.step(shout_each)
    route = g.decision(node_id="route").branch(g.match(list).map(fork_id="fan").to(shout))
    g.add(g.edge_from(g.start_node).to(route), g.edge_from(shout).to(g.end_node))
    return g.build()


def after_a_join() -> Graph[Notes, Reaction[Flaky], list[str], str]:
    g = GraphBuilder(
        name="after_a_join",
        state_type=Notes,
        deps_type=Reaction[Flaky],
        input_type=list[str],
        output_type=str,
    )
    first = g.step(typed)
    each = g.step(unchanged_each)
    shout = g.step(shout_each)
    gathered = g.join(reduce_list_append, initial_factory=list[str], node_id="gathered")
    g.add(
        g.edge_from(g.start_node).to(first),
        g.edge_from(first).map(fork_id="each").to(each),
        g.edge_from(each).to(gathered),
        g.edge_from(gathered).map(fork_id="fan").to(shout),
        g.edge_from(shout).to(g.end_node),
    )
    return g.build()


@pytest.mark.parametrize(
    ("graph", "saved"),
    [
        pytest.param(fanned(), ["__start__", "__end__"], id="the start: the graph's input type"),
        pytest.param(fanned(typed), ["__start__", "typed", "__end__"], id="a typed step"),
        pytest.param(fanned(typed, transformed=True), ["__start__", "__end__"], id="a transform"),
        pytest.param(fanned(returns_any), ["__start__", "__end__"], id="a step returning Any"),
        pytest.param(fanned(unannotated), ["__start__", "__end__"], id="no annotation"),
        pytest.param(fanned(unwritable), ["__start__", "__end__"], id="a type with no schema"),
        pytest.param(after_a_decision(), ["__end__"], id="a decision"),
        pytest.param(after_a_join(), ["__start__", "typed", "__end__"], id="a join"),
    ],
)
async def test_a_fork_is_saved_before_only_if_the_edges_into_it_say_its_type(
    graph: Graph[Notes, Reaction[Flaky], list[str], str], saved: list[str], clock: FakeClock
) -> None:
    it = await setup(GraphAction(graph, inputs=lambda reaction: ["a"]), Flaky(), clock)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert done.output == "A"
    assert await progress(it.workspace) == saved


# ─── inputs that must not be written down ────────────────────────────────────


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


# ─── what would not read back as it was ──────────────────────────────────────

st = GraphBuilder(
    name="explained",
    state_type=Notes,
    deps_type=Reaction[Flaky],
    input_type=str,
    output_type=list[str],
)


@st.step
async def find(ctx: StepContext[Notes, Reaction[Flaky], str]) -> Finding:
    ctx.deps.deps.step("find")
    return Finding(service=ctx.inputs, cause="bad deploy")


@st.stream
async def explain(ctx: StepContext[Notes, Reaction[Flaky], Finding]) -> AsyncIterator[str]:
    ctx.deps.deps.step("explain")
    yield ctx.inputs.service
    yield ctx.inputs.cause


@st.step
async def echo(ctx: StepContext[Notes, Reaction[Flaky], str]) -> str:
    return ctx.inputs


lines = st.join(reduce_list_append, initial_factory=list[str], node_id="lines")
st.add(
    st.edge_from(st.start_node).to(find),
    st.edge_from(find).to(explain),
    st.edge_from(explain).map(fork_id="each").to(echo),
    st.edge_from(echo).to(lines),
    st.edge_from(lines).to(st.end_node),
)
explained = st.build()


async def test_a_model_given_to_a_stream_step_is_not_saved(clock: FakeClock) -> None:
    # A stream step's input type reads as a type variable, which pydantic takes for Any: find's
    # Finding would be saved as a dict, and every retry of explain would fail on it.
    deps = Flaky(fail_once={"explain"})
    it = await setup(GraphAction(explained, inputs=service), deps, clock)
    await it.reactor.settle()
    clock.advance(1)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert (done.status, done.attempts) == ("succeeded", 2)
    assert isinstance(done.output, list)
    assert sorted(map(str, done.output)) == ["auth", "bad deploy"]
    assert "find" not in await progress(it.workspace)
    assert deps.ran == ["find", "explain", "find", "explain"]


async def test_a_stream_steps_input_type_can_be_given(clock: FakeClock) -> None:
    deps = Flaky(fail_once={"explain"})
    action = GraphAction(explained, inputs=service, input_types={"explain": Finding})
    it = await setup(action, deps, clock)
    await it.reactor.settle()
    clock.advance(1)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert (done.status, done.attempts) == ("succeeded", 2)
    assert "find" in await progress(it.workspace)
    assert deps.ran == ["find", "explain", "explain"]  # resumed after find


@dataclass
class Escalate(BaseNode[Notes, Reaction[Flaky], str]):
    service: str

    async def run(self, ctx: GraphRunContext[Notes, Reaction[Flaky]]) -> End[str]:
        ctx.deps.deps.step("escalate")
        return End(f"escalated {self.service}")


bn = GraphBuilder(
    name="escalation", state_type=Notes, deps_type=Reaction[Flaky], input_type=str, output_type=str
)


@bn.step
async def judge(ctx: StepContext[Notes, Reaction[Flaky], str]) -> Escalate:
    ctx.deps.deps.step("judge")
    return Escalate(service=ctx.inputs)


bn.add(bn.edge_from(bn.start_node).to(judge), bn.node(Escalate))
escalation = bn.build()


async def test_a_base_node_given_as_input_is_not_saved(clock: FakeClock) -> None:
    # A BaseNode's input type says Any, so it would be saved as a dict, which is not the node.
    deps = Flaky(fail_once={"escalate"})
    it = await setup(GraphAction(escalation, inputs=service), deps, clock)
    await it.reactor.settle()
    clock.advance(1)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert (done.status, done.output) == ("succeeded", "escalated auth")
    assert "judge" not in await progress(it.workspace)
    assert deps.ran == ["judge", "escalate", "judge", "escalate"]


@dataclass
class Loose:
    finding: Any = None


ls = GraphBuilder(
    name="loose", state_type=Loose, deps_type=Reaction[Flaky], input_type=str, output_type=str
)


@ls.step
async def remember(ctx: StepContext[Loose, Reaction[Flaky], str]) -> str:
    ctx.deps.deps.step("remember")
    ctx.state.finding = Finding(service=ctx.inputs, cause="bad deploy")
    return ctx.inputs


@ls.step
async def recall(ctx: StepContext[Loose, Reaction[Flaky], str]) -> str:
    ctx.deps.deps.step("recall")
    return f"{ctx.inputs}: {ctx.state.finding.cause}"


ls.add(
    ls.edge_from(ls.start_node).to(remember),
    ls.edge_from(remember).to(recall),
    ls.edge_from(recall).to(ls.end_node),
)
loose = ls.build()


async def test_a_state_that_would_not_read_back_is_not_saved(clock: FakeClock) -> None:
    deps = Flaky(fail_once={"recall"})
    it = await setup(GraphAction(loose, inputs=service), deps, clock)
    await it.reactor.settle()
    clock.advance(1)
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert (done.status, done.output) == ("succeeded", "auth: bad deploy")
    # A Finding in a field that says Any would be saved as a dict, which recall cannot use, so
    # the retry resumes from before remember.
    assert await progress(it.workspace) == ["__start__", "__end__"]
    assert deps.ran == ["remember", "recall", "remember", "recall"]


def discarded(spans: InMemorySpanExporter) -> list[dict[str, Any]]:
    """The attributes of every checkpoint-discarded event, in order."""
    return [
        dict(event.attributes or {})
        for span in spans.get_finished_spans()
        for event in span.events
        if event.name == DISCARDED
    ]


def saved_before_mitigate(**changes: Any) -> dict[str, Any]:
    """A checkpoint of the runbook after diagnose, as it saves one, with some values changed."""
    task = {"node_id": "mitigate", "inputs": {"service": "auth", "cause": "x"}, "fork_stack": []}
    return {
        "v": CHECKPOINT_VERSION,
        "nodes": sorted(runbook.nodes),
        "inputs": "auth",
        "state": {"log": ["diagnosed"]},
        "frontier": [task | changes.pop("task", {})],
        **changes,
    }


@pytest.mark.parametrize(
    ("checkpoint", "error"),
    [
        pytest.param(saved_before_mitigate(), None, id="valid"),
        pytest.param(
            saved_before_mitigate(state={"log": 5}),
            "Notes: log: Input should be a valid list",
            id="state",
        ),
        pytest.param(
            saved_before_mitigate(task={"inputs": {"service": "auth"}}),
            "Finding: cause: Field required",
            id="inputs",
        ),
        pytest.param(
            saved_before_mitigate(task={"fork_stack": "not a stack"}),
            "tuple[ForkStackItem, ...]: the value: Input should be a valid tuple",
            id="fork stack",
        ),
    ],
)
async def test_a_checkpoint_that_no_longer_validates_starts_over(
    checkpoint: dict[str, Any], error: str | None, clock: FakeClock
) -> None:
    spans = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(spans))
    deps = Flaky()
    it = await setup(GraphAction(runbook, inputs=service), deps, clock, provider)
    await it.reactor.evaluate()
    [pending] = await it.workspace.runs()
    async with it.storage.transaction(ACME) as transaction:
        await transaction.save_runs([pending.model_copy(update={"checkpoint": checkpoint})])
    await it.reactor.settle()
    [done] = await it.workspace.runs()
    assert done.status == "succeeded"
    if error is None:  # a checkpoint that validates resumes: diagnose does not run again
        assert (deps.ran, discarded(spans)) == (["mitigate", "report"], [])
    else:
        assert deps.ran == ["diagnose", "mitigate", "report"]
        assert discarded(spans) == [
            {"reflexr.checkpoint.reason": "invalid", "reflexr.checkpoint.error": error}
        ]
    provider.shutdown()
