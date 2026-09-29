"""The reactor's evaluation: rules over workspaces' logs, exactly once, with their facts."""

import asyncio
from datetime import timedelta

import pytest
from opentelemetry.trace import SpanKind

from reflexr import F, Rule, SourceActor, UserActor, by, on, run
from reflexr.core import (
    Causation,
    Event,
    InvalidRule,
    NotFound,
    PredicateFilter,
    RuleErrored,
    RuleFired,
    RuleReset,
    Tick,
    ValidationFailed,
)
from reflexr.telemetry import attributes as a
from reflexr.workspace import (
    EVALUATION_LEASE,
    REACTOR,
    Entry,
    InMemoryStorage,
    Reaction,
    Reactor,
    Settled,
    Storage,
    Workspace,
    WorkspaceRef,
    Workspaces,
)
from tests.event_types import Deploy, Heartbeat, ServiceError
from tests.workspace.conftest import Build, FakeClock, Telemetry

ACME = WorkspaceRef("acme", "prod")
MINUTE = timedelta(minutes=1)

spike = Rule(
    name="spike",
    when=on(ServiceError).count(at_least=3, within=MINUTE),
    scope=by(F.service),
    then=run("triage"),
)
deploys = Rule(name="deploys", when=on(Deploy), then=run("note"))


async def noop(reaction: Reaction[None]) -> None:
    return None


ACTIONS = {"triage": noop, "note": noop, "page": noop}


async def open_(workspaces: Workspaces) -> Workspace:
    return await workspaces.open("acme", "prod", actor=SourceActor(name="monitor"))


async def errors(workspace: Workspace, service: str = "auth", n: int = 3) -> None:
    for _ in range(n):
        await workspace.publish(ServiceError(service=service))


async def test_rules_fire_once_and_create_runs(build: Build) -> None:
    workspaces = build([spike])
    reactor = Reactor(workspaces, actions=ACTIONS)
    workspace = await open_(workspaces)
    assert await reactor.evaluate() == 0  # the rule starts at the head of the log
    await errors(workspace)
    assert await reactor.evaluate() == 1
    assert await reactor.evaluate() == 0
    [pending] = await workspace.runs()
    third = (await workspace.read())[2]
    assert (pending.rule, pending.scope, pending.status) == (
        "spike",
        {"service": "auth"},
        "pending",
    )
    assert (pending.matched, pending.correlation_id, pending.depth) == ((1, 2, 3), third.id, 0)
    fired = (await workspace.read(after_seq=3))[0]
    assert fired.event == RuleFired(
        rule="spike",
        scope={"service": "auth"},
        scope_key=pending.scope_key,
        firing_id=pending.id,
        matched=(1, 2, 3),
    )
    assert (fired.actor, fired.correlation_id) == (REACTOR, third.id)
    assert fired.causation == Causation(firing_id=pending.id, run_id=pending.id, depth=1)
    assert (await workspace.rule_progress())["spike"].cursor == 4


async def test_a_new_workspace_meets_every_rule_at_its_first_event(build: Build) -> None:
    workspaces = build([deploys])
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))  # before any evaluation
    assert await Reactor(workspaces, actions=ACTIONS).evaluate() == 1


async def test_a_rule_added_later_starts_at_the_head_or_the_beginning(build: Build) -> None:
    workspace = await open_(build([deploys]))
    await workspace.publish(Deploy(service="auth"))
    backlog = Rule(name="backlog", when=on(Deploy), then=run("note"), start="beginning")
    late = Rule(name="late", when=on(Deploy), then=run("note"))
    assert await Reactor(build([deploys, backlog, late]), actions=ACTIONS).evaluate() == 2
    assert sorted(r.rule for r in await workspace.runs()) == ["backlog", "deploys"]


async def test_a_disabled_rule_holds_its_cursor_and_resumes_from_it(
    build: Build, telemetry: Telemetry
) -> None:
    off = spike.model_copy(update={"enabled": False})
    workspaces = build([off, deploys])
    workspace = await open_(workspaces)
    await errors(workspace)  # a new workspace meets the disabled rule too
    await workspace.publish(Deploy(service="auth"))
    assert await Reactor(workspaces, actions=ACTIONS).settle() == Settled(firings=1, attempts=1)
    progress = await workspace.rule_progress()
    assert (progress["spike"].cursor, progress["deploys"].cursor) == (0, 7)
    fired = [e.event for e in await workspace.read() if isinstance(e.event, RuleFired)]
    assert [f.rule for f in fired] == ["deploys"]
    lags = {str(attributes[a.RULE]) for attributes, _ in telemetry.points("reflexr.evaluation.lag")}
    assert lags == {"deploys"}  # behind by choice is not a lag
    # Enabled again, it resumes from its cursor: the errors it missed still count.
    assert await Reactor(build([spike, deploys]), actions=ACTIONS).evaluate() == 1
    assert (await workspace.rule_progress())["spike"].generation == 0  # nothing was reset
    assert not [e for e in await workspace.read() if isinstance(e.event, RuleReset)]


async def test_a_disabled_rule_added_later_starts_when_it_is_enabled(build: Build) -> None:
    workspace = await open_(build([deploys]))
    await workspace.publish(Deploy(service="auth"))
    late = Rule(name="late", when=on(Deploy), then=run("note"), start="beginning")
    off = late.model_copy(update={"enabled": False})
    assert await Reactor(build([deploys, off]), actions=ACTIONS).evaluate() == 1
    assert "late" not in await workspace.rule_progress()
    # Enabled, it starts where its start says: here, at the beginning.
    assert await Reactor(build([deploys, late]), actions=ACTIONS).evaluate() == 1


async def test_a_disabled_rules_backlog_is_skipped_by_replaying_it_from_the_head(
    build: Build,
) -> None:
    off = spike.model_copy(update={"enabled": False})
    workspace = await open_(build([off]))
    await errors(workspace)
    operator = workspace.as_actor(UserActor(id="ada"))
    await operator.replay_rule("spike", from_seq=await workspace.head_seq())
    assert await Reactor(build([spike]), actions=ACTIONS).evaluate() == 0
    await errors(workspace)
    assert await Reactor(build([spike]), actions=ACTIONS).evaluate() == 1


async def test_rules_react_to_each_others_facts_in_one_pass(build: Build) -> None:
    noticed = Rule(name="noticed", when=on(RuleFired).where(rule="deploys"), then=run("note"))
    workspaces = build([deploys, noticed])
    workspace = await open_(workspaces)
    await Reactor(workspaces, actions=ACTIONS).evaluate()
    await workspace.publish(Deploy(service="auth"))
    assert await Reactor(workspaces, actions=ACTIONS).evaluate() == 2
    runs = {r.rule: r for r in await workspace.runs()}
    assert (runs["deploys"].depth, runs["noticed"].depth) == (0, 1)


async def test_a_changed_rule_starts_afresh_at_the_head(build: Build, storage: Storage) -> None:
    workspaces = build([spike])
    workspace = await open_(workspaces)
    await Reactor(workspaces, actions=ACTIONS).evaluate()
    await errors(workspace, n=2)
    await Reactor(workspaces, actions=ACTIONS).evaluate()
    changed = spike.model_copy(update={"when": on(ServiceError).count(at_least=2, within=MINUTE)})
    reactor = Reactor(build([changed]), actions=ACTIONS)
    await errors(workspace, n=1)
    assert await reactor.evaluate() == 0  # the two earlier errors no longer count
    [reset] = [e for e in await workspace.read() if isinstance(e.event, RuleReset)]
    assert reset.event == RuleReset(rule="spike", generation=1, reason="changed", from_seq=3)
    assert (reset.actor, reset.correlation_id) == (REACTOR, reset.id)
    await errors(workspace, n=2)
    assert await reactor.evaluate() == 1
    assert (await workspace.rule_progress())["spike"].generation == 1


async def test_replaying_rebuilds_or_refires(build: Build) -> None:
    workspaces = build([spike])
    reactor = Reactor(workspaces, actions=ACTIONS)
    workspace = await open_(workspaces)
    await reactor.evaluate()
    await errors(workspace)
    await reactor.evaluate()
    operator = workspace.as_actor(UserActor(id="ada"))
    rebuilt = await operator.replay_rule("spike")
    assert (rebuilt.cursor, rebuilt.silent_through, rebuilt.generation) == (0, 4, 1)
    assert await reactor.evaluate() == 0  # the state is rebuilt without firing again
    await errors(workspace, n=3)
    assert await reactor.evaluate() == 1
    refired = await operator.replay_rule("spike", from_seq=0, mode="refire")
    assert refired.silent_through == 0
    assert await reactor.evaluate() == 2  # both counts fire again, as new runs
    runs = await workspace.runs()
    assert len(runs) == 4
    assert len({r.id for r in runs}) == 4
    resets = [(e.event, e.actor) for e in await workspace.read() if isinstance(e.event, RuleReset)]
    assert [(event.reason, actor) for event, actor in resets] == [
        ("replayed", UserActor(id="ada")),
        ("replayed", UserActor(id="ada")),
    ]


async def test_replaying_checks_the_rule_and_position(build: Build) -> None:
    workspace = await open_(build([spike]))
    with pytest.raises(NotFound, match="rule nope"):
        await workspace.replay_rule("nope")
    with pytest.raises(ValidationFailed, match="between 0 and the head of the log, 0"):
        await workspace.replay_rule("spike", from_seq=1)
    progress = await workspace.replay_rule("spike")  # never evaluated: it starts from scratch
    assert (progress.cursor, progress.generation) == (0, 1)


async def test_firings_beyond_the_depth_limit_are_refused(build: Build) -> None:
    workspaces = build([deploys])
    workspace = await open_(workspaces)
    await Reactor(workspaces, actions=ACTIONS).evaluate()
    deep = workspace.caused_by(Causation(firing_id="f", run_id="f", depth=3), correlation_id="c")
    await deep.publish(Deploy(service="auth"), correlation_id=None)
    assert await Reactor(workspaces, actions=ACTIONS).evaluate() == 0
    [letter] = await workspace.dead_letters(rule="deploys")
    assert "causation depth 4, beyond the limit of 3" in letter.error
    errored = (await workspace.read())[-1]
    assert isinstance(errored.event, RuleErrored)
    assert errored.depth == 3


async def test_evaluation_errors_are_dead_lettered_for_their_rule(build: Build) -> None:
    def boom(event: Event) -> bool:
        raise RuntimeError("no")

    fragile = Rule(
        name="fragile", when=on(Deploy).where(PredicateFilter(name="boom")), then=run("note")
    )
    workspaces = build([fragile, deploys], predicates={"boom": boom})
    workspace = await open_(workspaces)
    await Reactor(workspaces, actions=ACTIONS).evaluate()
    await workspace.publish(Deploy(service="auth"))
    assert (
        await Reactor(workspaces, actions=ACTIONS).evaluate() == 1
    )  # the other rule is unaffected
    [letter] = await workspace.dead_letters()
    assert (letter.rule, letter.seq) == ("fragile", 1)


async def test_absence_fires_when_the_log_moves_past_its_deadline(
    build: Build, storage: Storage, clock: FakeClock
) -> None:
    quiet = Rule(
        name="quiet",
        when=on(Heartbeat).absent(within=timedelta(minutes=5)),
        scope=by(F.service),
        then=run("page"),
    )
    workspaces = build([quiet])
    workspace = await open_(workspaces)
    await Reactor(workspaces, actions=ACTIONS).evaluate()
    await workspace.publish(Heartbeat(service="auth"))
    assert await Reactor(workspaces, actions=ACTIONS).evaluate() == 0
    clock.advance(301)
    async with storage.transaction(ACME) as transaction:  # the schedules' job, from phase 2d
        tick = Tick(schedule="clock", at=clock())
        await transaction.append([Entry(id="t1", actor=REACTOR, event=tick)])
    assert await Reactor(workspaces, actions=ACTIONS).evaluate() == 1


async def test_a_workspace_another_reactor_holds_is_skipped(build: Build, storage: Storage) -> None:
    workspaces = build([deploys])
    workspace = await open_(workspaces)
    reactor = Reactor(workspaces, actions=ACTIONS, holder="mine")
    await reactor.evaluate()
    await workspace.publish(Deploy(service="auth"))
    assert await storage.acquire_lease(ACME, EVALUATION_LEASE, "theirs", MINUTE)
    assert await reactor.evaluate() == 0
    await storage.release_lease(ACME, EVALUATION_LEASE, "theirs")
    assert await reactor.evaluate(ACME) == 1
    assert reactor.holder == "mine"
    assert await storage.acquire_lease(ACME, EVALUATION_LEASE, "theirs", MINUTE)  # released


async def test_settling_waits_for_a_workspace_another_reactor_evaluates(
    build: Build, storage: Storage
) -> None:
    workspaces = build([deploys])
    workspace = await open_(workspaces)
    reactor = Reactor(workspaces, actions=ACTIONS, holder="mine")
    await reactor.evaluate()
    await workspace.publish(Deploy(service="auth"))
    assert await storage.acquire_lease(ACME, EVALUATION_LEASE, "theirs", MINUTE)

    async def let_go() -> None:  # as a reactor that finished just before the deploy would
        await asyncio.sleep(0.02)
        await storage.release_lease(ACME, EVALUATION_LEASE, "theirs")

    letting_go = asyncio.create_task(let_go())
    assert await reactor.settle() == Settled(firings=1, attempts=1)
    await letting_go


class LosesLease(InMemoryStorage):
    """Storage whose evaluation lease is lost after it is first renewed."""

    def __init__(self, clock: FakeClock) -> None:
        super().__init__(clock=clock)
        self.renewals = 0

    async def acquire_lease(
        self, workspace: WorkspaceRef, key: str, holder: str, ttl: timedelta
    ) -> bool:
        self.renewals += 1
        return self.renewals <= 2 and await super().acquire_lease(workspace, key, holder, ttl)


async def test_a_reactor_that_loses_its_lease_stops(clock: FakeClock) -> None:
    storage = LosesLease(clock)
    workspaces = Workspaces(storage, rules=[deploys])
    workspace = await open_(workspaces)
    for _ in range(3):
        await workspace.publish(Deploy(service="auth"))
    # The lease is taken, renewed once after the first batch, and lost after the second.
    assert await Reactor(workspaces, actions=ACTIONS, batch_size=1).evaluate() == 2
    assert (await workspace.rule_progress())["deploys"].cursor == 2


async def test_batches_are_bounded(build: Build) -> None:
    workspaces = build([deploys])
    workspace = await open_(workspaces)
    await Reactor(workspaces, actions=ACTIONS).evaluate()
    for _ in range(5):
        await workspace.publish(Deploy(service="auth"))
    assert await Reactor(workspaces, actions=ACTIONS, batch_size=2).evaluate() == 5
    with pytest.raises(ValueError, match="batch_size must be at least 1"):
        Reactor(workspaces, actions=ACTIONS, batch_size=0)


async def test_rules_are_checked_when_the_workspaces_are_built(storage: Storage) -> None:
    with pytest.raises(InvalidRule, match="no event type 'heartbeat'"):
        Workspaces(
            storage, events=[Deploy], rules=[Rule(name="r", when=on(Heartbeat), then=run("a"))]
        )
    with pytest.raises(InvalidRule, match="no predicate 'boom'"):
        Workspaces(
            storage,
            rules=[
                Rule(name="r", when=on(Deploy).where(PredicateFilter(name="boom")), then=run("a"))
            ],
        )
    with pytest.raises(ValueError, match="two rules are named 'deploys'"):
        Workspaces(storage, rules=[deploys, deploys])
    workspaces = Workspaces(storage, rules=[deploys], predicates={"p": lambda e: True})
    assert (list(workspaces.rules), list(workspaces.predicates)) == (["deploys"], ["p"])


async def test_evaluation_is_traced_and_measured(build: Build, telemetry: Telemetry) -> None:
    workspaces = build([spike])
    workspace = await open_(workspaces)
    reactor = Reactor(workspaces, actions=ACTIONS)
    await reactor.evaluate()
    await errors(workspace, n=4)
    telemetry.spans.clear()
    await reactor.evaluate()
    [span] = [s for s in telemetry.spans.get_finished_spans() if s.name == "reflexr.evaluate"]
    assert span.kind == SpanKind.INTERNAL
    assert dict(span.attributes or {}) == {
        a.TENANT_ID: "acme",
        a.WORKSPACE_ID: "prod",
        a.EVALUATED_RULES: ("spike",),
        a.FIRING_COUNT: 1,
    }
    fired = next(e for e in await workspace.read() if isinstance(e.event, RuleFired))
    context = span.get_span_context()
    assert context is not None
    assert fired.traceparent is not None
    assert f"{context.trace_id:032x}" in fired.traceparent
    assert telemetry.points("reflexr.firings")[0][1] == 1
    runs = [p for p in telemetry.points("reflexr.runs") if p[0][a.RUN_STATUS] == "pending"]
    assert runs[0][1] == 1
    lags = [value for attributes, value in telemetry.points("reflexr.evaluation.lag")]
    assert lags[-1] == 4  # four envelopes behind when the pass began
    assert telemetry.points("reflexr.evaluation.duration")
