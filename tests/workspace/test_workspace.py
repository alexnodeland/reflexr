"""Workspace handles: publishing, feedback, run operations and reads, with their telemetry."""

from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any, Literal, cast

import pytest
from opentelemetry.trace import SpanKind

from reflexr import (
    AgentActor,
    Event,
    Feedback,
    InvalidRule,
    RetryPolicy,
    Rule,
    SourceActor,
    UserActor,
    on,
    run,
)
from reflexr.core import (
    Causation,
    ChainTarget,
    DepthExceeded,
    Envelope,
    EvaluationError,
    EventRegistry,
    FeedbackGiven,
    FiringTarget,
    Forbidden,
    InvalidState,
    NotFound,
    RuleFired,
    RuleProgress,
    RunCancelled,
    RunRequeued,
    RunSkipped,
    RunTarget,
    Tick,
    ValidationFailed,
    fail,
    load_event,
    retry,
    start,
)
from reflexr.telemetry import attributes as a
from reflexr.workspace import (
    InMemoryStorage,
    Storage,
    Workspace,
    WorkspaceRef,
    Workspaces,
)
from tests.clock import START, FakeClock
from tests.event_types import AppEvent, Deploy, ServiceError
from tests.workspace.conftest import Build, Telemetry
from tests.workspace.helpers import fired

ACME = WorkspaceRef("acme", "prod")


OURS = EventRegistry()


class OursEvent(Event, abstract=True, event_namespace="ours", registry=OURS):
    """Another application's events, in a registry of its own."""


class Ping(OursEvent):
    pass


class Escalated(AppEvent):
    """Registered, but not among the types the test workspaces accept."""

    service: str


class Relevant(Feedback, name="relevant", targets={"firing"}):
    relevant: bool


class RunQuality(Feedback, name="run_quality", targets={"run"}):
    score: int
    note: str = ""


class Resolved(Feedback, name="resolved", targets={"chain"}):
    outcome: Literal["resolved", "escalated"]


async def seed_run(storage: Storage, run_id: str = "fir_1", *, chain: str = "evt_1") -> None:
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([fired(run_id).model_copy(update={"correlation_id": chain})])


# ─── publishing ──────────────────────────────────────────────────────────────


async def test_publishing_logs_an_attributed_envelope(workspace: Workspace) -> None:
    published = await workspace.publish(ServiceError(service="auth", severity=8))
    envelope = published.envelope
    assert not published.duplicate
    assert (envelope.seq, envelope.ts, envelope.workspace_id) == (1, START, "prod")
    assert envelope.actor == UserActor(id="ada")
    assert envelope.correlation_id == envelope.id  # a new causal chain
    assert envelope.causation is None
    assert envelope.id.startswith("evt_")
    assert await workspace.read() == [envelope]
    assert await workspace.head_seq() == 1


async def test_publishing_an_id_again_appends_nothing(workspace: Workspace) -> None:
    first = await workspace.publish(ServiceError(service="auth"), id="alert-7")
    again = await workspace.publish(ServiceError(service="other"), id="alert-7")
    assert again.duplicate
    assert again.envelope == first.envelope
    assert await workspace.head_seq() == 1


async def test_a_batch_is_published_atomically_and_in_order(workspace: Workspace) -> None:
    await workspace.publish(Deploy(service="auth"), id="d1")
    results = await workspace.publish_many(
        [ServiceError(service="auth"), Deploy(service="auth"), ServiceError(service="db")],
        ids=["e1", "d1", "e1"],
    )
    assert [(r.envelope.seq, r.duplicate) for r in results] == [(2, False), (1, True), (2, True)]
    with pytest.raises(Forbidden):
        await workspace.publish_many([ServiceError(service="auth"), Tick(schedule="c", at=START)])
    assert await workspace.head_seq() == 2
    with pytest.raises(ValueError, match="2 events but 1 ids"):
        await workspace.publish_many([Deploy(service="a"), Deploy(service="b")], ids=["x"])


async def test_events_can_join_an_existing_chain(workspace: Workspace) -> None:
    alert = (await workspace.publish(ServiceError(service="auth"))).envelope
    resolved = await workspace.publish(Deploy(service="auth"), correlation_id=alert.id)
    assert resolved.envelope.correlation_id == alert.id
    with pytest.raises(NotFound, match="chain nope"):
        await workspace.publish(Deploy(service="auth"), correlation_id="nope")


async def test_a_chain_is_joined_by_its_first_events_id(workspace: Workspace) -> None:
    alert = (await workspace.publish(ServiceError(service="auth"), id="alert-7")).envelope
    joined = await workspace.publish(Deploy(service="auth"), id="d1", correlation_id=alert.id)
    later = f"event d1 did not start a causal chain: it belongs to chain {alert.id}"
    with pytest.raises(ValidationFailed, match=later) as rejected:
        await workspace.publish(Deploy(service="auth"), correlation_id=joined.envelope.id)
    assert rejected.value.payload()["type"] == "validation_failed"
    with pytest.raises(ValidationFailed, match=later):
        await workspace.publish_many(
            [Deploy(service="auth"), ServiceError(service="auth")], correlation_id="d1"
        )
    assert await workspace.head_seq() == 2


async def test_only_accepted_types_can_be_published(workspace: Workspace) -> None:
    with pytest.raises(NotFound, match="event type app:escalated"):
        await workspace.publish(Escalated(service="auth"))
    with pytest.raises(NotFound, match=r"event type app:pager\.sent"):
        await workspace.publish(load_event({"type": "app:pager.sent"}))
    with pytest.raises(ValidationFailed, match=r"did you mean 'app:deploy\.finished'"):
        await workspace.publish(load_event({"type": "deploy.finished", "service": "auth"}))
    fact = RuleFired(rule="app:r", scope={}, scope_key="[]", firing_id="f", matched=(1,))
    with pytest.raises(Forbidden, match="reflexr:rule_fired events are recorded by reflexr"):
        await workspace.publish(fact)
    with pytest.raises(ValidationFailed, match=r"did you mean 'app:deploy\.finished'"):
        await workspace.read(types=["deploy.finished"])
    assert await workspace.head_seq() == 0


async def test_without_an_allowlist_every_registered_type_is_accepted(
    storage: Storage,
) -> None:
    workspaces = Workspaces(storage)
    workspace = await workspaces.open("acme", "prod", actor=SourceActor(name="ci"))
    assert (await workspace.publish(Deploy(service="auth"))).envelope.seq == 1
    with pytest.raises(Forbidden):
        await workspace.publish(Tick(schedule="clock", at=START))


async def test_a_registry_scopes_the_types_workspaces_accept(storage: Storage) -> None:
    workspaces = Workspaces(storage, registry=OURS)
    workspace = await workspaces.open("acme", "prod", actor=SourceActor(name="ci"))
    assert (await workspace.publish(Ping())).envelope.event_type == "ours:ping"
    with pytest.raises(NotFound, match=r"event type app:deploy\.finished"):
        await workspace.publish(Deploy(service="auth"))
    with pytest.raises(ValueError, match=r"app:deploy\.finished is not in the registry"):
        Workspaces(storage, registry=OURS, emitted=[Deploy])
    deploys = Rule(name="ours:deploys", when=on(Deploy), then=run("note"))
    with pytest.raises(InvalidRule, match=r"no event type 'app:deploy\.finished'"):
        Workspaces(storage, registry=OURS, rules=[deploys])


async def test_a_runs_events_carry_its_causation_and_chain(workspace: Workspace) -> None:
    cause = (await workspace.publish(ServiceError(service="auth"))).envelope
    agent = AgentActor(rule="app:triage", run_id="fir_1", name="triage")
    caused = workspace.as_actor(agent).caused_by(
        Causation(firing_id="fir_1", run_id="fir_1", depth=1), correlation_id=cause.id
    )
    assert (caused.actor, caused.causation.depth if caused.causation else 0) == (agent, 1)
    assert caused.as_actor(UserActor(id="ada")).causation == caused.causation
    envelope = (await caused.publish(Deploy(service="auth"))).envelope
    assert envelope.correlation_id == cause.id
    assert (envelope.actor, envelope.depth) == (agent, 1)


async def test_causal_chains_have_a_depth_limit(workspace: Workspace) -> None:
    root = (await workspace.publish(ServiceError(service="auth"))).envelope

    def at(depth: int) -> Workspace:
        causation = Causation(firing_id="f", run_id="f", depth=depth)
        return workspace.caused_by(causation, correlation_id=root.id)

    assert (await at(3).publish(Deploy(service="auth"))).envelope.depth == 3
    with pytest.raises(DepthExceeded) as raised:
        await at(4).publish(Deploy(service="auth"))
    assert raised.value.payload()["limit"] == 3
    with pytest.raises(DepthExceeded):
        await at(4).give_feedback(
            Resolved(outcome="resolved"), on=ChainTarget(correlation_id=root.id)
        )


# ─── feedback ────────────────────────────────────────────────────────────────


async def test_feedback_is_recorded_in_its_targets_chain(
    workspace: Workspace, storage: Storage
) -> None:
    alert = (await workspace.publish(ServiceError(service="auth"))).envelope
    await seed_run(storage, chain=alert.id)
    judge = workspace.as_actor(UserActor(id="grace"))
    on_run = await judge.give_feedback(RunQuality(score=4), on=RunTarget(run_id="fir_1"))
    on_firing = await judge.give_feedback(
        Relevant(relevant=False), on=FiringTarget(firing_id="fir_1")
    )
    on_chain = await judge.give_feedback(
        Resolved(outcome="escalated"), on=ChainTarget(correlation_id=alert.id)
    )
    assert {e.correlation_id for e in (on_run, on_firing, on_chain)} == {alert.id}
    assert on_run.event == FeedbackGiven(
        feedback_type="run_quality",
        target=RunTarget(run_id="fir_1"),
        value={"score": 4, "note": ""},
    )
    assert on_run.actor == UserActor(id="grace")


async def test_feedback_must_suit_and_find_its_target(workspace: Workspace) -> None:
    with pytest.raises(ValidationFailed, match="run_quality feedback cannot be given on a chain"):
        await workspace.give_feedback(RunQuality(score=1), on=ChainTarget(correlation_id="e"))
    with pytest.raises(NotFound, match="run fir_9"):
        await workspace.give_feedback(RunQuality(score=1), on=RunTarget(run_id="fir_9"))
    with pytest.raises(NotFound, match="firing fir_9"):
        await workspace.give_feedback(Relevant(relevant=True), on=FiringTarget(firing_id="fir_9"))
    with pytest.raises(NotFound, match="chain evt_9"):
        await workspace.give_feedback(
            Resolved(outcome="resolved"), on=ChainTarget(correlation_id="evt_9")
        )
    assert await workspace.head_seq() == 0
    alert = (await workspace.publish(ServiceError(service="auth"), id="alert-7")).envelope
    await workspace.publish(Deploy(service="auth"), id="d1", correlation_id=alert.id)
    with pytest.raises(ValidationFailed, match=r"d1 did not start a causal chain.*alert-7"):
        await workspace.give_feedback(
            Resolved(outcome="resolved"), on=ChainTarget(correlation_id="d1")
        )
    assert await workspace.head_seq() == 2


# ─── run operations ──────────────────────────────────────────────────────────


async def test_operators_can_skip_cancel_and_retry_runs(
    workspace: Workspace, storage: Storage, clock: FakeClock
) -> None:
    await seed_run(storage, "fir_1")
    await seed_run(storage, "fir_2")
    clock.advance(10)
    skipped = await workspace.skip_run("fir_1", reason="duplicate incident")
    cancelled = await workspace.cancel_run("fir_2")
    retried = await workspace.retry_run("fir_1")
    assert (skipped.status, cancelled.status, retried.status) == ("skipped", "cancelled", "pending")
    assert retried.updated_at == START.replace(second=10)
    assert await workspace.run("fir_1") == retried
    events = [e.event for e in await workspace.read()]
    assert events == [
        RunSkipped(run_id="fir_1", rule="app:triage", reason="duplicate incident"),
        RunCancelled(run_id="fir_2", rule="app:triage"),
        RunRequeued(run_id="fir_1", rule="app:triage"),
    ]
    envelopes = await workspace.read()
    assert [e.actor for e in envelopes] == [UserActor(id="ada")] * 3
    assert {e.correlation_id for e in envelopes} == {"evt_1"}
    assert {e.depth for e in envelopes} == {1}  # facts about a run are one step deeper


async def test_run_operations_check_the_run(workspace: Workspace, storage: Storage) -> None:
    with pytest.raises(NotFound, match="run fir_9"):
        await workspace.retry_run("fir_9")
    with pytest.raises(NotFound, match="run fir_9"):
        await workspace.run("fir_9")
    await seed_run(storage)
    await workspace.cancel_run("fir_1")
    with pytest.raises(InvalidState, match="cannot cancel run fir_1, which is cancelled"):
        await workspace.cancel_run("fir_1")
    assert await workspace.head_seq() == 1


# ─── reads ───────────────────────────────────────────────────────────────────


async def test_reads_cover_runs_progress_and_dead_letters(
    workspace: Workspace, storage: Storage
) -> None:
    running, _ = start(fired("fir_1"), now=START)
    paging = Rule(
        name="app:paging",
        when=on(ServiceError),
        then=run("page"),
        retry=RetryPolicy(max_attempts=1),
    )
    dead, _ = fail(
        start(fired("fir_2", rule="app:paging"), now=START)[0], paging, now=START, error="x"
    )
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([running, dead])
    assert await workspace.runs() == [dead, running]
    assert await workspace.runs(rule="app:triage", status="running", scope_key='["auth"]') == [
        running
    ]
    assert await workspace.runs(limit=1) == [dead]
    assert await workspace.dead_letters() == []
    assert await workspace.rule_progress() == {}
    assert retry(dead, now=START)[0].status == "pending"


async def test_the_rule_statuses_list_every_registered_rule(build: Build, storage: Storage) -> None:
    triage = Rule(name="app:triage", when=on(ServiceError), then=run("page"))
    paused = Rule(name="app:paused", when=on(Deploy), then=run("page"), enabled=False)
    workspace = await build([triage, paused]).open("acme", "prod", actor=UserActor(id="ada"))
    for _ in range(3):
        await workspace.publish(Deploy(service="auth"))
    async with storage.transaction(ACME) as transaction:
        await transaction.save_progress("app:triage", RuleProgress(cursor=2, generation=1))
        await transaction.save_progress(
            "app:retired", RuleProgress(cursor=3)
        )  # no longer registered
        await transaction.dead_letter(
            [
                EvaluationError(rule="app:triage", seq=1, error="x"),
                EvaluationError(rule="app:triage", seq=2, error="y"),
                EvaluationError(rule="app:retired", seq=3, error="z"),
            ]
        )
    added = Rule(
        name="app:added", when=on(Deploy), then=run("page")
    )  # registered after the log began
    later = await build([triage, paused, added]).open("acme", "prod", actor=UserActor(id="ada"))
    statuses = await later.rule_statuses()
    assert [(s.rule, s.origin, s.version) for s in statuses] == [
        ("app:triage", "code", None),
        ("app:paused", "code", None),
        ("app:added", "code", None),
    ]
    assert [(s.enabled, s.cursor, s.lag, s.generation, s.dead_letters) for s in statuses] == [
        (True, 2, 1, 1, 2),
        (False, 0, 3, 0, 0),
        (True, 0, 3, 0, 0),
    ]


async def test_subscribing_follows_the_log(workspace: Workspace) -> None:
    await workspace.publish(Deploy(service="auth"))
    stream = workspace.subscribe()
    assert (await anext(stream)).seq == 1
    await workspace.publish(Deploy(service="auth"))
    assert (await anext(stream)).seq == 2
    await stream.aclose()


async def test_a_subscription_ends_when_its_storage_stream_ends(workspace: Workspace) -> None:
    await workspace.publish(Deploy(service="auth"))
    [stored] = await workspace.read()

    class FiniteStorage:
        async def subscribe(
            self, workspace: WorkspaceRef, *, after_seq: int = 0
        ) -> AsyncGenerator[Envelope]:
            yield stored

    finite = Workspaces(cast(Any, FiniteStorage()))
    handle = await finite.open("acme", "prod", actor=UserActor(id="ada"))
    assert [envelope.seq async for envelope in handle.subscribe()] == [1]


async def test_a_handle_saves_its_workspaces_cursors(
    workspace: Workspace, storage: Storage
) -> None:
    await workspace.save_cursor("mirror", 2)
    assert await workspace.cursor("mirror") == 2
    assert await storage.cursor(WorkspaceRef("acme", "prod"), "mirror") == 2


async def test_reading_passes_the_window_filter_and_tail_to_storage(
    workspace: Workspace,
) -> None:
    for service in ("auth", "billing", "search"):
        await workspace.publish(Deploy(service=service))
        await workspace.publish(ServiceError(service=service))
    tail = await workspace.read(types=["app:deploy.finished"], last=2)
    assert [e.seq for e in tail] == [3, 5]
    earlier = await workspace.read(types=["app:deploy.finished"], last=2, before_seq=tail[0].seq)
    assert [e.seq for e in earlier] == [1]
    assert [e.seq for e in await workspace.read(after_seq=1, before_seq=4, limit=1)] == [2]


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"limit": 1, "last": 1}, "give limit or last, not both"),
        ({"after_seq": -1}, "after_seq cannot be negative"),
        ({"before_seq": -1}, "before_seq cannot be negative"),
        ({"limit": -1}, "limit cannot be negative"),
        ({"last": -1}, "last cannot be negative"),
    ],
)
async def test_a_read_takes_limit_or_last_and_no_negative_numbers(
    workspace: Workspace, options: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValidationFailed, match=message):
        await workspace.read(**options)


async def test_handles_are_scoped_to_their_tenant(workspaces: Workspaces) -> None:
    acme = await workspaces.open("acme", "prod", actor=UserActor(id="ada"))
    globex = await workspaces.open("globex", "prod", actor=UserActor(id="bob"))
    await acme.publish(Deploy(service="auth"))
    assert (acme.tenant_id, acme.workspace_id) == ("acme", "prod")
    assert await globex.read() == []


# ─── telemetry ───────────────────────────────────────────────────────────────


async def test_publishing_is_traced_with_its_trace_context(
    workspace: Workspace, telemetry: Telemetry
) -> None:
    envelope = (await workspace.publish(ServiceError(service="auth"), id="e1")).envelope
    [span] = telemetry.spans.get_finished_spans()
    assert span.name == "reflexr.publish app:service.error"
    assert span.kind == SpanKind.PRODUCER
    context = span.get_span_context()
    assert context is not None
    flags = f"{int(context.trace_flags):02x}"
    assert envelope.traceparent == f"00-{context.trace_id:032x}-{context.span_id:016x}-{flags}"
    assert dict(span.attributes or {}) == {
        a.TENANT_ID: "acme",
        a.WORKSPACE_ID: "prod",
        a.ACTOR_KIND: "user",
        a.USER_ID: "ada",
        a.EVENT_COUNT: 1,
        a.EVENT_TYPE: "app:service.error",
        a.EVENT_ID: "e1",
        a.EVENT_SEQ: 1,
        a.DUPLICATE: False,
        a.SESSION_ID: "e1",
        a.CONVERSATION_ID: "e1",
    }
    await workspace.publish_many([Deploy(service="auth"), Deploy(service="db")])
    assert telemetry.span_names()[-1] == "reflexr.publish"


async def test_publishing_counts_events_by_type(workspace: Workspace, telemetry: Telemetry) -> None:
    await workspace.publish(ServiceError(service="auth"), id="e1")
    await workspace.publish(ServiceError(service="auth"), id="e1")
    points = sorted(telemetry.points("reflexr.events.published"), key=lambda p: str(p[0]))
    common = {a.TENANT_ID: "acme", a.WORKSPACE_ID: "prod", a.EVENT_TYPE: "app:service.error"}
    assert points == [
        ({**common, a.ACTOR_KIND: "user", a.DUPLICATE: False}, 1),
        ({**common, a.ACTOR_KIND: "user", a.DUPLICATE: True}, 1),
    ]


async def test_feedback_and_run_operations_are_traced_and_counted(
    workspace: Workspace, storage: Storage, telemetry: Telemetry
) -> None:
    await seed_run(storage)
    await workspace.give_feedback(RunQuality(score=5), on=RunTarget(run_id="fir_1"))
    await workspace.skip_run("fir_1")
    assert telemetry.span_names() == ["reflexr.feedback run_quality", "reflexr.skip_run"]
    feedback_span, skip_span = telemetry.spans.get_finished_spans()
    assert (feedback_span.attributes or {})[a.SESSION_ID] == "evt_1"
    assert (skip_span.attributes or {})[a.RULE] == "app:triage"
    [(feedback, _)] = telemetry.points("reflexr.feedback")
    assert (feedback[a.FEEDBACK_TYPE], feedback[a.FEEDBACK_TARGET]) == ("run_quality", "run")
    [(runs, count)] = telemetry.points("reflexr.runs")
    assert (runs[a.RULE], runs[a.RUN_STATUS], count) == ("app:triage", "skipped", 1)


async def test_by_default_time_is_utc_and_nothing_is_traced() -> None:
    workspaces = Workspaces(InMemoryStorage())
    workspace = await workspaces.open("acme", "prod", actor=UserActor(id="ada"))
    envelope = (await workspace.publish(Deploy(service="auth"))).envelope
    assert envelope.ts.utcoffset() == timedelta(0)
    assert envelope.traceparent is None


async def test_events_only_runs_may_publish_are_refused_to_clients(storage: Storage) -> None:
    incident = Rule(name="app:incidents", when=on(Escalated), then=run("note"))
    workspaces = Workspaces(storage, events=[Deploy], emitted=[Escalated], rules=[incident])
    client = await workspaces.open("acme", "prod", actor=UserActor(id="ada"))
    with pytest.raises(Forbidden, match="escalated events are published by runs, not clients"):
        await client.publish(Escalated(service="auth"))
    root = (await client.publish(Deploy(service="auth"))).envelope
    triage = client.as_actor(AgentActor(rule="app:triage", run_id="fir_1", name="triage"))
    run_handle = triage.caused_by(
        Causation(firing_id="fir_1", run_id="fir_1", depth=1), correlation_id=root.id
    )
    assert not (await run_handle.publish(Escalated(service="auth"))).duplicate
