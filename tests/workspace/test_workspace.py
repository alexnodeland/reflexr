"""Workspace handles: publishing, feedback, run operations and reads, with their telemetry."""

from datetime import timedelta
from typing import Literal

import pytest
from opentelemetry.trace import SpanKind

from reflexr import AgentActor, Event, Feedback, RetryPolicy, Rule, SourceActor, UserActor, on, run
from reflexr.core import (
    Causation,
    ChainTarget,
    DepthExceeded,
    FeedbackGiven,
    FiringTarget,
    Forbidden,
    InvalidState,
    NotFound,
    RuleFired,
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
from reflexr.workspace import InMemoryStorage, Storage, Workspace, WorkspaceRef, Workspaces
from tests.event_types import Deploy, ServiceError
from tests.workspace.conftest import START, FakeClock, Telemetry
from tests.workspace.helpers import fired

ACME = WorkspaceRef("acme", "prod")


class Escalated(Event):
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


async def test_only_accepted_types_can_be_published(workspace: Workspace) -> None:
    with pytest.raises(NotFound, match="event type escalated"):
        await workspace.publish(Escalated(service="auth"))
    with pytest.raises(NotFound, match=r"event type pager\.sent"):
        await workspace.publish(load_event({"type": "pager.sent"}))
    fact = RuleFired(rule="r", scope={}, scope_key="[]", firing_id="f", matched=(1,))
    with pytest.raises(Forbidden, match="rule_fired events are recorded by reflexr"):
        await workspace.publish(fact)
    assert await workspace.head_seq() == 0


async def test_without_an_allowlist_every_registered_type_is_accepted(
    storage: Storage,
) -> None:
    workspaces = Workspaces(storage)
    workspace = await workspaces.open("acme", "prod", actor=SourceActor(name="ci"))
    assert (await workspace.publish(Deploy(service="auth"))).envelope.seq == 1
    with pytest.raises(Forbidden):
        await workspace.publish(Tick(schedule="clock", at=START))


async def test_a_runs_events_carry_its_causation_and_chain(workspace: Workspace) -> None:
    cause = (await workspace.publish(ServiceError(service="auth"))).envelope
    agent = AgentActor(rule="triage", run_id="fir_1", name="triage")
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
        RunSkipped(run_id="fir_1", rule="triage", reason="duplicate incident"),
        RunCancelled(run_id="fir_2", rule="triage"),
        RunRequeued(run_id="fir_1", rule="triage"),
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
        name="paging", when=on(ServiceError), then=run("page"), retry=RetryPolicy(max_attempts=1)
    )
    dead, _ = fail(start(fired("fir_2", rule="paging"), now=START)[0], paging, now=START, error="x")
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([running, dead])
    assert await workspace.runs() == [dead, running]
    assert await workspace.runs(rule="triage", status="running", scope_key='["auth"]') == [running]
    assert await workspace.runs(limit=1) == [dead]
    assert await workspace.dead_letters() == []
    assert await workspace.rule_progress() == {}
    assert retry(dead, now=START)[0].status == "pending"


async def test_subscribing_follows_the_log(workspace: Workspace) -> None:
    await workspace.publish(Deploy(service="auth"))
    stream = workspace.subscribe()
    assert (await anext(stream)).seq == 1
    await workspace.publish(Deploy(service="auth"))
    assert (await anext(stream)).seq == 2
    await stream.aclose()


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
    assert span.name == "reflexr.publish service.error"
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
        a.EVENT_TYPE: "service.error",
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
    common = {a.TENANT_ID: "acme", a.WORKSPACE_ID: "prod", a.EVENT_TYPE: "service.error"}
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
    assert (skip_span.attributes or {})[a.RULE] == "triage"
    [(feedback, _)] = telemetry.points("reflexr.feedback")
    assert (feedback[a.FEEDBACK_TYPE], feedback[a.FEEDBACK_TARGET]) == ("run_quality", "run")
    [(runs, count)] = telemetry.points("reflexr.runs")
    assert (runs[a.RULE], runs[a.RUN_STATUS], count) == ("triage", "skipped", 1)


async def test_by_default_time_is_utc_and_nothing_is_traced() -> None:
    workspaces = Workspaces(InMemoryStorage())
    workspace = await workspaces.open("acme", "prod", actor=UserActor(id="ada"))
    envelope = (await workspace.publish(Deploy(service="auth"))).envelope
    assert envelope.ts.utcoffset() == timedelta(0)
    assert envelope.traceparent is None
