"""The feedback mirror: which trace or session each piece of feedback is scored on."""

import asyncio

import pytest
from evalr.memory import InMemoryScoreSink
from opentelemetry.sdk.trace import TracerProvider

from reflexr import Rule, SourceActor, UserActor, on, run
from reflexr.core import ChainTarget, FeedbackGiven, FiringTarget, RunTarget
from reflexr.scores import FeedbackMirror, Score, sync_score_configs
from reflexr.workspace import (
    Entry,
    InMemoryStorage,
    Reaction,
    Reactor,
    Workspace,
    WorkspaceRef,
    Workspaces,
)
from tests.event_types import Deploy
from tests.scores.fakes import seeded
from tests.scores.kinds import Accuracy, Helpfulness

deploys = Rule(name="deploys", when=on(Deploy), then=run("note"))
audit = Rule(name="audit", when=on(Deploy), then=run("note"))


async def note(reaction: Reaction[None]) -> None:
    return None


async def fired(traced: bool) -> tuple[Workspace, str, str]:
    """A workspace where a deploy fired a rule whose run succeeded: its run id and chain."""
    provider = TracerProvider() if traced else None
    workspaces = Workspaces(InMemoryStorage(), rules=[audit, deploys], tracer_provider=provider)
    workspace = await workspaces.open("acme", "prod", actor=SourceActor(name="ci"))
    deploy = (await workspace.publish(Deploy(service="auth"))).envelope
    await Reactor(workspaces, actions={"note": note}).settle()
    # The audit rule fires first at the same deploy, so the mirror must look past its fact.
    [done] = await workspace.runs(rule="deploys")
    return workspace.as_actor(UserActor(id="ada")), done.id, deploy.id


async def mirrored(workspace: Workspace, sink: InMemoryScoreSink) -> list[Score]:
    mirror = FeedbackMirror(workspace, sink)
    return [s for e in await workspace.read() for s in await mirror.mirror(e)]


async def test_run_and_firing_feedback_is_scored_on_their_traces() -> None:
    workspace, run_id, chain = await fired(traced=True)
    await workspace.give_feedback(Helpfulness(rating=4), on=RunTarget(run_id=run_id))
    await workspace.give_feedback(Accuracy(correct=False), on=FiringTarget(firing_id=run_id))
    sink = InMemoryScoreSink()
    rating, correct, verdict, tone, confidence = await mirrored(workspace, sink)
    done = await workspace.run(run_id)
    assert (rating.name, rating.value, rating.trace_id) == (
        "helpfulness.rating",
        4.0,
        done.trace_ids[-1],
    )
    assert rating.session_id is None
    fired_at = next(e for e in await workspace.read() if e.event_type == "rule_fired")
    assert correct.trace_id is not None
    assert fired_at.traceparent is not None
    assert correct.trace_id in fired_at.traceparent
    assert correct.trace_id != rating.trace_id  # the evaluation, not the run
    assert [s.value for s in (correct, verdict, tone, confidence)] == [0.0, "right", "formal", 1.0]
    assert rating.metadata == {
        "tenant_id": "acme",
        "workspace_id": "prod",
        "feedback_type": "helpfulness",
        "target": "run",
        "actor": "user:ada",
        "actor_kind": "user",
        "seq": str(rating.metadata["seq"]),
    }
    assert chain


async def test_chain_feedback_and_untraced_targets_are_scored_on_the_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace, run_id, chain = await fired(traced=False)
    await workspace.give_feedback(Helpfulness(rating=2), on=ChainTarget(correlation_id=chain))
    await workspace.give_feedback(Helpfulness(rating=3), on=RunTarget(run_id=run_id))
    await workspace.give_feedback(Accuracy(correct=True), on=FiringTarget(firing_id=run_id))
    scores = await mirrored(workspace, InMemoryScoreSink())
    assert {(s.trace_id, s.session_id) for s in scores} == {(None, chain)}
    monkeypatch.setattr("reflexr.scores.mirror.FIRING_SEARCH", 0)
    traced, traced_run, _ = await fired(traced=True)
    await traced.give_feedback(Accuracy(correct=True), on=FiringTarget(firing_id=traced_run))
    [missed, *_] = await mirrored(traced, InMemoryScoreSink())
    assert missed.trace_id is None  # its rule_fired was beyond the search


async def test_mirroring_again_replaces_and_other_events_score_nothing() -> None:
    workspace, run_id, _ = await fired(traced=True)
    await workspace.give_feedback(Helpfulness(rating=5), on=RunTarget(run_id=run_id))
    sink = InMemoryScoreSink()
    first = await mirrored(workspace, sink)
    second = await mirrored(workspace, sink)
    assert [s.id for s in first] == [s.id for s in second]
    assert len(sink.scores) == 1
    unregistered = FeedbackGiven(
        feedback_type="nobody_registered_this", target=RunTarget(run_id=run_id), value={}
    )
    storage = InMemoryStorage()
    async with storage.transaction(WorkspaceRef("acme", "prod")) as transaction:
        [envelope] = await transaction.append(
            [Entry(id="f1", actor=SourceActor(name="x"), event=unregistered)]
        )
    assert await FeedbackMirror(workspace, sink).scores(envelope) == []


async def test_following_mirrors_feedback_as_it_is_given() -> None:
    workspace, run_id, _ = await fired(traced=True)
    sink = InMemoryScoreSink()
    following = asyncio.create_task(FeedbackMirror(workspace, sink).follow())
    await workspace.give_feedback(Helpfulness(rating=1), on=RunTarget(run_id=run_id))
    for _ in range(100):
        if sink.scores:
            break
        await asyncio.sleep(0.001)
    following.cancel()
    with pytest.raises(asyncio.CancelledError):
        await following
    assert [s.value for s in sink.scores.values()] == [1.0]


async def test_score_configs_are_created_once() -> None:
    store = seeded("helpfulness.rating")
    assert await sync_score_configs(store, [Helpfulness]) == ["helpfulness.reason"]
    assert await sync_score_configs(store, [Helpfulness]) == []
    created = await sync_score_configs(seeded())
    assert {"helpfulness.rating", "accuracy.correct"} <= set(created)
