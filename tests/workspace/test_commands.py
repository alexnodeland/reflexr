"""The command handler every surface shares."""

from typing import Literal

import pytest

from reflexr import Feedback, Rule, SourceActor, on, run
from reflexr.core import (
    CancelRun,
    ChainTarget,
    GiveFeedback,
    NotFound,
    Publish,
    PublishedOutcome,
    RecordedOutcome,
    ReplayRule,
    RetryRun,
    RuleOutcome,
    RunOutcome,
    SkipRun,
    ValidationFailed,
    load_event,
)
from reflexr.workspace import Storage, WorkspaceRef, Workspaces, execute
from tests.event_types import Deploy
from tests.workspace.helpers import fired


class Handled(Feedback, name="handled", targets={"chain"}):
    outcome: Literal["resolved", "escalated"]


async def test_every_command_goes_through_the_workspace(storage: Storage) -> None:
    rule = Rule(name="deploys", when=on(Deploy), then=run("note"))
    workspace = await Workspaces(storage, rules=[rule]).open(
        "acme", "prod", actor=SourceActor(name="ci")
    )
    published = await execute(workspace, Publish(event=Deploy(service="auth"), id="d1"))
    assert published == PublishedOutcome(seq=1, id="d1")
    again = await execute(workspace, Publish(event=Deploy(service="auth"), id="d1"))
    assert again == PublishedOutcome(seq=1, id="d1", duplicate=True)
    feedback = GiveFeedback(
        feedback_type="handled",
        target=ChainTarget(correlation_id="d1"),
        value={"outcome": "resolved"},
    )
    assert await execute(workspace, feedback) == RecordedOutcome(
        seq=2, id=(await workspace.read())[1].id
    )
    async with storage.transaction(WorkspaceRef("acme", "prod")) as transaction:
        await transaction.save_runs([fired("r1"), fired("r2")])
    skipped = await execute(workspace, SkipRun(run_id="r1", reason="duplicate"))
    assert isinstance(skipped, RunOutcome)
    assert skipped.run.status == "skipped"
    retried = await execute(workspace, RetryRun(run_id="r1"))
    assert isinstance(retried, RunOutcome)
    assert retried.run.status == "pending"
    cancelled = await execute(workspace, CancelRun(run_id="r2"))
    assert isinstance(cancelled, RunOutcome)
    assert cancelled.run.status == "cancelled"
    replayed = await execute(workspace, ReplayRule(rule="deploys", mode="refire"))
    assert isinstance(replayed, RuleOutcome)
    assert (replayed.rule, replayed.progress.generation) == ("deploys", 1)


async def test_rejections_come_from_the_workspace(storage: Storage) -> None:
    workspace = await Workspaces(storage).open("acme", "prod", actor=SourceActor(name="ci"))
    with pytest.raises(NotFound, match="event type pager"):
        await execute(workspace, Publish(event=load_event({"type": "pager"})))
    with pytest.raises(NotFound, match="feedback type nope"):
        await execute(
            workspace,
            GiveFeedback(feedback_type="nope", target=ChainTarget(correlation_id="x"), value={}),
        )
    with pytest.raises(ValidationFailed, match="invalid handled feedback"):
        await execute(
            workspace,
            GiveFeedback(feedback_type="handled", target=ChainTarget(correlation_id="x"), value={}),
        )
