"""The command handler every surface shares."""

from datetime import timedelta
from typing import Literal

import pytest

from reflexr import Feedback, Rule, SourceActor, on, run
from reflexr.core import (
    Actor,
    ArchiveRule,
    CancelRun,
    ChainTarget,
    GiveFeedback,
    InstallRule,
    NotFound,
    Publish,
    PublishedOutcome,
    RecordedOutcome,
    ReplayRule,
    RetryRun,
    RuleChange,
    RuleOutcome,
    RuleVersionOutcome,
    RunOutcome,
    SkipRun,
    StoredRules,
    TenantId,
    UpdateRule,
    ValidationFailed,
    WorkspaceId,
    load_event,
)
from reflexr.workspace import Storage, WorkspaceRef, Workspaces, execute
from tests.event_types import Deploy
from tests.workspace.helpers import fired


class Handled(Feedback, name="handled", targets={"chain"}):
    outcome: Literal["resolved", "escalated"]


async def test_every_command_goes_through_the_workspace(storage: Storage) -> None:
    rule = Rule(name="app:deploys", when=on(Deploy), then=run("note"))
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
    replayed = await execute(workspace, ReplayRule(rule="app:deploys", mode="refire"))
    assert isinstance(replayed, RuleOutcome)
    assert (replayed.rule, replayed.progress.generation) == ("app:deploys", 1)


async def test_rejections_come_from_the_workspace(storage: Storage) -> None:
    workspace = await Workspaces(storage).open("acme", "prod", actor=SourceActor(name="ci"))
    with pytest.raises(NotFound, match="event type app:pager"):
        await execute(workspace, Publish(event=load_event({"type": "app:pager"})))
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


async def anyone(
    tenant_id: TenantId, workspace_id: WorkspaceId, actor: Actor, change: RuleChange
) -> bool:
    return True


async def test_stored_rules_are_changed_through_the_workspace(storage: Storage) -> None:
    config = StoredRules(allow=anyone, actions={"note": None}, namespaces={"chat"})
    workspace = await Workspaces(storage, stored_rules=config).open(
        "acme", "prod", actor=SourceActor(name="relayr")
    )
    rule = Rule(
        name="chat:deploys",
        when=on(Deploy).at_most(1, per=timedelta(minutes=5)),
        then=run("note"),
        ordering="none",
        timeout=timedelta(minutes=1),
    )
    installed = await execute(workspace, InstallRule(rule=rule, provenance={"proposal": "prp_12"}))
    assert installed == RuleVersionOutcome(rule="chat:deploys", version=1, seq=1)
    assert await execute(workspace, InstallRule(rule=rule, provenance={"proposal": "prp_12"})) == (
        RuleVersionOutcome(rule="chat:deploys", version=1, seq=1, duplicate=True)
    )
    retried = rule.model_copy(update={"retry": rule.retry.model_copy(update={"max_attempts": 2})})
    updated = await execute(workspace, UpdateRule(rule=retried, expected_version=1))
    assert updated == RuleVersionOutcome(rule="chat:deploys", version=2, seq=2)
    archived = await execute(workspace, ArchiveRule(rule="chat:deploys", reason="done"))
    assert archived == RuleVersionOutcome(rule="chat:deploys", version=3, seq=3)
