"""The command handler every surface shares: each command carried out once per id."""

from datetime import timedelta
from typing import Literal

from reflexr import Feedback, Rule, SourceActor, UserActor, on, run
from reflexr.core import (
    Actor,
    ArchiveRule,
    CancelRun,
    ChainTarget,
    Command,
    CommandResult,
    GiveFeedback,
    InstallRule,
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
    WorkspaceId,
    load_event,
    new_id,
)
from reflexr.workspace import (
    CommandKey,
    InMemoryCommandResults,
    Storage,
    Workspace,
    WorkspaceRef,
    Workspaces,
)
from tests.event_types import Deploy
from tests.workspace.helpers import fired

DEPLOY = Publish(event=Deploy(service="auth"))
"""A publish with no event id, so each time it is carried out it appends an event."""


class Handled(Feedback, name="handled", targets={"chain"}):
    outcome: Literal["resolved", "escalated"]


async def execute(workspaces: Workspaces, workspace: Workspace, command: Command) -> CommandResult:
    """Carry out a command under a new id, as an MCP tool call without a ``command_id`` is."""
    return await workspaces.execute(workspace, command, command_id=new_id("cmd"))


async def test_every_command_goes_through_the_workspace(storage: Storage) -> None:
    rule = Rule(name="app:deploys", when=on(Deploy), then=run("note"))
    workspaces = Workspaces(storage, rules=[rule])
    workspace = await workspaces.open("acme", "prod", actor=SourceActor(name="ci"))
    published = await execute(workspaces, workspace, Publish(event=Deploy(service="auth"), id="d1"))
    assert published == CommandResult(
        command_id=published.command_id, ok=True, outcome=PublishedOutcome(seq=1, id="d1")
    )
    again = await execute(workspaces, workspace, Publish(event=Deploy(service="auth"), id="d1"))
    assert again.outcome == PublishedOutcome(seq=1, id="d1", duplicate=True)
    feedback = GiveFeedback(
        feedback_type="handled",
        target=ChainTarget(correlation_id="d1"),
        value={"outcome": "resolved"},
    )
    assert (await execute(workspaces, workspace, feedback)).outcome == RecordedOutcome(
        seq=2, id=(await workspace.read())[1].id
    )
    async with storage.transaction(WorkspaceRef("acme", "prod")) as transaction:
        await transaction.save_runs([fired("r1"), fired("r2")])
    skipped = await execute(workspaces, workspace, SkipRun(run_id="r1", reason="duplicate"))
    assert isinstance(skipped.outcome, RunOutcome)
    assert skipped.outcome.run.status == "skipped"
    retried = await execute(workspaces, workspace, RetryRun(run_id="r1"))
    assert isinstance(retried.outcome, RunOutcome)
    assert retried.outcome.run.status == "pending"
    cancelled = await execute(workspaces, workspace, CancelRun(run_id="r2"))
    assert isinstance(cancelled.outcome, RunOutcome)
    assert cancelled.outcome.run.status == "cancelled"
    replay = ReplayRule(rule="app:deploys", mode="refire")
    replayed = await execute(workspaces, workspace, replay)
    assert isinstance(replayed.outcome, RuleOutcome)
    assert (replayed.outcome.rule, replayed.outcome.progress.generation) == ("app:deploys", 1)


async def test_a_rejection_is_the_result_not_raised(storage: Storage) -> None:
    workspaces = Workspaces(storage)
    workspace = await workspaces.open("acme", "prod", actor=SourceActor(name="ci"))
    commands: list[Command] = [
        Publish(event=load_event({"type": "app:pager"})),
        GiveFeedback(feedback_type="nope", target=ChainTarget(correlation_id="x"), value={}),
        GiveFeedback(feedback_type="handled", target=ChainTarget(correlation_id="x"), value={}),
    ]
    results = [await execute(workspaces, workspace, command) for command in commands]
    assert [(result.ok, result.outcome) for result in results] == [(False, None)] * 3
    assert [result.rejection for result in results] == [
        {
            "type": "not_found",
            "message": "event type app:pager does not exist",
            "entity": "event type",
            "id": "app:pager",
        },
        {
            "type": "not_found",
            "message": "feedback type nope does not exist",
            "entity": "feedback type",
            "id": "nope",
        },
        {
            "type": "validation_failed",
            "message": "invalid handled feedback",
            "errors": [{"loc": ["outcome"], "msg": "Field required", "type": "missing"}],
        },
    ]


async def anyone(
    tenant_id: TenantId, workspace_id: WorkspaceId, actor: Actor, change: RuleChange
) -> bool:
    return True


async def test_stored_rules_are_changed_through_the_workspace(storage: Storage) -> None:
    config = StoredRules(allow=anyone, actions={"note": None}, namespaces={"chat"})
    workspaces = Workspaces(storage, stored_rules=config)
    workspace = await workspaces.open("acme", "prod", actor=SourceActor(name="relayr"))
    rule = Rule(
        name="chat:deploys",
        when=on(Deploy).at_most(1, per=timedelta(minutes=5)),
        then=run("note"),
        ordering="none",
        timeout=timedelta(minutes=1),
    )
    install = InstallRule(rule=rule, provenance={"proposal": "prp_12"})
    installed = await execute(workspaces, workspace, install)
    assert installed.outcome == RuleVersionOutcome(rule="chat:deploys", version=1, seq=1)
    assert (await execute(workspaces, workspace, install)).outcome == (
        RuleVersionOutcome(rule="chat:deploys", version=1, seq=1, duplicate=True)
    )
    retried = rule.model_copy(update={"retry": rule.retry.model_copy(update={"max_attempts": 2})})
    update = UpdateRule(rule=retried, expected_version=1)
    updated = await execute(workspaces, workspace, update)
    assert updated.outcome == RuleVersionOutcome(rule="chat:deploys", version=2, seq=2)
    archive = ArchiveRule(rule="chat:deploys", reason="done")
    archived = await execute(workspaces, workspace, archive)
    assert archived.outcome == RuleVersionOutcome(rule="chat:deploys", version=3, seq=3)


async def test_a_command_is_carried_out_once_per_id(
    workspaces: Workspaces, workspace: Workspace
) -> None:
    first = await workspaces.execute(workspace, DEPLOY, command_id="c1")
    assert isinstance(first.outcome, PublishedOutcome)
    assert (first.command_id, first.outcome.seq) == ("c1", 1)
    again = await workspaces.execute(workspace, DEPLOY, command_id="c1")
    assert again == first, "the first result, and the event was not published twice"
    rejected = await workspaces.execute(workspace, RetryRun(run_id="nope"), command_id="c2")
    assert rejected.rejection is not None
    assert rejected.rejection["type"] == "not_found"
    other = await workspaces.execute(workspace, DEPLOY, command_id="c2")
    assert other == rejected, "the id alone names the command, and a rejection is remembered"
    assert [envelope.seq for envelope in await workspace.read()] == [1]


async def test_the_same_id_elsewhere_or_from_someone_else_is_another_command(
    workspaces: Workspaces, workspace: Workspace
) -> None:
    grace = workspace.as_actor(UserActor(id="grace"))  # another participant
    staging = await workspaces.open("acme", "staging", actor=workspace.actor)  # another workspace
    theirs = await workspaces.open("globex", "prod", actor=workspace.actor)  # another tenant's
    first = await workspaces.execute(workspace, DEPLOY, command_id="c1")
    for sender in (grace, staging, theirs):
        assert (await workspaces.execute(sender, DEPLOY, command_id="c1")).ok
    renamed = workspace.as_actor(UserActor(id="ada", name="Ada Lovelace"))
    repeated = await workspaces.execute(renamed, DEPLOY, command_id="c1")
    assert repeated == first, "the same participant, whatever its name"
    assert [len(await w.read()) for w in (workspace, staging, theirs)] == [2, 1, 1]


async def test_a_forgotten_command_is_carried_out_again(storage: Storage) -> None:
    workspaces = Workspaces(storage, results=InMemoryCommandResults(capacity=1))
    workspace = await workspaces.open("acme", "prod", actor=UserActor(id="ada"))
    for command_id in ("c1", "c2", "c1"):
        await workspaces.execute(workspace, DEPLOY, command_id=command_id)
    assert len(await workspace.read()) == 3, "c2 made the workspaces forget c1"


async def test_only_the_most_recent_results_are_remembered() -> None:
    results = InMemoryCommandResults(capacity=1)
    first, second = (CommandKey("acme", "prod", "user:ada", c) for c in ("c1", "c2"))
    result = CommandResult(command_id="c1", ok=True, outcome=RecordedOutcome(seq=1, id="e1"))
    await results.put(first, result)
    assert await results.get(first) == result
    await results.put(second, result)
    assert await results.get(first) is None, "the oldest is forgotten"
    assert await results.get(second) == result
