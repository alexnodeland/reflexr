"""Stored rules: installed, updated and archived in a workspace, and run as code rules are."""

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta

import pytest
from pydantic import BaseModel, JsonValue

from reflexr import Rule, UserActor, on, run
from reflexr.core import (
    MAX_STORED_RULES,
    Actor,
    AgentActor,
    Causation,
    Forbidden,
    InvalidState,
    NotFound,
    RuleArchived,
    RuleChange,
    RuleInstalled,
    RuleReset,
    RunCancelled,
    StoredRule,
    StoredRules,
    TenantId,
    ValidationFailed,
    WorkspaceId,
)
from reflexr.telemetry import attributes as a
from reflexr.workspace import (
    InMemoryStorage,
    Reaction,
    Reactor,
    RuleStatus,
    RuleVersion,
    Workspace,
    WorkspaceRef,
    WorkspaceRule,
    Workspaces,
    with_params,
)
from tests.clock import FakeClock
from tests.event_types import Deploy
from tests.workspace.conftest import Build, Telemetry

ACME = WorkspaceRef("acme", "prod")
RELAYR = AgentActor(rule="relayr:install-rule", run_id="fir_5c1e", name="install-rule")
PROVENANCE: dict[str, JsonValue] = {"source": "artifactr", "artifact": "art_7", "version": 3}


class NotifyParams(BaseModel):
    thread_id: str


@dataclass
class Notices:
    sent: list[tuple[str, str]] = field(default_factory=list[tuple[str, str]])
    arrived: asyncio.Event = field(default_factory=asyncio.Event)


async def notify(reaction: Reaction[Notices], params: NotifyParams) -> None:
    reaction.deps.sent.append((reaction.rule.name, params.thread_id))
    reaction.deps.arrived.set()


ACTIONS = {"notify": with_params(notify, NotifyParams)}


async def relayr_only(
    tenant_id: TenantId, workspace_id: WorkspaceId, actor: Actor, change: RuleChange
) -> bool:
    return actor == RELAYR


CONFIG = StoredRules(allow=relayr_only, actions={"notify": NotifyParams}, namespaces={"chat"})

code_rule = Rule(name="app:deploys", when=on(Deploy), then=run("notify", thread_id="thr_1"))


def deploys(name: str = "chat:deploys", *, thread: str = "thr_4", service: str = "auth") -> Rule:
    """A stored rule that tells a thread of each deploy of a service."""
    return Rule(
        name=name,
        when=on(Deploy).where(service=service).at_most(10, per=timedelta(hours=1)),
        then=run("notify", thread_id=thread),
        ordering="none",
        timeout=timedelta(minutes=1),
    )


@pytest.fixture
def workspaces(build: Build) -> Workspaces:
    return build(stored_rules=CONFIG)


@pytest.fixture
async def relayr(workspaces: Workspaces) -> Workspace:
    return await workspaces.open("acme", "prod", actor=RELAYR)


async def test_a_rule_is_installed_at_its_fact_in_the_chain_that_installed_it(
    build: Build,
) -> None:
    workspaces = build([code_rule], stored_rules=CONFIG)
    relayr = await workspaces.open("acme", "prod", actor=RELAYR)
    accepted = (await relayr.publish(Deploy(service="auth"))).envelope
    causation = Causation(firing_id="fir_5c1e", run_id="fir_5c1e", depth=2)
    rule = deploys()
    installed = await relayr.caused_by(causation, correlation_id=accepted.id).install_rule(
        rule, provenance=PROVENANCE
    )
    assert installed == RuleVersion(StoredRule(rule=rule, version=1, provenance=PROVENANCE), seq=2)
    fact = (await relayr.read())[-1]
    assert fact.event == RuleInstalled(
        rule="chat:deploys", version=1, spec=rule.model_dump(mode="json"), provenance=PROVENANCE
    )
    assert (fact.seq, fact.actor, fact.causation, fact.correlation_id) == (
        2,
        RELAYR,
        causation,
        accepted.id,
    )
    # The rule starts at its own fact: the deploy before it is not its to act on.
    assert (await relayr.rule_progress())["chat:deploys"].cursor == 2
    code = WorkspaceRule(rule=code_rule, origin="code")
    stored = WorkspaceRule(rule=rule, origin="stored", version=1, provenance=PROVENANCE)
    assert (await relayr.get_rule("app:deploys"), await relayr.get_rule("chat:deploys")) == (
        code,
        stored,
    )
    assert await workspaces.rules_in(ACME) == {"app:deploys": code, "chat:deploys": stored}
    async with workspaces.storage.transaction(ACME) as transaction:
        assert await workspaces.rules_in(transaction) == {
            "app:deploys": code,
            "chat:deploys": stored,
        }
    assert (await relayr.rule_statuses())[1] == RuleStatus(
        rule="chat:deploys",
        origin="stored",
        version=1,
        enabled=True,
        cursor=2,
        lag=0,
        generation=0,
        dead_letters=0,
    )


async def test_a_stored_rule_fires_beside_code_rules_and_its_action_gets_its_params(
    build: Build,
) -> None:
    workspaces = build([code_rule], stored_rules=CONFIG)
    relayr = await workspaces.open("acme", "prod", actor=RELAYR)
    await relayr.install_rule(deploys())
    await relayr.publish_many([Deploy(service="auth"), Deploy(service="billing")])
    notices = Notices()
    await Reactor(workspaces, actions=ACTIONS, deps=notices).settle()
    assert sorted(notices.sent) == [
        ("app:deploys", "thr_1"),
        ("app:deploys", "thr_1"),
        ("chat:deploys", "thr_4"),
    ]
    [done] = await relayr.runs(rule="chat:deploys")
    assert (done.status, done.fired_seq, done.scope_key) == ("succeeded", 2, "[]")


async def test_a_rule_installed_while_two_reactors_serve_fires_on_the_next_matching_event(
    workspaces: Workspaces, relayr: Workspace
) -> None:
    await relayr.publish(Deploy(service="auth"))  # so the reactors have a workspace to pass
    notices = Notices()
    stop = asyncio.Event()
    async with asyncio.TaskGroup() as group:
        for holder in ("one", "two"):
            reactor = Reactor(workspaces, actions=ACTIONS, deps=notices, holder=holder)
            group.create_task(reactor.serve(poll_interval=timedelta(milliseconds=10), stop=stop))
        await relayr.install_rule(deploys())
        # Published after the rule's fact, and before either reactor's next pass.
        await relayr.publish(Deploy(service="auth"))
        async with asyncio.timeout(10):
            await notices.arrived.wait()
        stop.set()
    assert notices.sent == [("chat:deploys", "thr_4")]
    [done] = await relayr.runs()
    assert (done.rule, done.status, done.fired_seq) == ("chat:deploys", "succeeded", 3)


async def test_a_change_that_would_change_nothing_is_a_duplicate(relayr: Workspace) -> None:
    rule = deploys()
    installed = await relayr.install_rule(rule, provenance=PROVENANCE)
    again = await relayr.install_rule(rule, provenance=PROVENANCE)
    assert again == RuleVersion(installed.stored, seq=1, duplicate=True)
    # The same rule, from elsewhere, is a change.
    assert (await relayr.update_rule(rule)).stored.version == 2
    # A retry of an update that succeeded is a duplicate, not a conflict with its old version.
    updated = await relayr.update_rule(deploys(thread="thr_9"), expected_version=2)
    retried = await relayr.update_rule(deploys(thread="thr_9"), expected_version=2)
    assert retried == RuleVersion(updated.stored, seq=3, duplicate=True)
    archived = await relayr.archive_rule("chat:deploys", reason="done")
    again = await relayr.archive_rule("chat:deploys", expected_version=1)
    assert again == RuleVersion(archived.stored, seq=4, duplicate=True)
    assert await relayr.head_seq() == 4


async def test_an_update_resets_the_rule_only_when_its_condition_or_scope_changes(
    workspaces: Workspaces, relayr: Workspace
) -> None:
    await relayr.install_rule(deploys())
    await relayr.publish(Deploy(service="auth"))
    await Reactor(workspaces, actions=ACTIONS, deps=Notices()).evaluate()
    retargeted = await relayr.update_rule(deploys(thread="thr_9"), expected_version=1)
    assert (retargeted.stored.version, retargeted.seq) == (2, 4)
    assert (await relayr.rule_progress())["chat:deploys"].cursor == 3  # its state carries on
    changed = await relayr.update_rule(deploys(service="billing"), provenance=PROVENANCE)
    assert (changed.stored.version, changed.stored.provenance, changed.seq) == (3, PROVENANCE, 5)
    installed, reset = await relayr.read(after_seq=4)
    assert reset.event == RuleReset(rule="chat:deploys", generation=1, reason="changed", from_seq=5)
    assert (reset.actor, reset.correlation_id) == (RELAYR, installed.id)
    progress = (await relayr.rule_progress())["chat:deploys"]
    assert (progress.cursor, progress.generation) == (5, 1)
    with pytest.raises(InvalidState, match="rule chat:deploys is at version 3, not 2"):
        await relayr.update_rule(deploys(), expected_version=2)


async def test_archiving_cancels_unfinished_runs_and_installing_again_starts_afresh(
    workspaces: Workspaces, relayr: Workspace, telemetry: Telemetry
) -> None:
    await relayr.install_rule(deploys())
    await relayr.publish_many([Deploy(service="auth") for _ in range(3)])
    reactor = Reactor(workspaces, actions=ACTIONS, deps=Notices())
    await reactor.evaluate()
    assert await reactor.execute(limit=1) == 1
    pending = [r for r in reversed(await relayr.runs()) if r.status == "pending"]
    archived = await relayr.archive_rule("chat:deploys", expected_version=1, reason="done")
    assert (archived.stored.version, archived.stored.status, archived.seq) == (2, "archived", 10)
    assert [e.event for e in await relayr.read(after_seq=9)] == [
        RuleArchived(rule="chat:deploys", version=2, reason="done", cancelled=2),
        *(RunCancelled(run_id=r.id, rule=r.rule, reason="its rule was archived") for r in pending),
    ]
    assert [r.status for r in await relayr.runs()] == ["cancelled", "cancelled", "succeeded"]
    [(attributes, count)] = [
        point for point in telemetry.points("reflexr.runs") if point[0][a.RUN_STATUS] == "cancelled"
    ]
    assert (attributes[a.RULE], attributes[a.ACTOR_KIND], count) == ("chat:*", "agent", 2)
    with pytest.raises(NotFound, match="rule chat:deploys does not exist"):
        await relayr.get_rule("chat:deploys")
    assert await relayr.rule_statuses() == []
    await relayr.publish(Deploy(service="auth"))  # while it is archived
    await reactor.settle()
    assert len(await relayr.runs()) == 3
    # Installed again, it starts at its new fact, in its next generation.
    reinstalled = await relayr.install_rule(deploys())
    assert (reinstalled.stored.version, reinstalled.seq) == (3, 14)
    [reset] = await relayr.read(after_seq=14)
    assert reset.event == RuleReset(
        rule="chat:deploys", generation=1, reason="changed", from_seq=14
    )
    await relayr.publish(Deploy(service="auth"))
    await reactor.settle()
    runs = await relayr.runs()
    assert (len(runs), runs[0].fired_seq, runs[0].status) == (4, 16, "succeeded")
    assert len({r.id for r in runs}) == 4


async def test_a_namespace_a_deploy_drops_is_hidden_and_its_runs_cancelled(
    build: Build, workspaces: Workspaces, relayr: Workspace
) -> None:
    await relayr.install_rule(deploys())
    await relayr.publish(Deploy(service="auth"))
    await Reactor(workspaces, actions=ACTIONS, deps=Notices()).evaluate()
    ops = StoredRules(allow=relayr_only, actions={"notify": NotifyParams}, namespaces={"ops"})
    redeployed = build(stored_rules=ops)
    assert await redeployed.rules_in(ACME) == {}
    assert await Reactor(redeployed, actions=ACTIONS, deps=Notices()).execute() == 0
    [cancelled] = await relayr.runs()
    assert cancelled.status == "cancelled"
    assert (await relayr.read())[-1].event == RunCancelled(
        run_id=cancelled.id, rule="chat:deploys", reason="its rule is gone from the workspace"
    )
    # A code rule may take the name, and no stored row overwrites it.
    coded = code_rule.model_copy(update={"name": "chat:deploys"})
    assert await build([coded], stored_rules=ops).rules_in(ACME) == {
        "chat:deploys": WorkspaceRule(rule=coded, origin="code")
    }
    # The hidden rule can still be archived.
    hidden = await redeployed.open("acme", "prod", actor=RELAYR)
    assert (await hidden.archive_rule("chat:deploys")).stored.status == "archived"


async def test_changes_are_forbidden_without_the_configuration_or_its_allowance(
    build: Build,
) -> None:
    off = await build([code_rule]).open("acme", "prod", actor=RELAYR)
    with pytest.raises(Forbidden, match="stored rules are off in these workspaces"):
        await off.install_rule(deploys())
    seen: list[RuleChange] = []

    async def allow(
        tenant_id: TenantId, workspace_id: WorkspaceId, actor: Actor, change: RuleChange
    ) -> bool:
        seen.append(change)
        return actor == RELAYR

    config = StoredRules(allow=allow, actions={"notify": NotifyParams}, namespaces={"chat"})
    relayr = await build([code_rule], stored_rules=config).open("acme", "prod", actor=RELAYR)
    with pytest.raises(Forbidden, match="rule app:deploys is registered in code"):
        await relayr.archive_rule("app:deploys")
    ada = relayr.as_actor(UserActor(id="ada"))
    for change in (
        ada.install_rule(deploys()),
        ada.update_rule(deploys()),
        ada.archive_rule("chat:deploys"),
    ):
        with pytest.raises(Forbidden, match="this change to rule chat:deploys is not yours"):
            await change
    assert seen == [
        RuleChange(kind="install", rule="chat:deploys", spec=deploys()),
        RuleChange(kind="update", rule="chat:deploys", spec=deploys()),
        RuleChange(kind="archive", rule="chat:deploys"),
    ]
    assert await relayr.head_seq() == 0


async def test_a_rule_is_refused_with_every_problem_it_has(relayr: Workspace) -> None:
    unchecked = Rule(name="chat:deploys", when=on("app:nope"), then=run("notify", thread_id="t"))
    heavy: dict[str, JsonValue] = {"note": "x" * 4096}
    for change in (
        relayr.install_rule(unchecked, provenance=heavy),
        relayr.update_rule(unchecked, provenance=heavy),
    ):
        with pytest.raises(ValidationFailed, match="rule chat:deploys cannot be stored") as refused:
            await change
        assert refused.value.errors == [
            "no event type 'app:nope'",
            "when.throttle is required",
            "timeout is required",
            'ordering must be "none"',
            "provenance must be at most 4096 bytes of JSON",
        ]
    with pytest.raises(ValidationFailed) as refused:
        await relayr.install_rule(deploys("ops:deploys"))
    assert refused.value.errors == ["the namespace 'ops' is not one stored rules may use"]
    assert await relayr.head_seq() == 0


async def test_a_workspace_has_at_most_fifty_active_stored_rules(relayr: Workspace) -> None:
    for number in range(MAX_STORED_RULES):
        await relayr.install_rule(deploys(f"chat:deploys-{number}"))
    with pytest.raises(ValidationFailed, match="the workspace has 50 active stored rules"):
        await relayr.install_rule(deploys("chat:one-more"))
    await relayr.update_rule(deploys("chat:deploys-1", thread="thr_9"))  # adds no rule
    await relayr.archive_rule("chat:deploys-0")
    assert (await relayr.install_rule(deploys("chat:one-more"))).stored.version == 1


async def test_each_command_meets_the_rule_as_it_is(relayr: Workspace) -> None:
    for change in (relayr.update_rule(deploys()), relayr.archive_rule("chat:deploys")):
        with pytest.raises(NotFound, match="stored rule chat:deploys does not exist"):
            await change
    await relayr.install_rule(deploys())
    with pytest.raises(InvalidState, match="rule chat:deploys is installed already: update it"):
        await relayr.install_rule(deploys(thread="thr_9"))
    with pytest.raises(InvalidState, match="rule chat:deploys is at version 1, not 7"):
        await relayr.archive_rule("chat:deploys", expected_version=7)
    await relayr.archive_rule("chat:deploys")
    with pytest.raises(InvalidState, match="rule chat:deploys is archived: install it again"):
        await relayr.update_rule(deploys(thread="thr_9"))


async def test_a_run_executes_the_version_its_claim_reads(
    workspaces: Workspaces, relayr: Workspace
) -> None:
    await relayr.install_rule(deploys())
    await relayr.publish(Deploy(service="auth"))
    notices = Notices()
    reactor = Reactor(workspaces, actions=ACTIONS, deps=notices)
    await reactor.evaluate()
    await relayr.update_rule(deploys(thread="thr_9"))
    await reactor.execute()
    assert notices.sent == [("chat:deploys", "thr_9")]


async def test_a_stored_rule_is_replayed_as_a_code_rule_is(
    workspaces: Workspaces, relayr: Workspace
) -> None:
    await relayr.install_rule(deploys())
    await relayr.publish(Deploy(service="auth"))
    reactor = Reactor(workspaces, actions=ACTIONS, deps=Notices())
    await reactor.evaluate()
    progress = await relayr.replay_rule("chat:deploys", from_seq=1, mode="refire")
    assert (progress.cursor, progress.generation) == (1, 1)
    await reactor.evaluate()
    assert len(await relayr.runs()) == 2


async def test_metrics_record_a_stored_rule_as_its_namespace(
    build: Build, telemetry: Telemetry
) -> None:
    workspaces = build([code_rule], stored_rules=CONFIG)
    relayr = await workspaces.open("acme", "prod", actor=RELAYR)
    await relayr.install_rule(deploys("chat:a"))
    await relayr.publish_many([Deploy(service="auth"), Deploy(service="auth")])
    await relayr.install_rule(deploys("chat:b"))
    await relayr.publish(Deploy(service="billing"))
    reactor = Reactor(workspaces, actions=ACTIONS, deps=Notices())
    await reactor.evaluate()
    lags = {str(attrs[a.RULE]): lag for attrs, lag in telemetry.points("reflexr.evaluation.lag")}
    # The code rule, met at the start, was 5 behind; chat:a 4 and chat:b 1, so chat:* 4.
    assert lags == {"app:deploys": 5, "chat:*": 4}
    await reactor.settle()
    fired = {str(attrs[a.RULE]): count for attrs, count in telemetry.points("reflexr.firings")}
    assert fired == {"app:deploys": 3, "chat:*": 2}
    ran = {str(attrs[a.RULE]) for attrs, _ in telemetry.points("reflexr.run.duration")}
    assert ran == {"app:deploys", "chat:*"}
    spans = {s.name for s in telemetry.spans.get_finished_spans()}
    assert {"reflexr.install_rule", "invoke_workflow chat:a"} <= spans


async def test_stored_rules_are_checked_when_workspaces_and_reactors_are_built(
    build: Build,
) -> None:
    chatty = code_rule.model_copy(update={"name": "chat:deploys"})
    with pytest.raises(ValueError, match="rule 'chat:deploys' is in 'chat', which stored rules"):
        build([chatty], stored_rules=CONFIG)
    workspaces = build(stored_rules=CONFIG)
    with pytest.raises(ValueError, match="stored rules may run 'notify', which is not among"):
        Reactor(workspaces)

    class Other(BaseModel):
        channel: str

    async def post(reaction: Reaction[None], params: Other) -> None: ...

    async def page(reaction: Reaction[None]) -> None: ...

    with pytest.raises(ValueError, match="action 'notify' NotifyParams, but it declares Other"):
        Reactor(workspaces, actions={"notify": with_params(post, Other)})
    with pytest.raises(ValueError, match="'notify' NotifyParams, but it declares no params"):
        Reactor(workspaces, actions={"notify": page})
    paging = build(stored_rules=StoredRules(relayr_only, {"page": None}, {"chat"}))
    with pytest.raises(ValueError, match="'page' no params, but it declares Other"):
        Reactor(paging, actions={"page": with_params(post, Other)})
    Reactor(paging, actions={"page": page})


class ArchivesMidPass(InMemoryStorage):
    """Storage that archives the stored rule just after a pass has read the workspace's rules."""

    relayr: Workspace | None = None

    async def stored_rules(self, workspace: WorkspaceRef) -> list[StoredRule]:
        found = await super().stored_rules(workspace)
        if self.relayr is not None and found:
            await self.relayr.archive_rule("chat:deploys")
        return found


async def test_a_rule_archived_during_a_pass_is_not_evaluated(clock: FakeClock) -> None:
    storage = ArchivesMidPass(clock=clock)
    workspaces = Workspaces(storage, stored_rules=CONFIG, clock=clock)
    relayr = await workspaces.open("acme", "prod", actor=RELAYR)
    await relayr.install_rule(deploys())
    await relayr.publish(Deploy(service="auth"))
    storage.relayr = relayr
    assert await Reactor(workspaces, actions=ACTIONS, deps=Notices()).evaluate() == 0
    assert await relayr.runs() == []
    assert (await relayr.rule_progress())["chat:deploys"].cursor == 1
