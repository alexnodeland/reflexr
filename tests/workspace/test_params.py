"""Params: validated as the action's model when the reactor is built and at each attempt."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, assert_type

import pytest
from pydantic import BaseModel, JsonValue

from reflexr import Rule, SourceActor, on, run
from reflexr.core import (
    Actor,
    InvalidRule,
    RuleChange,
    RunDeadLettered,
    StoredRules,
    TenantId,
    WorkspaceId,
)
from reflexr.workspace import (
    Action,
    InMemoryStorage,
    Reaction,
    Reactor,
    Workspace,
    Workspaces,
    with_params,
)
from tests.event_types import Deploy
from tests.workspace.conftest import Build
from tests.workspace.helpers import fired


class NotifyParams(BaseModel):
    thread_id: str
    loud: bool = False


@dataclass
class Notices:
    sent: list[NotifyParams] = field(default_factory=list[NotifyParams])


async def notify(reaction: Reaction[Notices], params: NotifyParams) -> dict[str, str]:
    reaction.deps.sent.append(params)
    return {"thread": params.thread_id}


def notice(**params: JsonValue) -> Rule:
    return Rule(name="app:notice", when=on(Deploy), then=run("notify", **params))


async def deployed(workspaces: Workspaces) -> Workspace:
    workspace = await workspaces.open("acme", "prod", actor=SourceActor(name="ci"))
    await workspace.publish(Deploy(service="auth"))
    return workspace


async def test_a_function_gets_its_rules_params_typed(build: Build) -> None:
    action = with_params(notify, NotifyParams)
    assert_type(action, Action[Notices])  # pyright checks that notify takes NotifyParams
    workspaces = build([notice(thread_id="thr_4")])
    workspace = await deployed(workspaces)
    notices = Notices()
    await Reactor(workspaces, actions={"notify": action}, deps=notices).settle()
    assert notices.sent == [NotifyParams(thread_id="thr_4")]
    [done] = await workspace.runs()
    assert (done.status, done.output) == ("succeeded", {"thread": "thr_4"})


async def test_an_action_without_a_model_gets_none(build: Build) -> None:
    seen: list[BaseModel | None] = []

    async def page(reaction: Reaction[None]) -> None:
        seen.append(reaction.params)

    workspaces = build([Rule(name="app:page", when=on(Deploy), then=run("page"))])
    await deployed(workspaces)
    await Reactor(workspaces, actions={"page": page}).settle()
    assert seen == [None]


async def test_the_reactor_checks_each_rules_params_when_it_is_built(build: Build) -> None:
    actions = {"notify": with_params(notify, NotifyParams)}
    with pytest.raises(InvalidRule, match="rule 'app:notice' is invalid") as invalid:
        Reactor(build([notice(loud="very")]), actions=actions, deps=Notices())
    assert invalid.value.problems == [
        "then.params.thread_id: Field required",
        "then.params.loud: Input should be a valid boolean, unable to interpret input",
    ]

    async def page(reaction: Reaction[None]) -> None: ...

    paging = Rule(name="app:page", when=on(Deploy), then=run("page", urgent=True))
    with pytest.raises(InvalidRule, match=r"then\.params must be empty"):
        Reactor(build([paging]), actions={"page": page})


class Renamed(BaseModel):
    channel: str


async def post(reaction: Reaction[Notices], params: Renamed) -> None: ...


async def anyone(
    tenant_id: TenantId, workspace_id: WorkspaceId, actor: Actor, change: RuleChange
) -> bool:
    return True


def stored(actions: Mapping[str, type[BaseModel] | None]) -> StoredRules:
    return StoredRules(allow=anyone, actions=actions, namespaces={"chat"})


@pytest.mark.parametrize(
    ("allowed", "actions", "reason", "error"),
    [
        # A deploy changed the model, so the rule's params no longer validate.
        (
            {"notify": Renamed},
            {"notify": with_params(post, Renamed)},
            "invalid_params",
            "then.params.channel: Field required",
        ),
        # A deploy removed the action.
        ({}, {}, "unknown_action", "no action 'notify'"),
    ],
)
async def test_an_attempt_that_cannot_run_its_action_fails_permanently(
    build: Build,
    allowed: Mapping[str, type[BaseModel] | None],
    actions: dict[str, Action[Notices]],
    reason: str,
    error: str,
) -> None:
    # A stored rule whose params were checked when it was installed...
    rule = Rule(
        name="chat:notice",
        when=on(Deploy).at_most(1, per=timedelta(hours=1)),
        then=run("notify", thread_id="thr_4"),
        ordering="none",
        timeout=timedelta(minutes=1),
    )
    installing = build(stored_rules=stored({"notify": NotifyParams}))
    workspace = await installing.open("acme", "prod", actor=SourceActor(name="relayr"))
    await workspace.install_rule(rule)
    await workspace.publish(Deploy(service="auth"))
    # ...meets a deploy that changed its action, which no reactor could check.
    redeployed = build(stored_rules=stored(allowed))
    assert (await Reactor(redeployed, actions=actions, deps=Notices()).settle()).attempts == 1
    [dead] = await workspace.runs()
    assert (dead.status, dead.attempts, dead.reason, dead.error) == ("dead", 1, reason, error)
    [fact] = [e.event for e in await workspace.read() if isinstance(e.event, RunDeadLettered)]
    assert fact.reason == reason


async def test_params_are_read_as_the_model_the_action_declares() -> None:
    workspace = await Workspaces(InMemoryStorage()).open(
        "acme", "prod", actor=SourceActor(name="ci")
    )
    rule = notice(thread_id="thr_4")

    def reaction(params: BaseModel | None) -> Reaction[Any]:
        return Reaction(
            workspace=workspace, run=fired("r1"), rule=rule, events=(), deps=None, params=params
        )

    given = reaction(NotifyParams(thread_id="thr_4"))
    assert given.params_as(NotifyParams).thread_id == "thr_4"
    with pytest.raises(TypeError, match="the action declares NotifyParams, not Renamed"):
        given.params_as(Renamed)
    with pytest.raises(TypeError, match="the action declares no params, not NotifyParams"):
        reaction(None).params_as(NotifyParams)
