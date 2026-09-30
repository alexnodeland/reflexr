"""An application built from the router, with header-based auth, and stored rules to change."""

from collections.abc import Iterable
from datetime import timedelta
from typing import Any

from fastapi import FastAPI
from starlette.requests import HTTPConnection

from reflexr import Actor, F, Feedback, Rule, UserActor, by, on, run
from reflexr.core import Predicates, RuleChange, StoredRules, TenantId, WorkspaceId
from reflexr.fastapi import Unauthorized, reflexr_router
from reflexr.workspace import (
    Clock,
    InMemoryStorage,
    Reaction,
    Reactor,
    Schedule,
    Workspaces,
    utc_now,
)
from tests.event_types import Deploy, Heartbeat, ServiceError

spike = Rule(
    name="app:spike",
    when=on(ServiceError).count(at_least=2, within=timedelta(minutes=1)),
    scope=by(F.service),
    then=run("page"),
)
heartbeat = Schedule(name="heartbeat-check", every=timedelta(seconds=30))


class Useful(Feedback, name="useful", targets={"run", "chain"}):
    useful: bool


async def page(reaction: Reaction[None]) -> dict[str, Any]:
    return {"paged": reaction.scope["service"]}


async def resolve_actor(connection: HTTPConnection) -> tuple[TenantId, Actor]:
    if connection.headers.get("x-token") == "bad":
        raise Unauthorized("bad token")
    headers = connection.headers
    user = UserActor(id=headers.get("x-user", "ada"), name=headers.get("x-name"))
    return headers.get("x-tenant", "acme"), user


async def authorize(tenant_id: TenantId, workspace_id: WorkspaceId, actor: Actor) -> bool:
    return workspace_id != "secret"


async def ada_only(
    tenant_id: TenantId, workspace_id: WorkspaceId, actor: Actor, change: RuleChange
) -> bool:
    return actor.participant == "user:ada"


STORED_RULES = StoredRules(allow=ada_only, actions={"page": None}, namespaces={"chat"})
"""Stored rules in the ``chat`` namespace, which only Ada may change."""


def chat_rule(name: str = "chat:deploys", *, service: str = "auth") -> dict[str, Any]:
    """A stored rule that pages on each deploy of a service, as JSON."""
    rule = Rule(
        name=name,
        when=on(Deploy).where(service=service).at_most(10, per=timedelta(hours=1)),
        then=run("page"),
        ordering="none",
        timeout=timedelta(minutes=1),
    )
    return rule.model_dump(mode="json")


def command(command_id: str, **body: Any) -> dict[str, Any]:
    """A command frame."""
    return {"type": "command", "command_id": command_id, "command": body}


def build(
    *,
    rules: Iterable[Rule] = (spike,),
    schedules: Iterable[Schedule] = (heartbeat,),
    predicates: Predicates | None = None,
    stored_rules: StoredRules | None = None,
    clock: Clock = utc_now,
    **options: Any,
) -> tuple[FastAPI, Workspaces, Reactor[None]]:
    workspaces = Workspaces(
        InMemoryStorage(clock=clock),
        events=[ServiceError, Deploy, Heartbeat],
        rules=rules,
        predicates=predicates,
        stored_rules=stored_rules,
        schedules=schedules,
        clock=clock,
    )
    app = FastAPI()
    router = reflexr_router(workspaces, resolve_actor=resolve_actor, authorize=authorize, **options)
    app.include_router(router, prefix="/v1")
    return app, workspaces, Reactor(workspaces, actions={"page": page})
