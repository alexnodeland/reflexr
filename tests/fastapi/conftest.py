"""An application built from the router, with header-based auth."""

from collections.abc import Iterable
from datetime import timedelta
from typing import Any

from fastapi import FastAPI
from starlette.requests import HTTPConnection

from reflexr import Actor, F, Feedback, Rule, UserActor, by, on, run
from reflexr.core import Predicates, TenantId, WorkspaceId
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
    name="spike",
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
    return connection.headers.get("x-tenant", "acme"), UserActor(
        id=connection.headers.get("x-user", "ada")
    )


async def authorize(tenant_id: TenantId, workspace_id: WorkspaceId, actor: Actor) -> bool:
    return workspace_id != "secret"


def build(
    *,
    rules: Iterable[Rule] = (spike,),
    schedules: Iterable[Schedule] = (heartbeat,),
    predicates: Predicates | None = None,
    clock: Clock = utc_now,
    **options: Any,
) -> tuple[FastAPI, Workspaces, Reactor[None]]:
    workspaces = Workspaces(
        InMemoryStorage(clock=clock),
        events=[ServiceError, Deploy, Heartbeat],
        rules=rules,
        predicates=predicates,
        schedules=schedules,
        clock=clock,
    )
    app = FastAPI()
    router = reflexr_router(workspaces, resolve_actor=resolve_actor, authorize=authorize, **options)
    app.include_router(router, prefix="/v1")
    return app, workspaces, Reactor(workspaces, actions={"page": page})
