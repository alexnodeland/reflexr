"""The router: REST endpoints and the WebSocket stream, over one command handler."""

from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, WebSocket
from opentelemetry import trace
from pydantic import BaseModel, ConfigDict
from starlette.requests import HTTPConnection

from reflexr.core import (
    Actor,
    AnyEvent,
    CommandFrame,
    CommandResult,
    Envelope,
    EvaluationError,
    EventId,
    PublishedOutcome,
    Rejection,
    Rule,
    RuleName,
    Run,
    RunStatus,
    TenantId,
    WorkspaceId,
)
from reflexr.fastapi.stream import Stream
from reflexr.telemetry import actor_attributes, workspace_attributes
from reflexr.workspace import (
    Authorize,
    RuleStatus,
    Schedule,
    ScheduleStatus,
    Workspace,
    Workspaces,
    execute,
)

ResolveActor = Callable[[HTTPConnection], Awaitable[tuple[TenantId, Actor]]]
"""Authenticates a request or connection: returns its tenant and actor, or raises Unauthorized."""

STATUS_CODES: dict[str, int] = {
    "not_found": 404,
    "invalid_state": 409,
    "validation_failed": 422,
    "depth_exceeded": 422,
    "forbidden": 403,
    "unsupported_protocol": 400,
}
"""The HTTP status for each rejection ``type``."""


class Unauthorized(Exception):
    """Raise from ``resolve_actor`` to refuse a request (401) or connection (close 4401)."""


class _Body(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class PublishItem(_Body):
    """One event to publish, with an optional id for idempotency."""

    event: AnyEvent
    id: EventId | None = None


class PublishBatch(_Body):
    """Events to publish atomically and in order, optionally joining a causal chain."""

    events: list[PublishItem]
    correlation_id: str | None = None


def reflexr_router(
    workspaces: Workspaces,
    *,
    resolve_actor: ResolveActor,
    authorize: Authorize | None = None,
    hello_timeout: float = 10.0,
    outbox_size: int = 1000,
    remembered_commands: int = 10_000,
) -> APIRouter:
    """Build the router for publishing, reads, administration and the WebSocket stream.

    Args:
        workspaces: Opens tenant-scoped workspaces, and holds the rules and schedules.
        resolve_actor: Authenticates each request and connection.
        authorize: Whether an actor may use a workspace; allows everything if omitted.
        hello_timeout: Seconds a new connection has to send ``hello``.
        outbox_size: Frames buffered for a slow connection before it is closed (4429).
        remembered_commands: Command ids remembered for deduplication, per process.

    Each REST request's span (FastAPI's own, when it is instrumented) and each connection's
    ``reflexr.stream`` span are attributed to the tenant, workspace and actor.
    """
    router = APIRouter()
    results = _Results(remembered_commands)

    async def open_workspace(connection: HTTPConnection, workspace_id: WorkspaceId) -> Workspace:
        tenant_id, actor = await _authenticate(connection)
        trace.get_current_span().set_attributes(
            {**workspace_attributes(tenant_id, workspace_id), **actor_attributes(actor)}
        )
        if authorize is not None and not await authorize(tenant_id, workspace_id, actor):
            raise HTTPException(status_code=403, detail="this workspace is not yours to use")
        return await workspaces.open(tenant_id, workspace_id, actor=actor)

    async def _authenticate(connection: HTTPConnection) -> tuple[TenantId, Actor]:
        try:
            return await resolve_actor(connection)
        except Unauthorized as error:
            raise HTTPException(status_code=401, detail=str(error) or "unauthorized") from error

    async def workspace_dependency(request: Request, workspace_id: WorkspaceId) -> Workspace:
        return await open_workspace(request, workspace_id)

    async def authenticated(request: Request) -> None:
        await _authenticate(request)

    current_workspace = Depends(workspace_dependency)
    signed_in = Depends(authenticated)

    async def run_command(workspace: Workspace, frame: CommandFrame) -> CommandResult:
        key = (workspace.tenant_id, workspace.workspace_id, repr(workspace.actor), frame.command_id)
        if (remembered := results.get(key)) is not None:
            return remembered
        try:
            outcome = await execute(workspace, frame.command)
        except Rejection as rejection:
            result = CommandResult(
                command_id=frame.command_id, ok=False, rejection=rejection.payload()
            )
        else:
            result = CommandResult(command_id=frame.command_id, ok=True, outcome=outcome)
        results.put(key, result)
        return result

    @router.post("/workspaces/{workspace_id}/commands")
    async def post_command(
        frame: CommandFrame, response: Response, workspace: Workspace = current_workspace
    ) -> CommandResult:
        """Submit one command. The body is the same frame as over the WebSocket."""
        result = await run_command(workspace, frame)
        if result.rejection is not None:
            response.status_code = STATUS_CODES.get(str(result.rejection["type"]), 400)
        return result

    @router.post("/workspaces/{workspace_id}/events")
    async def post_events(
        body: PublishItem | PublishBatch, workspace: Workspace = current_workspace
    ) -> list[PublishedOutcome]:
        """Publish one event, or a batch in order: all of them, or none."""
        batch = body if isinstance(body, PublishBatch) else PublishBatch(events=[body])
        published = await _or_http(
            workspace.publish_many(
                [item.event for item in batch.events],
                ids=[item.id for item in batch.events],
                correlation_id=batch.correlation_id,
            )
        )
        return [
            PublishedOutcome(seq=p.envelope.seq, id=p.envelope.id, duplicate=p.duplicate)
            for p in published
        ]

    @router.get("/workspaces/{workspace_id}/events")
    async def list_events(
        after_seq: int = 0,
        before_seq: int | None = None,
        limit: int | None = None,
        last: int | None = None,
        type: Annotated[list[str] | None, Query()] = None,
        workspace: Workspace = current_workspace,
    ) -> list[Envelope]:
        """Return a page of the log, optionally of some event types. ``type`` may repeat.

        The window is ``after_seq < seq < before_seq``. ``limit`` returns its first envelopes,
        ``last`` its last ones, oldest first either way.
        """
        return await _or_http(
            workspace.read(
                after_seq=after_seq, before_seq=before_seq, types=type, limit=limit, last=last
            )
        )

    @router.get("/rules", dependencies=[signed_in])
    async def list_rules() -> list[Rule]:
        """Return the registered rules."""
        return list(workspaces.rules.values())

    @router.get("/workspaces/{workspace_id}/rules")
    async def list_rule_status(workspace: Workspace = current_workspace) -> list[RuleStatus]:
        """Return whether each rule is enabled, and its cursor, lag, generation and dead letters."""
        return await workspace.rule_statuses()

    @router.get("/workspaces/{workspace_id}/runs")
    async def list_runs(
        rule: RuleName | None = None,
        scope_key: str | None = None,
        status: RunStatus | None = None,
        limit: int | None = None,
        workspace: Workspace = current_workspace,
    ) -> list[Run]:
        """Return runs, newest first."""
        return await workspace.runs(rule=rule, scope_key=scope_key, status=status, limit=limit)

    @router.get("/workspaces/{workspace_id}/runs/{run_id}")
    async def get_run(run_id: str, workspace: Workspace = current_workspace) -> Run:
        """Return a run, with its attempts, last error and checkpoint."""
        return await _or_http(workspace.run(run_id))

    @router.get("/workspaces/{workspace_id}/dead-letters")
    async def list_dead_letters(
        rule: RuleName | None = None, workspace: Workspace = current_workspace
    ) -> list[EvaluationError]:
        """Return the envelopes rules could not evaluate, oldest first."""
        return await workspace.dead_letters(rule=rule)

    @router.get("/schedules")
    async def list_schedules(request: Request) -> list[Schedule]:
        """Return the registered schedules that tick in the caller's tenant.

        A schedule that targets particular workspaces lists only the tenant's own, so no
        tenant sees another's ids; one that targets none of them is left out.
        """
        tenant_id, _ = await _authenticate(request)
        return [
            seen
            for schedule in workspaces.schedules.values()
            if (seen := _seen_by(schedule, tenant_id)) is not None
        ]

    @router.get("/workspaces/{workspace_id}/schedules")
    async def list_schedule_status(
        workspace: Workspace = current_workspace,
    ) -> list[ScheduleStatus]:
        """Return when each schedule targeting this workspace last ticked and ticks next."""
        return await workspace.schedule_statuses()

    @router.websocket("/workspaces/{workspace_id}/stream")
    async def stream(websocket: WebSocket, workspace_id: WorkspaceId) -> None:
        """The workspace protocol: a resumable subscription to the log, and commands."""
        await Stream(
            websocket,
            workspace_id,
            open_workspace=open_workspace,
            execute=run_command,
            hello_timeout=hello_timeout,
            outbox_size=outbox_size,
            telemetry=workspaces.telemetry,
        ).serve()

    return router


def _seen_by(schedule: Schedule, tenant_id: TenantId) -> Schedule | None:
    """The schedule as one tenant may see it: its targets narrowed to the tenant's workspaces."""
    if schedule.workspaces == "all":
        return schedule
    own = tuple(target for target in schedule.workspaces if target[0] == tenant_id)
    return schedule.model_copy(update={"workspaces": own}) if own else None


async def _or_http[T](awaitable: Awaitable[T]) -> T:
    try:
        return await awaitable
    except Rejection as rejection:
        status = STATUS_CODES.get(rejection.code, 400)
        raise HTTPException(status_code=status, detail=rejection.payload()) from rejection


class _Results:
    """Recently seen command results, so a repeated ``command_id`` is not executed twice."""

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._results: OrderedDict[tuple[str, str, str, str], CommandResult] = OrderedDict()

    def get(self, key: tuple[str, str, str, str]) -> CommandResult | None:
        return self._results.get(key)

    def put(self, key: tuple[str, str, str, str], result: CommandResult) -> None:
        self._results[key] = result
        if len(self._results) > self._capacity:
            self._results.popitem(last=False)
