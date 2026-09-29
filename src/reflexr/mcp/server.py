"""The MCP server: external agents feed and operate workspaces (ADR-0011)."""

import asyncio
import contextlib
import json
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any, Literal

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ResourceError, ToolError
from mcp.server.subscriptions import InMemorySubscriptionBus, ResourceUpdated, SubscriptionBus
from opentelemetry import trace
from pydantic import JsonValue
from starlette.applications import Starlette

from reflexr.core import (
    SYSTEM_EVENTS,
    CancelRun,
    Command,
    ExternalAgentActor,
    FeedbackTarget,
    Forbidden,
    GiveFeedback,
    Outcome,
    Publish,
    Rejection,
    ReplayRule,
    RetryRun,
    RunStatus,
    SkipRun,
    TenantId,
    WorkspaceId,
    load_event,
)
from reflexr.core import scope_key as key_of
from reflexr.telemetry import actor_attributes, workspace_attributes
from reflexr.workspace import (
    Authorize,
    RuleStatus,
    ScheduleStatus,
    Workspace,
    Workspaces,
    execute,
)

ResolveClient = Callable[[Context], Awaitable[tuple[TenantId, ExternalAgentActor]]]
"""Authenticates an MCP request: returns the client's tenant and actor."""

INSTRUCTIONS = (
    "This server is a set of event logs, one per workspace, watched by rules that run agents "
    "and workflows. Publish events to trigger them, read the log to see what happened, and "
    "operate runs and rules. Events you publish are attributed to you."
)

_RUN_FACTS = frozenset(t.event_type for t in SYSTEM_EVENTS if t.event_type.startswith("run_"))


def run_uri(tenant_id: TenantId, workspace_id: WorkspaceId, run_id: str) -> str:
    """Return a run's resource URI."""
    return f"reflexr://{tenant_id}/{workspace_id}/runs/{run_id}"


class ReflexrMcp:
    """An MCP server over reflexr workspaces.

    Mount :meth:`http_app` in the application, and run :meth:`lifespan` in the application's
    lifespan. Every tool goes through the same command handler as REST and the WebSocket,
    attributed to the client's :class:`~reflexr.core.ExternalAgentActor`.

    The MCP SDK traces each request itself; the server adds the tenant, workspace and actor to
    those spans.

    Args:
        workspaces: Opens tenant-scoped workspaces, and holds the rules.
        resolve: Authenticates each request.
        authorize: Whether a client may use a workspace of its tenant, asked on every tool call
            and resource read that names a workspace; allows everything if omitted. A refusal
            is a tool error carrying the ``forbidden`` rejection's message.
        name: The server's name.
        bus: Where resource-change notifications go; in-process by default.
    """

    def __init__(
        self,
        workspaces: Workspaces,
        *,
        resolve: ResolveClient,
        authorize: Authorize | None = None,
        name: str = "reflexr",
        bus: SubscriptionBus | None = None,
    ) -> None:
        self._workspaces = workspaces
        self._resolve = resolve
        self._authorize = authorize
        self._bus = bus or InMemorySubscriptionBus()
        self._watchers: dict[tuple[TenantId, WorkspaceId], asyncio.Task[None]] = {}
        self.server = MCPServer(name=name, instructions=INSTRUCTIONS, subscriptions=self._bus)
        self._register()

    def http_app(self, **options: Any) -> Starlette:
        """Return the Streamable HTTP app to mount, e.g. at ``/mcp``."""
        return self.server.streamable_http_app(**options)

    @contextlib.asynccontextmanager
    async def lifespan(self) -> AsyncGenerator[None]:
        """Run the HTTP session manager; stop watching workspaces afterwards."""
        async with self.server.session_manager.run():
            try:
                yield
            finally:
                await self.aclose()

    async def aclose(self) -> None:
        """Stop the tasks that turn run facts into resource notifications."""
        for task in self._watchers.values():
            task.cancel()
        await asyncio.gather(*self._watchers.values(), return_exceptions=True)
        self._watchers.clear()

    async def _open(
        self,
        ctx: Context,
        workspace_id: WorkspaceId,
        client: tuple[TenantId, ExternalAgentActor] | None = None,
    ) -> Workspace:
        """Open a workspace for the request's client, resolving it unless it is given.

        Raises:
            Forbidden: If ``authorize`` refuses the client this workspace.
        """
        tenant_id, actor = client or await self._resolve(ctx)
        trace.get_current_span().set_attributes(
            {**workspace_attributes(tenant_id, workspace_id), **actor_attributes(actor)}
        )
        if self._authorize is not None and not await self._authorize(
            tenant_id, workspace_id, actor
        ):
            raise Forbidden("this workspace is not yours to use")
        workspace = await self._workspaces.open(tenant_id, workspace_id, actor=actor)
        key = (tenant_id, workspace_id)
        if key not in self._watchers:
            self._watchers[key] = asyncio.create_task(self._notify(tenant_id, workspace))
        return workspace

    async def _notify(self, tenant_id: TenantId, workspace: Workspace) -> None:
        head = await workspace.head_seq()
        async for envelope in workspace.subscribe(after_seq=head):
            if envelope.event_type in _RUN_FACTS:
                run_id = str(envelope.data["run_id"])
                await self._bus.publish(
                    ResourceUpdated(uri=run_uri(tenant_id, workspace.workspace_id, run_id))
                )

    async def _workspace(self, ctx: Context, workspace_id: WorkspaceId) -> Workspace:
        """Open a workspace for a tool, whose refusal is a tool error."""
        return await _tool(self._open(ctx, workspace_id))

    async def _execute(self, ctx: Context, workspace_id: WorkspaceId, command: Command) -> str:
        workspace = await self._workspace(ctx, workspace_id)
        outcome: Outcome = await _tool(execute(workspace, command))
        return outcome.model_dump_json()

    def _register(self) -> None:
        server = self.server

        @server.tool()
        async def publish_event(
            workspace_id: str,
            event: dict[str, Any],
            ctx: Context,
            id: str | None = None,
            correlation_id: str | None = None,
        ) -> str:
            """Publish an event, an object with its ``type`` and fields.

            Publishing an ``id`` that is already in the log adds nothing. ``correlation_id``
            joins the causal chain that event started; a later event of a chain is refused.
            """
            try:
                loaded = load_event(event)
            except Rejection as rejection:
                raise ToolError(rejection.message) from rejection
            command = Publish(event=loaded, id=id, correlation_id=correlation_id)
            return await self._execute(ctx, workspace_id, command)

        @server.tool()
        async def read_events(
            workspace_id: str,
            ctx: Context,
            after_seq: int = 0,
            types: list[str] | None = None,
            limit: int = 50,
        ) -> str:
            """Read envelopes from a workspace's log, oldest first, as JSON lines."""
            workspace = await self._workspace(ctx, workspace_id)
            found = [
                e
                for e in await workspace.read(after_seq=after_seq)
                if types is None or e.event_type in types
            ][:limit]
            return "\n".join(e.model_dump_json() for e in found) or "No events."

        @server.tool()
        async def list_rules(ctx: Context) -> str:
            """List the rules every workspace evaluates, as JSON."""
            await self._resolve(ctx)
            rules = [r.model_dump(mode="json") for r in self._workspaces.rules.values()]
            return json.dumps(rules)

        @server.tool()
        async def rule_status(workspace_id: str, ctx: Context) -> str:
            """Show every rule: enabled or not, its cursor, lag, generation and dead letters."""
            workspace = await self._workspace(ctx, workspace_id)
            statuses = await workspace.rule_statuses()
            return "\n".join(map(_rule_line, statuses)) or "No rules are registered."

        @server.tool()
        async def schedule_status(workspace_id: str, ctx: Context) -> str:
            """Show each schedule that ticks in a workspace: when it last ticked and ticks next."""
            workspace = await self._workspace(ctx, workspace_id)
            statuses = await workspace.schedule_statuses()
            return "\n".join(map(_schedule_line, statuses)) or "No schedule targets this workspace."

        @server.tool()
        async def replay_rule(
            workspace_id: str,
            rule: str,
            ctx: Context,
            from_seq: int = 0,
            mode: Literal["rebuild", "refire"] = "rebuild",
        ) -> str:
            """Evaluate a rule again from ``from_seq``: rebuild its state quietly, or refire."""
            command = ReplayRule(rule=rule, from_seq=from_seq, mode=mode)
            return await self._execute(ctx, workspace_id, command)

        @server.tool()
        async def list_runs(
            workspace_id: str,
            ctx: Context,
            rule: str | None = None,
            scope_key: list[JsonValue] | None = None,
            status: RunStatus | None = None,
            limit: int = 20,
        ) -> str:
            """List runs, newest first, as JSON lines, optionally of one rule, scope or status.

            ``scope_key`` holds the scope's values, as a run's ``scope_key`` does: ``["auth"]``.
            """
            workspace = await self._workspace(ctx, workspace_id)
            key = None if scope_key is None else key_of(scope_key)
            runs = await workspace.runs(rule=rule, scope_key=key, status=status, limit=limit)
            return "\n".join(run.model_dump_json() for run in runs) or "No runs."

        @server.tool()
        async def get_run(workspace_id: str, run_id: str, ctx: Context) -> str:
            """Return a run as JSON: its status, attempts, error, output and checkpoint."""
            workspace = await self._workspace(ctx, workspace_id)
            return (await _tool(workspace.run(run_id))).model_dump_json()

        @server.tool()
        async def retry_run(workspace_id: str, run_id: str, ctx: Context) -> str:
            """Make a run runnable now, with a fresh retry budget if it had finished."""
            return await self._execute(ctx, workspace_id, RetryRun(run_id=run_id))

        @server.tool()
        async def skip_run(
            workspace_id: str, run_id: str, ctx: Context, reason: str | None = None
        ) -> str:
            """Give up on a waiting or dead-lettered run, unblocking its scope."""
            return await self._execute(ctx, workspace_id, SkipRun(run_id=run_id, reason=reason))

        @server.tool()
        async def cancel_run(
            workspace_id: str, run_id: str, ctx: Context, reason: str | None = None
        ) -> str:
            """Cancel a run that has not finished, stopping it if it is running."""
            command = CancelRun(run_id=run_id, reason=reason)
            return await self._execute(ctx, workspace_id, command)

        @server.tool()
        async def list_dead_letters(
            workspace_id: str, ctx: Context, rule: str | None = None
        ) -> str:
            """List the envelopes rules could not evaluate, as JSON lines."""
            workspace = await self._workspace(ctx, workspace_id)
            letters = await workspace.dead_letters(rule=rule)
            return "\n".join(letter.model_dump_json() for letter in letters) or "None."

        @server.tool()
        async def give_feedback(
            workspace_id: str,
            feedback_type: str,
            target: FeedbackTarget,
            ctx: Context,
            value: dict[str, Any] | None = None,
        ) -> str:
            """Give feedback of an application-defined type on a run, a firing or a chain.

            ``value`` holds the feedback type's fields.
            """
            command = GiveFeedback(feedback_type=feedback_type, target=target, value=value or {})
            return await self._execute(ctx, workspace_id, command)

        @server.resource(
            "reflexr://{tenant_id}/{workspace_id}/runs/{run_id}",
            mime_type="application/json",
            description="A run's current state, as JSON.",
        )
        async def run_resource(tenant_id: str, workspace_id: str, run_id: str, ctx: Context) -> str:
            resolved, actor = await self._resolve(ctx)
            if resolved != tenant_id:
                raise ResourceError(f"runs of tenant {tenant_id} are not available")
            try:
                workspace = await self._open(ctx, workspace_id, (resolved, actor))
                run = await workspace.run(run_id)
            except Rejection as rejection:
                raise ResourceError(rejection.message) from rejection
            return run.model_dump_json()


def _rule_line(status: RuleStatus) -> str:
    letters = "1 dead letter" if status.dead_letters == 1 else f"{status.dead_letters} dead letters"
    return (
        f"- {status.rule}: {'enabled' if status.enabled else 'disabled'}, cursor {status.cursor}, "
        f"{status.lag} behind, generation {status.generation}, {letters}"
    )


def _schedule_line(status: ScheduleStatus) -> str:
    if status.last_tick is None:
        return f"- {status.schedule}: not started in this workspace yet"
    ticks = status.model_dump(mode="json")  # the times as REST writes them
    return f"- {status.schedule}: last tick {ticks['last_tick']}, next tick {ticks['next_tick']}"


async def _tool[T](awaitable: Awaitable[T]) -> T:
    try:
        return await awaitable
    except Rejection as rejection:
        raise ToolError(rejection.message) from rejection
