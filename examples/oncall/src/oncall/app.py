"""The server: REST and the WebSocket stream at /v1, MCP at /mcp, and the reactor.

Run it with ``oncall-serve`` (or ``uv run --package oncall oncall-serve``), then watch it with
``oncall watch`` and publish alerts with ``oncall alert``.

Workspaces are kept in memory, or in the database at ``ONCALL_DATABASE_URL`` (for example
``sqlite+aiosqlite:///oncall.db``), which is migrated to reflexr's schema at startup.

With ``OTEL_EXPORTER_OTLP_ENDPOINT`` set, oncall reports its traces, metrics and logs there;
with ``LANGFUSE_PUBLIC_KEY`` too, it files each run in Langfuse. stackr's stack provides both.

Authentication here is a demo: the user is whatever the ``x-user`` header (or ``user`` query
parameter) says, and every user shares one tenant. Real applications resolve actors from their
own sessions or tokens.
"""

import asyncio
import contextlib
import os
from collections.abc import AsyncGenerator
from datetime import timedelta
from importlib.metadata import version
from typing import Any, Never

import uvicorn
from fastapi import FastAPI
from pydantic_ai.models import Model
from sqlalchemy import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from starlette.requests import HTTPConnection

from oncall.actions import build_actions
from oncall.events import EVENTS
from oncall.rules import RULES, SCHEDULES
from oncall.services import OncallDeps
from reflexr import Actor, ExternalAgentActor, UserActor
from reflexr.core import TenantId
from reflexr.fastapi import reflexr_router
from reflexr.langfuse import langfuse_run
from reflexr.mcp import ReflexrMcp
from reflexr.otel import TelemetryHandle, configure_telemetry
from reflexr.sql import SqlStorage, create_sqlite_engine, migrate
from reflexr.workspace import Clock, InMemoryStorage, Reactor, Storage, Workspaces, utc_now

TENANT: TenantId = "demo"


async def resolve_actor(connection: HTTPConnection) -> tuple[TenantId, Actor]:
    """Demo authentication: trust the ``x-user`` header or ``user`` query parameter."""
    user = connection.headers.get("x-user") or connection.query_params.get("user") or "guest"
    return TENANT, UserActor(id=user, name=user)


async def resolve_client(_ctx: object) -> tuple[TenantId, ExternalAgentActor]:
    """Demo authentication for MCP clients: every client is one external agent."""
    return TENANT, ExternalAgentActor(client_id="mcp", name="MCP client")


class Oncall:
    """The oncall system: its workspaces and rules, the reactor, and the services it acts on.

    Args:
        model: The triage agent's model; see :func:`oncall.triage.build_triage_agent`.
        storage: Where workspaces live, if not in a database of oncall's own. Any reflexr
            ``Storage`` plugs in here.
        database_url: A SQLAlchemy URL with an async driver, such as
            ``sqlite+aiosqlite:///oncall.db`` or ``postgresql+asyncpg://host/db``. Without a
            storage or a URL, workspaces live in memory and are gone when the process stops.
        clock: The time, for the log and for runs; tests pass a fake one.
        deps: The pager and deployer the workflows act on. Defaults to in-memory fakes.
        telemetry: OpenTelemetry, set up by :func:`telemetry_from_environment`; also Langfuse
            when it has a client.
    """

    def __init__(
        self,
        *,
        model: Model | str | None = None,
        storage: Storage | None = None,
        database_url: str | None = None,
        clock: Clock = utc_now,
        deps: OncallDeps | None = None,
        telemetry: TelemetryHandle | None = None,
    ) -> None:
        self.engine = open_database(database_url) if storage is None and database_url else None
        if self.engine is not None:
            storage = SqlStorage(self.engine, clock=clock)
        if telemetry is not None and self.engine is not None:
            telemetry.instrument_engine(self.engine)
        self.telemetry = telemetry
        self.deps = deps or OncallDeps()
        providers: dict[str, Any] = (
            {
                "tracer_provider": telemetry.tracer_provider,
                "meter_provider": telemetry.meter_provider,
            }
            if telemetry
            else {}
        )
        self.workspaces = Workspaces(
            storage or InMemoryStorage(clock=clock),
            events=EVENTS,
            rules=RULES,
            schedules=SCHEDULES,
            clock=clock,
            **providers,
        )
        self.reactor = Reactor(
            self.workspaces,
            actions=build_actions(
                model, capabilities=[telemetry.capability()] if telemetry else []
            ),
            deps=self.deps,
            # Langfuse files each run under its rule, in its causal chain's session.
            run_context=langfuse_run if telemetry and telemetry.langfuse else None,
        )

    def app(
        self, *, serve: bool = True, poll_interval: timedelta = timedelta(seconds=1)
    ) -> FastAPI:
        """Build the web application.

        Args:
            serve: Whether the application runs the reactor while it is up. Tests that drive
                the reactor themselves, with ``reactor.settle()``, turn it off.
            poll_interval: How often the reactor looks for work.
        """
        mcp = ReflexrMcp(self.workspaces, resolve=resolve_client, name="oncall")
        reactor, engine = self.reactor, self.engine

        @contextlib.asynccontextmanager
        async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
            async with contextlib.AsyncExitStack() as stack:
                if engine is not None:
                    await migrate(engine)
                    stack.push_async_callback(engine.dispose)
                await stack.enter_async_context(mcp.lifespan())
                if serve:
                    work = asyncio.create_task(reactor.serve(poll_interval=poll_interval))
                    stack.push_async_callback(_stop, work)
                yield

        app = FastAPI(title="oncall", lifespan=lifespan)
        app.include_router(
            reflexr_router(self.workspaces, resolve_actor=resolve_actor), prefix="/v1"
        )
        app.mount("/mcp", mcp.http_app(streamable_http_path="/"))
        if self.telemetry is not None:
            self.telemetry.instrument_app(app)

        @app.get("/")
        async def about() -> dict[str, str]:
            return {
                "name": "oncall",
                "events": "/v1/workspaces/{workspace_id}/events",
                "stream": "/v1/workspaces/{workspace_id}/stream",
                "commands": "/v1/workspaces/{workspace_id}/commands",
                "mcp": "/mcp",
            }

        return app


async def _stop(work: "asyncio.Task[Never]") -> None:
    """Stop the reactor at shutdown; a run it was executing is attempted again later."""
    work.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await work


def open_database(url: str) -> AsyncEngine:
    """Create an engine for ``url``; SQLite needs reflexr's own engine settings."""
    if make_url(url).get_backend_name() == "sqlite":
        return create_sqlite_engine(url)
    return create_async_engine(url)


def create_app(
    *,
    model: Model | str | None = None,
    storage: Storage | None = None,
    database_url: str | None = None,
    telemetry: TelemetryHandle | None = None,
    poll_interval: timedelta = timedelta(seconds=1),
) -> FastAPI:
    """Build the oncall application, which runs its reactor while it is up.

    Args:
        model: The triage agent's model; see :func:`oncall.triage.build_triage_agent`.
        storage: Where workspaces live, if not in a database of oncall's own.
        database_url: A SQLAlchemy URL with an async driver. Defaults to
            ``ONCALL_DATABASE_URL``. Without a storage or a URL, workspaces live in memory.
        telemetry: OpenTelemetry, set up by :func:`telemetry_from_environment`.
        poll_interval: How often the reactor looks for work.
    """
    database_url = database_url or os.environ.get("ONCALL_DATABASE_URL")
    oncall = Oncall(model=model, storage=storage, database_url=database_url, telemetry=telemetry)
    return oncall.app(poll_interval=poll_interval)


def telemetry_from_environment() -> TelemetryHandle | None:
    """Set up OpenTelemetry, and Langfuse, as the environment asks.

    OpenTelemetry when ``OTEL_EXPORTER_OTLP_ENDPOINT`` is set, and Langfuse with it when
    ``LANGFUSE_PUBLIC_KEY`` is set; otherwise, nothing.
    """
    if not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return None
    return configure_telemetry(
        service_name="oncall",
        service_version=version("oncall"),
        environment=os.environ.get("ONCALL_ENVIRONMENT", "development"),
        langfuse=bool(os.environ.get("LANGFUSE_PUBLIC_KEY")),
    )


def main() -> None:
    """Serve oncall on ``ONCALL_HOST``:``ONCALL_PORT`` (default 127.0.0.1:8000)."""
    host = os.environ.get("ONCALL_HOST", "127.0.0.1")
    port = int(os.environ.get("ONCALL_PORT", "8000"))
    telemetry = telemetry_from_environment()
    try:
        uvicorn.run(create_app(telemetry=telemetry), host=host, port=port)
    finally:
        if telemetry is not None:
            telemetry.shutdown()
