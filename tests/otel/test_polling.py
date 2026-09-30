"""An idle application exports no spans: the reactor, subscriptions and mirrors poll untraced.

The reactor checks storage for work every pass, and SQL storage polls the log for other
processes' commits. With the SQLAlchemy instrumentation on, each poll's queries used to be a
trace of their own; now polls are untraced, and only the work they find is traced, where it
happens (ADR-0040).
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import timedelta
from pathlib import Path

import pytest
from evalr.memory import InMemoryScoreSink
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy.ext.asyncio import AsyncEngine

from reflexr import Rule, SourceActor, on, run
from reflexr.core import Envelope
from reflexr.otel import TelemetryHandle, configure_telemetry
from reflexr.scores import FeedbackMirror
from reflexr.sql import SqlStorage, create_schema, create_sqlite_engine
from reflexr.telemetry import untraced
from reflexr.workspace import Reaction, Reactor, Schedule, Workspace, Workspaces
from tests.event_types import Deploy, ServiceError

POLL = timedelta(milliseconds=5)
RULES = [Rule(name="app:triage", when=on(ServiceError), then=run("respond"))]
SCHEDULES = [Schedule(name="hourly", every=timedelta(hours=1))]


async def respond(reaction: Reaction[None]) -> None:
    return None


def workspaces(engine: AsyncEngine, telemetry: TelemetryHandle | None = None) -> Workspaces:
    providers = (
        {"tracer_provider": telemetry.tracer_provider, "meter_provider": telemetry.meter_provider}
        if telemetry
        else {}
    )
    return Workspaces(
        SqlStorage(engine, poll_interval=POLL),
        events=[ServiceError, Deploy],
        rules=RULES,
        schedules=SCHEDULES,
        **providers,
    )


class App:
    """A workspace on SQLite, with telemetry, the reactor, a subscriber and a mirror running."""

    def __init__(
        self,
        telemetry: TelemetryHandle,
        workspaces: Workspaces,
        workspace: Workspace,
        exported: InMemorySpanExporter,
        reader: InMemoryMetricReader,
    ) -> None:
        self.telemetry = telemetry
        self.workspaces = workspaces
        self.workspace = workspace
        self.exported = exported
        self.reader = reader
        self.received: list[Envelope] = []
        self.stop = asyncio.Event()
        self.tasks: list[asyncio.Task[None]] = []

    def start(self) -> None:
        reactor = Reactor(self.workspaces, actions={"respond": respond})
        mirror = FeedbackMirror(self.workspace, InMemoryScoreSink(), cursor="langfuse")
        self.tasks = [
            asyncio.create_task(reactor.serve(poll_interval=POLL, stop=self.stop)),
            asyncio.create_task(self.subscribe()),
            asyncio.create_task(mirror.follow()),
        ]

    async def close(self) -> None:
        """Stop the reactor gracefully, and the subscriber and the mirror; idempotent."""
        self.stop.set()
        serving, *following = self.tasks
        await serving
        for task in following:
            task.cancel()
        await asyncio.gather(*following, return_exceptions=True)

    async def subscribe(self) -> None:
        async for envelope in self.workspace.subscribe():
            self.received.append(envelope)

    def spans(self) -> list[ReadableSpan]:
        self.telemetry.tracer_provider.force_flush()
        return list(self.exported.get_finished_spans())

    def pool(self) -> dict[str, float]:
        """The SQLAlchemy instrumentation's connection pool metric, by state."""
        data = self.reader.get_metrics_data()
        return {
            str(dict(point.attributes or {})["state"]): point.value
            for resource in (data.resource_metrics if data else ())
            for scope in resource.scope_metrics
            for metric in scope.metrics
            if metric.name == "db.client.connections.usage"
            for point in metric.data.data_points
            if isinstance(point, NumberDataPoint)
        }


async def eventually(condition: Callable[[], Awaitable[bool]]) -> None:
    for _ in range(300):
        if await condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("never happened")


@pytest.fixture
async def app(tmp_path: Path) -> AsyncIterator[App]:
    url = f"sqlite+aiosqlite:///{tmp_path / 'app.db'}"
    # A workspace with a log that the rule has caught up with, and a schedule that is not due.
    setup = create_sqlite_engine(url)
    await create_schema(setup)
    before = workspaces(setup)
    ci = await before.open("acme", "prod", actor=SourceActor(name="ci"))
    await ci.publish(Deploy(service="auth"))
    await Reactor(before, actions={"respond": respond}).settle()
    await setup.dispose()
    # A fresh engine, whose first connection a poll opens: the pool's metrics must still count it.
    engine = create_sqlite_engine(url)
    exported, reader = InMemorySpanExporter(), InMemoryMetricReader()
    telemetry = configure_telemetry(
        service_name="app",
        instrument=("sqlalchemy",),
        engines=[engine],
        logs=False,
        set_global=False,
        span_exporter=exported,
        metric_reader=reader,
    )
    running = workspaces(engine, telemetry)
    workspace = await running.open("acme", "prod", actor=SourceActor(name="ci"))
    app = App(telemetry, running, workspace, exported, reader)
    app.start()
    try:
        yield app
    finally:
        await app.close()
        telemetry.shutdown()
        await engine.dispose()


async def test_an_idle_application_exports_no_spans(app: App) -> None:
    await asyncio.sleep(20 * POLL.total_seconds())
    assert app.spans() == [], "no trace per poll"
    pool = app.pool()
    assert sum(pool.values()) == 1, f"the pool's one connection, which a poll opened: {pool}"


async def test_the_work_a_poll_finds_is_traced_where_it_happens(app: App) -> None:
    tracer = app.telemetry.tracer_provider.get_tracer("app")
    with tracer.start_as_current_span("POST /events") as request:
        await app.workspace.publish(ServiceError(service="auth"))

    async def ran() -> bool:
        with untraced():  # the test's own look, not the application's work
            runs = await app.workspace.runs(rule="app:triage")
            head = await app.workspace.head_seq()
        return [r.status for r in runs] == ["succeeded"] and len(app.received) == head

    await eventually(ran)
    await asyncio.sleep(10 * POLL.total_seconds())
    await app.close()  # so the reactor's last evaluation, of the run's facts, has ended
    spans = app.spans()
    roots = {s.context.trace_id: s.name for s in spans if s.context and s.parent is None}
    assert set(roots.values()) == {"POST /events", "reflexr.evaluate", "invoke_workflow app:triage"}
    assert request.get_span_context().trace_id in roots
    traces = {span.context.trace_id for span in spans if span.context}
    assert traces == set(roots), (
        "every span is in the trace of a request, an evaluation or a run attempt: the "
        "reactor's, the subscriber's and the mirror's polls make none"
    )
    scopes = {span.instrumentation_scope.name for span in spans if span.instrumentation_scope}
    assert "opentelemetry.instrumentation.sqlalchemy" in scopes, "the work's queries are traced"
