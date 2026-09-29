"""Fixtures for the workspace behaviour suite, which every storage must pass.

The suite runs on in-memory storage, on SQLite, and on PostgreSQL when it is configured.
"""

from collections.abc import AsyncIterator, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import tests.event_types  # noqa: F401  (registers the test event types)
from reflexr import Rule, UserActor
from reflexr.core import Predicates
from reflexr.sql import SqlStorage, create_schema
from reflexr.workspace import InMemoryStorage, Storage, Workspace, Workspaces
from tests.databases import SQL_BACKENDS, empty_database
from tests.event_types import Deploy, Flag, Heartbeat, ServiceError

START = datetime(2026, 1, 1, tzinfo=UTC)


class FakeClock:
    """A clock tests move by hand."""

    def __init__(self) -> None:
        self.now = START

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


@dataclass
class Telemetry:
    spans: InMemorySpanExporter
    metrics: InMemoryMetricReader
    tracer_provider: TracerProvider
    meter_provider: MeterProvider

    def span_names(self) -> list[str]:
        return [span.name for span in self.spans.get_finished_spans()]

    def points(self, name: str) -> list[tuple[dict[str, object], float]]:
        data = self.metrics.get_metrics_data()
        assert data is not None
        found: list[tuple[dict[str, object], float]] = []
        for resource in data.resource_metrics:
            for scope in resource.scope_metrics:
                for metric in scope.metrics:
                    if metric.name == name:
                        for point in metric.data.data_points:
                            value = getattr(point, "value", None)
                            found.append((dict(point.attributes or {}), value or 0))
        return found


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def telemetry() -> Iterator[Telemetry]:
    spans = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(spans))
    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[reader])
    yield Telemetry(spans, reader, tracer_provider, meter_provider)
    tracer_provider.shutdown()
    meter_provider.shutdown()


@pytest.fixture(params=["memory", *SQL_BACKENDS])
async def storage(
    request: pytest.FixtureRequest, clock: FakeClock, tmp_path: Path
) -> AsyncIterator[Storage]:
    if request.param == "memory":
        yield InMemoryStorage(clock=clock)
        return
    async with empty_database(request.param, tmp_path) as database:
        engine = database.engine()
        await create_schema(engine)
        yield SqlStorage(engine, clock=clock)


class Build(Protocol):
    def __call__(
        self, rules: Iterable[Rule] = (), predicates: Predicates | None = None
    ) -> Workspaces: ...


@pytest.fixture
def build(storage: Storage, clock: FakeClock, telemetry: Telemetry) -> Build:
    """Build workspaces over the test storage, with rules and predicates."""

    def build(rules: Iterable[Rule] = (), predicates: Predicates | None = None) -> Workspaces:
        return Workspaces(
            storage,
            events=[ServiceError, Deploy, Heartbeat, Flag],
            rules=rules,
            predicates=predicates,
            clock=clock,
            max_depth=3,
            tracer_provider=telemetry.tracer_provider,
            meter_provider=telemetry.meter_provider,
        )

    return build


@pytest.fixture
def workspaces(build: Build) -> Workspaces:
    return build()


@pytest.fixture
async def workspace(workspaces: Workspaces) -> Workspace:
    return await workspaces.open("acme", "prod", actor=UserActor(id="ada"))
