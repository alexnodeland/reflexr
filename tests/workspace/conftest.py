"""Fixtures for the workspace behaviour suite, which every storage must pass."""

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import tests.event_types  # noqa: F401  (registers the test event types)
from reflexr import UserActor
from reflexr.workspace import InMemoryStorage, Storage, Workspace, Workspaces
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


@pytest.fixture(params=["memory"])
def storage(request: pytest.FixtureRequest, clock: FakeClock) -> Storage:
    assert request.param == "memory"
    return InMemoryStorage(clock=clock)


@pytest.fixture
def workspaces(storage: Storage, clock: FakeClock, telemetry: Telemetry) -> Workspaces:
    return Workspaces(
        storage,
        events=[ServiceError, Deploy, Heartbeat, Flag],
        clock=clock,
        max_depth=3,
        tracer_provider=telemetry.tracer_provider,
        meter_provider=telemetry.meter_provider,
    )


@pytest.fixture
async def workspace(workspaces: Workspaces) -> Workspace:
    return await workspaces.open("acme", "prod", actor=UserActor(id="ada"))
