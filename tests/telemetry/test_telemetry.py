"""The metric registry's cardinality policy, and recording measurements through it."""

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from reflexr import SystemActor, UserActor
from reflexr.telemetry import (
    METRICS,
    SCOPE_ATTRIBUTES,
    Metric,
    MetricsDetail,
    Telemetry,
    actor_attributes,
    chain_attributes,
    workspace_attributes,
)
from reflexr.telemetry import attributes as a
from reflexr.telemetry.metrics import EVALUATION_DURATION, EVALUATION_LAG, EVENTS_PUBLISHED

# The attributes that identify a single thing: tracing gives them, metrics never do.
IDENTIFYING = {a.EVENT_ID, a.EVENT_SEQ, a.RUN_ID, a.SCOPE, a.SESSION_ID, a.CONVERSATION_ID}


def _points(reader: InMemoryMetricReader) -> dict[str, list[tuple[dict[str, object], float]]]:
    data = reader.get_metrics_data()
    found: dict[str, list[tuple[dict[str, object], float]]] = {}
    if data is None:
        return found
    for resource in data.resource_metrics:
        for scope in resource.scope_metrics:
            assert scope.scope.name == "reflexr"
            for metric in scope.metrics:
                for point in metric.data.data_points:
                    value = getattr(point, "value", None)
                    if value is None:
                        value = getattr(point, "sum", 0)
                    found.setdefault(metric.name, []).append((dict(point.attributes or {}), value))
    return found


def test_the_registry_never_declares_identifying_attributes() -> None:
    for metric in METRICS.values():
        assert metric.name.startswith("reflexr."), metric.name
        assert not metric.attributes & IDENTIFYING, metric.name
        assert not metric.attributes & SCOPE_ATTRIBUTES, "tenant and workspace are added by detail"
        assert METRICS[metric.name] is metric


@pytest.mark.parametrize(
    ("detail", "expected"),
    [
        ("workspace", {a.TENANT_ID: "acme", a.WORKSPACE_ID: "prod"}),
        ("tenant", {a.TENANT_ID: "acme"}),
        ("none", {}),
    ],
)
def test_metrics_detail_limits_tenant_and_workspace(
    detail: MetricsDetail, expected: dict[str, str]
) -> None:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    telemetry = Telemetry(meter_provider=provider, metrics_detail=detail)
    telemetry.record(
        EVENTS_PUBLISHED,
        1,
        tenant_id="acme",
        workspace_id="prod",
        attributes={a.EVENT_TYPE: "deploy"},
    )
    [(attributes, value)] = _points(reader)["reflexr.events.published"]
    assert (attributes, value) == ({a.EVENT_TYPE: "deploy", **expected}, 1)
    provider.shutdown()


def test_every_kind_of_instrument_records() -> None:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    telemetry = Telemetry(meter_provider=provider, metrics_detail="none")
    telemetry.record(EVALUATION_LAG, 7, tenant_id="t", workspace_id="w", attributes={a.RULE: "r"})
    telemetry.record(EVALUATION_DURATION, 0.25, tenant_id="t", workspace_id="w")
    points = _points(reader)
    assert points["reflexr.evaluation.lag"] == [({a.RULE: "r"}, 7)]
    assert points["reflexr.evaluation.duration"] == [({}, 0.25)]
    provider.shutdown()


def test_only_registered_metrics_and_declared_attributes_are_recorded() -> None:
    telemetry = Telemetry()
    with pytest.raises(ValueError, match=r"does not declare \['reflexr.run.id'\]"):
        telemetry.record(
            EVENTS_PUBLISHED, 1, tenant_id="t", workspace_id="w", attributes={a.RUN_ID: "r1"}
        )
    unregistered = Metric("reflexr.unregistered", "counter", "1", "Not in the registry.")
    with pytest.raises(KeyError):
        telemetry.record(unregistered, 1, tenant_id="t", workspace_id="w")


def test_span_attribute_helpers() -> None:
    assert workspace_attributes("acme", "prod") == {a.TENANT_ID: "acme", a.WORKSPACE_ID: "prod"}
    assert actor_attributes(UserActor(id="ada")) == {a.ACTOR_KIND: "user", a.USER_ID: "ada"}
    assert actor_attributes(SystemActor(name="reactor")) == {a.ACTOR_KIND: "system"}
    assert chain_attributes("evt_1") == {a.SESSION_ID: "evt_1", a.CONVERSATION_ID: "evt_1"}
