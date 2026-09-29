"""The metric registry: every metric reflexr records, and the ones its dashboards read.

Each :class:`Metric` declares its name, instrument, unit, description and the attributes it
may carry. reflexr records only declared attributes, so a metric cannot grow an attribute by
accident, and the dashboards are tested against this registry.

The cardinality policy, as in artifactr (its ADR-0036):

- Run, firing, event, chain and scope values are never metric attributes. Those granularities
  come from traces. Rule names are, since code bounds them.
- Tenant and workspace are attributes by default. :data:`MetricsDetail` names how much of
  that detail to keep, and ``reflexr.otel.metric_views`` turns it into OpenTelemetry views
  that drop the rest before aggregation.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Literal

from reflexr.telemetry import attributes as a

type Instrument = Literal["counter", "up_down_counter", "histogram", "gauge"]
"""The kind of OpenTelemetry instrument a metric is recorded with."""

type MetricsDetail = Literal["workspace", "tenant", "none"]
"""How much tenancy detail metrics keep: tenant and workspace, the tenant only, or neither."""

SCOPE: Final = "reflexr"
"""The instrumentation scope of reflexr's own spans and metrics."""

SCOPED: Final = frozenset({a.TENANT_ID, a.WORKSPACE_ID})
"""The tenancy attributes, which :data:`MetricsDetail` limits."""

DURATION_BUCKETS: Final = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)
"""Bucket boundaries for quick durations, in seconds: evaluating a batch of the log."""

RUN_BUCKETS: Final = (0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600)
"""Bucket boundaries for run attempts, in seconds: agents and graphs take seconds to minutes."""

_PROMETHEUS_UNITS: Mapping[str, str] = {"s": "seconds", "ms": "milliseconds", "By": "bytes"}


@dataclass(frozen=True)
class Metric:
    """One metric: what it measures, and the attributes it may carry.

    Args:
        name: The OpenTelemetry metric name.
        instrument: The instrument it is recorded with.
        unit: The UCUM unit, or an annotation in braces such as ``{run}``.
        description: What it measures.
        attributes: The attributes it may carry, tenant and workspace included.
        buckets: For a histogram, the bucket boundaries it advises the SDK to use.
    """

    name: str
    instrument: Instrument
    unit: str
    description: str
    attributes: frozenset[str] = SCOPED
    buckets: tuple[float, ...] | None = None

    @property
    def prometheus_name(self) -> str:
        """The metric's name in Prometheus, as the OTLP translation writes it.

        Dots become underscores, a unit of time or size is appended as a word (annotations in
        braces are not), and counters end in ``_total``.
        """
        name = re.sub(r"[^a-zA-Z0-9_:]", "_", self.name)
        unit = _PROMETHEUS_UNITS.get(self.unit, "")
        if unit and not name.endswith(f"_{unit}"):
            name = f"{name}_{unit}"
        return f"{name}_total" if self.instrument == "counter" else name

    @property
    def prometheus_series(self) -> frozenset[str]:
        """Every series name a Prometheus query may use for this metric."""
        name = self.prometheus_name
        if self.instrument == "histogram":
            return frozenset({f"{name}_bucket", f"{name}_sum", f"{name}_count"})
        return frozenset({name})


def _reflexr(
    name: str,
    instrument: Instrument,
    unit: str,
    description: str,
    *attributes: str,
    buckets: tuple[float, ...] | None = None,
) -> Metric:
    return Metric(
        name, instrument, unit, description, frozenset(attributes) | SCOPED, buckets=buckets
    )


EVENTS_PUBLISHED = _reflexr(
    "reflexr.events.published",
    "counter",
    "{event}",
    "Events published into workspaces, by type. Duplicates appended nothing.",
    a.EVENT_TYPE,
    a.ACTOR_KIND,
    a.DUPLICATE,
)
EVALUATION_LAG = _reflexr(
    "reflexr.evaluation.lag",
    "gauge",
    "{envelope}",
    "How many envelopes a rule was behind the head of the log when an evaluation began.",
    a.RULE,
)
EVALUATION_DURATION = _reflexr(
    "reflexr.evaluation.duration",
    "histogram",
    "s",
    "How long evaluating a workspace's new envelopes took.",
    buckets=DURATION_BUCKETS,
)
FIRINGS = _reflexr("reflexr.firings", "counter", "{firing}", "Times rules fired.", a.RULE)
RULE_ERRORS = _reflexr(
    "reflexr.rule.errors", "counter", "{error}", "Envelopes a rule could not evaluate.", a.RULE
)
RUNS = _reflexr(
    "reflexr.runs",
    "counter",
    "{run}",
    "Runs reaching a status, by rule, who moved them there, and why an attempt failed.",
    a.RULE,
    a.RUN_STATUS,
    a.ACTOR_KIND,
    a.RUN_REASON,
)
RUN_DURATION = _reflexr(
    "reflexr.run.duration",
    "histogram",
    "s",
    "How long run attempts took, by how they ended.",
    a.RULE,
    a.RUN_STATUS,
    buckets=RUN_BUCKETS,
)
RUN_ATTEMPTS = _reflexr(
    "reflexr.run.attempts", "counter", "{attempt}", "Run attempts started.", a.RULE
)
DEAD_LETTERS = _reflexr(
    "reflexr.dead_letters",
    "counter",
    "{run}",
    "Runs dead-lettered after exhausting their retries.",
    a.RULE,
)
SCHEDULE_TICKS = _reflexr(
    "reflexr.schedule.ticks", "counter", "{tick}", "Ticks schedules published.", a.SCHEDULE
)
FEEDBACK = _reflexr(
    "reflexr.feedback",
    "counter",
    "{feedback}",
    "Feedback given, by type, target kind and who gave it.",
    a.FEEDBACK_TYPE,
    a.FEEDBACK_TARGET,
    a.ACTOR_KIND,
)
STREAM_CONNECTIONS = _reflexr(
    "reflexr.stream.connections",
    "up_down_counter",
    "{connection}",
    "WebSocket connections open now.",
)
STREAM_DISCONNECTS = _reflexr(
    "reflexr.stream.disconnects",
    "counter",
    "{connection}",
    "WebSocket connections ended, by close code.",
    a.CLOSE_CODE,
)

METRICS: Mapping[str, Metric] = {
    metric.name: metric
    for metric in (
        EVENTS_PUBLISHED,
        EVALUATION_LAG,
        EVALUATION_DURATION,
        FIRINGS,
        RULE_ERRORS,
        RUNS,
        RUN_DURATION,
        RUN_ATTEMPTS,
        DEAD_LETTERS,
        SCHEDULE_TICKS,
        FEEDBACK,
        STREAM_CONNECTIONS,
        STREAM_DISCONNECTS,
    )
}
"""Every metric reflexr records, by name."""


def kept_attributes(metric: Metric, detail: MetricsDetail) -> frozenset[str]:
    """Return the attributes of ``metric`` that a deployment keeps at a level of detail."""
    dropped = {"workspace": frozenset[str](), "tenant": frozenset({a.WORKSPACE_ID}), "none": SCOPED}
    return metric.attributes - dropped[detail]
