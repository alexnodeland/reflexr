"""The metric registry: every metric reflexr records, declared in one place.

Dashboards are tested against this registry, so a metric cannot be renamed without the
dashboards noticing. The registry also enforces the cardinality policy: run, firing, event,
chain and scope values are never attributes, and only the attributes a metric declares may be
recorded on it. Tenant and workspace attributes are added according to ``metrics_detail``.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from reflexr.telemetry import attributes as a

type Instrument = Literal["counter", "histogram", "gauge"]
"""The kind of OpenTelemetry instrument a metric is recorded with."""

type MetricsDetail = Literal["workspace", "tenant", "none"]
"""Which of tenant and workspace become metric attributes.

``"workspace"`` (the default) records both; ``"tenant"`` records the tenant only, for
deployments with many workspaces; ``"none"`` records neither.
"""

SCOPE_ATTRIBUTES = frozenset({a.TENANT_ID, a.WORKSPACE_ID})
"""Attributes every metric may carry, subject to ``metrics_detail``."""


@dataclass(frozen=True)
class Metric:
    """A metric's declaration."""

    name: str
    instrument: Instrument
    unit: str
    description: str
    attributes: frozenset[str] = frozenset()
    """The attributes it may be recorded with, besides tenant and workspace."""


EVENTS_PUBLISHED = Metric(
    "reflexr.events.published",
    "counter",
    "{event}",
    "Events published into workspaces, by type. Duplicates appended nothing.",
    frozenset({a.EVENT_TYPE, a.ACTOR_KIND, a.DUPLICATE}),
)
EVALUATION_LAG = Metric(
    "reflexr.evaluation.lag",
    "gauge",
    "{envelope}",
    "How many envelopes a rule is behind the head of the log.",
    frozenset({a.RULE}),
)
EVALUATION_DURATION = Metric(
    "reflexr.evaluation.duration",
    "histogram",
    "s",
    "How long evaluating a batch of the log took.",
)
FIRINGS = Metric(
    "reflexr.firings",
    "counter",
    "{firing}",
    "Times rules fired.",
    frozenset({a.RULE}),
)
RULE_ERRORS = Metric(
    "reflexr.rule.errors",
    "counter",
    "{error}",
    "Envelopes a rule could not evaluate.",
    frozenset({a.RULE}),
)
RUNS = Metric(
    "reflexr.runs",
    "counter",
    "{run}",
    "Runs reaching a status.",
    frozenset({a.RULE, a.RUN_STATUS, a.ACTOR_KIND}),
)
RUN_DURATION = Metric(
    "reflexr.run.duration",
    "histogram",
    "s",
    "How long run attempts took, by how they ended.",
    frozenset({a.RULE, a.RUN_STATUS}),
)
RUN_ATTEMPTS = Metric(
    "reflexr.run.attempts",
    "counter",
    "{attempt}",
    "Run attempts started.",
    frozenset({a.RULE}),
)
DEAD_LETTERS = Metric(
    "reflexr.dead_letters",
    "counter",
    "{run}",
    "Runs dead-lettered after exhausting their retries.",
    frozenset({a.RULE}),
)
SCHEDULE_TICKS = Metric(
    "reflexr.schedule.ticks",
    "counter",
    "{tick}",
    "Ticks schedules published.",
    frozenset({a.SCHEDULE}),
)
FEEDBACK = Metric(
    "reflexr.feedback",
    "counter",
    "{feedback}",
    "Feedback given, by type, target kind and who gave it.",
    frozenset({a.FEEDBACK_TYPE, a.FEEDBACK_TARGET, a.ACTOR_KIND}),
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
    )
}
"""Every metric reflexr records, by name."""
