"""reflexr's Grafana dashboards, generated into deploy/grafana/dashboards/.

Run ``make dashboards`` after changing them. The dashboards read Prometheus (data source uid
``prometheus``, as stackr provisions it) with the names the OTLP translation gives reflexr's
metrics; ``tests/test_dashboards.py`` checks every query against the metric registry, and that
the checked-in files match this script.
"""

import json
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

OUT = Path(__file__).parent.parent / "deploy" / "grafana" / "dashboards"
PROMETHEUS = {"type": "prometheus", "uid": "prometheus"}
RATE = "$__rate_interval"
STEP = "1m"
"""Each query's min step: the OpenTelemetry SDK exports metrics every minute by default, and
Grafana makes ``$__rate_interval`` at least four steps, so every rate spans several exports."""
BRAND = "#B3122E"
"""reflexr's red, the overprint where the rule fires: the colour of single-number panels."""

# Selectors for the template variables each dashboard offers. `job` is the service's name and
# `deployment_environment_name` its environment, both from the resource, as stackr's Prometheus
# writes them.
JOB = 'job=~"$job", deployment_environment_name=~"$environment"'
TENANT = f'{JOB}, reflexr_tenant_id=~"$tenant"'
WORKSPACE = f'{TENANT}, reflexr_workspace_id=~"$workspace"'

# Run attempts that failed: the reactor moves the run to retrying, or dead-letters it.
FAILED = 'reflexr_run_status=~"retrying|dead", reflexr_actor_kind="system"'


def rate(metric: str, selector: str, *, by: str = "", extra: str = "") -> str:
    """``sum by (...) (rate(metric{selector}[...]))``."""
    matchers = ", ".join(part for part in (selector, extra) if part)
    grouping = f" by ({by})" if by else ""
    return f"sum{grouping} (rate({metric}{{{matchers}}}[{RATE}]))"


def quantile(q: float, histogram: str, selector: str, *, by: str = "") -> str:
    """A quantile of a histogram over the rate interval, optionally per label."""
    labels = f"le, {by}" if by else "le"
    return f"histogram_quantile({q}, {rate(f'{histogram}_bucket', selector, by=labels)})"


def ratio(numerator: str, denominator: str) -> str:
    """One rate over another, zero when there is nothing to divide by."""
    return f"({numerator}) / clamp_min({denominator}, 1e-9)"


def unit_range(unit: str) -> dict[str, Any]:
    """A unit, and for a share, the axis from 0 to 100%."""
    return {"unit": unit, "min": 0, "max": 1} if unit == "percentunit" else {"unit": unit}


def failed_share(selector: str, *, by: str = "") -> str:
    """The share of finished run attempts that failed.

    Both rates count attempts as they end (``reflexr.run.duration``, by how each ended), so the
    share stays within 0 and 1 however long attempts take.
    """
    ended = "reflexr_run_duration_seconds_count"
    failed = rate(ended, selector, by=by, extra='reflexr_run_status=~"retrying|dead"')
    return ratio(failed, rate(ended, selector, by=by))


def top(expr: str) -> str:
    """The ten largest series of an expression."""
    return f"topk(10, {expr})"


def per_minute(expr: str) -> str:
    """A per-second rate as a count per minute, for things that happen every few seconds."""
    return f"60 * {expr}"


class Layout:
    """Places panels on Grafana's 24-column grid, row by row."""

    def __init__(self) -> None:
        self.panels: list[dict[str, Any]] = []
        self._y = 0
        self._x = 0
        self._height = 0

    def row(self, title: str) -> None:
        """Start a row: a full-width title above the panels that follow."""
        self._newline()
        self.panels.append(
            {
                "type": "row",
                "title": title,
                "collapsed": False,
                "gridPos": {"x": 0, "y": self._y, "w": 24, "h": 1},
                "panels": [],
            }
        )
        self._y += 1

    def add(self, panel: dict[str, Any], *, width: int, height: int) -> None:
        """Place a panel after the previous one, wrapping to a new line when it is full."""
        if self._x + width > 24:
            self._newline()
        panel["gridPos"] = {"x": self._x, "y": self._y, "w": width, "h": height}
        self.panels.append(panel)
        self._x += width
        self._height = max(self._height, height)

    def _newline(self) -> None:
        self._y += self._height
        self._x = 0
        self._height = 0


def stat(layout: Layout, title: str, expr: str, *, unit: str, description: str) -> None:
    """Add a single-number panel, with its trend in reflexr's red."""
    layout.add(
        {
            "type": "stat",
            "title": title,
            "description": description,
            "datasource": PROMETHEUS,
            "targets": [{"refId": "A", "expr": expr, "interval": STEP, "datasource": PROMETHEUS}],
            "fieldConfig": {
                "defaults": {
                    **unit_range(unit),
                    "color": {"mode": "fixed", "fixedColor": BRAND},
                },
                "overrides": [],
            },
            "options": {
                "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                "colorMode": "none",
                "graphMode": "area",
            },
        },
        width=6,
        height=4,
    )


def series(
    layout: Layout,
    title: str,
    targets: list[tuple[str, str]],
    *,
    unit: str,
    description: str,
    width: int = 12,
    exemplars: bool = False,
) -> None:
    """Add a time series panel, one line per target."""
    layout.add(
        {
            "type": "timeseries",
            "title": title,
            "description": description,
            "datasource": PROMETHEUS,
            "targets": [
                {
                    "refId": chr(ord("A") + index),
                    "expr": expr,
                    "legendFormat": legend,
                    "interval": STEP,
                    "exemplar": exemplars,
                    "datasource": PROMETHEUS,
                }
                for index, (expr, legend) in enumerate(targets)
            ],
            "fieldConfig": {
                "defaults": {**unit_range(unit), "custom": {"fillOpacity": 10, "lineWidth": 1}},
                "overrides": [],
            },
            "options": {"legend": {"displayMode": "list", "placement": "bottom"}},
        },
        width=width,
        height=8,
    )


def variable(name: str, label: str, query: str, *, all_value: str | None = ".*") -> dict[str, Any]:
    """A template variable whose values come from a label.

    "All" matches any value, and series without the label, unless ``all_value`` is ``None``:
    then it matches the values found.
    """
    return {
        "name": name,
        "label": label,
        "type": "query",
        "datasource": PROMETHEUS,
        "query": {"query": query, "refId": name},
        "definition": query,
        "refresh": 2,
        "includeAll": True,
        "multi": True,
        "allValue": all_value,
        "sort": 1,
        "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
    }


def variables(scope: str) -> list[dict[str, Any]]:
    """The template variables down to ``scope``: job and environment, tenant, then workspace."""
    anchor = "reflexr_events_published_total"
    environments = f'label_values({anchor}{{job=~"$job"}}, deployment_environment_name)'
    found = [
        # "All" is every service reflexr's metrics come from, not every job in Prometheus:
        # pydantic-ai's metric names are shared, with stackr's LiteLLM proxy for one.
        variable("job", "Service", f"label_values({anchor}, job)", all_value=None),
        variable("environment", "Environment", environments),
    ]
    if scope in ("tenant", "workspace"):
        query = f"label_values({anchor}{{{JOB}}}, reflexr_tenant_id)"
        found.append(variable("tenant", "Tenant", query))
    if scope == "workspace":
        query = f"label_values({anchor}{{{TENANT}}}, reflexr_workspace_id)"
        found.append(variable("workspace", "Workspace", query))
    return found


def dashboard(uid: str, title: str, description: str, scope: str, layout: Layout) -> dict[str, Any]:
    """A dashboard around a layout's panels, with the variables down to ``scope``."""
    return {
        "uid": f"reflexr-{uid}",
        "title": f"reflexr / {title}",
        "description": description,
        "tags": ["reflexr"],
        "timezone": "browser",
        "editable": True,
        "graphTooltip": 1,
        "schemaVersion": 39,
        "version": 1,
        "refresh": "30s",
        "time": {"from": "now-6h", "to": "now"},
        "links": [
            {
                "title": "reflexr dashboards",
                "type": "dashboards",
                "tags": ["reflexr"],
                "asDropdown": True,
                "includeVars": True,
                "keepTime": True,
            }
        ],
        "templating": {"list": variables(scope)},
        "annotations": {"list": []},
        "panels": layout.panels,
    }


# ─── the dashboards ───────────────────────────────────────────────────────────


def overview() -> dict[str, Any]:
    """Every tenant at a glance."""
    layout = Layout()
    stat(
        layout,
        "Events",
        rate("reflexr_events_published_total", JOB, extra='reflexr_event_duplicate="false"'),
        unit="reqps",
        description="Events appended per second, every tenant. Duplicates appended nothing.",
    )
    stat(
        layout,
        "Firings",
        rate("reflexr_firings_total", JOB),
        unit="reqps",
        description="Times rules fired per second: each firing is a run.",
    )
    stat(
        layout,
        "Failed attempts",
        failed_share(JOB),
        unit="percentunit",
        description="The share of finished run attempts that failed, to be retried or "
        "dead-lettered.",
    )
    stat(
        layout,
        "Worst lag",
        f"max(reflexr_evaluation_lag{{{JOB}}})",
        unit="short",
        description="The most envelopes any rule was behind the log when an evaluation began.",
    )
    layout.row("Activity")
    series(
        layout,
        "Events by tenant",
        [
            (
                top(rate("reflexr_events_published_total", JOB, by="reflexr_tenant_id")),
                "{{reflexr_tenant_id}}",
            )
        ],
        unit="reqps",
        description="The ten busiest tenants.",
    )
    series(
        layout,
        "Runs by status",
        [
            (
                rate("reflexr_runs_total", JOB, by="reflexr_run_status"),
                "{{reflexr_run_status}}",
            )
        ],
        unit="reqps",
        description="Runs reaching each status: pending when a rule fires, then running, and "
        "succeeded, retrying, dead, skipped or cancelled.",
    )
    series(
        layout,
        "Run attempt duration",
        [
            (quantile(0.5, "reflexr_run_duration_seconds", JOB), "p50"),
            (quantile(0.95, "reflexr_run_duration_seconds", JOB), "p95"),
        ],
        unit="s",
        description="How long run attempts took. Exemplars link to the attempt's trace.",
        exemplars=True,
    )
    series(
        layout,
        "Dead letters by rule",
        [
            (
                rate("reflexr_dead_letters_total", JOB, by="reflexr_rule"),
                "{{reflexr_rule}}",
            )
        ],
        unit="reqps",
        description="Runs dead-lettered after their retries, or at once for a permanent failure.",
    )
    return dashboard(
        "overview",
        "Overview",
        "Every tenant: events, firings, runs, failures and lag.",
        "job",
        layout,
    )


def tenant() -> dict[str, Any]:
    """One tenant's workspaces."""
    layout = Layout()
    stat(
        layout,
        "Events",
        rate("reflexr_events_published_total", TENANT, extra='reflexr_event_duplicate="false"'),
        unit="reqps",
        description="Events appended per second.",
    )
    stat(
        layout,
        "Firings",
        rate("reflexr_firings_total", TENANT),
        unit="reqps",
        description="Times rules fired per second.",
    )
    stat(
        layout,
        "Dead letters",
        rate("reflexr_dead_letters_total", TENANT),
        unit="reqps",
        description="Runs dead-lettered per second.",
    )
    stat(
        layout,
        "Feedback",
        rate("reflexr_feedback_total", TENANT),
        unit="reqps",
        description="Feedback given per second, by people and evaluators.",
    )
    layout.row("Workspaces")
    series(
        layout,
        "Events by workspace",
        [
            (
                top(rate("reflexr_events_published_total", TENANT, by="reflexr_workspace_id")),
                "{{reflexr_workspace_id}}",
            )
        ],
        unit="reqps",
        description="The ten busiest workspaces.",
    )
    series(
        layout,
        "Firings by workspace",
        [
            (
                top(rate("reflexr_firings_total", TENANT, by="reflexr_workspace_id")),
                "{{reflexr_workspace_id}}",
            )
        ],
        unit="reqps",
        description="The ten workspaces whose rules fire most.",
    )
    series(
        layout,
        "Dead letters by workspace",
        [
            (
                rate("reflexr_dead_letters_total", TENANT, by="reflexr_workspace_id"),
                "{{reflexr_workspace_id}}",
            )
        ],
        unit="reqps",
        description="Where runs are dead-lettered, waiting for someone to retry or skip them.",
    )
    series(
        layout,
        "Run attempt duration",
        [(quantile(0.95, "reflexr_run_duration_seconds", TENANT), "p95")],
        unit="s",
        description="The 95th percentile of run attempt duration.",
        exemplars=True,
    )
    return dashboard(
        "tenant", "Tenant", "One tenant's workspaces: events, firings and runs.", "tenant", layout
    )


def workspace() -> dict[str, Any]:
    """One workspace's activity."""
    layout = Layout()
    series(
        layout,
        "Events by type",
        [
            (
                rate(
                    "reflexr_events_published_total",
                    WORKSPACE,
                    by="reflexr_event_type",
                    extra='reflexr_event_duplicate="false"',
                ),
                "{{reflexr_event_type}}",
            )
        ],
        unit="reqps",
        description="Events appended per second, by type.",
    )
    series(
        layout,
        "Events by kind of actor",
        [
            (
                rate("reflexr_events_published_total", WORKSPACE, by="reflexr_actor_kind"),
                "{{reflexr_actor_kind}}",
            )
        ],
        unit="reqps",
        description="Who publishes: people, sources, runs (agent), external agents over MCP.",
    )
    series(
        layout,
        "Duplicates",
        [
            (
                rate(
                    "reflexr_events_published_total",
                    WORKSPACE,
                    by="reflexr_event_type",
                    extra='reflexr_event_duplicate="true"',
                ),
                "{{reflexr_event_type}}",
            )
        ],
        unit="reqps",
        description="Publishes of an event id already in the log, such as retried deliveries. "
        "They appended nothing.",
    )
    series(
        layout,
        "Firings by rule",
        [(rate("reflexr_firings_total", WORKSPACE, by="reflexr_rule"), "{{reflexr_rule}}")],
        unit="reqps",
        description="Times each rule fired.",
    )
    series(
        layout,
        "Runs by rule and status",
        [
            (
                rate("reflexr_runs_total", WORKSPACE, by="reflexr_rule, reflexr_run_status"),
                "{{reflexr_rule}} {{reflexr_run_status}}",
            )
        ],
        unit="reqps",
        description="Runs reaching each status, by rule.",
    )
    series(
        layout,
        "Feedback",
        [
            (
                rate(
                    "reflexr_feedback_total",
                    WORKSPACE,
                    by="reflexr_feedback_type, reflexr_actor_kind",
                ),
                "{{reflexr_feedback_type}} by {{reflexr_actor_kind}}",
            )
        ],
        unit="reqps",
        description="Feedback given, by type and by people or evaluators.",
    )
    return dashboard(
        "workspace",
        "Workspace",
        "One workspace: events, firings, runs and feedback.",
        "workspace",
        layout,
    )


def rules() -> dict[str, Any]:
    """Lag, firings and errors."""
    layout = Layout()
    stat(
        layout,
        "Worst lag",
        f"max(reflexr_evaluation_lag{{{WORKSPACE}}})",
        unit="short",
        description="The most envelopes any rule was behind the log when an evaluation began.",
    )
    stat(
        layout,
        "Evaluation p95",
        quantile(0.95, "reflexr_evaluation_duration_seconds", WORKSPACE),
        unit="s",
        description="The 95th percentile of evaluating a workspace's new envelopes.",
    )
    stat(
        layout,
        "Firings",
        rate("reflexr_firings_total", WORKSPACE),
        unit="reqps",
        description="Times rules fired per second.",
    )
    stat(
        layout,
        "Rule errors",
        rate("reflexr_rule_errors_total", WORKSPACE),
        unit="reqps",
        description="Envelopes a rule could not evaluate, per second.",
    )
    layout.row("Rules")
    series(
        layout,
        "Lag by rule",
        [
            (
                f"max by (reflexr_rule) (reflexr_evaluation_lag{{{WORKSPACE}}})",
                "{{reflexr_rule}}",
            )
        ],
        unit="short",
        description="Envelopes each rule was behind the head of the log when an evaluation "
        "began: the first thing to watch when rules fall behind.",
    )
    series(
        layout,
        "Evaluation duration",
        [
            (quantile(0.5, "reflexr_evaluation_duration_seconds", WORKSPACE), "p50"),
            (quantile(0.95, "reflexr_evaluation_duration_seconds", WORKSPACE), "p95"),
            (quantile(0.99, "reflexr_evaluation_duration_seconds", WORKSPACE), "p99"),
        ],
        unit="s",
        description="Evaluating a workspace's new envelopes, every rule included. Exemplars "
        "link to the evaluation's trace.",
        exemplars=True,
    )
    series(
        layout,
        "Firings by rule",
        [(rate("reflexr_firings_total", WORKSPACE, by="reflexr_rule"), "{{reflexr_rule}}")],
        unit="reqps",
        description="Times each rule fired.",
    )
    series(
        layout,
        "Errors by rule",
        [(rate("reflexr_rule_errors_total", WORKSPACE, by="reflexr_rule"), "{{reflexr_rule}}")],
        unit="reqps",
        description="Envelopes each rule could not evaluate, such as a predicate that raised. "
        "They are dead-lettered.",
    )
    return dashboard(
        "rules",
        "Rules",
        "Rules: how far behind the log they are, how often they fire, and their errors.",
        "workspace",
        layout,
    )


def runs() -> dict[str, Any]:
    """Outcomes, durations, retries and dead letters."""
    layout = Layout()
    stat(
        layout,
        "Attempts",
        rate("reflexr_run_attempts_total", TENANT),
        unit="reqps",
        description="Run attempts started per second.",
    )
    stat(
        layout,
        "Failed attempts",
        failed_share(TENANT),
        unit="percentunit",
        description="The share of finished run attempts that failed, to be retried or "
        "dead-lettered.",
    )
    stat(
        layout,
        "Dead letters",
        rate("reflexr_dead_letters_total", TENANT),
        unit="reqps",
        description="Runs dead-lettered per second.",
    )
    stat(
        layout,
        "Attempt p95",
        quantile(0.95, "reflexr_run_duration_seconds", TENANT),
        unit="s",
        description="The 95th percentile of run attempt duration.",
    )
    layout.row("Outcomes")
    series(
        layout,
        "Runs by status",
        [
            (
                rate("reflexr_runs_total", TENANT, by="reflexr_run_status"),
                "{{reflexr_run_status}}",
            )
        ],
        unit="reqps",
        description="Runs reaching each status, whoever moved them there.",
    )
    series(
        layout,
        "Failures by reason",
        [
            (
                rate(
                    "reflexr_runs_total",
                    TENANT,
                    by="reflexr_run_reason",
                    extra=f'{FAILED}, reflexr_run_reason!=""',
                ),
                "{{reflexr_run_reason}}",
            )
        ],
        unit="reqps",
        description="Attempts that failed with a typed reason: timeout, abandoned, "
        "guardrail_blocked, or an action's own code.",
    )
    series(
        layout,
        "Retries by rule",
        [
            (
                rate(
                    "reflexr_runs_total",
                    TENANT,
                    by="reflexr_rule",
                    extra='reflexr_run_status="retrying", reflexr_actor_kind="system"',
                ),
                "{{reflexr_rule}}",
            )
        ],
        unit="reqps",
        description="Failed attempts the rule's retry policy will try again.",
    )
    series(
        layout,
        "Dead letters by rule",
        [(rate("reflexr_dead_letters_total", TENANT, by="reflexr_rule"), "{{reflexr_rule}}")],
        unit="reqps",
        description="Runs dead-lettered after their retries, or at once for a permanent failure.",
    )
    layout.row("Durations")
    series(
        layout,
        "Attempt duration by rule",
        [
            (
                quantile(0.95, "reflexr_run_duration_seconds", TENANT, by="reflexr_rule"),
                "{{reflexr_rule}} p95",
            )
        ],
        unit="s",
        description="The 95th percentile, by rule. Exemplars link to the attempt's trace.",
        exemplars=True,
    )
    series(
        layout,
        "Attempt duration by outcome",
        [
            (
                quantile(0.95, "reflexr_run_duration_seconds", TENANT, by="reflexr_run_status"),
                "{{reflexr_run_status}} p95",
            )
        ],
        unit="s",
        description="The 95th percentile, by how the attempt ended: slow failures are often "
        "timeouts.",
        exemplars=True,
    )
    layout.row("Operators")
    series(
        layout,
        "Operator actions",
        [
            (
                rate(
                    "reflexr_runs_total",
                    TENANT,
                    by="reflexr_run_status, reflexr_actor_kind",
                    extra='reflexr_actor_kind!="system"',
                ),
                "{{reflexr_run_status}} by {{reflexr_actor_kind}}",
            )
        ],
        unit="reqps",
        description="Runs people and external agents retried (pending), skipped or cancelled.",
    )
    series(
        layout,
        "Attempts by rule",
        [
            (
                rate("reflexr_run_attempts_total", TENANT, by="reflexr_rule"),
                "{{reflexr_rule}}",
            )
        ],
        unit="reqps",
        description="Run attempts started, by rule: first attempts and retries.",
    )
    return dashboard(
        "runs",
        "Runs",
        "Runs: outcomes, attempt durations, retries, dead letters and why attempts fail.",
        "tenant",
        layout,
    )


def agent() -> dict[str, Any]:
    """Models, and the runs they serve."""
    layout = Layout()
    layout.row("Models")
    series(
        layout,
        "Tokens by model",
        [
            (
                rate(
                    "gen_ai_client_token_usage_sum",
                    JOB,
                    by="gen_ai_request_model, gen_ai_token_type",
                ),
                "{{gen_ai_request_model}} {{gen_ai_token_type}}",
            )
        ],
        unit="short",
        description="Tokens per second, from pydantic-ai's gen_ai.client.token.usage. "
        "pydantic-ai's metrics carry the model, not the tenant.",
    )
    series(
        layout,
        "Cost by model",
        [(rate("operation_cost_sum", JOB, by="gen_ai_request_model"), "{{gen_ai_request_model}}")],
        unit="currencyUSD",
        description="Estimated spend per second, from pydantic-ai's operation.cost.",
    )
    series(
        layout,
        "Model requests",
        [
            (
                rate(
                    "gen_ai_client_token_usage_count",
                    JOB,
                    by="gen_ai_request_model",
                    extra='gen_ai_token_type="input"',
                ),
                "{{gen_ai_request_model}}",
            )
        ],
        unit="reqps",
        description="Model requests per second, counted by their input tokens.",
    )
    series(
        layout,
        "Tokens per request",
        [
            (
                quantile(
                    0.95,
                    "gen_ai_client_token_usage",
                    JOB,
                    by="gen_ai_request_model, gen_ai_token_type",
                ),
                "{{gen_ai_request_model}} {{gen_ai_token_type}} p95",
            )
        ],
        unit="short",
        description="The 95th percentile of one request's tokens: growing contexts show here.",
    )
    layout.row("Runs and the gateway")
    series(
        layout,
        "Guardrail blocks by rule",
        [
            (
                rate(
                    "reflexr_runs_total",
                    TENANT,
                    by="reflexr_rule",
                    extra=f'{FAILED}, reflexr_run_reason="guardrail_blocked"',
                ),
                "{{reflexr_rule}}",
            )
        ],
        unit="reqps",
        description="Runs the LLM gateway's guardrails blocked: permanent failures, "
        "dead-lettered without a retry.",
    )
    series(
        layout,
        "Failed attempts by rule",
        [
            (
                failed_share(TENANT, by="reflexr_rule"),
                "{{reflexr_rule}}",
            )
        ],
        unit="percentunit",
        description="The share of each rule's finished attempts that failed: an agent's model "
        "or tools failing, or a guardrail block.",
    )
    series(
        layout,
        "Attempt duration by rule",
        [
            (
                quantile(0.95, "reflexr_run_duration_seconds", TENANT, by="reflexr_rule"),
                "{{reflexr_rule}} p95",
            )
        ],
        unit="s",
        description="The 95th percentile, by rule: an agent's attempt includes every model "
        "request it makes. Exemplars link to the trace.",
        exemplars=True,
        width=24,
    )
    return dashboard(
        "agent",
        "Agent and LLM",
        "Model usage, cost and request sizes; guardrail blocks and failing rules.",
        "tenant",
        layout,
    )


def schedules() -> dict[str, Any]:
    """Ticks, by schedule and workspace."""
    ticks = "reflexr_schedule_ticks_total"
    layout = Layout()
    stat(
        layout,
        "Ticks per minute",
        per_minute(rate(ticks, TENANT)),
        unit="short",
        description="Ticks every schedule published, per minute.",
    )
    stat(
        layout,
        "Ticks in the range",
        f"sum(increase({ticks}{{{TENANT}}}[$__range]))",
        unit="short",
        description="Ticks published over the dashboard's time range.",
    )
    stat(
        layout,
        "Schedules ticking",
        f"count({rate(ticks, TENANT, by='reflexr_schedule')} > 0)",
        unit="short",
        description="Schedules that published a tick in the rate interval.",
    )
    stat(
        layout,
        "Workspaces ticking",
        f"count({rate(ticks, TENANT, by='reflexr_workspace_id')} > 0)",
        unit="short",
        description="Workspaces that received a tick in the rate interval.",
    )
    layout.row("Ticks")
    series(
        layout,
        "Ticks by schedule",
        [(per_minute(rate(ticks, TENANT, by="reflexr_schedule")), "{{reflexr_schedule}}")],
        unit="short",
        description="Ticks per minute, by schedule. Ticks move rules' clocks forward, so "
        "absence rules fire on time in quiet workspaces.",
    )
    series(
        layout,
        "Ticks by workspace",
        [
            (
                top(per_minute(rate(ticks, TENANT, by="reflexr_workspace_id"))),
                "{{reflexr_workspace_id}}",
            )
        ],
        unit="short",
        description="Ticks per minute, for the ten workspaces receiving the most.",
    )
    return dashboard(
        "schedules",
        "Schedules",
        "Schedules: the ticks they publish, by schedule and workspace.",
        "tenant",
        layout,
    )


def stream() -> dict[str, Any]:
    """WebSocket connections and how they end."""
    connections = "reflexr_stream_connections"
    disconnects = "reflexr_stream_disconnects_total"
    layout = Layout()
    stat(
        layout,
        "Open connections",
        f"sum({connections}{{{TENANT}}})",
        unit="short",
        description="WebSocket stream connections open now.",
    )
    stat(
        layout,
        "Disconnects",
        rate(disconnects, TENANT),
        unit="reqps",
        description="Connections ending per second, for any reason.",
    )
    stat(
        layout,
        "Refused",
        rate(disconnects, TENANT, extra='reflexr_stream_close_code=~"4400|4401|4403|4408"'),
        unit="reqps",
        description="Connections closed for a bad hello, authentication or access, or a hello "
        "that never came.",
    )
    stat(
        layout,
        "Too slow",
        rate(disconnects, TENANT, extra='reflexr_stream_close_code="4429"'),
        unit="reqps",
        description="Clients disconnected for not reading frames fast enough.",
    )
    layout.row("Connections")
    series(
        layout,
        "Open connections by tenant",
        [
            (
                f"sum by (reflexr_tenant_id) ({connections}{{{TENANT}}})",
                "{{reflexr_tenant_id}}",
            )
        ],
        unit="short",
        description="Stream connections open now, by tenant.",
    )
    series(
        layout,
        "Open connections by workspace",
        [
            (
                top(f"sum by (reflexr_workspace_id) ({connections}{{{TENANT}}})"),
                "{{reflexr_workspace_id}}",
            )
        ],
        unit="short",
        description="The ten workspaces with the most connections open now.",
    )
    series(
        layout,
        "Disconnects by close code",
        [
            (
                rate(disconnects, TENANT, by="reflexr_stream_close_code"),
                "{{reflexr_stream_close_code}}",
            )
        ],
        unit="reqps",
        description="1000 is normal; 4400, 4401, 4403 and 4408 refusals; 4429 a client too slow "
        "to keep up; 1011 a server error.",
    )
    series(
        layout,
        "Slow clients by workspace",
        [
            (
                rate(
                    disconnects,
                    TENANT,
                    by="reflexr_workspace_id",
                    extra='reflexr_stream_close_code="4429"',
                ),
                "{{reflexr_workspace_id}}",
            )
        ],
        unit="reqps",
        description="Where clients fall behind the log: a busy workspace, or a slow consumer.",
    )
    return dashboard(
        "stream",
        "Stream",
        "The WebSocket stream: open connections, and how and why they end.",
        "tenant",
        layout,
    )


DASHBOARDS = {
    "overview": overview,
    "tenant": tenant,
    "workspace": workspace,
    "rules": rules,
    "runs": runs,
    "agent": agent,
    "schedules": schedules,
    "stream": stream,
}


def rendered() -> Iterator[tuple[str, str]]:
    """Every dashboard's file name and JSON text."""
    for name, build in DASHBOARDS.items():
        yield f"reflexr-{name}.json", json.dumps(build(), indent=2) + "\n"


def main() -> None:
    """Write every dashboard."""
    OUT.mkdir(parents=True, exist_ok=True)
    for name, text in rendered():
        (OUT / name).write_text(text)
    sys.stdout.write(f"wrote {len(DASHBOARDS)} dashboards to {OUT}\n")


if __name__ == "__main__":
    main()
