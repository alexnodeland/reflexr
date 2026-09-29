"""The Grafana dashboards cannot drift from the metric registry, or from their generator."""

import importlib.util
import json
import re
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from reflexr.telemetry import EXTERNAL_METRICS, METRICS, Metric, kept_attributes

ROOT = Path(__file__).parent.parent
DASHBOARDS = ROOT / "deploy" / "grafana" / "dashboards"

RESOURCE_LABELS = {"job", "instance", "le", "deployment_environment_name", "service_version"}
"""Labels every series has: from the resource (stackr's Prometheus promotes the last two)."""

KEYWORDS = {"by", "without", "on", "ignoring", "group_left", "group_right", "bool", "and", "or"}
KEYWORDS |= {"unless", "offset", "inf", "nan"}

LEGEND = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")
"""A label a legend shows, such as ``{{reflexr_rule}}``."""

SERIES: dict[str, Metric] = {
    series: metric
    for metric in (*METRICS.values(), *EXTERNAL_METRICS.values())
    for series in metric.prometheus_series
}
"""Every Prometheus series name a dashboard may use, and the metric it belongs to."""


def _generator() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "grafana_dashboards", ROOT / "scripts" / "grafana_dashboards.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _dashboards() -> dict[str, dict[str, Any]]:
    return {path.name: json.loads(path.read_text()) for path in sorted(DASHBOARDS.glob("*.json"))}


def _panels(panels: list[dict[str, Any]]) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for panel in panels:
        found.append(panel)
        found.extend(_panels(panel.get("panels", [])))
    return found


def _targets(dashboard: dict[str, Any]) -> list[dict[str, Any]]:
    return [target for panel in _panels(dashboard["panels"]) for target in panel.get("targets", [])]


def _queries(dashboard: dict[str, Any]) -> list[str]:
    queries = [target["expr"] for target in _targets(dashboard)]
    for variable in dashboard["templating"]["list"]:
        selector, _ = _label_values(variable["query"]["query"])
        queries.append(selector)
    return queries


def _label_values(query: str) -> tuple[str, str]:
    match = re.fullmatch(r"label_values\((.*),\s*([a-zA-Z_][a-zA-Z0-9_]*)\)", query)
    assert match, f"not a label_values query: {query}"
    return match.group(1), match.group(2)


def metric_names(expr: str) -> set[str]:
    """The series a PromQL expression reads."""
    text = re.sub(r'"(?:[^"\\]|\\.)*"', '""', expr)
    text = re.sub(r"\{[^}]*\}", "", text)
    text = re.sub(r"\[[^\]]*\]", "", text)
    text = re.sub(r"\b(by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)", "", text)
    names: set[str] = set()
    for match in re.finditer(r"[a-zA-Z_:][a-zA-Z0-9_:]*", text):
        before = text[match.start() - 1] if match.start() else " "
        after = text[match.end() :].lstrip()
        if before.isdigit() or before == "." or after.startswith("("):
            continue  # a number's exponent, or a function
        if match.group() not in KEYWORDS:
            names.add(match.group())
    return names


def label_names(expr: str) -> set[str]:
    """The labels a PromQL expression matches on or groups by."""
    labels: set[str] = set()
    for matchers in re.findall(r"\{([^}]*)\}", expr):
        labels |= set(re.findall(r"([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=~|!~|!=|=)", matchers))
    for grouping in re.findall(r"\b(?:by|without)\s*\(([^)]*)\)", expr):
        labels |= {label.strip() for label in grouping.split(",") if label.strip()}
    return labels


def _prometheus(attribute: str) -> str:
    return attribute.replace(".", "_")


def problems(query: str) -> list[str]:
    """What is wrong with a query: metrics outside the registry, or labels a metric lacks.

    Labels are checked for reflexr's own metrics: an attribute the registry declares and a
    deployment keeps at the most detail, or a resource label.
    """
    names = metric_names(query)
    if not names:
        return [f"no metric in {query}"]
    unknown = names - set(SERIES)
    if unknown:
        return [f"{sorted(unknown)} in {query} are not in the registry"]
    metrics = {SERIES[series] for series in names}
    if any(metric.name not in METRICS for metric in metrics):
        return []
    allowed = RESOURCE_LABELS | {
        _prometheus(attribute)
        for metric in metrics
        for attribute in kept_attributes(metric, "workspace")
    }
    extra = label_names(query) - allowed
    return [f"{sorted(extra)} are not attributes of {query}"] if extra else []


def test_there_is_a_dashboard_for_each_view() -> None:
    assert set(_dashboards()) == {
        "reflexr-overview.json",
        "reflexr-tenant.json",
        "reflexr-workspace.json",
        "reflexr-rules.json",
        "reflexr-runs.json",
        "reflexr-agent.json",
        "reflexr-schedules.json",
        "reflexr-stream.json",
    }


@pytest.mark.parametrize("name", sorted(_dashboards()))
def test_every_query_reads_a_metric_in_the_registry(name: str) -> None:
    for query in _queries(_dashboards()[name]):
        assert problems(query) == [], name


@pytest.mark.parametrize("name", sorted(_dashboards()))
def test_legends_name_labels_their_query_keeps(name: str) -> None:
    for target in _targets(_dashboards()[name]):
        legend = set(LEGEND.findall(target.get("legendFormat", "")))
        assert legend <= label_names(target["expr"]), (name, target["expr"])


def test_drift_is_caught() -> None:
    assert problems("sum(rate(reflexr_firings_total[5m]))") == []
    assert problems("sum(rate(reflexr_fires_total[5m]))") == [
        "['reflexr_fires_total'] in sum(rate(reflexr_fires_total[5m])) are not in the registry"
    ]
    by_run = "sum by (reflexr_run_id) (rate(reflexr_runs_total[5m]))"
    assert problems(by_run) == [f"['reflexr_run_id'] are not attributes of {by_run}"]
    assert problems('sum by (gen_ai_request_model) (rate(operation_cost_sum{x="y"}[5m]))') == []
    assert problems("vector(1)") == ["no metric in vector(1)"]


@pytest.mark.parametrize("name", sorted(_dashboards()))
def test_dashboards_read_stackrs_prometheus(name: str) -> None:
    dashboard = _dashboards()[name]
    assert dashboard["uid"] == name.removesuffix(".json")
    assert "reflexr" in dashboard["tags"]
    names = [variable["name"] for variable in dashboard["templating"]["list"]]
    assert names[:2] == ["job", "environment"], "every dashboard filters by service and environment"
    job = dashboard["templating"]["list"][0]
    assert job["allValue"] is None, "All is reflexr's services, not every job in Prometheus"
    for panel in _panels(dashboard["panels"]):
        if panel["type"] != "row":
            assert panel["datasource"] == {"type": "prometheus", "uid": "prometheus"}
            assert panel["description"], f"{name}: {panel['title']} has no description"
    for target in _targets(dashboard):
        assert target["interval"] == "1m", "rates span several of the SDK's minutely exports"


def test_the_checked_in_dashboards_are_generated() -> None:
    generated = dict(_generator().rendered())
    assert generated == {path.name: path.read_text() for path in DASHBOARDS.glob("*.json")}, (
        "run `make dashboards`"
    )


def test_the_query_parser() -> None:
    expr = (
        "histogram_quantile(0.95, sum by (le, x) "
        '(rate(a_bucket{j=~"$j", y="1"}[$__rate_interval])))'
        " / clamp_min(b_total offset 5m, 1e-9)"
    )
    assert metric_names(expr) == {"a_bucket", "b_total"}
    assert label_names(expr) == {"le", "x", "j", "y"}
    assert metric_names("60 * count(sum by (s) (rate(c_total[$__range])) > 0)") == {"c_total"}
    assert _label_values('label_values(m{job=~"$job"}, t)') == ('m{job=~"$job"}', "t")
