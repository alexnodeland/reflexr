"""The conformance suite: JSON cases that specify how core evaluates rules.

Every case is checked twice: in one batch, and one envelope at a time with the state saved as
JSON between batches, as a storage host would. Both must decide the same firings, with the same
ids, which exercises :func:`needs` and the state's serialization as well as the rules.
"""

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

import tests.event_types  # noqa: F401  (registers the test event types)
from reflexr.core import (
    Causation,
    Envelope,
    Evaluation,
    Event,
    Rule,
    RuleProgress,
    ScopeState,
    SourceActor,
    begin,
    evaluate,
    load_event,
    needs,
)
from tests.event_types import ServiceError

CASES = Path(__file__).parent.parent / "conformance" / "cases"
BASE = datetime(2026, 1, 1, tzinfo=UTC)


def _explodes(event: Event) -> bool:
    raise ZeroDivisionError("division by zero")


def _even_severity(event: Event) -> bool:
    assert isinstance(event, ServiceError)
    return event.severity % 2 == 0


PREDICATES = {"even_severity": _even_severity, "explodes": _explodes}


def _cases() -> Iterator[Any]:
    for path in sorted(CASES.glob("*.json")):
        for case in json.loads(path.read_text())["cases"]:
            yield pytest.param(case, id=f"{path.stem}: {case['name']}")


def envelope(item: dict[str, Any]) -> Envelope:
    depth = item.get("depth", 0)
    return Envelope(
        seq=item["seq"],
        id=f"evt_{item['seq']}",
        ts=BASE + timedelta(seconds=item["at"]),
        workspace_id="w1",
        actor=SourceActor(name="test"),
        causation=Causation(firing_id="fir_x", run_id="fir_x", depth=depth) if depth else None,
        correlation_id=f"evt_{item['seq']}",
        event=load_event(item["event"]),
    )


@pytest.mark.parametrize("case", list(_cases()))
def test_conformance(case: dict[str, Any]) -> None:
    rule = Rule.model_validate({"name": "rule", "then": {"action": "act"}, **case["rule"]})
    envelopes = [envelope(item) for item in case["envelopes"]]
    progress = begin(rule, head_seq=0)
    whole = _in_one_batch(rule, progress, envelopes)
    firings, errors = _one_at_a_time(rule, progress, envelopes)
    assert [f.model_dump() for f in whole.firings] == firings, "batching changed the decisions"
    assert [e.model_dump() for e in whole.errors] == errors
    expect = case["expect"]
    assert len(whole.firings) == len(expect["firings"]), whole.firings
    for firing, expected in zip(whole.firings, expect["firings"], strict=True):
        assert (firing.seq, list(firing.matched)) == (expected["seq"], expected["matched"])
        if "scope" in expected:
            assert firing.scope == expected["scope"]
        if "depth" in expected:
            assert firing.depth == expected["depth"]
    assert [(e.seq, e.error) for e in whole.errors] == [
        (e["seq"], _containing(whole.errors, e)) for e in expect["errors"]
    ]


def _containing(errors: Any, expected: dict[str, Any]) -> str:
    [error] = [e.error for e in errors if e.seq == expected["seq"]]
    assert expected["contains"] in error, error
    return error


def _in_one_batch(rule: Rule, progress: RuleProgress, envelopes: list[Envelope]) -> Evaluation:
    assert needs(rule, progress, envelopes, predicates=PREDICATES) is not None
    return evaluate(rule, progress, {}, envelopes, predicates=PREDICATES)


def _one_at_a_time(
    rule: Rule, progress: RuleProgress, envelopes: list[Envelope]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    stored: dict[str, str] = {}
    firings: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for item in envelopes:
        keys = needs(rule, progress, [item], predicates=PREDICATES)
        loaded = {k: ScopeState.model_validate_json(stored[k]) for k in keys if k in stored}
        result = evaluate(rule, progress, loaded, [item], predicates=PREDICATES)
        stored.update({k: s.model_dump_json() for k, s in result.states.items()})
        progress = RuleProgress.model_validate_json(result.progress.model_dump_json())
        firings.extend(f.model_dump() for f in result.firings)
        errors.extend(e.model_dump() for e in result.errors)
    return firings, errors
