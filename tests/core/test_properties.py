"""Properties that hold for any log: batching never changes decisions, and limits hold."""

from datetime import timedelta
from typing import Any

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests.event_types  # noqa: F401  (registers the test event types)
from reflexr.core import (
    Envelope,
    F,
    Rule,
    RuleProgress,
    ScopeState,
    begin,
    by,
    evaluate,
    needs,
    on,
    run,
    sequence,
)
from tests.core.test_conformance import envelope
from tests.event_types import Deploy, Heartbeat, ServiceError

MINUTE = timedelta(minutes=1)

RULES = [
    Rule(
        name="spike",
        when=on(ServiceError).where(F.severity >= 5).count(at_least=3, within=MINUTE),
        scope=by(F.service),
        then=run("act"),
    ),
    Rule(
        name="regression",
        when=sequence(on(Deploy), on(ServiceError).where(F.severity >= 7), within=5 * MINUTE),
        scope=by(F.service),
        then=run("act"),
    ),
    Rule(
        name="quiet",
        when=on(Heartbeat).absent(within=2 * MINUTE),
        scope=by(F.service),
        then=run("act"),
    ),
    Rule(
        name="deduped",
        when=on(ServiceError).distinct(F.fingerprint, within=MINUTE).at_most(2, per=3 * MINUTE),
        scope=by(F.service),
        then=run("act"),
    ),
]

SERVICES = st.sampled_from(["auth", "billing"])
EVENTS = st.one_of(
    st.builds(
        lambda s, sev, fp: {
            "type": "service.error",
            "service": s,
            "severity": sev,
            "fingerprint": fp,
        },
        SERVICES,
        st.integers(1, 10),
        st.sampled_from(["A", "B", "C"]),
    ),
    st.builds(lambda s: {"type": "deploy.finished", "service": s}, SERVICES),
    st.builds(lambda s: {"type": "heartbeat", "service": s}, SERVICES),
    st.just({"type": "tick", "schedule": "clock", "at": "2026-01-01T00:00:00Z"}),
)
LOGS = st.lists(st.tuples(st.integers(0, 90), EVENTS), min_size=1, max_size=40)


def _envelopes(log: list[tuple[int, dict[str, Any]]]) -> list[Envelope]:
    at = 0
    items: list[Envelope] = []
    for seq, (gap, event) in enumerate(log, start=1):
        at += gap
        items.append(envelope({"seq": seq, "at": at, "event": event}))
    return items


def _in_batches(rule: Rule, envelopes: list[Envelope], sizes: list[int]) -> list[Any]:
    progress: RuleProgress = begin(rule, head_seq=0)
    stored: dict[str, str] = {}
    decisions: list[Any] = []
    start = 0
    for size in [*sizes, len(envelopes)]:
        batch = envelopes[start : start + max(size, 1)]
        if not batch:
            break
        start += len(batch)
        keys = needs(rule, progress, batch)
        loaded = {k: ScopeState.model_validate_json(stored[k]) for k in keys if k in stored}
        result = evaluate(rule, progress, loaded, batch)
        stored.update({k: s.model_dump_json() for k, s in result.states.items()})
        progress = RuleProgress.model_validate_json(result.progress.model_dump_json())
        decisions.extend(f.model_dump() for f in result.firings)
        decisions.extend(e.model_dump() for e in result.errors)
    return decisions


@settings(max_examples=150, deadline=None)
@example(
    # A deadline that passes exactly at the last envelope of a batch.
    log=[
        (0, {"type": "heartbeat", "service": "auth"}),
        (120, {"type": "heartbeat", "service": "b"}),
    ],
    rule=RULES[2],
    sizes=[1],
)
@example(
    log=[
        (0, {"type": "heartbeat", "service": "auth"}),
        (120, {"type": "heartbeat", "service": "b"}),
    ],
    rule=RULES[2],
    sizes=[2],
)
@given(
    log=LOGS,
    rule=st.sampled_from(RULES),
    sizes=st.lists(st.integers(1, 10), max_size=10),
)
def test_batching_never_changes_decisions(
    log: list[tuple[int, dict[str, Any]]], rule: Rule, sizes: list[int]
) -> None:
    envelopes = _envelopes(log)
    assert _in_batches(rule, envelopes, sizes) == _in_batches(rule, envelopes, [len(envelopes)])


@settings(max_examples=150, deadline=None)
@given(log=LOGS)
def test_a_throttle_never_allows_more_than_its_limit(log: list[tuple[int, dict[str, Any]]]) -> None:
    rule = RULES[3]
    throttle = rule.when.throttle
    assert throttle is not None
    envelopes = _envelopes(log)
    result = evaluate(rule, begin(rule, head_seq=0), {}, envelopes)
    for firing in result.firings:
        window = [
            f
            for f in result.firings
            if f.scope_key == firing.scope_key and timedelta(0) <= firing.at - f.at < throttle.per
        ]
        assert len(window) <= throttle.at_most


@settings(max_examples=150, deadline=None)
@given(log=LOGS)
def test_a_count_fires_with_enough_envelopes_inside_its_window(
    log: list[tuple[int, dict[str, Any]]],
) -> None:
    rule = RULES[0]
    envelopes = {e.seq: e for e in _envelopes(log)}
    result = evaluate(rule, begin(rule, head_seq=0), {}, list(envelopes.values()))
    for firing in result.firings:
        times = [envelopes[seq].ts for seq in firing.matched]
        assert len(firing.matched) == 3
        assert max(times) - min(times) < MINUTE
