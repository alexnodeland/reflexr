"""The evaluator's contract with its host: starting, resetting, loading and ordering."""

from datetime import timedelta

import pytest

import tests.event_types  # noqa: F401  (registers the test event types)
from reflexr.core import (
    Envelope,
    F,
    NotLoaded,
    PredicateFilter,
    Rule,
    RuleProgress,
    begin,
    by,
    evaluate,
    needs,
    on,
    reset,
    run,
)
from tests.core.test_conformance import PREDICATES, envelope
from tests.event_types import Heartbeat, ServiceError

MINUTE = timedelta(minutes=1)


def quiet(start: str = "now") -> Rule:
    return Rule.model_validate(
        {
            "name": "quiet",
            "when": on(Heartbeat).absent(within=MINUTE),
            "scope": by(F.service),
            "then": run("page"),
            "start": start,
        }
    )


def hb(seq: int, at: int, service: str = "auth") -> Envelope:
    return envelope({"seq": seq, "at": at, "event": {"type": "heartbeat", "service": service}})


def test_a_rule_starts_at_the_head_or_the_beginning() -> None:
    assert begin(quiet(), head_seq=41).cursor == 41
    assert begin(quiet("beginning"), head_seq=41).cursor == 0
    assert begin(quiet(), head_seq=0).definition == quiet().definition()


def test_reset_starts_a_new_generation() -> None:
    progress = RuleProgress(cursor=9, generation=2, definition="old", deadlines={"[]": hb(1, 0).ts})
    new, event = reset(quiet(), progress, from_seq=3)
    assert (new.cursor, new.generation, new.definition, new.deadlines) == (
        3,
        3,
        quiet().definition(),
        {},
    )
    assert (event.reason, event.generation, event.from_seq) == ("changed", 3, 3)
    assert reset(quiet(), progress, from_seq=0, replayed=True)[1].reason == "replayed"


def test_a_changed_rule_must_be_reset_before_evaluating() -> None:
    with pytest.raises(ValueError, match="reset it before evaluating"):
        evaluate(quiet(), RuleProgress(definition="old"), {}, [hb(1, 0)])


def test_envelopes_must_come_after_the_cursor() -> None:
    progress = begin(quiet(), head_seq=5)
    with pytest.raises(ValueError, match="not after the cursor 5"):
        evaluate(quiet(), progress, {}, [hb(5, 0)])


def test_needs_lists_filtered_scopes_and_passing_deadlines() -> None:
    rule = quiet()
    progress = begin(rule, head_seq=0)
    assert needs(rule, progress, []) == frozenset()
    first = evaluate(rule, progress, {}, [hb(1, 0), hb(2, 0, "billing")])
    later = [envelope({"seq": 3, "at": 90, "event": {"type": "service.error", "service": "x"}})]
    assert needs(rule, first.progress, later) == {'["auth"]', '["billing"]'}
    early = [envelope({"seq": 3, "at": 30, "event": {"type": "service.error", "service": "x"}})]
    assert needs(rule, first.progress, early) == frozenset()


def test_needs_skips_what_evaluation_reports_as_errors() -> None:
    rule = Rule(
        name="odd",
        when=on(ServiceError).where(PredicateFilter(name="explodes")),
        scope=by(F.service),
        then=run("page"),
    )
    batch = [envelope({"seq": 1, "at": 0, "event": {"type": "service.error", "service": "a"}})]
    assert needs(rule, begin(rule, head_seq=0), batch, predicates=PREDICATES) == frozenset()


def test_needs_ignores_a_rules_facts_about_itself() -> None:
    rule = Rule(name="self", when=on("rule_fired"), then=run("page"))
    fired = envelope(
        {
            "seq": 1,
            "at": 0,
            "event": {
                "type": "rule_fired",
                "rule": "self",
                "scope": {},
                "scope_key": "[]",
                "firing_id": "f",
                "matched": [1],
            },
        }
    )
    assert needs(rule, begin(rule, head_seq=0), [fired]) == frozenset()


def test_a_scope_whose_deadline_passes_must_be_loaded() -> None:
    rule = quiet()
    first = evaluate(rule, begin(rule, head_seq=0), {}, [hb(1, 0)])
    with pytest.raises(NotLoaded, match=r'scope \["auth"\]'):
        evaluate(rule, first.progress, {}, [hb(2, 120, "billing")])
