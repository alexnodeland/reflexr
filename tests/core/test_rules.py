"""Rules: scopes, actions, retries, definitions and checks."""

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from reflexr.core import (
    ActionRef,
    AllFilter,
    AnyFilter,
    Condition,
    F,
    Filter,
    InvalidRule,
    NotFilter,
    OnFilter,
    PredicateFilter,
    RetryPolicy,
    Rule,
    Scope,
    SequencePattern,
    by,
    event_types,
    on,
    run,
    scope_key,
    sequence,
)
from tests.event_types import Deploy, Heartbeat, ServiceError

MINUTE = timedelta(minutes=1)
NOW = datetime(2026, 1, 1, tzinfo=UTC)


class Triage:
    name = "triage"


def spike(**changes: object) -> Rule:
    fields: dict[str, object] = {
        "name": "error-spike",
        "when": on(ServiceError).where(F.severity >= 7).count(at_least=3, within=MINUTE),
        "scope": by(F.service),
        "then": run(Triage()),
    }
    return Rule.model_validate({**fields, **changes})


def test_scopes_extract_keys_and_values() -> None:
    assert by(F.service, "region") == Scope(fields=("service", "region"))
    assert by(F.service).of({"service": "auth"}) == ('["auth"]', {"service": "auth"})
    assert by(F.service).of({}) is None
    assert Scope().of({"service": "auth"}) == ("[]", {})
    assert scope_key([{"b": 1, "a": 2}]) == '[{"a":2,"b":1}]'


def test_actions_are_referred_to_by_name() -> None:
    assert run("page") == ActionRef(action="page")
    assert run(Triage()) == ActionRef(action="triage")
    assert spike().then.action == "triage"


def test_retry_policies_back_off_exponentially() -> None:
    policy = RetryPolicy(
        max_attempts=4, backoff=timedelta(seconds=2), max_backoff=timedelta(seconds=5)
    )
    assert [policy.delay(n).total_seconds() for n in (1, 2, 3)] == [2, 4, 5]
    assert policy.next_attempt(1, NOW) == NOW + timedelta(seconds=2)
    assert policy.next_attempt(4, NOW) is None


def test_rule_names_are_checked() -> None:
    with pytest.raises(ValidationError):
        spike(name="Has Spaces")


def test_the_definition_changes_only_with_what_the_rule_decides() -> None:
    rule = spike()
    assert (
        rule.definition()
        == spike(then=run("other"), retry=RetryPolicy(max_attempts=1)).definition()
    )
    assert rule.definition() != spike(scope=by(F.region)).definition()


def test_turning_a_rule_off_and_on_keeps_its_definition() -> None:
    assert spike(enabled=False).definition() == spike().definition()


def test_a_valid_rule_passes_its_check() -> None:
    spike().check(events=event_types(), actions={"triage"})
    watch = Rule(
        name="dead-letters",
        when=on("run_dead_lettered"),
        then=run("page"),
    )
    watch.check(events={}, actions={"page"})


def test_the_check_lists_every_problem() -> None:
    rule = Rule(
        name="broken",
        when=on("service.error", "nope")
        .where(NotFilter(filter=PredicateFilter(name="missing")), F.labels.env.eq("prod"))
        .distinct("fingerprint", within=MINUTE),
        scope=by("region"),
        then=run("absent"),
    )
    with pytest.raises(InvalidRule) as invalid:
        rule.check(events={"service.error": ServiceError, "heartbeat": Heartbeat}, actions=())
    assert invalid.value.problems == [
        "no event type 'nope'",
        "no predicate 'missing'",
        "no action 'absent'",
    ]
    heartbeats = Rule.model_validate(
        {
            "name": "quiet",
            "when": {
                "filter": {
                    "kind": "all",
                    "of": [
                        {"kind": "on", "types": ["heartbeat"]},
                        {"kind": "where", "field": "severity", "op": "eq", "value": 1},
                    ],
                }
            },
            "scope": {"fields": ["region"]},
            "then": {"action": "page"},
        }
    )
    with pytest.raises(InvalidRule, match="rule 'quiet' is invalid") as invalid:
        heartbeats.check(events=event_types(), actions={"page"})
    assert invalid.value.problems == [
        "no field 'severity' on heartbeat",
        "no field 'region' on heartbeat",
    ]


def test_sequence_steps_are_checked_too() -> None:
    rule = Rule(
        name="regression",
        when=sequence(on(Deploy), on(ServiceError).where(PredicateFilter(name="p")), within=MINUTE),
        scope=by(F.service),
        then=run("rollback"),
    )
    with pytest.raises(InvalidRule) as invalid:
        rule.check(events=event_types(), actions={"rollback"})
    assert invalid.value.problems == ["no predicate 'p'"]  # once, though two places name it
    rule.check(events=event_types(), actions={"rollback"}, predicates={"p"})


ERRORS = OnFilter(types=("service.error",))
DEPLOYS = OnFilter(types=("deploy.finished",))
SEVERE = F.severity >= 7


def problems(filter: Filter, **condition: object) -> list[str]:
    """Check a rule with this filter, and return its problems."""
    when = Condition.model_validate({"filter": filter, **condition})
    rule = Rule(name="checked", when=when, scope=by(F.service), then=run("act"))
    try:
        rule.check(events=event_types(), actions={"act"})
    except InvalidRule as invalid:
        return invalid.problems
    return []


def test_a_field_is_checked_on_the_types_of_its_own_conjunction() -> None:
    # A sequence whose steps filter different types.
    severe_after_deploy = sequence(
        on(Deploy), on(ServiceError).where(F.severity >= 7), within=MINUTE
    )
    assert problems(severe_after_deploy.filter, pattern=severe_after_deploy.pattern) == []
    # on(A).where(x) | on(B): x is compared on A only.
    assert problems(AnyFilter(of=(AllFilter(of=(ERRORS, SEVERE)), DEPLOYS))) == []
    # where(x) & (on(A) | on(B)): x is compared on both.
    assert problems(AllFilter(of=(SEVERE, AnyFilter(of=(ERRORS, DEPLOYS))))) == [
        "no field 'severity' on deploy.finished"
    ]
    # A where inside an any is compared on what the enclosing conjunction admits.
    either = AnyFilter(of=(SEVERE, OnFilter(types=("deploy.finished",))))
    assert problems(AllFilter(of=(ERRORS, either))) == []


def test_a_not_neither_admits_nor_hides_types() -> None:
    both = OnFilter(types=("service.error", "deploy.finished"))
    assert problems(AllFilter(of=(both, NotFilter(filter=DEPLOYS), SEVERE))) == []
    assert problems(AllFilter(of=(ERRORS, NotFilter(filter=F.nope.eq(1))))) == [
        "no field 'nope' on service.error"
    ]
    # With no on to go by, a where cannot be checked: a negated on keeps types out, no more.
    assert problems(AllFilter(of=(NotFilter(filter=DEPLOYS), SEVERE))) == []
    assert problems(NotFilter(filter=AllFilter(of=(DEPLOYS, SEVERE)))) == [
        "no field 'severity' on deploy.finished"
    ]


def test_sequence_steps_see_only_what_passed_the_filter() -> None:
    both = OnFilter(types=("service.error", "deploy.finished"))
    steps: tuple[Filter, ...] = (DEPLOYS, SEVERE)
    pattern = SequencePattern(steps=steps, within=MINUTE)
    assert problems(both, pattern=pattern) == ["no field 'severity' on deploy.finished"]
    narrowed = SequencePattern(steps=(DEPLOYS, AllFilter(of=(ERRORS, SEVERE))), within=MINUTE)
    assert problems(both, pattern=narrowed) == []
    assert problems(ERRORS, pattern=pattern) == []
