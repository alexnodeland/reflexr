"""Rules: scopes, actions, retries, definitions and checks."""

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from reflexr.core import (
    ActionRef,
    F,
    InvalidRule,
    NotFilter,
    PredicateFilter,
    RetryPolicy,
    Rule,
    Scope,
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
    assert invalid.value.problems == ["no predicate 'p'", "no predicate 'p'"]
    rule.check(events=event_types(), actions={"rollback"}, predicates={"p"})
