"""Building conditions and field references."""

from datetime import timedelta

import pytest
from pydantic import ValidationError

from reflexr.core import (
    AllFilter,
    AnyFilter,
    Condition,
    CountPattern,
    F,
    NotFilter,
    OnFilter,
    PredicateFilter,
    WhereFilter,
    field,
    on,
    sequence,
)
from reflexr.core.conditions import admitted_types, has_field, resolve_field
from tests.event_types import Deploy, ServiceError

MINUTE = timedelta(minutes=1)


def test_field_references_build_where_filters() -> None:
    assert (F.severity >= 7) == WhereFilter(field="severity", op="ge", value=7)
    cases = {
        "lt": F.severity < 1,
        "le": F.severity <= 1,
        "gt": F.severity > 1,
        "ge": F.severity >= 1,
        "eq": F.severity.eq(1),
        "ne": F.severity.ne(1),
        "in": F.severity.is_in([1, 2]),
        "contains": F.tags.contains("db"),
        "matches": F.message.matches("^db"),
        "exists": F.message.exists(),
    }
    assert {op: w.op for op, w in cases.items()} == {op: op for op in cases}
    assert F.labels.env.eq("prod").field == "labels.env"
    assert field("contains").eq(1).field == "contains"
    assert F.message.exists(False).value is False
    assert repr(F.labels.env) == "F.labels.env"


def test_private_names_are_not_field_references() -> None:
    with pytest.raises(AttributeError):
        _ = F._private
    with pytest.raises(AttributeError):
        _ = F.labels._private


def test_where_values_are_checked() -> None:
    with pytest.raises(ValidationError, match="list of values"):
        WhereFilter(field="a", op="in", value=1)
    with pytest.raises(ValidationError, match="regular expression"):
        WhereFilter(field="a", op="matches", value=1)
    with pytest.raises(ValidationError, match="invalid regular expression"):
        WhereFilter(field="a", op="matches", value="(")
    with pytest.raises(ValidationError, match="true or false"):
        WhereFilter(field="a", op="exists", value=1)
    with pytest.raises(ValidationError):
        WhereFilter(field="not a path", op="eq", value=1)


def test_where_narrows_the_filter_and_checks_fields() -> None:
    condition = on(ServiceError).where(F.severity >= 7).where(service="auth")
    assert isinstance(condition.filter, AllFilter)
    assert [type(f) for f in condition.filter.of] == [OnFilter, WhereFilter, WhereFilter]
    with pytest.raises(ValueError, match=r"no field 'sevrity' on service\.error"):
        on(ServiceError).where(F.sevrity >= 7)
    with pytest.raises(ValueError, match=r"no field 'labels\.nope'"):
        on(ServiceError).where(AnyFilter(of=(NotFilter(filter=F.labels.nope.eq(1)),)))
    # Types given by name and unregistered types cannot be checked.
    on("legacy.alert").where(F.anything.eq(1))
    on(ServiceError).where(PredicateFilter(name="p"))


def test_patterns_and_stages() -> None:
    condition = (
        on(ServiceError)
        .distinct(F.fingerprint, within=MINUTE)
        .count(at_least=3, within=MINUTE)
        .at_most(1, per=MINUTE)
    )
    assert condition.dedupe is not None
    assert condition.dedupe.key == ("fingerprint",)
    assert isinstance(condition.pattern, CountPattern)
    assert condition.throttle is not None
    assert condition.throttle.at_most == 1
    with pytest.raises(ValueError, match="already has a count pattern"):
        condition.absent(within=MINUTE)
    with pytest.raises(ValueError, match="no field 'nope'"):
        on(ServiceError).distinct("nope", within=MINUTE)


def test_sequences_are_built_from_filter_only_steps() -> None:
    condition = sequence(on(Deploy), on(ServiceError).where(F.severity >= 7), within=MINUTE)
    assert isinstance(condition.filter, AnyFilter)
    with pytest.raises(ValueError, match="a filter only"):
        sequence(on(Deploy), on(ServiceError).count(at_least=2, within=MINUTE), within=MINUTE)
    with pytest.raises(ValueError, match="at least one event type"):
        on()


def test_conditions_round_trip_through_json() -> None:
    condition = sequence(on(Deploy), on(ServiceError).where(F.severity >= 7), within=MINUTE)
    assert Condition.model_validate_json(condition.model_dump_json()) == condition


def test_field_helpers() -> None:
    assert has_field(ServiceError, "labels.env")
    assert has_field(ServiceError, "extra.anything")  # a dict: not checkable
    assert not has_field(ServiceError, "labels.nope")
    assert not has_field(ServiceError, "nope")
    assert has_field(ServiceError, "owner.team")  # through an optional model
    assert not has_field(ServiceError, "owner.nope")
    assert resolve_field({"a": {"b": 1}}, "a.b") == (True, 1)
    assert resolve_field({"a": 1}, "a.b") == (False, None)
    both = OnFilter(types=("service.error", "service.error", "legacy.alert"))
    assert admitted_types(AllFilter(of=(both, NotFilter(filter=both)))) == [ServiceError]
