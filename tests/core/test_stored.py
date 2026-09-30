"""Stored rules: the configuration, the change an allow hook sees, and every problem listed."""

from typing import Any

import pytest
from pydantic import BaseModel

from reflexr.core import (
    MAX_STORED_BYTES,
    MAX_STORED_PROVENANCE,
    Actor,
    AgentActor,
    Rule,
    RuleChange,
    StoredRules,
    TenantId,
    WorkspaceId,
    check_provenance,
    check_stored,
)

# The rule RFC-0003 installs from chat: a valid stored rule.
EXAMPLE: dict[str, Any] = {
    "name": "chat:prod-deploy-failures",
    "description": "Whenever a deploy to production fails, tell me here.",
    "when": {
        "filter": {
            "kind": "all",
            "of": [
                {"kind": "on", "types": ["oncall:deploy.completed"]},
                {"kind": "where", "field": "environment", "op": "eq", "value": "production"},
                {"kind": "where", "field": "status", "op": "eq", "value": "failed"},
            ],
        },
        "throttle": {"at_most": 1, "per": "PT15M"},
    },
    "then": {"action": "notify", "params": {"thread_id": "thr_4"}},
    "ordering": "none",
    "timeout": "PT1M",
}


class NotifyParams(BaseModel):
    thread_id: str


async def allow(
    tenant_id: TenantId, workspace_id: WorkspaceId, actor: Actor, change: RuleChange
) -> bool:
    return isinstance(actor, AgentActor) and actor.rule == "relayr:install-rule"


CONFIG = StoredRules(
    allow=allow, actions={"notify": NotifyParams, "page": None}, namespaces={"chat"}
)


def stored(**changes: Any) -> Rule:
    """The example, with some of its fields changed."""
    return Rule.model_validate({**EXAMPLE, **changes})


def when(**changes: Any) -> dict[str, Any]:
    """The example's condition, with some of its stages changed."""
    return {**EXAMPLE["when"], **changes}


def only(*filters: dict[str, Any]) -> dict[str, Any]:
    """The example's filter, with more filters."""
    return {"kind": "all", "of": [*EXAMPLE["when"]["filter"]["of"], *filters]}


STEPS = [
    {"kind": "on", "types": ["oncall:deploy.completed"]},
    {"kind": "predicate", "name": "rolled_back"},
]
"""A sequence's steps, the second a predicate."""


def test_the_rfcs_example_may_be_stored() -> None:
    assert check_stored(stored(), CONFIG) == []
    assert check_stored(stored(then={"action": "page"}), CONFIG) == []


@pytest.mark.parametrize(
    ("changes", "problems"),
    [
        (
            {"name": "oncall:prod-deploy-failures"},
            ["the namespace 'oncall' is not one stored rules may use"],
        ),
        ({"description": "x" * 2001}, ["description must be at most 2000 characters"]),
        (
            {"when": when(filter=only({"kind": "predicate", "name": "business_hours"}))},
            ["the predicate 'business_hours' is not allowed"],
        ),
        (
            {
                "when": when(
                    filter=only(
                        {"kind": "where", "field": "service", "op": "matches", "value": "^a"}
                    )
                )
            },
            ["the 'matches' operator is not allowed"],
        ),
        (
            {"when": when(pattern={"kind": "sequence", "steps": STEPS, "within": "PT1H"})},
            ["the predicate 'rolled_back' is not allowed"],
        ),
        (
            # As sequence() builds it: the filter admits either step, so both name it.
            {
                "when": when(
                    filter={"kind": "any", "of": STEPS},
                    pattern={"kind": "sequence", "steps": STEPS, "within": "PT1H"},
                )
            },
            ["the predicate 'rolled_back' is not allowed"],
        ),
        ({"when": when(throttle=None)}, ["when.throttle is required"]),
        (
            {"when": when(throttle={"at_most": 2, "per": "PT1M"})},
            ["when.throttle must allow at most 60 firings in any hour"],
        ),
        (
            {"when": when(dedupe={"key": ["service"], "within": "P2D"})},
            ["when.dedupe.within must be at most P1D"],
        ),
        (
            {"when": when(pattern={"kind": "count", "at_least": 3, "within": "P2D"})},
            ["when.pattern.within must be at most P1D"],
        ),
        (
            {"when": when(pattern={"kind": "absence", "within": "P2D"})},
            ["when.pattern.within must be at most P1D"],
        ),
        (
            {"when": when(throttle={"at_most": 1, "per": "P2D"})},
            ["when.throttle.per must be at most P1D"],
        ),
        ({"scope": {"fields": ["service"]}}, ["scope.fields must be empty"]),
        (
            {"then": {"action": "restart"}},
            ["the action 'restart' is not one stored rules may run"],
        ),
        ({"then": {"action": "notify"}}, ["then.params.thread_id: Field required"]),
        (
            {"then": {"action": "page", "params": {"urgent": True}}},
            ["then.params must be empty"],
        ),
        ({"retry": {"max_attempts": 6}}, ["retry.max_attempts must be at most 5"]),
        ({"ordering": "scope"}, ['ordering must be "none"']),
        ({"on_dead_letter": "block"}, ['on_dead_letter must be "continue"']),
        ({"start": "beginning"}, ['start must be "now"']),
        ({"enabled": False}, ["enabled must be true"]),
        ({"timeout": None}, ["timeout is required"]),
        ({"timeout": "PT10M"}, ["timeout must be at most PT5M"]),
        (
            {
                "when": when(
                    filter=only(
                        {
                            "kind": "where",
                            "field": "service",
                            "op": "in",
                            "value": ["x" * 100] * 200,
                        }
                    )
                )
            },
            [f"the rule's JSON must be at most {MAX_STORED_BYTES} bytes"],
        ),
    ],
)
def test_a_bad_stored_rule_is_refused(changes: dict[str, Any], problems: list[str]) -> None:
    assert check_stored(stored(**changes), CONFIG) == problems


def test_every_problem_is_listed_together() -> None:
    rule = stored(
        name="oncall:everything",
        when={
            "filter": {"kind": "predicate", "name": "business_hours"},
            "dedupe": {"key": ["service"], "within": "P2D"},
            "pattern": {"kind": "count", "at_least": 3, "within": "P2D"},
        },
        scope={"fields": ["service"]},
        then={"action": "restart"},
        ordering="scope",
        timeout=None,
    )
    assert check_stored(rule, CONFIG) == [
        "the namespace 'oncall' is not one stored rules may use",
        "the predicate 'business_hours' is not allowed",
        "when.dedupe.within must be at most P1D",
        "when.pattern.within must be at most P1D",
        "when.throttle is required",
        "scope.fields must be empty",
        "the action 'restart' is not one stored rules may run",
        "timeout is required",
        'ordering must be "none"',
    ]


@pytest.mark.parametrize(
    ("at_most", "per", "allowed"),
    [
        (60, "PT1H", True),
        (15, "PT15M", True),
        (16, "PT15M", False),
        (1, "PT1M", True),
        (1, "PT45S", False),  # 80 periods an hour
        (1000, "P1D", False),  # all of them may fire in its first hour
    ],
)
def test_the_throttle_bounds_the_firings_any_hour_can_have(
    at_most: int, per: str, allowed: bool
) -> None:
    rule = stored(when=when(throttle={"at_most": at_most, "per": per}))
    assert (check_stored(rule, CONFIG) == []) is allowed


def test_provenance_is_held_to_its_size() -> None:
    assert check_provenance({"source": "artifactr", "proposal": "prp_12"}) == []
    padding = MAX_STORED_PROVENANCE - len('{"note":""}')
    assert check_provenance({"note": "x" * padding}) == []
    assert check_provenance({"note": "x" * (padding + 1)}) == [
        "provenance must be at most 4096 bytes of JSON"
    ]


def test_the_reflexr_namespace_is_reserved() -> None:
    with pytest.raises(ValueError, match="the reflexr namespace is reserved"):
        StoredRules(allow=allow, actions={}, namespaces={"chat", "reflexr"})


def test_a_change_says_which_command_which_rule_and_the_new_rule() -> None:
    install = RuleChange(kind="install", rule="chat:prod-deploy-failures", spec=stored())
    assert (install.kind, install.rule, install.spec) == (
        "install",
        "chat:prod-deploy-failures",
        stored(),
    )
    assert RuleChange(kind="archive", rule="chat:prod-deploy-failures").spec is None
