"""End-to-end measures of workflows, computed from a workspace's log (RFC-0002).

They are deterministic, so they need no judge:

- **dead-letter and retry rates**: the share of a rule's runs that were dead-lettered, or
  retried after failing
- **operator intervention rate**: the share of a rule's runs a person or an external agent
  retried, skipped or cancelled
- **time to resolution**: from a causal chain's first event to the event that resolved it, when
  the application says which events resolve

Whether a chain achieved its goal is a judgement: an evaluator gives it, as feedback on the
chain.
"""

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

from reflexr.core import (
    Envelope,
    RuleFired,
    RuleName,
    RunCancelled,
    RunDeadLettered,
    RunId,
    RunRequeued,
    RunRetrying,
    RunSkipped,
    RunSucceeded,
)
from reflexr.workspace import Workspace

OPERATORS = frozenset({"user", "external_agent"})
"""The actor kinds whose run operations count as interventions."""

type _Outcome = Literal["runs", "succeeded", "dead_lettered", "retried", "intervened"]


@dataclass(frozen=True)
class RuleOutcomes:
    """How a rule's runs went.

    Attributes:
        rule: The rule.
        runs: How many runs its firings created.
        succeeded: How many succeeded.
        dead_lettered: How many were dead-lettered at least once.
        retried: How many failed and were retried at least once.
        intervened: How many a person or an external agent retried, skipped or cancelled.
    """

    rule: RuleName
    runs: int = 0
    succeeded: int = 0
    dead_lettered: int = 0
    retried: int = 0
    intervened: int = 0

    @property
    def dead_letter_rate(self) -> float:
        """The share of runs dead-lettered at least once."""
        return self.dead_lettered / self.runs if self.runs else 0.0

    @property
    def retry_rate(self) -> float:
        """The share of runs retried after failing."""
        return self.retried / self.runs if self.runs else 0.0

    @property
    def intervention_rate(self) -> float:
        """The share of runs an operator retried, skipped or cancelled."""
        return self.intervened / self.runs if self.runs else 0.0


async def rule_outcomes(workspace: Workspace) -> dict[RuleName, RuleOutcomes]:
    """Count how each rule's runs went, from the workspace's log."""
    seen: dict[RuleName, dict[_Outcome, set[RunId]]] = defaultdict(lambda: defaultdict(set))
    for envelope in await workspace.read():
        match envelope.event:
            case RuleFired(rule=rule, firing_id=run_id):
                seen[rule]["runs"].add(run_id)
            case RunSucceeded(rule=rule, run_id=run_id):
                seen[rule]["succeeded"].add(run_id)
            case RunDeadLettered(rule=rule, run_id=run_id):
                seen[rule]["dead_lettered"].add(run_id)
            case RunRetrying(rule=rule, run_id=run_id):
                seen[rule]["retried"].add(run_id)
            case (
                RunRequeued(rule=rule, run_id=run_id)
                | RunSkipped(rule=rule, run_id=run_id)
                | RunCancelled(rule=rule, run_id=run_id)
            ) if envelope.actor.kind in OPERATORS:
                seen[rule]["intervened"].add(run_id)
            case _:
                pass
    return {
        rule: RuleOutcomes(
            rule=rule,
            runs=len(counted["runs"]),
            succeeded=len(counted["succeeded"]),
            dead_lettered=len(counted["dead_lettered"]),
            retried=len(counted["retried"]),
            intervened=len(counted["intervened"]),
        )
        for rule, counted in seen.items()
    }


async def time_to_resolution(
    workspace: Workspace, *, resolves: Callable[[Envelope], bool]
) -> dict[str, timedelta]:
    """Return, for each resolved causal chain, the time from its first event to its resolution.

    Args:
        workspace: The workspace.
        resolves: Whether an envelope resolves its chain, such as an ``incident.resolved``.

    Returns:
        The time to resolution of each chain that was resolved, by correlation id.
    """
    started: dict[str, Envelope] = {}
    resolved: dict[str, timedelta] = {}
    for envelope in await workspace.read():
        chain = envelope.correlation_id
        first = started.setdefault(chain, envelope)
        if chain not in resolved and resolves(envelope):
            resolved[chain] = envelope.ts - first.ts
    return resolved
