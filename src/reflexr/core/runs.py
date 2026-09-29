"""Runs: the durable record of executing one firing's action, and its lifecycle.

Every transition is a pure function returning the updated run and the fact to append to the
log. The host applies them inside the workspace's transaction.

```mermaid
stateDiagram-v2
    [*] --> pending: fired
    pending --> running: start
    retrying --> running: start
    running --> succeeded
    running --> retrying: fail, attempts left
    running --> dead: fail, none left
    pending --> skipped
    retrying --> skipped
    dead --> skipped
    running --> cancelled
    pending --> cancelled
    retrying --> cancelled
    dead --> pending: retry
    cancelled --> pending: retry
    skipped --> pending: retry
    retrying --> pending: retry
```
"""

from collections.abc import Sequence
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, JsonValue

from reflexr.core.errors import InvalidState
from reflexr.core.events import (
    Causation,
    RunCancelled,
    RunDeadLettered,
    RunProgressed,
    RunRequeued,
    RunRetrying,
    RunSkipped,
    RunStarted,
    RunSucceeded,
)
from reflexr.core.ids import RuleName, RunId, ScopeKey
from reflexr.core.rules import Rule
from reflexr.core.state import Firing

RunStatus = Literal["pending", "running", "retrying", "succeeded", "dead", "cancelled", "skipped"]
"""Where a run is in its lifecycle."""

FINISHED: frozenset[RunStatus] = frozenset({"succeeded", "dead", "cancelled", "skipped"})
"""Statuses in which a run does not run again unless retried."""


class Run(BaseModel):
    """One firing's action: its status, attempts, and a graph's latest checkpoint."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: RunId
    """The firing's id, which is also the action's idempotency key."""

    rule: RuleName
    scope_key: ScopeKey
    scope: dict[str, JsonValue]
    fired_seq: int
    matched: tuple[int, ...]
    correlation_id: str
    """The causal chain the run belongs to, which is its session in traces."""

    depth: int = 0
    status: RunStatus = "pending"
    attempts: int = 0
    next_attempt_at: AwareDatetime
    created_at: AwareDatetime
    updated_at: AwareDatetime
    error: str | None = None
    checkpoint: JsonValue = None
    """A graph run's state and pending steps after its last completed step."""

    step: str | None = None
    """The last step a graph run completed."""

    output: JsonValue = None
    trace_ids: tuple[str, ...] = ()
    """The trace id of each attempt, so feedback on the run can be attached to its traces."""

    @property
    def causation(self) -> Causation:
        """The causation of the events the run emits, and of reflexr's facts about it.

        It is one step deeper than the envelopes that made the rule fire.
        """
        return Causation(firing_id=self.id, run_id=self.id, depth=self.depth + 1)


def create_run(firing: Firing, *, now: AwareDatetime) -> Run:
    """Return the pending run for a firing."""
    return Run(
        id=firing.id,
        rule=firing.rule,
        scope_key=firing.scope_key,
        scope=firing.scope,
        fired_seq=firing.seq,
        matched=firing.matched,
        correlation_id=firing.correlation_id,
        depth=firing.depth,
        next_attempt_at=now,
        created_at=now,
        updated_at=now,
    )


def start(run: Run, *, now: AwareDatetime, trace_id: str | None = None) -> tuple[Run, RunStarted]:
    """Begin an attempt of a pending or retrying run, recording its trace if there is one."""
    _require(run, "start", "pending", "retrying")
    attempt = run.attempts + 1
    traces = (*run.trace_ids, trace_id) if trace_id else run.trace_ids
    started = _update(run, now, status="running", attempts=attempt, trace_ids=traces)
    return started, RunStarted(
        run_id=run.id, rule=run.rule, scope_key=run.scope_key, attempt=attempt
    )


def succeed(run: Run, *, now: AwareDatetime, output: JsonValue = None) -> tuple[Run, RunSucceeded]:
    """Finish a running run."""
    _require(run, "succeed", "running")
    done = _update(run, now, status="succeeded", output=output, error=None)
    return done, RunSucceeded(run_id=run.id, rule=run.rule, output=output)


def fail(
    run: Run, rule: Rule, *, now: AwareDatetime, error: str
) -> tuple[Run, RunRetrying | RunDeadLettered]:
    """Record a failed attempt: retry later under the rule's policy, or dead-letter the run."""
    _require(run, "fail", "running")
    next_at = rule.retry.next_attempt(run.attempts, now)
    if next_at is None:
        dead = _update(run, now, status="dead", error=error)
        return dead, RunDeadLettered(
            run_id=run.id, rule=run.rule, attempts=run.attempts, error=error
        )
    retrying = _update(run, now, status="retrying", error=error, next_attempt_at=next_at)
    return retrying, RunRetrying(
        run_id=run.id,
        rule=run.rule,
        attempt=run.attempts,
        error=error,
        next_attempt_at=next_at,
    )


def checkpoint(
    run: Run, *, now: AwareDatetime, step: str, state: JsonValue
) -> tuple[Run, RunProgressed]:
    """Save a running graph run's state after it completed ``step``."""
    _require(run, "checkpoint", "running")
    saved = _update(run, now, checkpoint=state, step=step)
    return saved, RunProgressed(run_id=run.id, rule=run.rule, step=step)


def cancel(run: Run, *, now: AwareDatetime, reason: str | None = None) -> tuple[Run, RunCancelled]:
    """Cancel a run that has not finished."""
    _require(run, "cancel", "pending", "running", "retrying", "dead")
    cancelled = _update(run, now, status="cancelled")
    return cancelled, RunCancelled(run_id=run.id, rule=run.rule, reason=reason)


def skip(run: Run, *, now: AwareDatetime, reason: str | None = None) -> tuple[Run, RunSkipped]:
    """Give up on a run that is waiting, unblocking later runs of its scope."""
    _require(run, "skip", "pending", "retrying", "dead")
    skipped = _update(run, now, status="skipped")
    return skipped, RunSkipped(run_id=run.id, rule=run.rule, reason=reason)


def retry(run: Run, *, now: AwareDatetime) -> tuple[Run, RunRequeued]:
    """Make a run runnable now, with a fresh retry budget if it had finished."""
    _require(run, "retry", "pending", "retrying", "dead", "cancelled", "skipped")
    attempts = 0 if run.status in FINISHED else run.attempts
    requeued = _update(run, now, status="pending", attempts=attempts, next_attempt_at=now)
    return requeued, RunRequeued(run_id=run.id, rule=run.rule)


def runnable(runs: Sequence[Run], rule: Rule, *, now: AwareDatetime) -> list[Run]:
    """Return the runs of one rule and scope that may start now.

    Args:
        runs: Every run of the scope that has not succeeded, in firing order.
        rule: Their rule, whose ordering and dead-letter policy apply.
        now: The current time.
    """
    due = [r for r in runs if r.status in ("pending", "retrying") and r.next_attempt_at <= now]
    if rule.ordering == "none":
        return due
    for candidate in runs:
        if candidate.status in ("cancelled", "skipped", "succeeded"):
            continue
        if candidate.status == "dead":
            if rule.on_dead_letter == "block":
                return []
            continue
        return [candidate] if candidate in due else []
    return []


def _require(run: Run, action: str, *statuses: RunStatus) -> None:
    if run.status not in statuses:
        raise InvalidState(f"cannot {action} run {run.id}, which is {run.status}")


def _update(run: Run, now: AwareDatetime, **changes: object) -> Run:
    return run.model_copy(update={**changes, "updated_at": now})
