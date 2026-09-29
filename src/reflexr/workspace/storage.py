"""The storage protocol: where a workspace's log, rule state and runs live.

reflexr ships :class:`~reflexr.workspace.InMemoryStorage` for tests and examples, and
``reflexr.sql.SqlStorage`` for production. Applications with other needs implement
:class:`Storage` themselves; the workspace behaviour suite in the test tree shows what an
implementation must do.
"""

from collections.abc import AsyncGenerator, Collection, Iterable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol, Self

from reflexr.core import (
    Actor,
    Causation,
    Envelope,
    EvaluationError,
    Event,
    EventId,
    Rule,
    RuleName,
    RuleProgress,
    Run,
    RunId,
    RunStatus,
    ScopeKey,
    ScopeState,
    TenantId,
    WorkspaceId,
)


@dataclass(frozen=True)
class WorkspaceRef:
    """A tenant's workspace: the unit of isolation, ordering and locking.

    (Not to be confused with a rule's :class:`~reflexr.core.Scope`, which groups a rule's state
    within a workspace.)
    """

    tenant_id: TenantId
    workspace_id: WorkspaceId


def run_lease(run_id: RunId) -> str:
    """Return the lease key an executor holds on a run while it runs it."""
    return f"run:{run_id}"


@dataclass(frozen=True)
class RunPolicy:
    """What :meth:`Storage.due_runs` needs to know about the rules to find runs that can start.

    Storage does not see the rules, so the executor describes them with :meth:`of`. A rule
    named in no set has all of its due runs returned, which is always safe: the executor checks
    every run again as it claims it, so a set that says too little costs claims that are
    declined, never a run that is not attempted.
    """

    disabled: frozenset[RuleName] = frozenset()
    """Rules whose runs wait, and are left out altogether."""

    ordered: frozenset[RuleName] = frozenset()
    """Rules whose runs execute in firing order per scope (``ordering="scope"``)."""

    blocking: frozenset[RuleName] = frozenset()
    """Ordered rules whose dead-lettered runs hold back their scope (``on_dead_letter="block"``)."""

    @classmethod
    def of(cls, rules: Iterable[Rule]) -> Self:
        """Describe the given rules."""
        chosen = list(rules)
        ordered = [rule for rule in chosen if rule.ordering == "scope"]
        return cls(
            disabled=frozenset(rule.name for rule in chosen if not rule.enabled),
            ordered=frozenset(rule.name for rule in ordered),
            blocking=frozenset(rule.name for rule in ordered if rule.on_dead_letter == "block"),
        )

    def holds(self, run: Run) -> bool:
        """Whether a run holds back the later runs of its scope.

        A pending, retrying or running run of an ordered rule does, and so does a dead-lettered
        one if the rule blocks.
        """
        if run.rule not in self.ordered:
            return False
        return run.status in ("pending", "retrying", "running") or (
            run.status == "dead" and run.rule in self.blocking
        )


@dataclass(frozen=True)
class Entry:
    """An event to append. Storage assigns its ``seq`` and ``ts``."""

    id: EventId
    actor: Actor
    event: Event
    causation: Causation | None = None
    correlation_id: str | None = None
    """The causal chain it belongs to; None starts a new chain with the event's own id."""

    traceparent: str | None = None


class Transaction(Protocol):
    """One atomic unit of work on a workspace.

    A transaction must be serialized with every other transaction on the same scope from the
    moment it begins, so nothing it reads can change before it commits. In-memory storage holds
    a per-workspace lock; SQL storage locks the workspace's row. What a transaction writes is
    visible to its own later reads.
    """

    async def append(self, entries: Sequence[Entry]) -> list[Envelope]:
        """Append events to the log, assigning each the next ``seq`` and a ``ts``.

        ``ts`` is the later of the clock and the previous envelope's ``ts``, so it never
        decreases within a workspace. Callers make publishing idempotent by looking an id up
        with :meth:`envelope` first; appending an id that is already in the log, or earlier in
        the same transaction, raises ``ValueError``.
        """
        ...

    async def envelope(self, event_id: EventId) -> Envelope | None:
        """Return the envelope of an event id, or None."""
        ...

    async def head_seq(self) -> int:
        """Return the log's latest ``seq``, or 0 if it is empty."""
        ...

    async def read(self, *, after_seq: int, limit: int) -> list[Envelope]:
        """Return up to ``limit`` envelopes after ``after_seq``, in order."""
        ...

    async def progress(self, rule: RuleName) -> RuleProgress | None:
        """Return a rule's progress in this workspace, or None if it has none yet."""
        ...

    async def save_progress(self, rule: RuleName, progress: RuleProgress) -> None:
        """Save a rule's progress."""
        ...

    async def states(
        self, rule: RuleName, keys: Collection[ScopeKey]
    ) -> dict[ScopeKey, ScopeState]:
        """Return the stored state of the given scopes of a rule; missing scopes are left out."""
        ...

    async def save_states(self, rule: RuleName, states: Mapping[ScopeKey, ScopeState]) -> None:
        """Save the state of scopes of a rule."""
        ...

    async def clear_states(self, rule: RuleName) -> None:
        """Delete every scope state of a rule, when it is reset."""
        ...

    async def run(self, run_id: RunId) -> Run | None:
        """Return a run, or None."""
        ...

    async def save_runs(self, runs: Sequence[Run]) -> None:
        """Insert or update runs."""
        ...

    async def scope_runs(self, rule: RuleName, scope_key: ScopeKey) -> list[Run]:
        """Return every run of a rule's scope that has not succeeded, in firing order."""
        ...

    async def dead_letter(self, errors: Sequence[EvaluationError]) -> None:
        """Record envelopes a rule could not evaluate."""
        ...

    async def schedule(self, name: str) -> datetime | None:
        """Return the time of a schedule's last tick in this workspace, or None."""
        ...

    async def save_schedule(self, name: str, at: datetime) -> None:
        """Save the time of a schedule's last tick."""
        ...


class Storage(Protocol):
    """Persistence for workspaces. Every method except discovery is scoped to one workspace."""

    def transaction(self, workspace: WorkspaceRef) -> AbstractAsyncContextManager[Transaction]:
        """Begin a transaction: it commits if the block exits normally, and rolls back if not."""
        ...

    async def head_seq(self, workspace: WorkspaceRef) -> int:
        """Return the log's latest ``seq``, or 0 if it is empty."""
        ...

    async def read(
        self,
        workspace: WorkspaceRef,
        *,
        after_seq: int = 0,
        before_seq: int | None = None,
        types: Collection[str] | None = None,
        limit: int | None = None,
        last: int | None = None,
    ) -> list[Envelope]:
        """Return logged envelopes in the window ``after_seq < seq < before_seq``, in order.

        Args:
            workspace: The workspace whose log to read.
            after_seq: Only envelopes after this ``seq``.
            before_seq: Only envelopes before this ``seq``; ``None`` reads to the head.
            types: Only envelopes of these event types; ``None`` reads every type.
            limit: At most this many: the first ones in the window that match.
            last: At most this many: the last ones in the window that match, still returned
                in order. This is the tail of the log.

        Callers give at most one of ``limit`` and ``last``, and no negative numbers;
        :meth:`~reflexr.workspace.Workspace.read` checks.
        """
        ...

    def subscribe(self, workspace: WorkspaceRef, *, after_seq: int = 0) -> AsyncGenerator[Envelope]:
        """Yield envelopes after ``after_seq``: the stored ones, then each one as it commits."""
        ...

    async def run(self, workspace: WorkspaceRef, run_id: RunId) -> Run | None:
        """Return a run, or None."""
        ...

    async def runs(
        self,
        workspace: WorkspaceRef,
        *,
        rule: RuleName | None = None,
        status: RunStatus | None = None,
        scope_key: ScopeKey | None = None,
        limit: int | None = None,
    ) -> list[Run]:
        """Return runs, newest first, optionally filtered."""
        ...

    async def dead_letters(
        self, workspace: WorkspaceRef, *, rule: RuleName | None = None
    ) -> list[EvaluationError]:
        """Return the envelopes rules could not evaluate, oldest first."""
        ...

    async def progress(self, workspace: WorkspaceRef) -> dict[RuleName, RuleProgress]:
        """Return every rule's progress in a workspace."""
        ...

    async def schedules(self, workspace: WorkspaceRef) -> dict[str, datetime]:
        """Return the time of each schedule's last tick in a workspace."""
        ...

    async def workspaces(self) -> list[WorkspaceRef]:
        """Return every workspace with a log, for the reactor to evaluate."""
        ...

    async def due_runs(
        self, *, now: datetime, limit: int, policy: RunPolicy
    ) -> list[tuple[WorkspaceRef, Run]]:
        """Return the runs to attempt now, across workspaces, oldest first.

        These are the pending and retrying runs due by ``now`` that can start, and running
        runs whose :func:`run_lease` has lapsed, because their executor stopped. A pending or
        retrying run of an ordered rule can start only if no earlier run of its scope, in
        firing order (``fired_seq``, then creation), :meth:`~RunPolicy.holds` the scope, so a
        scope's backlog yields one run however deep it is. Runs of the disabled rules are left
        out. So neither the runs waiting behind others nor those of disabled rules take up the
        ``limit``.
        """
        ...

    async def acquire_lease(
        self, workspace: WorkspaceRef, key: str, holder: str, ttl: timedelta
    ) -> bool:
        """Take or renew an exclusive lease. Return False if another holder has it."""
        ...

    async def release_lease(self, workspace: WorkspaceRef, key: str, holder: str) -> None:
        """Release a lease if ``holder`` has it."""
        ...
