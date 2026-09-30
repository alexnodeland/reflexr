"""The storage protocol: where a workspace's log, rule state, runs and stored rules live.

reflexr ships :class:`~reflexr.workspace.InMemoryStorage` for tests and examples, and
``reflexr.sql.SqlStorage`` for production. Applications with other needs implement
:class:`Storage` themselves; the workspace behaviour suite in the test tree shows what an
implementation must do.
"""

from collections.abc import AsyncGenerator, Callable, Collection, Iterable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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
    StoredRule,
    TenantId,
    WorkspaceId,
    holds,
)


@dataclass(frozen=True)
class WorkspaceRef:
    """A tenant's workspace: the unit of isolation, ordering and locking.

    (Not to be confused with a rule's :class:`~reflexr.core.Scope`, which groups a rule's state
    within a workspace.)
    """

    tenant_id: TenantId
    workspace_id: WorkspaceId


Clock = Callable[[], datetime]
"""Returns the current time; injectable so tests control time."""


def utc_now() -> datetime:
    """Return the current time in UTC: the default clock."""
    return datetime.now(UTC)


RUN_LEASE_PREFIX = "run:"
"""The start of every run's lease key, which ends with the run's id."""


def run_lease(run_id: RunId) -> str:
    """Return the lease key an executor holds on a run while it runs it."""
    return RUN_LEASE_PREFIX + run_id


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
        """Whether a run holds back the later runs of its scope (:func:`~reflexr.core.holds`)."""
        return run.rule in self.ordered and holds(run, blocking=run.rule in self.blocking)


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

    A transaction must be serialized with every other transaction on the same workspace from the
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

    async def stored_rule(self, name: RuleName) -> StoredRule | None:
        """Return a stored rule, active or archived, or None if no rule of that name is stored."""
        ...

    async def stored_rules(self) -> list[StoredRule]:
        """Return the workspace's active stored rules, by name, in code-point order."""
        ...

    async def save_stored_rule(self, stored: StoredRule) -> None:
        """Save a stored rule's next version, which archives it if its status is ``archived``.

        Versions are numbered from 1 per name, so callers check an expected version by reading
        :meth:`stored_rule` first; saving any version but the next raises ``ValueError``.
        """
        ...


class Storage(Protocol):
    """Persistence for workspaces. Every method except discovery is scoped to one workspace.

    A caller can be cancelled at any await, by a rule's timeout, a stopped reactor or a closed
    connection, so every method is cancel-safe: a cancelled caller leaves no lock or connection
    behind; cancellation may be deferred until the current statement ends. So a cancelled call
    may still have taken effect: a transaction may have committed, or ``acquire_lease`` taken
    the lease. A wait for a pooled connection cannot be interrupted either, so a task that holds
    a transaction must not await a task it cancelled. The event loop's shutdown is not covered:
    it cancels the storage's own tasks too, so stop the reactor with ``serve(stop=)`` first.
    """

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

    async def stored_rules(self, workspace: WorkspaceRef) -> list[StoredRule]:
        """Return a workspace's active stored rules, by name, in code-point order."""
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

    async def cursor(self, workspace: WorkspaceRef, name: str) -> int:
        """Return how far a named consumer of the log has got: the ``seq`` saved, or 0."""
        ...

    async def save_cursor(self, workspace: WorkspaceRef, name: str, seq: int) -> None:
        """Save how far a named consumer of the log has got, such as a feedback mirror.

        A cursor only moves forward: saving a ``seq`` below the saved one leaves it, so a
        consumer running in several processes cannot move it back.
        """
        ...
