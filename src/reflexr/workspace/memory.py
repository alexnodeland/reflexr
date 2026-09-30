"""In-memory storage, for tests, examples and single-process prototypes.

It implements the full :class:`~reflexr.workspace.Storage` protocol, including transactions
that roll back, a live log subscription and leases, but keeps nothing across restarts and
cannot be shared between processes.
"""

import asyncio
from collections.abc import AsyncGenerator, Collection, Iterable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from reflexr.core import (
    WAITING,
    Envelope,
    EvaluationError,
    EventId,
    RuleName,
    RuleProgress,
    Run,
    RunId,
    RunStatus,
    ScopeKey,
    ScopeState,
    StoredRule,
)
from reflexr.workspace.storage import Clock, Entry, RunPolicy, WorkspaceRef, run_lease, utc_now


@dataclass
class _Data:
    log: list[Envelope] = field(default_factory=list[Envelope])
    ids: dict[EventId, Envelope] = field(default_factory=dict[EventId, Envelope])
    progress: dict[RuleName, RuleProgress] = field(default_factory=dict[RuleName, RuleProgress])
    states: dict[RuleName, dict[ScopeKey, ScopeState]] = field(
        default_factory=dict[RuleName, dict[ScopeKey, ScopeState]]
    )
    runs: dict[RunId, Run] = field(default_factory=dict[RunId, Run])
    dead_letters: list[EvaluationError] = field(default_factory=list[EvaluationError])
    schedules: dict[str, datetime] = field(default_factory=dict[str, datetime])
    leases: dict[str, tuple[str, datetime]] = field(default_factory=dict[str, tuple[str, datetime]])
    cursors: dict[str, int] = field(default_factory=dict[str, int])
    rules: dict[RuleName, StoredRule] = field(default_factory=dict[RuleName, StoredRule])
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    appended: asyncio.Condition = field(default_factory=asyncio.Condition)


class _Transaction:
    """Buffers writes, reads its own writes, and applies them all on commit."""

    def __init__(self, data: _Data, workspace: WorkspaceRef, clock: Clock) -> None:
        self._data = data
        self._workspace = workspace
        self._clock = clock
        self._log: list[Envelope] = []
        self._ids: dict[EventId, Envelope] = {}
        self._progress: dict[RuleName, RuleProgress] = {}
        self._states: dict[RuleName, dict[ScopeKey, ScopeState]] = {}
        self._cleared: set[RuleName] = set()
        self._runs: dict[RunId, Run] = {}
        self._dead_letters: list[EvaluationError] = []
        self._schedules: dict[str, datetime] = {}
        self._rules: dict[RuleName, StoredRule] = {}

    async def append(self, entries: Sequence[Entry]) -> list[Envelope]:
        envelopes: list[Envelope] = []
        for entry in entries:
            if await self.envelope(entry.id) is not None:
                raise ValueError(f"event {entry.id} is already in the log")
            last = self._log[-1] if self._log else self._data.log[-1] if self._data.log else None
            now = self._clock()
            envelope = Envelope(
                seq=(last.seq if last else 0) + 1,
                id=entry.id,
                ts=max(now, last.ts) if last else now,
                workspace_id=self._workspace.workspace_id,
                actor=entry.actor,
                causation=entry.causation,
                correlation_id=entry.correlation_id or entry.id,
                traceparent=entry.traceparent,
                event=entry.event,
            )
            self._log.append(envelope)
            self._ids[envelope.id] = envelope
            envelopes.append(envelope)
        return envelopes

    async def envelope(self, event_id: EventId) -> Envelope | None:
        return _latest(self._ids, self._data.ids, event_id)

    async def head_seq(self) -> int:
        return len(self._data.log) + len(self._log)

    async def read(self, *, after_seq: int, limit: int) -> list[Envelope]:
        committed = self._data.log[after_seq : after_seq + limit]
        start = max(after_seq - len(self._data.log), 0)
        return [*committed, *self._log[start : start + limit - len(committed)]]

    async def progress(self, rule: RuleName) -> RuleProgress | None:
        return _latest(self._progress, self._data.progress, rule)

    async def save_progress(self, rule: RuleName, progress: RuleProgress) -> None:
        self._progress[rule] = progress

    async def states(
        self, rule: RuleName, keys: Collection[ScopeKey]
    ) -> dict[ScopeKey, ScopeState]:
        committed = {} if rule in self._cleared else self._data.states.get(rule, {})
        merged = {**committed, **self._states.get(rule, {})}
        return {key: merged[key] for key in keys if key in merged}

    async def save_states(self, rule: RuleName, states: Mapping[ScopeKey, ScopeState]) -> None:
        self._states.setdefault(rule, {}).update(states)

    async def clear_states(self, rule: RuleName) -> None:
        self._cleared.add(rule)
        self._states.pop(rule, None)

    async def run(self, run_id: RunId) -> Run | None:
        return _latest(self._runs, self._data.runs, run_id)

    async def save_runs(self, runs: Sequence[Run]) -> None:
        self._runs.update((run.id, run) for run in runs)

    async def scope_runs(self, rule: RuleName, scope_key: ScopeKey) -> list[Run]:
        merged = {**self._data.runs, **self._runs}
        found = [
            run
            for run in merged.values()
            if run.rule == rule and run.scope_key == scope_key and run.status != "succeeded"
        ]
        return sorted(found, key=lambda run: run.fired_seq)

    async def dead_letter(self, errors: Sequence[EvaluationError]) -> None:
        self._dead_letters.extend(errors)

    async def schedule(self, name: str) -> datetime | None:
        return _latest(self._schedules, self._data.schedules, name)

    async def save_schedule(self, name: str, at: datetime) -> None:
        self._schedules[name] = at

    async def stored_rule(self, name: RuleName) -> StoredRule | None:
        return _latest(self._rules, self._data.rules, name)

    async def stored_rules(self) -> list[StoredRule]:
        return _active_rules({**self._data.rules, **self._rules})

    async def save_stored_rule(self, stored: StoredRule) -> None:
        name = stored.rule.name
        current = await self.stored_rule(name)
        following = 1 if current is None else current.version + 1
        if stored.version != following:
            raise ValueError(f"the next version of {name} is {following}, not {stored.version}")
        self._rules[name] = stored

    def commit(self) -> None:
        data = self._data
        data.log.extend(self._log)
        data.ids.update(self._ids)
        data.progress.update(self._progress)
        for rule in self._cleared:
            data.states.pop(rule, None)
        for rule, states in self._states.items():
            data.states.setdefault(rule, {}).update(states)
        data.runs.update(self._runs)
        data.dead_letters.extend(self._dead_letters)
        data.schedules.update(self._schedules)
        data.rules.update(self._rules)


def _latest[K, V](pending: dict[K, V], committed: dict[K, V], key: K) -> V | None:
    return pending[key] if key in pending else committed.get(key)


def _active_rules(rules: Mapping[RuleName, StoredRule]) -> list[StoredRule]:
    """Return the active stored rules, by name."""
    return [rules[name] for name in sorted(rules) if rules[name].status == "active"]


def _first_holding(
    runs: Iterable[Run], policy: RunPolicy
) -> dict[tuple[RuleName, ScopeKey], RunId]:
    """Return the first run of each scope that holds it, in firing order.

    The sort is stable, so runs fired at the same ``seq`` keep their creation order, as in
    :meth:`_Transaction.scope_runs`.
    """
    first: dict[tuple[RuleName, ScopeKey], RunId] = {}
    for run in sorted(runs, key=lambda run: run.fired_seq):
        if policy.holds(run):
            first.setdefault((run.rule, run.scope_key), run.id)
    return first


class InMemoryStorage:
    """Storage that keeps every workspace in process memory.

    Args:
        clock: Returns the current time, for envelope timestamps and lease expiry. Defaults to
            the system clock in UTC.
    """

    def __init__(self, *, clock: Clock = utc_now) -> None:
        self._workspaces: dict[WorkspaceRef, _Data] = {}
        self._clock = clock

    def _data(self, workspace: WorkspaceRef) -> _Data:
        return self._workspaces.setdefault(workspace, _Data())

    @asynccontextmanager
    async def transaction(self, workspace: WorkspaceRef) -> AsyncGenerator[_Transaction]:
        """Begin a transaction; it holds the workspace's lock until it ends."""
        data = self._data(workspace)
        async with data.lock:
            transaction = _Transaction(data, workspace, self._clock)
            yield transaction
            transaction.commit()
            async with data.appended:
                data.appended.notify_all()

    async def head_seq(self, workspace: WorkspaceRef) -> int:
        """Return the log's latest ``seq``, or 0 if it is empty."""
        return len(self._data(workspace).log)

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

        Of those of ``types``, if given: the first ``limit`` or the last ``last``.
        """
        log = self._data(workspace).log
        window = log[after_seq : len(log) if before_seq is None else max(before_seq - 1, 0)]
        found = window if types is None else [e for e in window if e.event_type in types]
        if last is not None:
            return found[max(len(found) - last, 0) :]
        return found if limit is None else found[:limit]

    async def subscribe(
        self, workspace: WorkspaceRef, *, after_seq: int = 0
    ) -> AsyncGenerator[Envelope]:
        """Yield stored envelopes after ``after_seq``, then each new one as it commits."""
        data = self._data(workspace)
        cursor = after_seq
        while True:
            batch = await self._next_batch(data, cursor)
            for envelope in batch:
                yield envelope
            cursor = batch[-1].seq

    @staticmethod
    async def _next_batch(data: _Data, cursor: int) -> list[Envelope]:
        async with data.appended:
            await data.appended.wait_for(lambda: len(data.log) > cursor)
            return data.log[cursor:]

    async def run(self, workspace: WorkspaceRef, run_id: RunId) -> Run | None:
        """Return a run, or None."""
        return self._data(workspace).runs.get(run_id)

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
        found = [
            run
            for run in reversed(self._data(workspace).runs.values())
            if (rule is None or run.rule == rule)
            and (status is None or run.status == status)
            and (scope_key is None or run.scope_key == scope_key)
        ]
        return found if limit is None else found[:limit]

    async def dead_letters(
        self, workspace: WorkspaceRef, *, rule: RuleName | None = None
    ) -> list[EvaluationError]:
        """Return the envelopes rules could not evaluate, oldest first."""
        letters = self._data(workspace).dead_letters
        return [letter for letter in letters if rule is None or letter.rule == rule]

    async def progress(self, workspace: WorkspaceRef) -> dict[RuleName, RuleProgress]:
        """Return every rule's progress in a workspace."""
        return dict(self._data(workspace).progress)

    async def schedules(self, workspace: WorkspaceRef) -> dict[str, datetime]:
        """Return the time of each schedule's last tick in a workspace."""
        return dict(self._data(workspace).schedules)

    async def stored_rules(self, workspace: WorkspaceRef) -> list[StoredRule]:
        """Return a workspace's active stored rules, by name, in code-point order."""
        return _active_rules(self._data(workspace).rules)

    async def workspaces(self) -> list[WorkspaceRef]:
        """Return every workspace with a log."""
        return [ref for ref, data in self._workspaces.items() if data.log]

    async def due_runs(
        self, *, now: datetime, limit: int, policy: RunPolicy
    ) -> list[tuple[WorkspaceRef, Run]]:
        """Return the runs to attempt now that can start, across workspaces, oldest first."""

        def due(data: _Data, run: Run, first: Mapping[tuple[RuleName, ScopeKey], RunId]) -> bool:
            if run.rule in policy.disabled:
                return False
            if run.status == "running":
                lease = data.leases.get(run_lease(run.id))
                return lease is None or lease[1] <= now
            if run.status not in WAITING or run.next_attempt_at > now:
                return False
            return run.rule not in policy.ordered or first[run.rule, run.scope_key] == run.id

        found: list[tuple[WorkspaceRef, Run]] = []
        for ref, data in self._workspaces.items():
            first = _first_holding(data.runs.values(), policy)
            found.extend((ref, run) for run in data.runs.values() if due(data, run, first))
        return sorted(found, key=lambda item: item[1].next_attempt_at)[:limit]

    async def acquire_lease(
        self, workspace: WorkspaceRef, key: str, holder: str, ttl: timedelta
    ) -> bool:
        """Take or renew an exclusive lease. Return False if another holder has it."""
        leases = self._data(workspace).leases
        now = self._clock()
        current = leases.get(key)
        if current is not None and current[0] != holder and current[1] > now:
            return False
        leases[key] = (holder, now + ttl)
        return True

    async def release_lease(self, workspace: WorkspaceRef, key: str, holder: str) -> None:
        """Release a lease if ``holder`` has it."""
        leases = self._data(workspace).leases
        if key in leases and leases[key][0] == holder:
            del leases[key]

    async def cursor(self, workspace: WorkspaceRef, name: str) -> int:
        """Return how far a named consumer of the log has got: the ``seq`` saved, or 0."""
        return self._data(workspace).cursors.get(name, 0)

    async def save_cursor(self, workspace: WorkspaceRef, name: str, seq: int) -> None:
        """Save how far a named consumer of the log has got; a cursor only moves forward."""
        cursors = self._data(workspace).cursors
        cursors[name] = max(cursors.get(name, 0), seq)
