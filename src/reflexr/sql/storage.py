"""SQL storage: workspaces in a database, through SQLAlchemy's asyncio extension.

It implements the full :class:`~reflexr.workspace.Storage` protocol with the same code on
PostgreSQL and SQLite. A transaction locks its workspace's row before it reads anything, so
transactions on one workspace run one at a time, across processes; ``seq`` and ``ts`` are
assigned under that lock. Subscriptions poll the log, and wake at once for commits made through
the same :class:`SqlStorage`.

Every database call is awaited to its end, even when its caller is cancelled, and the
cancellation is raised after it. SQLAlchemy takes a statement cancelled part-way for a lost
connection, which on SQLite can keep the database's write lock, or the pool's one connection.
"""

import asyncio
import contextlib
import functools
from collections.abc import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Collection,
    Iterable,
    Mapping,
    Sequence,
)
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from itertools import batched
from types import CoroutineType
from typing import Any

from sqlalchemy import (
    ColumnElement,
    Exists,
    Select,
    and_,
    delete,
    exists,
    insert,
    or_,
    select,
    tuple_,
    update,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import InstrumentedAttribute, aliased

from reflexr.core import (
    HOLDING,
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
from reflexr.sql.tables import (
    CursorRow,
    DeadLetterRow,
    EventRow,
    LeaseRow,
    ProgressRow,
    RuleRow,
    RunRow,
    ScheduleRow,
    ScopedRow,
    StateRow,
    WorkspaceRow,
)
from reflexr.workspace import RUN_LEASE_PREFIX, Clock, Entry, RunPolicy, WorkspaceRef, utc_now

_PAGE = 500
"""How many envelopes a subscription reads at a time."""

_CHUNK = 500
"""How many keys one ``IN`` clause lists, well within every driver's parameter limit."""


# Shared verbatim with artifactr's src/artifactr/sql/storage.py; change both.
async def _to_the_end[T](call: Awaitable[T]) -> T:
    """Await a database call to its end; a cancellation that came meanwhile is raised after."""
    task = asyncio.ensure_future(call)
    cancelled: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.wait([task])
        except asyncio.CancelledError as error:
            cancelled = error
    if cancelled is not None:
        if not task.cancelled():
            task.exception()
        raise cancelled
    return task.result()


# Shared verbatim with artifactr's src/artifactr/sql/storage.py; change both.
def _awaited_to_the_end[**P, T](
    method: Callable[P, Awaitable[T]],
) -> Callable[P, "CoroutineType[Any, Any, T]"]:  # subscriptable at run time from Python 3.13
    """Await every call of a method to its end, with :func:`_to_the_end`."""

    @functools.wraps(method)
    async def call(*args: P.args, **kwargs: P.kwargs) -> T:
        return await _to_the_end(method(*args, **kwargs))

    return call


class _Transaction:
    """Reads and writes one workspace's rows in the session that holds its row's lock.

    Writes go to the session at once, and the session flushes them before every query, so
    later reads in the transaction see them. Each method is awaited to its end.
    """

    def __init__(
        self, session: AsyncSession, ref: WorkspaceRef, workspace: WorkspaceRow, clock: Clock
    ) -> None:
        self._session = session
        self._ref = ref
        self._workspace = workspace
        self._clock = clock

    @_awaited_to_the_end
    async def append(self, entries: Sequence[Entry]) -> list[Envelope]:
        envelopes: list[Envelope] = []
        workspace = self._workspace
        for entry in entries:
            if await self.envelope(entry.id) is not None:
                raise ValueError(f"event {entry.id} is already in the log")
            now = self._clock()
            last = workspace.head_ts
            envelope = Envelope(
                seq=workspace.head_seq + 1,
                id=entry.id,
                ts=now if last is None else max(now, last),
                workspace_id=self._ref.workspace_id,
                actor=entry.actor,
                causation=entry.causation,
                correlation_id=entry.correlation_id or entry.id,
                traceparent=entry.traceparent,
                event=entry.event,
            )
            self._session.add(
                EventRow(
                    **self._key,
                    seq=envelope.seq,
                    event_id=envelope.id,
                    event_type=envelope.event_type,
                    envelope=envelope.model_dump(mode="json"),
                )
            )
            workspace.head_seq, workspace.head_ts = envelope.seq, envelope.ts
            envelopes.append(envelope)
        return envelopes

    @_awaited_to_the_end
    async def envelope(self, event_id: EventId) -> Envelope | None:
        row = await self._session.scalar(
            _scoped(EventRow, self._ref).where(EventRow.event_id == event_id)
        )
        return None if row is None else _envelope(row)

    @_awaited_to_the_end
    async def head_seq(self) -> int:
        return self._workspace.head_seq

    @_awaited_to_the_end
    async def read(self, *, after_seq: int, limit: int) -> list[Envelope]:
        rows = await self._session.scalars(_log(self._ref, after_seq=after_seq, limit=limit))
        return [_envelope(row) for row in rows]

    @_awaited_to_the_end
    async def progress(self, rule: RuleName) -> RuleProgress | None:
        row = await self._session.get(ProgressRow, self._pk(rule))
        return None if row is None else _progress(row)

    @_awaited_to_the_end
    async def save_progress(self, rule: RuleName, progress: RuleProgress) -> None:
        row = await self._session.get(ProgressRow, self._pk(rule))
        if row is None:
            row = ProgressRow(**self._key, rule=rule, position=self._next_position())
            self._session.add(row)
        row.body = progress.model_dump(mode="json")

    @_awaited_to_the_end
    async def states(
        self, rule: RuleName, keys: Collection[ScopeKey]
    ) -> dict[ScopeKey, ScopeState]:
        rows = await self._states(rule, keys)
        return {key: ScopeState.model_validate(rows[key].body) for key in keys if key in rows}

    @_awaited_to_the_end
    async def save_states(self, rule: RuleName, states: Mapping[ScopeKey, ScopeState]) -> None:
        rows = await self._states(rule, states)
        for key, state in states.items():
            row = rows.get(key)
            if row is None:
                row = StateRow(**self._key, rule=rule, scope_key=key)
                self._session.add(row)
            row.body = state.model_dump(mode="json")

    async def _states(self, rule: RuleName, keys: Iterable[ScopeKey]) -> dict[ScopeKey, StateRow]:
        query = _scoped(StateRow, self._ref).where(StateRow.rule == rule)
        return {row.scope_key: row for row in await self._in(query, StateRow.scope_key, keys)}

    @_awaited_to_the_end
    async def clear_states(self, rule: RuleName) -> None:
        await self._session.execute(
            delete(StateRow).where(*_in_workspace(StateRow, self._ref), StateRow.rule == rule)
        )

    @_awaited_to_the_end
    async def run(self, run_id: RunId) -> Run | None:
        row = await self._session.get(RunRow, self._pk(run_id))
        return None if row is None else _run(row)

    @_awaited_to_the_end
    async def save_runs(self, runs: Sequence[Run]) -> None:
        # The last version of each run wins, in the order the runs first appear.
        latest = {run.id: run for run in runs}
        query = _scoped(RunRow, self._ref)
        rows = {row.id: row for row in await self._in(query, RunRow.id, latest)}
        for run in latest.values():
            row = rows.get(run.id)
            if row is None:
                row = RunRow(**self._key, id=run.id, position=self._next_position())
                self._session.add(row)
            row.rule = run.rule
            row.scope_key = run.scope_key
            row.status = run.status
            row.fired_seq = run.fired_seq
            row.next_attempt_at = run.next_attempt_at
            row.body = run.model_dump(mode="json")

    @_awaited_to_the_end
    async def scope_runs(self, rule: RuleName, scope_key: ScopeKey) -> list[Run]:
        query = (
            _scoped(RunRow, self._ref)
            .where(RunRow.rule == rule, RunRow.scope_key == scope_key, RunRow.status != "succeeded")
            .order_by(RunRow.fired_seq, RunRow.position)
        )
        return [_run(row) for row in await self._session.scalars(query)]

    @_awaited_to_the_end
    async def dead_letter(self, errors: Sequence[EvaluationError]) -> None:
        self._session.add_all(
            DeadLetterRow(
                **self._key,
                position=self._next_position(),
                rule=error.rule,
                body=error.model_dump(mode="json"),
            )
            for error in errors
        )

    @_awaited_to_the_end
    async def schedule(self, name: str) -> datetime | None:
        row = await self._session.get(ScheduleRow, self._pk(name))
        return None if row is None else row.at

    @_awaited_to_the_end
    async def save_schedule(self, name: str, at: datetime) -> None:
        row = await self._session.get(ScheduleRow, self._pk(name))
        if row is None:
            row = ScheduleRow(**self._key, name=name, position=self._next_position())
            self._session.add(row)
        row.at = at

    @_awaited_to_the_end
    async def stored_rule(self, name: RuleName) -> StoredRule | None:
        row = await self._session.get(RuleRow, self._pk(name))
        return None if row is None else _stored_rule(row)

    @_awaited_to_the_end
    async def stored_rules(self) -> list[StoredRule]:
        rows = await self._session.scalars(_active_rules(self._ref))
        return [_stored_rule(row) for row in rows]

    @_awaited_to_the_end
    async def save_stored_rule(self, stored: StoredRule) -> None:
        name = stored.rule.name
        row = await self._session.get(RuleRow, self._pk(name))
        following = 1 if row is None else row.version + 1
        if stored.version != following:
            raise ValueError(f"the next version of {name} is {following}, not {stored.version}")
        if row is None:
            row = RuleRow(**self._key, name=name)
            self._session.add(row)
        row.version = stored.version
        row.status = stored.status
        row.rule = stored.rule.model_dump(mode="json")
        row.provenance = stored.provenance

    @property
    def _key(self) -> dict[str, str]:
        return {"tenant_id": self._ref.tenant_id, "workspace_id": self._ref.workspace_id}

    def _pk(self, key: str) -> tuple[str, str, str]:
        return (self._ref.tenant_id, self._ref.workspace_id, key)

    def _next_position(self) -> int:
        self._workspace.last_position += 1
        return self._workspace.last_position

    async def _in[R: ScopedRow](
        self, query: Select[R], column: InstrumentedAttribute[str], keys: Iterable[str]
    ) -> list[R]:
        """Return the rows of a query whose ``column`` is one of ``keys``."""
        rows: list[R] = []
        for chunk in batched(keys, _CHUNK):
            rows.extend(await self._session.scalars(query.where(column.in_(chunk))))
        return rows


async def _lock_workspace(session: AsyncSession, ref: WorkspaceRef) -> WorkspaceRow:
    """Lock the workspace's row, creating it first if the workspace is new."""
    new = WorkspaceRow(
        tenant_id=ref.tenant_id,
        workspace_id=ref.workspace_id,
        head_seq=0,
        head_ts=None,
        last_position=0,
    )
    return await _locked(session, WorkspaceRow, (ref.tenant_id, ref.workspace_id), new)


async def _locked[R: ScopedRow](
    session: AsyncSession, model: type[R], key: tuple[str, ...], new: R
) -> R:
    """Lock a row (``SELECT ... FOR UPDATE``), inserting ``new`` first if it does not exist."""
    while (row := await session.get(model, key, with_for_update=True)) is None:
        # A concurrent transaction may insert the row first. Then this insert fails, and the
        # next lookup waits for that transaction to end and locks its row instead.
        with contextlib.suppress(IntegrityError):
            async with session.begin_nested():
                session.add(new)
    return row


def _in_workspace(model: type[ScopedRow], ref: WorkspaceRef) -> tuple[ColumnElement[bool], ...]:
    return (model.tenant_id == ref.tenant_id, model.workspace_id == ref.workspace_id)


def _scoped[R: ScopedRow](model: type[R], ref: WorkspaceRef) -> Select[R]:
    return select(model).where(*_in_workspace(model, ref))


def _log(
    ref: WorkspaceRef,
    *,
    after_seq: int,
    before_seq: int | None = None,
    types: Collection[str] | None = None,
    limit: int | None = None,
    last: int | None = None,
) -> Select[EventRow]:
    """Select the envelopes in a window of the log: the first ``limit``, or the last ``last``.

    The last are selected newest first, so the caller reverses them.
    """
    query = _scoped(EventRow, ref).where(EventRow.seq > after_seq)
    if before_seq is not None:
        query = query.where(EventRow.seq < before_seq)
    if types is not None:
        query = query.where(EventRow.event_type.in_(sorted(types)))
    if last is not None:
        return query.order_by(EventRow.seq.desc()).limit(last)
    return query.order_by(EventRow.seq).limit(limit)


def _envelope(row: EventRow) -> Envelope:
    return Envelope.model_validate(row.envelope)


def _run(row: RunRow) -> Run:
    return Run.model_validate(row.body)


def _progress(row: ProgressRow) -> RuleProgress:
    return RuleProgress.model_validate(row.body)


def _active_rules(ref: WorkspaceRef) -> Select[RuleRow]:
    """Select a workspace's active stored rules, by name."""
    return _scoped(RuleRow, ref).where(RuleRow.status == "active").order_by(RuleRow.name)


def _stored_rule(row: RuleRow) -> StoredRule:
    return StoredRule.model_validate(row, from_attributes=True)


class SqlStorage:
    """Storage in a SQL database, through SQLAlchemy's asyncio extension.

    Create the tables with :func:`~reflexr.sql.migrate` first. The same code runs on
    PostgreSQL (with its default ``READ COMMITTED`` isolation) and on SQLite, whose engines
    must come from :func:`~reflexr.sql.create_sqlite_engine`.

    Args:
        engine: The database to use. The storage does not dispose of it.
        clock: Returns the current time, for envelope timestamps and lease expiry. Defaults
            to the system clock in UTC.
        poll_interval: How often a subscription checks the log for envelopes committed by
            other processes. Commits made through this storage wake its subscriptions at once.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        clock: Clock = utc_now,
        poll_interval: timedelta = timedelta(seconds=0.5),
    ) -> None:
        self._engine = engine
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._clock = clock
        self._poll_interval = poll_interval.total_seconds()
        self._heads: dict[WorkspaceRef, int] = {}
        self._committed = asyncio.Condition()

    @asynccontextmanager
    async def transaction(self, workspace: WorkspaceRef) -> AsyncGenerator[_Transaction]:
        """Begin a transaction; it holds the workspace row's lock until it ends.

        Closing the session rolls back whatever it did not commit, and returns its connection.
        """
        session = self._sessions()
        try:
            row = await _to_the_end(_lock_workspace(session, workspace))
            yield _Transaction(session, workspace, row, self._clock)
            await _to_the_end(session.commit())
        finally:
            await _to_the_end(session.close())
        async with self._committed:
            self._heads[workspace] = max(row.head_seq, self._heads.get(workspace, 0))
            self._committed.notify_all()

    @_awaited_to_the_end
    async def head_seq(self, workspace: WorkspaceRef) -> int:
        """Return the log's latest ``seq``, or 0 if it is empty."""
        async with self._sessions() as session:
            head = await session.scalar(
                select(WorkspaceRow.head_seq).where(*_in_workspace(WorkspaceRow, workspace))
            )
        return head or 0

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

        Of those of ``types``, if given: the first ``limit`` or the last ``last``. The filter
        and both ends of the window are in the query, over ``ix_reflexr_events_type`` when it
        names types, and the last are read from the end of the log.
        """
        query = _log(
            workspace,
            after_seq=after_seq,
            before_seq=before_seq,
            types=types,
            limit=limit,
            last=last,
        )
        envelopes = [_envelope(row) for row in await self._all(query)]
        return envelopes if last is None else envelopes[::-1]

    async def subscribe(
        self, workspace: WorkspaceRef, *, after_seq: int = 0
    ) -> AsyncGenerator[Envelope]:
        """Yield stored envelopes after ``after_seq``, then each new one as it commits.

        New envelopes arrive at once when they were committed through this storage, and
        within ``poll_interval`` otherwise.
        """
        cursor = after_seq
        while True:
            # Read a page and close the session before yielding, so a subscriber that stops
            # iterating holds no connection.
            page = await self.read(workspace, after_seq=cursor, limit=_PAGE)
            for envelope in page:
                yield envelope
            if page:
                cursor = page[-1].seq
            else:
                await self._wait_for_commit(workspace, cursor)

    async def _wait_for_commit(self, workspace: WorkspaceRef, cursor: int) -> None:
        async with self._committed:
            committed = self._committed.wait_for(lambda: self._heads.get(workspace, 0) > cursor)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(committed, self._poll_interval)

    @_awaited_to_the_end
    async def run(self, workspace: WorkspaceRef, run_id: RunId) -> Run | None:
        """Return a run, or None."""
        async with self._sessions() as session:
            row = await session.get(RunRow, (workspace.tenant_id, workspace.workspace_id, run_id))
        return None if row is None else _run(row)

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
        query = _scoped(RunRow, workspace).order_by(RunRow.position.desc()).limit(limit)
        if rule is not None:
            query = query.where(RunRow.rule == rule)
        if status is not None:
            query = query.where(RunRow.status == status)
        if scope_key is not None:
            query = query.where(RunRow.scope_key == scope_key)
        return [_run(row) for row in await self._all(query)]

    async def dead_letters(
        self, workspace: WorkspaceRef, *, rule: RuleName | None = None
    ) -> list[EvaluationError]:
        """Return the envelopes rules could not evaluate, oldest first."""
        query = _scoped(DeadLetterRow, workspace).order_by(DeadLetterRow.position)
        if rule is not None:
            query = query.where(DeadLetterRow.rule == rule)
        return [EvaluationError.model_validate(row.body) for row in await self._all(query)]

    async def progress(self, workspace: WorkspaceRef) -> dict[RuleName, RuleProgress]:
        """Return every rule's progress in a workspace."""
        query = _scoped(ProgressRow, workspace).order_by(ProgressRow.position)
        return {row.rule: _progress(row) for row in await self._all(query)}

    async def schedules(self, workspace: WorkspaceRef) -> dict[str, datetime]:
        """Return the time of each schedule's last tick in a workspace."""
        query = _scoped(ScheduleRow, workspace).order_by(ScheduleRow.position)
        return {row.name: row.at for row in await self._all(query)}

    async def stored_rules(self, workspace: WorkspaceRef) -> list[StoredRule]:
        """Return a workspace's active stored rules, by name, in code-point order."""
        return [_stored_rule(row) for row in await self._all(_active_rules(workspace))]

    async def workspaces(self) -> list[WorkspaceRef]:
        """Return every workspace with a log."""
        query = (
            select(WorkspaceRow)
            .where(WorkspaceRow.head_seq > 0)
            .order_by(WorkspaceRow.tenant_id, WorkspaceRow.workspace_id)
        )
        return [WorkspaceRef(row.tenant_id, row.workspace_id) for row in await self._all(query)]

    async def due_runs(
        self, *, now: datetime, limit: int, policy: RunPolicy
    ) -> list[tuple[WorkspaceRef, Run]]:
        """Return the runs to attempt now that can start, across workspaces, oldest first.

        A running run is due when its lease is missing or has expired by ``now``. A pending or
        retrying run of an ordered rule is due only if no earlier run of its scope holds it, as
        a ``NOT EXISTS`` over ``ix_reflexr_runs_scope_head`` finds. Runs of the disabled rules
        are left out.
        """
        lease = and_(
            LeaseRow.tenant_id == RunRow.tenant_id,
            LeaseRow.workspace_id == RunRow.workspace_id,
            LeaseRow.key == RUN_LEASE_PREFIX + RunRow.id,
        )
        waiting = and_(RunRow.status.in_(sorted(WAITING)), RunRow.next_attempt_at <= now)
        if policy.ordered:
            waiting = and_(waiting, _first_in_scope(policy))
        orphaned = and_(
            RunRow.status == "running", or_(LeaseRow.key.is_(None), LeaseRow.expires_at <= now)
        )
        due = or_(waiting, orphaned)
        if policy.disabled:
            due = and_(due, RunRow.rule.not_in(sorted(policy.disabled)))
        query = (
            select(RunRow)
            .outerjoin(LeaseRow, lease)
            .where(due)
            .order_by(
                RunRow.next_attempt_at, RunRow.tenant_id, RunRow.workspace_id, RunRow.position
            )
            .limit(limit)
        )
        return [
            (WorkspaceRef(row.tenant_id, row.workspace_id), _run(row))
            for row in await self._all(query)
        ]

    @_awaited_to_the_end
    async def acquire_lease(
        self, workspace: WorkspaceRef, key: str, holder: str, ttl: timedelta
    ) -> bool:
        """Take or renew an exclusive lease. Return False if another holder has it."""
        now = self._clock()
        lease = _lease(workspace, key)
        try:
            async with self._engine.begin() as connection:
                taken = await connection.execute(
                    update(LeaseRow)
                    .where(*lease, or_(LeaseRow.holder == holder, LeaseRow.expires_at <= now))
                    .values(holder=holder, expires_at=now + ttl)
                )
                if taken.rowcount == 0:
                    await connection.execute(
                        insert(LeaseRow).values(
                            tenant_id=workspace.tenant_id,
                            workspace_id=workspace.workspace_id,
                            key=key,
                            holder=holder,
                            expires_at=now + ttl,
                        )
                    )
        except IntegrityError:
            # Another holder has an unexpired lease, or took it first.
            return False
        return True

    @_awaited_to_the_end
    async def release_lease(self, workspace: WorkspaceRef, key: str, holder: str) -> None:
        """Release a lease if ``holder`` has it."""
        async with self._engine.begin() as connection:
            await connection.execute(
                delete(LeaseRow).where(*_lease(workspace, key), LeaseRow.holder == holder)
            )

    @_awaited_to_the_end
    async def cursor(self, workspace: WorkspaceRef, name: str) -> int:
        """Return how far a named consumer of the log has got: the ``seq`` saved, or 0."""
        async with self._sessions() as session:
            cursor = await session.get(
                CursorRow, (workspace.tenant_id, workspace.workspace_id, name)
            )
        return 0 if cursor is None else cursor.seq

    @_awaited_to_the_end
    async def save_cursor(self, workspace: WorkspaceRef, name: str, seq: int) -> None:
        """Save how far a named consumer of the log has got; a cursor only moves forward.

        The cursor's row is locked while it is compared, so saves from several processes
        leave the furthest.
        """
        key = (workspace.tenant_id, workspace.workspace_id, name)
        new = CursorRow(
            tenant_id=workspace.tenant_id, workspace_id=workspace.workspace_id, name=name, seq=seq
        )
        async with self._sessions() as session, session.begin():
            cursor = await _locked(session, CursorRow, key, new)
            cursor.seq = max(cursor.seq, seq)

    @_awaited_to_the_end
    async def _all[R: ScopedRow](self, query: Select[R]) -> Sequence[R]:
        async with self._sessions() as session:
            return (await session.scalars(query)).all()


def _lease(workspace: WorkspaceRef, key: str) -> tuple[ColumnElement[bool], ...]:
    return (*_in_workspace(LeaseRow, workspace), LeaseRow.key == key)


def _first_in_scope(policy: RunPolicy) -> ColumnElement[bool]:
    """Whether no earlier run of the outer run's scope holds it, as ``RunPolicy.holds`` says.

    A dead-lettered run holds the scopes of the blocking rules only, so their runs are checked
    apart from the other ordered rules', each with a plain list of statuses that the index can
    seek to.
    """
    blocking = policy.ordered & policy.blocking
    groups = [(policy.ordered - blocking, HOLDING), (blocking, HOLDING | {"dead"})]
    return or_(
        RunRow.rule.not_in(sorted(policy.ordered)),
        *(
            and_(RunRow.rule.in_(sorted(rules)), ~_held_back(statuses))
            for rules, statuses in groups
            if rules
        ),
    )


def _held_back(statuses: Collection[str]) -> Exists:
    """Whether an earlier run of the outer run's scope, in firing order, has one of ``statuses``.

    Firing order is ``fired_seq``, then creation position, as in ``scope_runs``. The status
    comes before ``fired_seq`` in ``ix_reflexr_runs_scope_head``, so each probe seeks once per
    status and never reads the scope's succeeded runs.
    """
    earlier = aliased(RunRow)
    return exists().where(
        earlier.tenant_id == RunRow.tenant_id,
        earlier.workspace_id == RunRow.workspace_id,
        earlier.rule == RunRow.rule,
        earlier.scope_key == RunRow.scope_key,
        earlier.status.in_(sorted(statuses)),
        tuple_(earlier.fired_seq, earlier.position) < tuple_(RunRow.fired_seq, RunRow.position),
    )
