"""Workspaces: tenant-scoped handles over a workspace's log, runs and rule progress (ADR-0016).

Every write goes through a :class:`Workspace` handle, inside one storage transaction: publishing
events, giving feedback, and operating runs. Each write is attributed to the handle's actor, and
each is traced (ADR-0018).
"""

import asyncio
from collections import Counter
from collections.abc import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Collection,
    Coroutine,
    Generator,
    Iterable,
    Mapping,
    Sequence,
)
from contextlib import AbstractContextManager, asynccontextmanager, contextmanager, nullcontext
from contextvars import ContextVar, copy_context
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from opentelemetry.metrics import MeterProvider
from opentelemetry.trace import Span, SpanKind, TracerProvider
from pydantic import BaseModel, ConfigDict, JsonValue

from reflexr.core import (
    SYSTEM_EVENTS,
    Actor,
    Causation,
    ChainTarget,
    Envelope,
    EvaluationError,
    Event,
    EventId,
    Feedback,
    FeedbackGiven,
    FeedbackTarget,
    InvalidState,
    NotFound,
    Predicates,
    Rule,
    RuleName,
    RuleProgress,
    Run,
    RunCancelled,
    RunId,
    RunRequeued,
    RunSkipped,
    RunStatus,
    RunTarget,
    ScopeKey,
    TenantId,
    UnknownEvent,
    ValidationFailed,
    WorkspaceId,
    begin,
    cancel,
    checkpoint,
    event_types,
    new_event_id,
    reset,
    retry,
    skip,
    type_of,
)
from reflexr.core.errors import DepthExceeded, Forbidden
from reflexr.telemetry import (
    Metric,
    Telemetry,
    actor_attributes,
    chain_attributes,
    current_traceparent,
    workspace_attributes,
)
from reflexr.telemetry import attributes as a
from reflexr.telemetry.metrics import EVENTS_PUBLISHED, FEEDBACK, RUNS
from reflexr.telemetry.telemetry import Attributes
from reflexr.workspace.memory import Clock, utc_now
from reflexr.workspace.schedules import Schedule
from reflexr.workspace.storage import Entry, Storage, Transaction, WorkspaceRef

type _Transition = Callable[[Run, datetime], tuple[Run, RunRequeued | RunSkipped | RunCancelled]]

Authorize = Callable[[TenantId, WorkspaceId, Actor], Awaitable[bool]]
"""Decides whether an actor may use a workspace of its tenant.

The surfaces that serve workspaces, the FastAPI router and the MCP server, take one as their
``authorize`` hook, and ask it before opening a workspace for a request.
"""


@dataclass(frozen=True)
class Published:
    """The outcome of publishing an event."""

    envelope: Envelope
    """The event's envelope: the new one, or the one already logged with its id."""

    duplicate: bool = False
    """Whether the id was already in the log, so nothing was appended."""


class OpenTransactions:
    """Counts the storage calls a task's workspace handles have in flight.

    Each is a transaction: a write's, or on SQL a read's too, which on SQLite holds the
    database's write lock. The executor runs each action in a task started by :meth:`task`, and
    stops it with :meth:`cancel`, which waits until none is in flight (ADR-0027): a transaction
    cancelled part-way can leave its connection holding the database's locks, or lose the
    connection to the pool. A subscription is not counted, since it waits between reads for as
    long as nothing is logged.
    """

    def __init__(self) -> None:
        self._open = 0
        self._closed = asyncio.Event()

    def task[T](self, coroutine: Coroutine[Any, Any, T]) -> asyncio.Task[T]:
        """Run a coroutine in a task whose transactions are counted here."""
        context = copy_context()
        context.run(_open_transactions.set, self)
        return asyncio.create_task(coroutine, context=context)

    async def cancel(self, task: asyncio.Task[Any]) -> None:
        """Cancel a task started by :meth:`task` as soon as it has no storage call in flight."""
        while self._open:
            self._closed.clear()
            await self._closed.wait()
        task.cancel()  # nothing is awaited between the check and this, so none has opened

    @contextmanager
    def opening(self) -> Generator[None]:
        """Count a storage call from before it begins until after it ends."""
        self._open += 1
        try:
            yield
        finally:
            self._open -= 1
            self._closed.set()


_open_transactions: ContextVar[OpenTransactions | None] = ContextVar(
    "reflexr_open_transactions", default=None
)


class RuleStatus(BaseModel):
    """A rule's progress in a workspace."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    rule: RuleName
    enabled: bool
    """Whether the reactor evaluates the rule. A disabled rule's cursor holds."""

    cursor: int
    lag: int
    """How many envelopes the rule is behind the head of the log."""

    generation: int
    dead_letters: int


class ScheduleStatus(BaseModel):
    """When a schedule last ticked in a workspace, and when it ticks next."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schedule: str
    last_tick: datetime | None
    next_tick: datetime | None


class Workspaces:
    """Opens scoped :class:`Workspace` handles over one storage, and holds the rules.

    Args:
        storage: Where workspaces are kept.
        events: The event types clients may publish. Others are rejected even if they are
            registered, so clients cannot publish arbitrary types. ``None`` accepts every
            registered type. reflexr's own events can never be published.
        emitted: Event types only runs may publish, such as an incident a triage agent opens.
            Clients, over REST, the WebSocket or MCP, are refused them.
        rules: The rules every workspace evaluates. Each is checked against the event types
            and predicates when the workspaces are created, so a mistake fails at startup. A
            disabled rule is registered and checked too, but the reactor leaves it be.
        predicates: The Python predicates rules refer to, by name. They must be pure.
        schedules: The schedules that publish ticks into the workspaces.
        clock: Returns the current time for run transitions. Defaults to the system clock.
        max_depth: The deepest causal chain an event may extend: a run's events beyond it are
            rejected and firings beyond it refused, so workflows that trigger themselves stop.
        tracer_provider: Where spans go. Defaults to OpenTelemetry's global provider.
        meter_provider: Where metrics go. Defaults to OpenTelemetry's global provider.

    Raises:
        InvalidRule: If a rule refers to an event type, field or predicate that does not exist.
        ValueError: If two rules, or two schedules, have the same name.
    """

    def __init__(
        self,
        storage: Storage,
        *,
        events: Iterable[type[Event]] | None = None,
        emitted: Iterable[type[Event]] = (),
        rules: Iterable[Rule] = (),
        predicates: Predicates | None = None,
        schedules: Iterable[Schedule] = (),
        clock: Clock = utc_now,
        max_depth: int = 8,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
    ) -> None:
        accepted = None if events is None else {t.event_type: t for t in events}
        from_runs = {t.event_type: t for t in emitted}
        known = {**(event_types() if accepted is None else accepted), **from_runs}
        chosen = dict(predicates or {})
        named: dict[RuleName, Rule] = {}
        for rule in rules:
            if rule.name in named:
                raise ValueError(f"two rules are named {rule.name!r}")
            rule.check(events=known, predicates=chosen)
            named[rule.name] = rule
        timetables: dict[str, Schedule] = {}
        for schedule in schedules:
            if schedule.name in timetables:
                raise ValueError(f"two schedules are named {schedule.name!r}")
            timetables[schedule.name] = schedule
        self._context = _Context(
            storage=storage,
            types=None if accepted is None else frozenset(accepted),
            emitted=frozenset(from_runs),
            rules=named,
            predicates=chosen,
            schedules=timetables,
            clock=clock,
            max_depth=max_depth,
            telemetry=Telemetry(tracer_provider=tracer_provider, meter_provider=meter_provider),
        )

    @property
    def storage(self) -> Storage:
        """Where the workspaces are kept."""
        return self._context.storage

    @property
    def rules(self) -> Mapping[RuleName, Rule]:
        """The rules every workspace evaluates, by name."""
        return self._context.rules

    @property
    def schedules(self) -> Mapping[str, Schedule]:
        """The schedules that publish ticks, by name."""
        return self._context.schedules

    @property
    def predicates(self) -> Predicates:
        """The predicates rules refer to, by name."""
        return self._context.predicates

    @property
    def clock(self) -> Clock:
        """Returns the current time."""
        return self._context.clock

    @property
    def max_depth(self) -> int:
        """The deepest causal chain an event may extend."""
        return self._context.max_depth

    @property
    def telemetry(self) -> Telemetry:
        """The tracer and instruments reflexr records with."""
        return self._context.telemetry

    async def open(
        self, tenant_id: TenantId, workspace_id: WorkspaceId, *, actor: Actor
    ) -> "Workspace":
        """Return a handle on a tenant's workspace, acting as ``actor``.

        This is the only place a tenant id enters; nothing on the handle can reach another
        tenant.
        """
        return Workspace(self._context, WorkspaceRef(tenant_id, workspace_id), actor)


@dataclass(frozen=True)
class _Context:
    storage: Storage
    types: frozenset[str] | None
    emitted: frozenset[str]
    rules: Mapping[RuleName, Rule]
    predicates: Predicates
    schedules: Mapping[str, Schedule]
    clock: Clock
    max_depth: int
    telemetry: Telemetry


@dataclass(frozen=True)
class _Cause:
    causation: Causation
    correlation_id: str


class Workspace:
    """A handle on one tenant's workspace, bound to the actor it acts as.

    Create handles with :meth:`Workspaces.open`, derive handles for other actors with
    :meth:`as_actor`, and handles whose events a run caused with :meth:`caused_by`.
    """

    def __init__(
        self,
        context: _Context,
        ref: WorkspaceRef,
        actor: Actor,
        cause: _Cause | None = None,
    ) -> None:
        self._context = context
        self._ref = ref
        self._actor = actor
        self._cause = cause

    @property
    def tenant_id(self) -> TenantId:
        """The tenant that owns the workspace."""
        return self._ref.tenant_id

    @property
    def workspace_id(self) -> WorkspaceId:
        """The workspace's id."""
        return self._ref.workspace_id

    @property
    def actor(self) -> Actor:
        """Who this handle acts as."""
        return self._actor

    @property
    def telemetry(self) -> Telemetry:
        """The tracer and instruments reflexr records with, for actions' own spans."""
        return self._context.telemetry

    @property
    def causation(self) -> Causation | None:
        """What caused the events this handle publishes, if a run did."""
        return self._cause.causation if self._cause else None

    def as_actor(self, actor: Actor) -> "Workspace":
        """Return a handle on the same workspace that acts as ``actor``."""
        return Workspace(self._context, self._ref, actor, self._cause)

    def caused_by(self, causation: Causation, *, correlation_id: str) -> "Workspace":
        """Return a handle whose events a run caused, in that run's causal chain.

        The reactor gives each run such a handle, so the events its action emits record the
        firing and run behind them and count towards the chain's depth.
        """
        return Workspace(self._context, self._ref, self._actor, _Cause(causation, correlation_id))

    # ─── writes ───────────────────────────────────────────────────────────────

    async def publish(
        self, event: Event, *, id: EventId | None = None, correlation_id: str | None = None
    ) -> Published:
        """Publish an event into the workspace's log.

        Args:
            event: The event.
            id: Its id. Publishing an id that is already in the log appends nothing and returns
                the logged envelope, so producers can retry safely. Defaults to a new id.
            correlation_id: The causal chain to join: the id of the chain's first event. By
                default the event starts a new chain, unless this handle belongs to a run.

        Raises:
            NotFound: If the event's type is not accepted, or the chain does not exist.
            ValidationFailed: If ``correlation_id`` names an event that did not start its
                chain. The message names the chain it belongs to.
            Forbidden: If the event is one reflexr records itself.
            DepthExceeded: If a run's event would extend its chain beyond the limit.
        """
        [published] = await self.publish_many([event], ids=[id], correlation_id=correlation_id)
        return published

    async def publish_many(
        self,
        events: Sequence[Event],
        *,
        ids: Sequence[EventId | None] | None = None,
        correlation_id: str | None = None,
    ) -> list[Published]:
        """Publish events atomically and in order: all of them are logged, or none.

        Args:
            events: The events.
            ids: Their ids, in the same order; ``None`` for a new id.
            correlation_id: The causal chain every event joins, as in :meth:`publish`.

        Raises:
            ValueError: If ``ids`` is not as long as ``events``.
        """
        chosen = list(ids) if ids is not None else [None] * len(events)
        if len(chosen) != len(events):
            raise ValueError(f"{len(events)} events but {len(chosen)} ids")
        for event in events:
            self._check_publishable(event)
        self._check_depth()
        name = f"reflexr.publish {type_of(events[0])}" if len(events) == 1 else "reflexr.publish"
        with self._span(name, SpanKind.PRODUCER) as span:
            span.set_attribute(a.EVENT_COUNT, len(events))
            traceparent = current_traceparent()
            async with self._transaction() as transaction:
                chain = await self._chain(transaction, correlation_id)
                results = [
                    await self._publish_one(transaction, event, event_id, chain, traceparent)
                    for event, event_id in zip(events, chosen, strict=True)
                ]
            if len(results) == 1:
                envelope = results[0].envelope
                span.set_attributes(
                    {
                        a.EVENT_TYPE: envelope.event_type,
                        a.EVENT_ID: envelope.id,
                        a.EVENT_SEQ: envelope.seq,
                        a.DUPLICATE: results[0].duplicate,
                        **chain_attributes(envelope.correlation_id),
                    }
                )
        for result in results:
            self._record(
                EVENTS_PUBLISHED,
                {
                    a.EVENT_TYPE: result.envelope.event_type,
                    a.ACTOR_KIND: self._actor.kind,
                    a.DUPLICATE: result.duplicate,
                },
            )
        return results

    async def give_feedback(self, feedback: Feedback, *, on: FeedbackTarget) -> Envelope:
        """Record feedback on a run, a firing or a whole causal chain.

        The feedback joins the causal chain of what it is about, so it appears in that chain's
        session.

        Raises:
            ValidationFailed: If the feedback's type cannot be given on this kind of target, or
                a chain target names an event that did not start its chain.
            NotFound: If the target does not exist.
        """
        kind = on.kind
        if kind not in feedback.targets:
            raise ValidationFailed(
                f"{feedback.feedback_type} feedback cannot be given on a {kind}", []
            )
        self._check_depth()
        event = FeedbackGiven(
            feedback_type=feedback.feedback_type,
            target=on,
            value=feedback.model_dump(mode="json"),
        )
        with self._span(f"reflexr.feedback {feedback.feedback_type}") as span:
            span.set_attributes({a.FEEDBACK_TYPE: feedback.feedback_type, a.FEEDBACK_TARGET: kind})
            traceparent = current_traceparent()
            async with self._transaction() as transaction:
                chain = await self._target_chain(transaction, on)
                envelope = await self._append(
                    transaction, event, None, chain, traceparent, self.causation
                )
            span.set_attributes({a.EVENT_ID: envelope.id, **chain_attributes(chain)})
        self._record(
            FEEDBACK,
            {
                a.FEEDBACK_TYPE: feedback.feedback_type,
                a.FEEDBACK_TARGET: kind,
                a.ACTOR_KIND: self._actor.kind,
            },
        )
        return envelope

    async def retry_run(self, run_id: RunId) -> Run:
        """Make a run runnable now.

        A dead-lettered, cancelled or skipped run gets a fresh retry budget; a run waiting to
        retry stops waiting.

        Raises:
            NotFound: If the run does not exist.
            InvalidState: If the run succeeded or is running.
        """
        return await self._operate(run_id, "retry", lambda run, now: retry(run, now=now))

    async def skip_run(self, run_id: RunId, *, reason: str | None = None) -> Run:
        """Give up on a waiting or dead-lettered run, unblocking later runs of its scope.

        Raises:
            NotFound: If the run does not exist.
            InvalidState: If the run is running or has finished otherwise.
        """
        return await self._operate(
            run_id, "skip", lambda run, now: skip(run, now=now, reason=reason)
        )

    async def cancel_run(self, run_id: RunId, *, reason: str | None = None) -> Run:
        """Cancel a run that has not finished. A running attempt's result is discarded.

        Raises:
            NotFound: If the run does not exist.
            InvalidState: If the run has finished.
        """
        return await self._operate(
            run_id, "cancel", lambda run, now: cancel(run, now=now, reason=reason)
        )

    async def checkpoint_run(
        self, run_id: RunId, *, attempt: int, step: str, state: JsonValue
    ) -> Run:
        """Save a running run's progress after a completed step, and append ``run_progressed``.

        Actions call this through :meth:`Reaction.checkpoint
        <reflexr.workspace.Reaction.checkpoint>`, so a retry resumes after the last step.

        Args:
            run_id: The run.
            attempt: The attempt saving it, which must still be the run's current one.
            step: The step that completed.
            state: What resuming needs, as JSON.

        Raises:
            NotFound: If the run does not exist.
            InvalidState: If the run is no longer running this attempt: it was cancelled, or
                another executor took it over.
        """
        with self._span("reflexr.checkpoint_run") as span:
            span.set_attributes({a.RUN_ID: run_id, a.ATTEMPT: attempt})
            traceparent = current_traceparent()
            async with self._transaction() as transaction:
                run = await transaction.run(run_id)
                if run is None:
                    raise NotFound("run", run_id)
                if (run.status, run.attempts) != ("running", attempt):
                    raise InvalidState(f"attempt {attempt} of run {run_id} is no longer current")
                saved, event = checkpoint(run, now=self._context.clock(), step=step, state=state)
                await transaction.save_runs([saved])
                await self._append(
                    transaction, event, None, run.correlation_id, traceparent, run.causation
                )
        return saved

    async def replay_rule(
        self,
        rule: RuleName,
        *,
        from_seq: int = 0,
        mode: Literal["rebuild", "refire"] = "rebuild",
    ) -> RuleProgress:
        """Reset a rule to evaluate the log again from after ``from_seq``.

        Args:
            rule: The rule's name.
            from_seq: Where to start again; 0 is the beginning of the log.
            mode: ``"rebuild"`` recomputes the rule's state up to the head of the log without
                firing, then carries on as normal. ``"refire"`` fires again for everything it
                finds, creating new runs with new ids.

        Returns:
            The rule's new progress. The reactor does the evaluating.

        Raises:
            NotFound: If there is no such rule.
            ValidationFailed: If ``from_seq`` is not between 0 and the head of the log.
        """
        definition = self._context.rules.get(rule)
        if definition is None:
            raise NotFound("rule", rule)
        with self._span("reflexr.replay_rule") as span:
            span.set_attribute(a.RULE, rule)
            traceparent = current_traceparent()
            async with self._transaction() as transaction:
                head = await transaction.head_seq()
                if not 0 <= from_seq <= head:
                    raise ValidationFailed(
                        f"from_seq must be between 0 and the head of the log, {head}", []
                    )
                progress = await transaction.progress(rule) or begin(definition, head_seq=head)
                restarted, event = reset(
                    definition,
                    progress,
                    from_seq=from_seq,
                    replayed=True,
                    silent_through=head if mode == "rebuild" else 0,
                )
                await transaction.clear_states(rule)
                await transaction.save_progress(rule, restarted)
                await self._append(transaction, event, None, None, traceparent, None)
        return restarted

    # ─── reads ────────────────────────────────────────────────────────────────

    async def head_seq(self) -> int:
        """Return the ``seq`` of the latest envelope, or 0 if the log is empty."""
        with self._using_storage():
            return await self._context.storage.head_seq(self._ref)

    async def read(
        self,
        *,
        after_seq: int = 0,
        before_seq: int | None = None,
        types: Collection[str] | None = None,
        limit: int | None = None,
        last: int | None = None,
    ) -> list[Envelope]:
        """Return envelopes in the window ``after_seq < seq < before_seq``, in order.

        Args:
            after_seq: Only envelopes after this ``seq``.
            before_seq: Only envelopes before this ``seq``; ``None`` reads to the head.
            types: Only envelopes of these event types; ``None`` reads every type.
            limit: At most this many: the first ones in the window that match.
            last: At most this many: the last ones in the window that match, still in order.
                To page backwards, read ``last=n``, then ``last=n`` before the oldest ``seq``
                returned.

        Raises:
            ValidationFailed: If both ``limit`` and ``last`` are given, or a number is negative.
        """
        _check_page(after_seq=after_seq, before_seq=before_seq, limit=limit, last=last)
        with self._using_storage():
            return await self._context.storage.read(
                self._ref,
                after_seq=after_seq,
                before_seq=before_seq,
                types=types,
                limit=limit,
                last=last,
            )

    def subscribe(self, *, after_seq: int = 0) -> AsyncGenerator[Envelope]:
        """Yield envelopes after ``after_seq``, then each new one as it is logged."""
        return self._context.storage.subscribe(self._ref, after_seq=after_seq)

    async def run(self, run_id: RunId) -> Run:
        """Return a run.

        Raises:
            NotFound: If the run does not exist.
        """
        with self._using_storage():
            run = await self._context.storage.run(self._ref, run_id)
        if run is None:
            raise NotFound("run", run_id)
        return run

    async def runs(
        self,
        *,
        rule: RuleName | None = None,
        status: RunStatus | None = None,
        scope_key: ScopeKey | None = None,
        limit: int | None = None,
    ) -> list[Run]:
        """Return runs, newest first, optionally of one rule, status or scope."""
        with self._using_storage():
            return await self._context.storage.runs(
                self._ref, rule=rule, status=status, scope_key=scope_key, limit=limit
            )

    async def dead_letters(self, *, rule: RuleName | None = None) -> list[EvaluationError]:
        """Return the envelopes rules could not evaluate, oldest first."""
        with self._using_storage():
            return await self._context.storage.dead_letters(self._ref, rule=rule)

    async def schedule_ticks(self) -> dict[str, datetime]:
        """Return when each schedule last ticked in this workspace."""
        with self._using_storage():
            return await self._context.storage.schedules(self._ref)

    async def rule_progress(self) -> dict[RuleName, RuleProgress]:
        """Return each rule's progress: its cursor, generation and pending deadlines."""
        with self._using_storage():
            return await self._context.storage.progress(self._ref)

    async def rule_statuses(self) -> list[RuleStatus]:
        """Return each registered rule's status: enabled, cursor, lag, generation, dead letters.

        Every surface reports this, so they agree. A rule that has not evaluated the workspace
        yet is at cursor 0, and the progress of a rule no longer registered is left out.
        """
        head = await self.head_seq()
        progress = await self.rule_progress()
        letters = Counter(letter.rule for letter in await self.dead_letters())
        statuses: list[RuleStatus] = []
        for name, rule in self._context.rules.items():
            cursor, generation = (
                (progress[name].cursor, progress[name].generation) if name in progress else (0, 0)
            )
            statuses.append(
                RuleStatus(
                    rule=name,
                    enabled=rule.enabled,
                    cursor=cursor,
                    lag=head - cursor,
                    generation=generation,
                    dead_letters=letters[name],
                )
            )
        return statuses

    async def schedule_statuses(self) -> list[ScheduleStatus]:
        """Return when each schedule that targets this workspace last ticked and ticks next.

        Every surface reports this, so they agree. A schedule that has not checked the
        workspace yet has neither.
        """
        ticks = await self.schedule_ticks()
        return [
            ScheduleStatus(
                schedule=name,
                last_tick=ticks.get(name),
                next_tick=next(schedule.after(ticks[name])) if name in ticks else None,
            )
            for name, schedule in self._context.schedules.items()
            if schedule.targets(self.tenant_id, self.workspace_id)
        ]

    # ─── internals ────────────────────────────────────────────────────────────

    def _check_publishable(self, event: Event) -> None:
        if isinstance(event, UnknownEvent):
            raise NotFound("event type", event.unknown_type)
        if isinstance(event, SYSTEM_EVENTS):
            raise Forbidden(f"{type_of(event)} events are recorded by reflexr, not published")
        name = type_of(event)
        if name in self._context.emitted:
            if self._cause is None:
                raise Forbidden(f"{name} events are published by runs, not clients")
            return
        types = self._context.types
        if types is not None and name not in types:
            raise NotFound("event type", name)

    async def _chain(self, transaction: Transaction, correlation_id: str | None) -> str | None:
        if correlation_id is None:
            return self._cause.correlation_id if self._cause else None
        return await _existing_chain(transaction, correlation_id)

    @staticmethod
    async def _target_chain(transaction: Transaction, target: FeedbackTarget) -> str:
        if isinstance(target, ChainTarget):
            return await _existing_chain(transaction, target.correlation_id)
        # A firing's id is its run's id, so both resolve through the run.
        run_id = target.run_id if isinstance(target, RunTarget) else target.firing_id
        run = await transaction.run(run_id)
        if run is None:
            raise NotFound(target.kind, run_id)
        return run.correlation_id

    async def _publish_one(
        self,
        transaction: Transaction,
        event: Event,
        event_id: EventId | None,
        chain: str | None,
        traceparent: str | None,
    ) -> Published:
        if event_id is not None:
            existing = await transaction.envelope(event_id)
            if existing is not None:
                return Published(existing, duplicate=True)
        appended = await self._append(
            transaction, event, event_id, chain, traceparent, self.causation
        )
        return Published(appended)

    def _check_depth(self) -> None:
        causation = self.causation
        if causation is not None and causation.depth > self._context.max_depth:
            raise DepthExceeded(causation.depth, self._context.max_depth)

    async def _append(
        self,
        transaction: Transaction,
        event: Event,
        event_id: EventId | None,
        chain: str | None,
        traceparent: str | None,
        causation: Causation | None,
    ) -> Envelope:
        await meet_rules(transaction, self._context.rules.values())
        entry = Entry(
            id=event_id or new_event_id(),
            actor=self._actor,
            event=event,
            causation=causation,
            correlation_id=chain,
            traceparent=traceparent,
        )
        [envelope] = await transaction.append([entry])
        return envelope

    async def _operate(
        self,
        run_id: RunId,
        action: Literal["retry", "skip", "cancel"],
        transition: _Transition,
    ) -> Run:
        with self._span(f"reflexr.{action}_run") as span:
            span.set_attribute(a.RUN_ID, run_id)
            traceparent = current_traceparent()
            async with self._transaction() as transaction:
                run = await transaction.run(run_id)
                if run is None:
                    raise NotFound("run", run_id)
                updated, event = transition(run, self._context.clock())
                await transaction.save_runs([updated])
                await self._append(
                    transaction, event, None, run.correlation_id, traceparent, run.causation
                )
            span.set_attributes({a.RULE: run.rule, **chain_attributes(run.correlation_id)})
        self._record(
            RUNS, {a.RULE: run.rule, a.RUN_STATUS: updated.status, a.ACTOR_KIND: self._actor.kind}
        )
        return updated

    @asynccontextmanager
    async def _transaction(self) -> AsyncGenerator[Transaction]:
        """Begin a transaction on the workspace, counted if the task counts its storage calls."""
        with self._using_storage():
            async with self._context.storage.transaction(self._ref) as transaction:
                yield transaction

    def _using_storage(self) -> AbstractContextManager[None]:
        """Count a storage call, if the task counts them (:class:`OpenTransactions`)."""
        counted = _open_transactions.get()
        return counted.opening() if counted else nullcontext()

    def _span(self, name: str, kind: SpanKind = SpanKind.INTERNAL) -> AbstractContextManager[Span]:
        return self._context.telemetry.tracer.start_as_current_span(
            name,
            kind=kind,
            attributes={
                **workspace_attributes(self.tenant_id, self.workspace_id),
                **actor_attributes(self._actor),
            },
        )

    def _record(self, metric: Metric, attributes: Attributes) -> None:
        self._context.telemetry.record(
            metric,
            1,
            tenant_id=self.tenant_id,
            workspace_id=self.workspace_id,
            attributes=attributes,
        )


def _check_page(
    *, after_seq: int, before_seq: int | None, limit: int | None, last: int | None
) -> None:
    """Refuse a read of the log that gives both ``limit`` and ``last``, or a negative number."""
    if limit is not None and last is not None:
        raise ValidationFailed("give limit or last, not both", [])
    given = {"after_seq": after_seq, "before_seq": before_seq, "limit": limit, "last": last}
    for name, value in given.items():
        if value is not None and value < 0:
            raise ValidationFailed(f"{name} cannot be negative", [])


async def _existing_chain(transaction: Transaction, correlation_id: str) -> str:
    """Return a chain's id, having checked that it is the id of the chain's first event."""
    envelope = await transaction.envelope(correlation_id)
    if envelope is None:
        raise NotFound("chain", correlation_id)
    if envelope.correlation_id != envelope.id:
        raise ValidationFailed(
            f"event {correlation_id} did not start a causal chain: it belongs to chain "
            f"{envelope.correlation_id}",
            [],
        )
    return correlation_id


async def meet_rules(transaction: Transaction, rules: Iterable[Rule]) -> None:
    """Start every rule at a new workspace's first event, before it is appended.

    In a new workspace every rule starts at the beginning, whatever its ``start``; a rule added
    later starts where the log is then.
    """
    if await transaction.head_seq() == 0:
        for rule in rules:
            await transaction.save_progress(rule.name, begin(rule, head_seq=0))
