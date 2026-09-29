"""Workspaces: tenant-scoped handles over a workspace's log, runs and rule progress (ADR-0016).

Every write goes through a :class:`Workspace` handle, inside one storage transaction: publishing
events, giving feedback, and operating runs. Each write is attributed to the handle's actor, and
each is traced (ADR-0018).
"""

from collections.abc import AsyncGenerator, Callable, Iterable, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from opentelemetry.metrics import MeterProvider
from opentelemetry.trace import Span, SpanKind, TracerProvider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

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
    NotFound,
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
    cancel,
    new_event_id,
    retry,
    skip,
    type_of,
)
from reflexr.core.errors import DepthExceeded, Forbidden
from reflexr.telemetry import (
    Metric,
    MetricsDetail,
    Telemetry,
    actor_attributes,
    chain_attributes,
    workspace_attributes,
)
from reflexr.telemetry import attributes as a
from reflexr.telemetry.metrics import EVENTS_PUBLISHED, FEEDBACK, RUNS
from reflexr.telemetry.telemetry import Attributes
from reflexr.workspace.memory import Clock, utc_now
from reflexr.workspace.storage import Entry, Storage, Transaction, WorkspaceRef

_W3C = TraceContextTextMapPropagator()

type _Transition = Callable[[Run, datetime], tuple[Run, RunRequeued | RunSkipped | RunCancelled]]


@dataclass(frozen=True)
class Published:
    """The outcome of publishing an event."""

    envelope: Envelope
    """The event's envelope: the new one, or the one already logged with its id."""

    duplicate: bool = False
    """Whether the id was already in the log, so nothing was appended."""


class Workspaces:
    """Opens scoped :class:`Workspace` handles over one storage.

    Args:
        storage: Where workspaces are kept.
        events: The event types clients may publish. Others are rejected even if they are
            registered, so clients cannot publish arbitrary types. ``None`` accepts every
            registered type. reflexr's own events can never be published.
        clock: Returns the current time for run transitions. Defaults to the system clock.
        max_depth: The deepest causal chain an event may extend: an event emitted by a run at
            this depth is rejected, so workflows that trigger themselves stop.
        tracer_provider: Where spans go. Defaults to OpenTelemetry's global provider.
        meter_provider: Where metrics go. Defaults to OpenTelemetry's global provider.
        metrics_detail: Which of tenant and workspace become metric attributes.
    """

    def __init__(
        self,
        storage: Storage,
        *,
        events: Iterable[type[Event]] | None = None,
        clock: Clock = utc_now,
        max_depth: int = 8,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        metrics_detail: MetricsDetail = "workspace",
    ) -> None:
        self._context = _Context(
            storage=storage,
            types=None if events is None else frozenset(t.event_type for t in events),
            clock=clock,
            max_depth=max_depth,
            telemetry=Telemetry(
                tracer_provider=tracer_provider,
                meter_provider=meter_provider,
                metrics_detail=metrics_detail,
            ),
        )

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
        name = f"reflexr.publish {type_of(events[0])}" if len(events) == 1 else "reflexr.publish"
        with self._span(name, SpanKind.PRODUCER) as span:
            span.set_attribute(a.EVENT_COUNT, len(events))
            traceparent = _traceparent()
            async with self._context.storage.transaction(self._ref) as transaction:
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
            ValidationFailed: If the feedback's type cannot be given on this kind of target.
            NotFound: If the target does not exist.
        """
        kind = on.kind
        if kind not in feedback.targets:
            raise ValidationFailed(
                f"{feedback.feedback_type} feedback cannot be given on a {kind}", []
            )
        event = FeedbackGiven(
            feedback_type=feedback.feedback_type,
            target=on,
            value=feedback.model_dump(mode="json"),
        )
        with self._span(f"reflexr.feedback {feedback.feedback_type}") as span:
            span.set_attributes({a.FEEDBACK_TYPE: feedback.feedback_type, a.FEEDBACK_TARGET: kind})
            traceparent = _traceparent()
            async with self._context.storage.transaction(self._ref) as transaction:
                chain = await self._target_chain(transaction, on)
                envelope = await self._append(transaction, event, None, chain, traceparent)
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

    # ─── reads ────────────────────────────────────────────────────────────────

    async def head_seq(self) -> int:
        """Return the ``seq`` of the latest envelope, or 0 if the log is empty."""
        return await self._context.storage.head_seq(self._ref)

    async def read(self, *, after_seq: int = 0, limit: int | None = None) -> list[Envelope]:
        """Return envelopes after ``after_seq``, in order."""
        return await self._context.storage.read(self._ref, after_seq=after_seq, limit=limit)

    def subscribe(self, *, after_seq: int = 0) -> AsyncGenerator[Envelope]:
        """Yield envelopes after ``after_seq``, then each new one as it is logged."""
        return self._context.storage.subscribe(self._ref, after_seq=after_seq)

    async def run(self, run_id: RunId) -> Run:
        """Return a run.

        Raises:
            NotFound: If the run does not exist.
        """
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
        return await self._context.storage.runs(
            self._ref, rule=rule, status=status, scope_key=scope_key, limit=limit
        )

    async def dead_letters(self, *, rule: RuleName | None = None) -> list[EvaluationError]:
        """Return the envelopes rules could not evaluate, oldest first."""
        return await self._context.storage.dead_letters(self._ref, rule=rule)

    async def rule_progress(self) -> dict[RuleName, RuleProgress]:
        """Return each rule's progress: its cursor, generation and pending deadlines."""
        return await self._context.storage.progress(self._ref)

    # ─── internals ────────────────────────────────────────────────────────────

    def _check_publishable(self, event: Event) -> None:
        if isinstance(event, UnknownEvent):
            raise NotFound("event type", event.unknown_type)
        if isinstance(event, SYSTEM_EVENTS):
            raise Forbidden(f"{type_of(event)} events are recorded by reflexr, not published")
        types = self._context.types
        if types is not None and type_of(event) not in types:
            raise NotFound("event type", type_of(event))

    async def _chain(self, transaction: Transaction, correlation_id: str | None) -> str | None:
        if correlation_id is None:
            return self._cause.correlation_id if self._cause else None
        if await transaction.envelope(correlation_id) is None:
            raise NotFound("chain", correlation_id)
        return correlation_id

    @staticmethod
    async def _target_chain(transaction: Transaction, target: FeedbackTarget) -> str:
        if isinstance(target, ChainTarget):
            if await transaction.envelope(target.correlation_id) is None:
                raise NotFound("chain", target.correlation_id)
            return target.correlation_id
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
        return Published(await self._append(transaction, event, event_id, chain, traceparent))

    async def _append(
        self,
        transaction: Transaction,
        event: Event,
        event_id: EventId | None,
        chain: str | None,
        traceparent: str | None,
    ) -> Envelope:
        causation = self.causation
        if causation is not None and causation.depth > self._context.max_depth:
            raise DepthExceeded(causation.depth, self._context.max_depth)
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
            traceparent = _traceparent()
            async with self._context.storage.transaction(self._ref) as transaction:
                run = await transaction.run(run_id)
                if run is None:
                    raise NotFound("run", run_id)
                updated, event = transition(run, self._context.clock())
                await transaction.save_runs([updated])
                await self._append(transaction, event, None, run.correlation_id, traceparent)
            span.set_attributes({a.RULE: run.rule, **chain_attributes(run.correlation_id)})
        self._record(
            RUNS, {a.RULE: run.rule, a.RUN_STATUS: updated.status, a.ACTOR_KIND: self._actor.kind}
        )
        return updated

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


def _traceparent() -> str | None:
    """Return the W3C trace context of the current span, if there is one."""
    carrier: dict[str, str] = {}
    _W3C.inject(carrier)
    return carrier.get("traceparent")
