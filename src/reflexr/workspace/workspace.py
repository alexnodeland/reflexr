"""Workspaces: tenant-scoped handles over a workspace's log, runs and rule progress (ADR-0016).

Every write goes through a :class:`Workspace` handle, inside one storage transaction: publishing
events, giving feedback, operating runs, and changing stored rules. Each write is attributed to
the handle's actor, and each is traced (ADR-0018).
"""

from collections import Counter
from collections.abc import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Collection,
    Iterable,
    Mapping,
    Sequence,
)
from contextlib import AbstractContextManager, aclosing
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from opentelemetry.metrics import MeterProvider
from opentelemetry.trace import Span, SpanKind, TracerProvider
from pydantic import BaseModel, ConfigDict, JsonValue

from reflexr.core import (
    DEFAULT_REGISTRY,
    FINISHED,
    MAX_STORED_RULES,
    Actor,
    Causation,
    ChainTarget,
    Envelope,
    EvaluationError,
    Event,
    EventId,
    EventRegistry,
    Feedback,
    FeedbackGiven,
    FeedbackTarget,
    InvalidRule,
    InvalidState,
    NotFound,
    Predicates,
    Rule,
    RuleArchived,
    RuleChange,
    RuleInstalled,
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
    StoredRule,
    StoredRules,
    TenantId,
    UnknownEvent,
    ValidationFailed,
    WorkspaceId,
    begin,
    cancel,
    check_event_name,
    check_provenance,
    check_stored,
    checkpoint,
    new_event_id,
    reset,
    retry,
    scope_key,
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
    untraced,
    workspace_attributes,
)
from reflexr.telemetry import attributes as a
from reflexr.telemetry.metrics import EVENTS_PUBLISHED, FEEDBACK, RUNS
from reflexr.telemetry.telemetry import Attributes
from reflexr.workspace.schedules import Schedule
from reflexr.workspace.storage import Clock, Entry, Storage, Transaction, WorkspaceRef, utc_now

type _Transition = Callable[[Run, datetime], tuple[Run, RunRequeued | RunSkipped | RunCancelled]]

Authorize = Callable[[TenantId, WorkspaceId, Actor], Awaitable[bool]]
"""Decides whether an actor may use a workspace of its tenant.

The surfaces that serve workspaces, the FastAPI router and the MCP server, take one as their
``authorize`` hook, and pass it to :meth:`Workspaces.open` for every request that names a
workspace.
"""


@dataclass(frozen=True)
class Published:
    """The outcome of publishing an event."""

    envelope: Envelope
    """The event's envelope: the new one, or the one already logged with its id."""

    duplicate: bool = False
    """Whether the id was already in the log, so nothing was appended."""


@dataclass(frozen=True)
class RuleVersion:
    """The outcome of a change to a stored rule."""

    stored: StoredRule
    """The stored rule as the change left it, or as it already was for a duplicate."""

    seq: int
    """The ``seq`` of the change's fact, ``reflexr:rule_installed`` or ``reflexr:rule_archived``.

    For a duplicate, the head of the log: nothing has changed the rule since its version's fact,
    so reading on from here misses nothing about it.
    """

    duplicate: bool = False
    """Whether the rule was already as the change would leave it, so nothing changed."""


class WorkspaceRule(BaseModel):
    """One of a workspace's rules: a code rule, or the current version of an active stored one."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    rule: Rule
    origin: Literal["code", "stored"]
    """``code``: registered in code, for every workspace. ``stored``: installed in this one."""

    version: int | None = None
    """A stored rule's current version. None for a code rule."""

    provenance: dict[str, JsonValue] | None = None
    """Where a stored rule's current version came from, as its installer said. None for a code
    rule."""


class RuleStatus(BaseModel):
    """A rule's progress in a workspace."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    rule: RuleName
    origin: Literal["code", "stored"]
    version: int | None = None
    """A stored rule's current version. None for a code rule."""

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
            registered, so clients cannot publish arbitrary types. ``None`` accepts every type
            in ``registry``. reflexr's own events can never be published.
        emitted: Event types only runs may publish, such as an incident a triage agent opens.
            Clients, over REST, the WebSocket or MCP, are refused them.
        registry: The namespaces whose types these workspaces accept. ``events`` and ``emitted``
            must be in it. Applications in one process each use their own, so neither
            accepts the other's types.
        rules: The rules every workspace evaluates. Each is checked against the event types
            and predicates when the workspaces are created, so a mistake fails at startup. A
            disabled rule is registered and checked too, but the reactor leaves it be.
        predicates: The Python predicates rules refer to, by name. They must be pure.
        stored_rules: What rules installed in a workspace at runtime may do. Without it, every
            change to a stored rule is forbidden, and no workspace reads any.
        schedules: The schedules that publish ticks into the workspaces.
        clock: Returns the current time for run transitions. Defaults to the system clock.
        max_depth: The deepest causal chain an event may extend: a run's events beyond it are
            rejected and firings beyond it refused, so workflows that trigger themselves stop.
        tracer_provider: Where spans go. Defaults to OpenTelemetry's global provider.
        meter_provider: Where metrics go. Defaults to OpenTelemetry's global provider.

    Raises:
        InvalidRule: If a rule refers to an event type, field or predicate that does not exist.
        ValueError: If two rules, or two schedules, have the same name, a type in ``events`` or
            ``emitted`` is not in ``registry``, or a rule is in a namespace of stored rules.
    """

    def __init__(
        self,
        storage: Storage,
        *,
        events: Iterable[type[Event]] | None = None,
        emitted: Iterable[type[Event]] = (),
        registry: EventRegistry = DEFAULT_REGISTRY,
        rules: Iterable[Rule] = (),
        predicates: Predicates | None = None,
        stored_rules: StoredRules | None = None,
        schedules: Iterable[Schedule] = (),
        clock: Clock = utc_now,
        max_depth: int = 8,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
    ) -> None:
        chosen_types = {t.event_type: t for t in events or ()}
        from_runs = {t.event_type: t for t in emitted}
        for name, event_type in {**chosen_types, **from_runs}.items():
            if registry.get(name) is not event_type:
                raise ValueError(f"{name} is not in the registry these workspaces use")
        accepted = registry if events is None else chosen_types
        known = {**accepted, **from_runs}
        chosen = dict(predicates or {})
        stored = stored_rules.namespaces if stored_rules else frozenset[str]()
        named: dict[RuleName, Rule] = {}
        for rule in rules:
            if rule.name in named:
                raise ValueError(f"two rules are named {rule.name!r}")
            namespace = rule.name.partition(":")[0]
            if namespace in stored:
                raise ValueError(f"rule {rule.name!r} is in {namespace!r}, which stored rules use")
            rule.check(events=known, predicates=chosen)
            named[rule.name] = rule
        timetables: dict[str, Schedule] = {}
        for schedule in schedules:
            if schedule.name in timetables:
                raise ValueError(f"two schedules are named {schedule.name!r}")
            timetables[schedule.name] = schedule
        self._context = _Context(
            storage=storage,
            accepted=accepted,
            emitted=frozenset(from_runs),
            known=known,
            rules=named,
            code={name: WorkspaceRule(rule=rule, origin="code") for name, rule in named.items()},
            predicates=chosen,
            stored_rules=stored_rules,
            schedules=timetables,
            clock=clock,
            max_depth=max_depth,
            telemetry=Telemetry(
                tracer_provider=tracer_provider,
                meter_provider=meter_provider,
                stored_namespaces=stored,
            ),
        )

    @property
    def storage(self) -> Storage:
        """Where the workspaces are kept."""
        return self._context.storage

    @property
    def rules(self) -> Mapping[RuleName, Rule]:
        """The code rules, which every workspace evaluates, by name."""
        return self._context.rules

    @property
    def stored_rules(self) -> StoredRules | None:
        """What stored rules may do, or None if they are off."""
        return self._context.stored_rules

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

    async def rules_in(self, at: WorkspaceRef | Transaction) -> dict[RuleName, WorkspaceRule]:
        """Return a workspace's rules: the code rules, then its active stored rules, by name.

        Everything that needs a workspace's rules finds them here, and nothing is cached.
        Without ``stored_rules`` there are only the code rules, and nothing to read; with it,
        only stored rules in its namespaces, so a namespace a deploy dropped is hidden.

        Args:
            at: A transaction on the workspace, to read its stored rules as the transaction
                sees them, or the workspace, to read them with a storage call of their own.
        """
        return await self._context.rules_in(at)

    async def open(
        self,
        tenant_id: TenantId,
        workspace_id: WorkspaceId,
        *,
        actor: Actor,
        authorize: Authorize | None = None,
    ) -> "Workspace":
        """Return a handle on a tenant's workspace, acting as ``actor``.

        This is the only place a tenant id enters; nothing on the handle can reach another
        tenant. The surfaces pass their ``authorize`` hook, so each refuses a workspace alike.

        Raises:
            Forbidden: If ``authorize`` is given and refuses the actor this workspace.
        """
        if authorize is not None and not await authorize(tenant_id, workspace_id, actor):
            raise Forbidden("this workspace is not yours to use")
        return Workspace(self._context, WorkspaceRef(tenant_id, workspace_id), actor)

    def schedules_for(self, tenant_id: TenantId) -> list[Schedule]:
        """Return the schedules that tick in a tenant's workspaces, as the tenant may see them.

        A schedule that targets particular workspaces lists only the tenant's own, so no tenant
        sees another's ids; one that targets none of them is left out.
        """
        seen: list[Schedule] = []
        for schedule in self._context.schedules.values():
            if schedule.workspaces == "all":
                seen.append(schedule)
            elif own := tuple(t for t in schedule.workspaces if t[0] == tenant_id):
                seen.append(schedule.model_copy(update={"workspaces": own}))
        return seen


@dataclass(frozen=True)
class _Context:
    storage: Storage
    accepted: Mapping[str, type[Event]]
    emitted: frozenset[str]
    known: Mapping[str, type[Event]]
    """The event types rules may watch: those clients may publish, and those only runs may."""

    rules: Mapping[RuleName, Rule]
    code: Mapping[RuleName, WorkspaceRule]
    """The code rules, as :meth:`rules_in` returns them."""

    predicates: Predicates
    stored_rules: StoredRules | None
    schedules: Mapping[str, Schedule]
    clock: Clock
    max_depth: int
    telemetry: Telemetry

    async def rules_in(self, at: WorkspaceRef | Transaction) -> dict[RuleName, WorkspaceRule]:
        rules = dict(self.code)
        config = self.stored_rules
        if config is None:
            return rules
        stored = await (
            self.storage.stored_rules(at) if isinstance(at, WorkspaceRef) else at.stored_rules()
        )
        for row in stored:
            # A namespace a deploy dropped is hidden, as stored rules are without a configuration.
            if row.rule.name.partition(":")[0] in config.namespaces:
                rules[row.rule.name] = WorkspaceRule(
                    rule=row.rule, origin="stored", version=row.version, provenance=row.provenance
                )
        return rules


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
            async with self._context.storage.transaction(self._ref) as transaction:
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
        """Save a running run's progress after a step, and append ``reflexr:run_progressed``.

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
            async with self._context.storage.transaction(self._ref) as transaction:
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
            NotFound: If the workspace has no such rule.
            ValidationFailed: If ``from_seq`` is not between 0 and the head of the log.
        """
        with self._span("reflexr.replay_rule") as span:
            span.set_attribute(a.RULE, rule)
            traceparent = current_traceparent()
            async with self._context.storage.transaction(self._ref) as transaction:
                found = (await self._context.rules_in(transaction)).get(rule)
                if found is None:
                    raise NotFound("rule", rule)
                definition = found.rule
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

    # ─── stored rules ─────────────────────────────────────────────────────────

    async def install_rule(
        self, rule: Rule, *, provenance: Mapping[str, JsonValue] | None = None
    ) -> RuleVersion:
        """Install a stored rule: its first version, or the next version of an archived one.

        The rule starts at its ``reflexr:rule_installed`` fact, so it acts on what is logged
        after it. A rule installed again is reset there, continuing its generation, so no firing
        id repeats.

        Args:
            rule: The rule, in one of the namespaces ``StoredRules`` names.
            provenance: Where it came from, such as the artifact a person accepted: JSON that
                reflexr keeps on the rule and its fact, and does not read.

        Returns:
            Its new version, or, if it is installed as given already, that version as a
            duplicate.

        Raises:
            Forbidden: If stored rules are off, the name is a code rule's, or ``allow`` refuses
                the change.
            ValidationFailed: If :meth:`~reflexr.core.Rule.check`,
                :func:`~reflexr.core.check_stored` or :func:`~reflexr.core.check_provenance`
                finds problems, listing them all, or the workspace has as many active stored
                rules as it may.
            InvalidState: If the rule is installed already: update it instead.
        """
        kept = dict(provenance or {})
        config = await self._allow(RuleChange(kind="install", rule=rule.name, spec=rule))
        self._validate(rule, kept, config)
        with self._span("reflexr.install_rule") as span:
            span.set_attribute(a.RULE, rule.name)
            traceparent = current_traceparent()
            async with self._context.storage.transaction(self._ref) as transaction:
                current = await transaction.stored_rule(rule.name)
                version = 1 if current is None else current.version + 1
                installed = StoredRule(rule=rule, version=version, provenance=kept)
                if current is not None:
                    if _same(current, installed):
                        return RuleVersion(current, await transaction.head_seq(), duplicate=True)
                    if current.status == "active":
                        raise InvalidState(f"rule {rule.name} is installed already: update it")
                if len(await transaction.stored_rules()) >= MAX_STORED_RULES:
                    raise ValidationFailed(
                        f"the workspace has {MAX_STORED_RULES} active stored rules, the most it "
                        "may have",
                        [],
                    )
                seq = await self._save_version(transaction, installed, traceparent, restart=True)
        return RuleVersion(installed, seq)

    async def update_rule(
        self,
        rule: Rule,
        *,
        expected_version: int | None = None,
        provenance: Mapping[str, JsonValue] | None = None,
    ) -> RuleVersion:
        """Replace an active stored rule with a new version.

        A new condition or scope resets the rule at its ``reflexr:rule_installed`` fact, as a
        changed code rule is reset; any other change keeps its state.

        Args:
            rule: The new version, with the name of the rule it replaces.
            expected_version: The version it replaces, so a change someone else made first is
                not overwritten.
            provenance: Where it came from, as for :meth:`install_rule`.

        Returns:
            Its new version, or, if it is as given already, that version as a duplicate, whatever
            ``expected_version`` says, so a retry of an update that succeeded is no conflict.

        Raises:
            Forbidden: As for :meth:`install_rule`.
            ValidationFailed: If the checks :meth:`install_rule` makes of a rule find problems.
            NotFound: If no stored rule has the name.
            InvalidState: If the rule is not at ``expected_version``, which the message names,
                or is archived: install it again.
        """
        kept = dict(provenance or {})
        config = await self._allow(RuleChange(kind="update", rule=rule.name, spec=rule))
        self._validate(rule, kept, config)
        with self._span("reflexr.update_rule") as span:
            span.set_attribute(a.RULE, rule.name)
            traceparent = current_traceparent()
            async with self._context.storage.transaction(self._ref) as transaction:
                current = await _stored(transaction, rule.name)
                updated = StoredRule(rule=rule, version=current.version + 1, provenance=kept)
                if _same(current, updated):
                    return RuleVersion(current, await transaction.head_seq(), duplicate=True)
                _expect(current, expected_version)
                if current.status == "archived":
                    raise InvalidState(f"rule {rule.name} is archived: install it again")
                seq = await self._save_version(transaction, updated, traceparent)
        return RuleVersion(updated, seq)

    async def archive_rule(
        self, rule: RuleName, *, expected_version: int | None = None, reason: str | None = None
    ) -> RuleVersion:
        """Archive a stored rule, and cancel its unfinished runs.

        Its scope states are cleared and its progress kept, so a rule installed again under its
        name continues its generation. The runs' ``reflexr:run_cancelled`` facts follow the
        ``reflexr:rule_archived`` fact, in the same transaction.

        Args:
            rule: The stored rule's name.
            expected_version: The version it archives, as for :meth:`update_rule`.
            reason: Why, for the fact.

        Returns:
            The archived version, or, if the rule is archived already, that version as a
            duplicate.

        Raises:
            Forbidden: As for :meth:`install_rule`.
            NotFound: If no stored rule has the name.
            InvalidState: If the rule is not at ``expected_version``, which the message names.
        """
        await self._allow(RuleChange(kind="archive", rule=rule))
        with self._span("reflexr.archive_rule") as span:
            span.set_attribute(a.RULE, rule)
            traceparent = current_traceparent()
            async with self._context.storage.transaction(self._ref) as transaction:
                current = await _stored(transaction, rule)
                archived = current.model_copy(
                    update={"version": current.version + 1, "status": "archived"}
                )
                if _same(current, archived):
                    return RuleVersion(current, await transaction.head_seq(), duplicate=True)
                _expect(current, expected_version)
                # A stored rule has no scope fields, so each of its runs is in its one scope.
                runs = await transaction.scope_runs(rule, scope_key([]))
                unfinished = [run for run in runs if run.status not in FINISHED]
                await transaction.save_stored_rule(archived)
                await transaction.clear_states(rule)
                fact = await self._append(
                    transaction,
                    RuleArchived(
                        rule=rule,
                        version=archived.version,
                        reason=reason,
                        cancelled=len(unfinished),
                    ),
                    None,
                    await self._chain(transaction, None),
                    traceparent,
                    self.causation,
                )
                now = self._context.clock()
                for run in unfinished:
                    cancelled, event = cancel(run, now=now, reason="its rule was archived")
                    await transaction.save_runs([cancelled])
                    await self._append(
                        transaction, event, None, run.correlation_id, traceparent, run.causation
                    )
        for _ in unfinished:
            self._record(
                RUNS, {a.RULE: rule, a.RUN_STATUS: "cancelled", a.ACTOR_KIND: self._actor.kind}
            )
        return RuleVersion(archived, fact.seq)

    # ─── reads ────────────────────────────────────────────────────────────────

    async def head_seq(self) -> int:
        """Return the ``seq`` of the latest envelope, or 0 if the log is empty."""
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
        for name in types or ():
            _check_name(name)
        return await self._context.storage.read(
            self._ref,
            after_seq=after_seq,
            before_seq=before_seq,
            types=types,
            limit=limit,
            last=last,
        )

    async def subscribe(self, *, after_seq: int = 0) -> AsyncGenerator[Envelope]:
        """Yield envelopes after ``after_seq``, then each new one as it is logged.

        Reading the log is untraced, so a subscription that polls storage makes no trace per
        poll (ADR-0040); what the subscriber does with each envelope is traced as usual.
        """
        stream = self._context.storage.subscribe(self._ref, after_seq=after_seq)
        async with aclosing(stream) as envelopes:
            while True:
                with untraced():
                    envelope = await anext(envelopes, None)
                if envelope is None:
                    return
                yield envelope

    async def cursor(self, name: str) -> int:
        """Return how far a named consumer of the log has got: the ``seq`` it saved, or 0.

        A consumer, such as a feedback mirror, saves its cursor with :meth:`save_cursor` and
        carries on after it when it starts again.
        """
        return await self._context.storage.cursor(self._ref, name)

    async def save_cursor(self, name: str, seq: int) -> None:
        """Save how far a named consumer of the log has got: it is done with ``seq``.

        A cursor only moves forward: saving a ``seq`` below the saved one leaves it, so a
        consumer that runs in several processes, each at its own pace, cannot move it back.
        """
        await self._context.storage.save_cursor(self._ref, name, seq)

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

    async def schedule_ticks(self) -> dict[str, datetime]:
        """Return when each schedule last ticked in this workspace."""
        return await self._context.storage.schedules(self._ref)

    async def rule_progress(self) -> dict[RuleName, RuleProgress]:
        """Return each rule's progress: its cursor, generation and pending deadlines."""
        return await self._context.storage.progress(self._ref)

    async def get_rule(self, rule: RuleName) -> WorkspaceRule:
        """Return one of the workspace's rules, with a stored rule's version and provenance.

        Raises:
            NotFound: If the workspace has no such rule, as for a stored rule that is archived.
        """
        found = (await self._context.rules_in(self._ref)).get(rule)
        if found is None:
            raise NotFound("rule", rule)
        return found

    async def rule_statuses(self) -> list[RuleStatus]:
        """Return the status of each of the workspace's rules, code rules first.

        Every surface reports this, so they agree. A rule that has not evaluated the workspace
        yet is at cursor 0, and the progress of a rule the workspace no longer has, such as a
        code rule no longer registered or an archived stored rule, is left out.
        """
        rules = await self._context.rules_in(self._ref)
        # Progress next: the head only grows, so no cursor read before it is past it.
        progress = await self.rule_progress()
        head = await self.head_seq()
        letters = Counter(letter.rule for letter in await self.dead_letters())
        statuses: list[RuleStatus] = []
        for name, found in rules.items():
            cursor, generation = (
                (progress[name].cursor, progress[name].generation) if name in progress else (0, 0)
            )
            statuses.append(
                RuleStatus(
                    rule=name,
                    origin=found.origin,
                    version=found.version,
                    enabled=found.rule.enabled,
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
        name = _check_name(type_of(event))
        if isinstance(event, UnknownEvent):
            raise NotFound("event type", name)
        if name.startswith("reflexr:"):
            raise Forbidden(f"{name} events are recorded by reflexr, not published")
        if name in self._context.emitted:
            if self._cause is None:
                raise Forbidden(f"{name} events are published by runs, not clients")
            return
        if name not in self._context.accepted:
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
            async with self._context.storage.transaction(self._ref) as transaction:
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

    async def _allow(self, change: RuleChange) -> StoredRules:
        """Return the configuration of stored rules, if the actor may make the change.

        Raises:
            Forbidden: If stored rules are off, the name is a code rule's, or ``allow`` refuses.
        """
        config = self._context.stored_rules
        if config is None:
            raise Forbidden("stored rules are off in these workspaces")
        if change.rule in self._context.rules:
            raise Forbidden(f"rule {change.rule} is registered in code")
        if not await config.allow(self.tenant_id, self.workspace_id, self._actor, change):
            raise Forbidden(f"this change to rule {change.rule} is not yours to make")
        return config

    def _validate(
        self, rule: Rule, provenance: Mapping[str, JsonValue], config: StoredRules
    ) -> None:
        """Refuse a rule to store, with every problem its checks find.

        It is checked as code rules are, against these workspaces' event types, and then as a
        stored rule, with its provenance.

        Raises:
            ValidationFailed: Listing the problems.
        """
        context = self._context
        problems: list[JsonValue] = []
        try:
            rule.check(events=context.known, predicates=context.predicates)
        except InvalidRule as invalid:
            problems.extend(invalid.problems)
        problems.extend(check_stored(rule, config))
        problems.extend(check_provenance(provenance))
        if problems:
            raise ValidationFailed(f"rule {rule.name} cannot be stored", problems)

    async def _save_version(
        self,
        transaction: Transaction,
        stored: StoredRule,
        traceparent: str | None,
        *,
        restart: bool = False,
    ) -> int:
        """Save an installed or updated version, and return the ``seq`` of its fact.

        A rule new to the workspace begins at its fact. One it knows is reset there, with
        ``reflexr:rule_reset`` after the fact, if ``restart`` or its definition changed.
        """
        rule = stored.rule
        await transaction.save_stored_rule(stored)
        event = RuleInstalled(
            rule=rule.name,
            version=stored.version,
            spec=rule.model_dump(mode="json"),
            provenance=stored.provenance,
        )
        chain = await self._chain(transaction, None)
        fact = await self._append(transaction, event, None, chain, traceparent, self.causation)
        progress = await transaction.progress(rule.name)
        if progress is None:
            await transaction.save_progress(rule.name, begin(rule, head_seq=fact.seq))
        elif restart or progress.definition != rule.definition():
            restarted, reset_event = reset(rule, progress, from_seq=fact.seq)
            await transaction.clear_states(rule.name)
            await transaction.save_progress(rule.name, restarted)
            await self._append(
                transaction, reset_event, None, fact.correlation_id, traceparent, self.causation
            )
        return fact.seq

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


def _check_name(name: str) -> str:
    try:
        return check_event_name(name)
    except ValueError as error:
        raise ValidationFailed(str(error), []) from None


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


async def _stored(transaction: Transaction, name: RuleName) -> StoredRule:
    """Return a stored rule, active or archived.

    Raises:
        NotFound: If no stored rule has the name.
    """
    stored = await transaction.stored_rule(name)
    if stored is None:
        raise NotFound("stored rule", name)
    return stored


def _same(current: StoredRule, following: StoredRule) -> bool:
    """Whether a change's version is the current one but for its number, so a duplicate."""
    return (following.rule, following.status, following.provenance) == (
        current.rule,
        current.status,
        current.provenance,
    )


def _expect(current: StoredRule, expected_version: int | None) -> None:
    """Refuse a change whose caller expected another version of the rule.

    Raises:
        InvalidState: Naming the current version.
    """
    if expected_version is not None and current.version != expected_version:
        raise InvalidState(
            f"rule {current.rule.name} is at version {current.version}, not {expected_version}"
        )


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
    later starts where the log is then. These are the code rules: a stored rule comes later,
    since installing one appends its fact, and it begins there.
    """
    if await transaction.head_seq() == 0:
        for rule in rules:
            await transaction.save_progress(rule.name, begin(rule, head_seq=0))
