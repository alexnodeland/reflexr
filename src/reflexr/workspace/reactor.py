"""The reactor: the runtime that evaluates rules and executes their runs.

Deciding is exact (ADR-0005). One reactor at a time holds a workspace's evaluation lease, and
for each rule it reads the envelopes after the rule's cursor, lets core decide, and saves the
new state, the cursor, the firings' runs and reflexr's facts in one transaction. A crash before
the commit means the same envelopes are evaluated again against the same state.

Acting is at least once: see :mod:`reflexr.workspace.executor`.
"""

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from time import perf_counter
from typing import Any, Never, overload

from reflexr.core import (
    Evaluation,
    Fact,
    InvalidRule,
    Rule,
    SystemActor,
    Tick,
    begin,
    create_run,
    evaluate,
    needs,
    new_event_id,
    new_id,
    reset,
)
from reflexr.telemetry import Metric, current_traceparent, workspace_attributes
from reflexr.telemetry import attributes as a
from reflexr.telemetry.metrics import (
    EVALUATION_DURATION,
    EVALUATION_LAG,
    FIRINGS,
    RULE_ERRORS,
    RUNS,
    SCHEDULE_TICKS,
)
from reflexr.telemetry.telemetry import Attributes
from reflexr.workspace.actions import Action, RunContext
from reflexr.workspace.executor import Executor
from reflexr.workspace.schedules import SCHEDULER, Schedule, tick_id
from reflexr.workspace.storage import Entry, Transaction, WorkspaceRef
from reflexr.workspace.workspace import Workspaces, meet_rules

logger = logging.getLogger("reflexr.reactor")

EVALUATION_LEASE = "evaluate"
"""The lease key a reactor holds on a workspace while it evaluates it."""

REACTOR = SystemActor(name="reactor")
"""The actor of the facts the reactor records."""


@dataclass(frozen=True)
class Settled:
    """What :meth:`Reactor.settle` did."""

    ticks: int = 0
    """How many ticks schedules published."""

    firings: int = 0
    """How many times rules fired."""

    attempts: int = 0
    """How many run attempts finished."""


class Reactor[D]:
    """Evaluates the rules of a set of :class:`Workspaces` and executes their runs.

    Any number of reactors, in any number of processes, can share the work: leases give each
    workspace one evaluator and each run one executor at a time.

    Args:
        workspaces: The workspaces, with their storage and rules.
        actions: What rules run, by the name they refer to them by. Every rule's action must
            be here.
        deps: The application's dependencies, handed to every action in its
            :class:`~reflexr.workspace.Reaction`.
        holder: This reactor's name in leases. Defaults to a new random id; give each process
            its own.
        batch_size: How many envelopes one transaction evaluates for one rule. The transaction
            holds the workspace's lock, so this bounds how long publishing can wait.
        lease_ttl: How long a lease lasts unless renewed. A reactor that dies holding one
            releases it when it lapses, and its runs are attempted again.
        concurrency: How many runs this reactor executes at once.
        run_context: Entered around each run attempt, inside its span, such as
            ``reflexr.langfuse.langfuse_run`` to attribute runs in Langfuse.

    Raises:
        InvalidRule: If a rule's action is not among ``actions``.
    """

    @overload
    def __init__(
        self: "Reactor[None]",
        workspaces: Workspaces,
        *,
        actions: Mapping[str, Action[None]] = ...,
        holder: str | None = None,
        batch_size: int = 500,
        lease_ttl: timedelta = ...,
        concurrency: int = 10,
        run_context: RunContext | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self,
        workspaces: Workspaces,
        *,
        actions: Mapping[str, Action[D]],
        deps: D,
        holder: str | None = None,
        batch_size: int = 500,
        lease_ttl: timedelta = ...,
        concurrency: int = 10,
        run_context: RunContext | None = None,
    ) -> None: ...

    def __init__(
        self,
        workspaces: Workspaces,
        *,
        actions: Mapping[str, Action[Any]] | None = None,
        deps: Any = None,
        holder: str | None = None,
        batch_size: int = 500,
        lease_ttl: timedelta = timedelta(seconds=30),
        concurrency: int = 10,
        run_context: RunContext | None = None,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        chosen = actions or {}
        for rule in workspaces.rules.values():
            if rule.then.action not in chosen:
                raise InvalidRule(rule.name, [f"no action {rule.then.action!r}"])
        self._workspaces = workspaces
        self._holder = holder or new_id("reactor")
        self._batch_size = batch_size
        self._lease_ttl = lease_ttl
        self._executor: Executor[D] = Executor(
            workspaces,
            actions=chosen,
            deps=deps,
            holder=self._holder,
            lease_ttl=lease_ttl,
            concurrency=concurrency,
            run_context=run_context,
        )

    @property
    def holder(self) -> str:
        """This reactor's name in leases."""
        return self._holder

    async def execute(self, *, limit: int = 100) -> int:
        """Attempt the runs that are due, up to ``limit`` of them.

        Returns:
            How many attempts finished, whether they succeeded or failed.
        """
        return await self._executor.execute(limit=limit)

    async def settle(self, *, max_rounds: int = 100) -> Settled:
        """Evaluate and execute until nothing more happens now.

        Useful in tests and scripts. Runs waiting to retry later are left waiting.

        Raises:
            RuntimeError: If work is still happening after ``max_rounds`` rounds, as when
                rules keep triggering each other within the depth limit.
        """
        ticks = firings = attempts = 0
        for _ in range(max_rounds):
            ticked = await self.tick()
            fired = await self.evaluate()
            attempted = await self.execute()
            ticks, firings, attempts = ticks + ticked, firings + fired, attempts + attempted
            if not ticked and not fired and not attempted:
                return Settled(ticks=ticks, firings=firings, attempts=attempts)
        raise RuntimeError(f"still busy after {max_rounds} rounds")

    async def serve(self, *, poll_interval: timedelta = timedelta(seconds=1)) -> Never:
        """Tick, evaluate and execute until cancelled, checking for work every ``poll_interval``.

        A pass that fails, as when the database is briefly unreachable, is logged and the next
        one tries again: leases and transactions leave nothing half done.
        """
        while True:
            try:
                await self.tick()
                await self.evaluate()
                await self.execute()
            except Exception:
                logger.exception("a reactor pass failed; the next one will try again")
            await asyncio.sleep(poll_interval.total_seconds())

    async def tick(self) -> int:
        """Publish the ticks that are due, for every schedule and the workspaces it targets.

        A schedule's first check in a workspace starts its timetable there; its first tick
        comes at its next time after that.

        Returns:
            How many ticks were published.
        """
        storage = self._workspaces.storage
        now = self._workspaces.clock()
        known = await storage.workspaces()
        published = 0
        for schedule in self._workspaces.schedules.values():
            refs = (
                known
                if schedule.workspaces == "all"
                else [WorkspaceRef(tenant, workspace) for tenant, workspace in schedule.workspaces]
            )
            for ref in refs:
                published += await self._tick(ref, schedule, now)
        return published

    async def _tick(self, ref: WorkspaceRef, schedule: Schedule, now: datetime) -> int:
        last = (await self._workspaces.storage.schedules(ref)).get(schedule.name)
        if last is not None and not schedule.due(last, now):
            return 0  # the common case, checked without a transaction or a span
        telemetry = self._workspaces.telemetry
        with telemetry.tracer.start_as_current_span(
            f"reflexr.schedule {schedule.name}",
            attributes={
                **workspace_attributes(ref.tenant_id, ref.workspace_id),
                a.SCHEDULE: schedule.name,
            },
        ) as span:
            async with self._workspaces.storage.transaction(ref) as transaction:
                last = await transaction.schedule(schedule.name)
                due = [] if last is None else schedule.due(last, now)
                entries = [
                    Entry(
                        id=tick_id(schedule.name, at),
                        actor=SCHEDULER,
                        event=Tick(schedule=schedule.name, at=at),
                        traceparent=current_traceparent(),
                    )
                    for at in due
                    if await transaction.envelope(tick_id(schedule.name, at)) is None
                ]
                if entries:
                    await meet_rules(transaction, self._workspaces.rules.values())
                    await transaction.append(entries)
                await transaction.save_schedule(schedule.name, due[-1] if due else now)
            span.set_attribute(a.EVENT_COUNT, len(entries))
        if entries:
            telemetry.record(
                SCHEDULE_TICKS,
                len(entries),
                tenant_id=ref.tenant_id,
                workspace_id=ref.workspace_id,
                attributes={a.SCHEDULE: schedule.name},
            )
        return len(entries)

    async def evaluate(self, workspace: WorkspaceRef | None = None) -> int:
        """Evaluate every enabled rule over new envelopes until each is caught up with the log.

        Args:
            workspace: One workspace to evaluate. Defaults to every workspace with a log.

        Returns:
            How many times rules fired. A workspace whose lease another reactor holds is
            skipped.
        """
        storage = self._workspaces.storage
        refs = [workspace] if workspace is not None else await storage.workspaces()
        fired = 0
        for ref in refs:
            fired += await self._evaluate_workspace(ref)
        return fired

    async def _evaluate_workspace(self, ref: WorkspaceRef) -> int:
        storage = self._workspaces.storage
        if not await storage.acquire_lease(ref, EVALUATION_LEASE, self._holder, self._lease_ttl):
            return 0
        telemetry = self._workspaces.telemetry
        try:
            await self._record_lag(ref)
            started = perf_counter()
            with telemetry.tracer.start_as_current_span(
                "reflexr.evaluate",
                attributes=workspace_attributes(ref.tenant_id, ref.workspace_id),
            ) as span:
                fired, evaluated = await self._catch_up(ref)
                span.set_attributes({a.EVALUATED_RULES: sorted(evaluated), a.FIRING_COUNT: fired})
            telemetry.record(
                EVALUATION_DURATION,
                perf_counter() - started,
                tenant_id=ref.tenant_id,
                workspace_id=ref.workspace_id,
            )
        finally:
            await storage.release_lease(ref, EVALUATION_LEASE, self._holder)
        return fired

    async def _catch_up(self, ref: WorkspaceRef) -> tuple[int, set[str]]:
        """Evaluate batches until no rule has new envelopes, or the lease is lost."""
        storage = self._workspaces.storage
        fired = 0
        evaluated: set[str] = set()
        while True:
            advanced = False
            for rule in self._workspaces.rules.values():
                if not rule.enabled:
                    continue  # its cursor holds until it is enabled again
                evaluation = await self._evaluate_batch(ref, rule)
                if evaluation is None:
                    continue
                advanced = True
                evaluated.add(rule.name)
                fired += len(evaluation.firings)
                self._record_evaluation(ref, rule, evaluation)
            renewed = await storage.acquire_lease(
                ref, EVALUATION_LEASE, self._holder, self._lease_ttl
            )
            if not advanced or not renewed:
                return fired, evaluated

    async def _evaluate_batch(self, ref: WorkspaceRef, rule: Rule) -> Evaluation | None:
        """Evaluate one batch of a rule's new envelopes in one transaction.

        Returns None if the rule had nothing new.
        """
        workspaces = self._workspaces
        async with workspaces.storage.transaction(ref) as transaction:
            head = await transaction.head_seq()
            progress = await transaction.progress(rule.name)
            if progress is None:
                progress = begin(rule, head_seq=head)
                await transaction.save_progress(rule.name, progress)
            elif progress.definition != rule.definition():
                # A changed rule starts afresh at the head; replaying is how to reprocess.
                progress, event = reset(rule, progress, from_seq=head)
                await transaction.clear_states(rule.name)
                await transaction.save_progress(rule.name, progress)
                entry = Entry(
                    id=new_event_id(),
                    actor=REACTOR,
                    event=event,
                    traceparent=current_traceparent(),
                )
                await transaction.append([entry])
            envelopes = await transaction.read(after_seq=progress.cursor, limit=self._batch_size)
            if not envelopes:
                return None
            predicates = workspaces.predicates
            keys = needs(rule, progress, envelopes, predicates=predicates)
            states = await transaction.states(rule.name, keys)
            evaluation = evaluate(
                rule,
                progress,
                states,
                envelopes,
                predicates=predicates,
                max_depth=workspaces.max_depth,
            )
            await transaction.save_states(rule.name, evaluation.states)
            await transaction.save_progress(rule.name, evaluation.progress)
            if evaluation.errors:
                await transaction.dead_letter(evaluation.errors)
            now = workspaces.clock()
            await transaction.save_runs([create_run(f, now=now) for f in evaluation.firings])
            await self._append(transaction, evaluation.facts)
        return evaluation

    @staticmethod
    async def _append(transaction: Transaction, facts: tuple[Fact, ...]) -> None:
        traceparent = current_traceparent()
        await transaction.append(
            [
                Entry(
                    id=new_event_id(),
                    actor=REACTOR,
                    event=fact.event,
                    causation=fact.causation,
                    correlation_id=fact.correlation_id,
                    traceparent=traceparent,
                )
                for fact in facts
            ]
        )

    async def _record_lag(self, ref: WorkspaceRef) -> None:
        storage = self._workspaces.storage
        head = await storage.head_seq(ref)
        progress = await storage.progress(ref)
        for name, rule in self._workspaces.rules.items():
            if not rule.enabled:
                continue  # behind by choice, so not a lag to alert on
            cursor = progress[name].cursor if name in progress else 0
            self._workspaces.telemetry.record(
                EVALUATION_LAG,
                head - cursor,
                tenant_id=ref.tenant_id,
                workspace_id=ref.workspace_id,
                attributes={a.RULE: name},
            )

    def _record_evaluation(self, ref: WorkspaceRef, rule: Rule, evaluation: Evaluation) -> None:
        def record(metric: Metric, count: int, attributes: Attributes) -> None:
            if count:
                self._workspaces.telemetry.record(
                    metric,
                    count,
                    tenant_id=ref.tenant_id,
                    workspace_id=ref.workspace_id,
                    attributes=attributes,
                )

        fired = len(evaluation.firings)
        record(FIRINGS, fired, {a.RULE: rule.name})
        record(RUNS, fired, {a.RULE: rule.name, a.RUN_STATUS: "pending", a.ACTOR_KIND: "system"})
        record(RULE_ERRORS, len(evaluation.errors), {a.RULE: rule.name})
