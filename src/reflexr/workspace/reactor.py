"""The reactor: the runtime that evaluates rules and executes their runs.

Deciding is exact (ADR-0005). One reactor at a time holds a workspace's evaluation lease, and
for each rule it reads the envelopes after the rule's cursor, lets core decide, and saves the
new state, the cursor, the firings' runs and reflexr's facts in one transaction. A crash before
the commit means the same envelopes are evaluated again against the same state.

Acting is at least once: see :mod:`reflexr.workspace.executor`.
"""

import asyncio
import contextlib
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from time import perf_counter
from typing import Any, overload

from pydantic import BaseModel

from reflexr.core import (
    Evaluation,
    Fact,
    InvalidRule,
    RuleName,
    StoredRules,
    Tick,
    begin,
    create_run,
    evaluate,
    load_params,
    needs,
    new_event_id,
    new_id,
    reset,
)
from reflexr.telemetry import Metric, current_traceparent, untraced, workspace_attributes
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
from reflexr.workspace.actions import Action, RunContext, params_model
from reflexr.workspace.executor import REACTOR, Executor
from reflexr.workspace.schedules import SCHEDULER, Schedule, tick_id
from reflexr.workspace.storage import Entry, Transaction, WorkspaceRef
from reflexr.workspace.workspace import WorkspaceRule, Workspaces, meet_rules

logger = logging.getLogger("reflexr.reactor")

EVALUATION_LEASE = "evaluate"
"""The lease key a reactor holds on a workspace while it evaluates it."""

_CONTENDED_WAIT = timedelta(milliseconds=2)
"""How long :meth:`Reactor.settle` first waits after a round in which another reactor held a
lease it needed. The wait doubles each time, up to :data:`_CONTENDED_WAIT_MAX`."""

_CONTENDED_WAIT_MAX = timedelta(milliseconds=100)

_GRACE = timedelta(seconds=5)
"""How long a stopping reactor gives running attempts to end, by default."""


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
        actions: What rules run, by the name they refer to them by. Every code rule's action
            must be here, and its params must validate as the params model the action declares.
            Every action stored rules may run must be here too, declaring the params model
            ``StoredRules`` gives it.
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
        InvalidRule: If a code rule's action is not among ``actions``, or its params do not
            validate as the action's params model.
        ValueError: If an action stored rules may run is not among ``actions``, or declares
            another params model than ``StoredRules`` gives it.
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
            action = chosen.get(rule.then.action)
            if action is None:
                raise InvalidRule(rule.name, [f"no action {rule.then.action!r}"])
            load_params(rule, params_model(action))
        if workspaces.stored_rules is not None:
            _check_stored_actions(workspaces.stored_rules, chosen)
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
        """Attempt the runs that are due and can start, up to ``limit`` of them.

        Returns:
            How many attempts finished, whether they succeeded or failed.
        """
        return (await self._executor.execute(limit=limit)).attempts

    async def settle(self, *, max_rounds: int = 100) -> Settled:
        """Evaluate and execute until nothing more happens now.

        Useful in tests and scripts. Runs waiting to retry later are left waiting. A round in
        which another reactor held a lease this one needed, on a workspace to evaluate or a run
        to attempt, is not idle, since the other may be about to let that work go: the reactor
        waits a few milliseconds, longer each time up to a tenth of a second, and tries again.

        Raises:
            RuntimeError: If work is still happening after ``max_rounds`` rounds, as when
                rules keep triggering each other within the depth limit, or another reactor
                keeps holding a lease this one needs.
        """
        ticks = firings = attempts = 0
        wait = _CONTENDED_WAIT
        for _ in range(max_rounds):
            ticked = await self.tick()
            fired, busy_elsewhere = await self._evaluate(None)
            executed = await self._executor.execute(limit=100)
            ticks, firings = ticks + ticked, firings + fired
            attempts += executed.attempts
            if ticked or fired or executed.attempts:
                wait = _CONTENDED_WAIT
            elif busy_elsewhere or executed.contended:
                await asyncio.sleep(wait.total_seconds())
                wait = min(wait * 2, _CONTENDED_WAIT_MAX)
            else:
                return Settled(ticks=ticks, firings=firings, attempts=attempts)
        raise RuntimeError(f"still busy after {max_rounds} rounds")

    async def serve(
        self,
        *,
        poll_interval: timedelta = timedelta(seconds=1),
        stop: asyncio.Event | None = None,
        grace: timedelta = _GRACE,
    ) -> None:
        """Tick, evaluate and execute until stopped, checking for work every ``poll_interval``.

        A pass that fails, as when the database is briefly unreachable, is logged and the next
        one tries again: leases and transactions leave nothing half done.

        Setting ``stop`` stops the reactor gracefully. Evaluation stops after its current batch,
        and attempts still waiting for a place do not start. Running attempts have ``grace`` to
        end; then their actions are cancelled, and each attempt is recorded as abandoned, a
        failed attempt that is retried under the rule's policy. Once every lease it held is
        released, ``serve`` returns. Without ``stop``, it runs until cancelled.

        Args:
            poll_interval: How long to wait between passes.
            stop: Set it to stop the reactor, as a FastAPI lifespan does at shutdown.
            grace: How long running attempts have to end once ``stop`` is set.
        """
        stop = stop or asyncio.Event()
        grace_over = asyncio.Event()

        async def end_grace() -> None:
            await stop.wait()
            await asyncio.sleep(grace.total_seconds())
            grace_over.set()

        async with asyncio.TaskGroup() as group:
            timer = group.create_task(end_grace())
            while not stop.is_set():
                try:
                    await self.tick()
                    await self._evaluate(None, stop)
                    await self._executor.execute(limit=100, stop=stop, grace_over=grace_over)
                except Exception:
                    logger.exception("a reactor pass failed; the next one will try again")
                with contextlib.suppress(TimeoutError):
                    async with asyncio.timeout(poll_interval.total_seconds()):
                        await stop.wait()
            timer.cancel()

    async def tick(self) -> int:
        """Publish the ticks that are due, for every schedule and the workspaces it targets.

        A schedule's first check in a workspace starts its timetable there; its first tick
        comes at its next time after that.

        Returns:
            How many ticks were published.
        """
        storage = self._workspaces.storage
        now = self._workspaces.clock()
        with untraced():
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
        with untraced():
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
        fired, _ = await self._evaluate(workspace)
        return fired

    async def _evaluate(
        self, workspace: WorkspaceRef | None, stop: asyncio.Event | None = None
    ) -> tuple[int, bool]:
        """Evaluate, and say whether another reactor held a workspace's evaluation lease.

        Once ``stop`` is set, evaluation stops after its current batch.
        """
        storage = self._workspaces.storage
        with untraced():
            refs = [workspace] if workspace is not None else await storage.workspaces()
        fired, contended = 0, False
        for ref in refs:
            if stop is not None and stop.is_set():
                break
            evaluated = await self._evaluate_workspace(ref, stop)
            if evaluated is None:
                contended = True
            else:
                fired += evaluated
        return fired, contended

    async def _evaluate_workspace(
        self, ref: WorkspaceRef, stop: asyncio.Event | None
    ) -> int | None:
        """Evaluate a workspace's rules; return None if another reactor holds its lease.

        Checking for work is untraced, so a workspace with nothing new makes no trace; an
        evaluation that finds work is traced as ``reflexr.evaluate`` (ADR-0040).
        """
        storage = self._workspaces.storage
        with untraced():
            if not await storage.acquire_lease(
                ref, EVALUATION_LEASE, self._holder, self._lease_ttl
            ):
                return None
        telemetry = self._workspaces.telemetry
        try:
            with untraced():
                rules = await self._workspaces.rules_in(ref)
                if not await self._has_work(ref, rules):
                    return 0
            started = perf_counter()
            with telemetry.tracer.start_as_current_span(
                "reflexr.evaluate",
                attributes=workspace_attributes(ref.tenant_id, ref.workspace_id),
            ) as span:
                enabled = [name for name, found in rules.items() if found.rule.enabled]
                fired, evaluated = await self._catch_up(ref, enabled, stop)
                span.set_attributes({a.EVALUATED_RULES: sorted(evaluated), a.FIRING_COUNT: fired})
            telemetry.record(
                EVALUATION_DURATION,
                perf_counter() - started,
                tenant_id=ref.tenant_id,
                workspace_id=ref.workspace_id,
            )
        finally:
            with untraced():
                await storage.release_lease(ref, EVALUATION_LEASE, self._holder)
        return fired

    async def _catch_up(
        self, ref: WorkspaceRef, rules: Sequence[RuleName], stop: asyncio.Event | None
    ) -> tuple[int, set[str]]:
        """Evaluate batches until no rule has new envelopes, the lease is lost, or a stop.

        ``rules`` are the enabled ones: a disabled rule's cursor holds until it is enabled again.
        """
        storage = self._workspaces.storage
        fired = 0
        evaluated: set[str] = set()
        while True:
            advanced = False
            for name in rules:
                if stop is not None and stop.is_set():
                    break  # whichever reactor evaluates next carries on from the cursors
                evaluation = await self._evaluate_batch(ref, name)
                if evaluation is None:
                    continue
                advanced = True
                evaluated.add(name)
                fired += len(evaluation.firings)
                self._record_evaluation(ref, name, evaluation)
            renewed = await storage.acquire_lease(
                ref, EVALUATION_LEASE, self._holder, self._lease_ttl
            )
            if not advanced or not renewed:
                return fired, evaluated

    async def _evaluate_batch(self, ref: WorkspaceRef, name: RuleName) -> Evaluation | None:
        """Evaluate one batch of a rule's new envelopes in one transaction, as the rule is now.

        Returns None if the rule had nothing new, or the workspace no longer has it.
        """
        workspaces = self._workspaces
        async with workspaces.storage.transaction(ref) as transaction:
            found = (await workspaces.rules_in(transaction)).get(name)
            if found is None:
                return None  # a stored rule archived since the pass began
            rule = found.rule
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

    async def _has_work(self, ref: WorkspaceRef, rules: Mapping[RuleName, WorkspaceRule]) -> bool:
        """Record the enabled rules' lag, and return whether any has work to do.

        These are the cases in which :meth:`_evaluate_batch` writes: a rule with no progress
        yet begins, one whose definition changed is reset, and one whose cursor is behind the
        head evaluates. Keep the two in step: in any other case a batch would find nothing.

        Stored rules share a series per namespace, so each records the most any of its rules
        is behind.
        """
        storage = self._workspaces.storage
        telemetry = self._workspaces.telemetry
        head = await storage.head_seq(ref)
        progress = await storage.progress(ref)
        lags: dict[str, int] = {}
        work = False
        for name, found in rules.items():
            rule = found.rule
            if not rule.enabled:
                continue  # behind by choice, so not a lag to alert on
            known = progress.get(name)
            cursor = known.cursor if known else 0
            series = telemetry.rule_attribute(name)
            lags[series] = max(lags.get(series, 0), head - cursor)
            work = work or known is None or cursor < head or known.definition != rule.definition()
        for series, lag in lags.items():
            telemetry.record(
                EVALUATION_LAG,
                lag,
                tenant_id=ref.tenant_id,
                workspace_id=ref.workspace_id,
                attributes={a.RULE: series},
            )
        return work

    def _record_evaluation(self, ref: WorkspaceRef, rule: RuleName, evaluation: Evaluation) -> None:
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
        record(FIRINGS, fired, {a.RULE: rule})
        record(RUNS, fired, {a.RULE: rule, a.RUN_STATUS: "pending", a.ACTOR_KIND: "system"})
        record(RULE_ERRORS, len(evaluation.errors), {a.RULE: rule})


def _check_stored_actions(config: StoredRules, actions: Mapping[str, Action[Any]]) -> None:
    """Confirm that each action stored rules may run is registered, with the model they give it.

    Raises:
        ValueError: Naming the first that is not.
    """
    for name, model in config.actions.items():
        action = actions.get(name)
        if action is None:
            raise ValueError(f"stored rules may run {name!r}, which is not among the actions")
        declared = params_model(action)
        if declared is not model:
            raise ValueError(
                f"stored rules give the action {name!r} {_model_name(model)}, but it declares "
                f"{_model_name(declared)}"
            )


def _model_name(model: type[BaseModel] | None) -> str:
    return "no params" if model is None else model.__name__
