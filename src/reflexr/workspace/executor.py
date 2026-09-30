"""Executing runs: at least once, under leases, in firing order per scope (ADR-0005).

An executor claims a due run under its lease, starts an attempt, runs the action while
renewing the lease, and records the outcome. Every step that changes the run is one
transaction, and the outcome is recorded only if the attempt is still the run's current one,
so an executor that lost its lease, or a run an operator cancelled, cannot overwrite a newer
state.
"""

import asyncio
import logging
from collections.abc import Coroutine, Mapping
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import timedelta
from time import perf_counter
from typing import Any, Literal

from opentelemetry import baggage, context
from opentelemetry.trace import Status, StatusCode
from opentelemetry.util.types import AttributeValue
from pydantic import BaseModel, JsonValue, TypeAdapter, ValidationError

from reflexr.core import (
    FINISHED,
    WAITING,
    AgentActor,
    Envelope,
    Rule,
    Run,
    RunCancelled,
    RunDeadLettered,
    RunRetrying,
    RunStarted,
    RunSucceeded,
    SystemActor,
    cancel,
    fail,
    new_event_id,
    runnable,
    start,
    succeed,
)
from reflexr.telemetry import (
    Metric,
    chain_attributes,
    current_trace_id,
    current_traceparent,
    parse_traceparent,
    untraced,
    workspace_attributes,
)
from reflexr.telemetry import attributes as a
from reflexr.telemetry.metrics import DEAD_LETTERS, RUN_ATTEMPTS, RUN_DURATION, RUNS
from reflexr.telemetry.telemetry import Attributes
from reflexr.workspace.actions import Action, Reaction, RunContext, RunFailure
from reflexr.workspace.storage import Entry, RunPolicy, Transaction, WorkspaceRef, run_lease
from reflexr.workspace.workspace import Workspaces

logger = logging.getLogger("reflexr.reactor")

REACTOR = SystemActor(name="reactor")
"""The actor of the facts the reactor records, about the rules it evaluates and the runs it
executes."""

_JSON: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)

type _Fact = RunStarted | RunSucceeded | RunRetrying | RunDeadLettered | RunCancelled


type _Attempt = Literal["finished", "declined", "contended"]
"""How an attempt ended: it finished, the run could not start, or another executor held it."""

ABANDONED = "the attempt was abandoned: its executor stopped"
"""The error recorded for an attempt whose executor stopped before it ended."""


@dataclass(frozen=True)
class Executed:
    """What one pass of an :class:`Executor` did."""

    attempts: int = 0
    """How many attempts finished, whether they succeeded or failed."""

    contended: int = 0
    """How many runs it left alone because another executor held their lease.

    Such a run is not idle: the other executor may be running it, or may be about to find it
    cannot start and let it go.
    """


@dataclass(frozen=True)
class _Outcome:
    kind: Literal["succeeded", "failed", "interrupted"]
    output: JsonValue = None
    error: str = ""
    reason: str | None = None
    permanent: bool = False


class Executor[D]:
    """Runs the actions of due runs. The :class:`~reflexr.workspace.Reactor` drives it."""

    def __init__(
        self,
        workspaces: Workspaces,
        *,
        actions: Mapping[str, Action[D]],
        deps: D,
        holder: str,
        lease_ttl: timedelta,
        concurrency: int,
        run_context: RunContext | None = None,
    ) -> None:
        self._workspaces = workspaces
        self._actions = dict(actions)
        self._deps = deps
        self._holder = holder
        self._lease_ttl = lease_ttl
        self._concurrency = concurrency
        self._run_context = run_context

    async def execute(
        self,
        *,
        limit: int,
        stop: asyncio.Event | None = None,
        grace_over: asyncio.Event | None = None,
    ) -> Executed:
        """Attempt up to ``limit`` of the runs that can start, ``concurrency`` at a time.

        Once ``stop`` is set, attempts still waiting for a place do not start. Once
        ``grace_over`` is set, running actions are cancelled, and their attempts recorded as
        abandoned.
        """
        stop = stop or asyncio.Event()
        grace_over = grace_over or asyncio.Event()
        rules = self._workspaces.rules.values()
        with untraced():  # polling: the attempts it finds are traced, each as its own trace
            due = await self._workspaces.storage.due_runs(
                now=self._workspaces.clock(), limit=limit, policy=RunPolicy.of(rules)
            )
        gate = asyncio.Semaphore(self._concurrency)

        async def attempt(ref: WorkspaceRef, run: Run) -> _Attempt:
            async with gate:
                if stop.is_set():
                    return "declined"  # left for whichever reactor runs next
                return await self._attempt(ref, run, grace_over)

        async with asyncio.TaskGroup() as group:
            attempts = [group.create_task(attempt(ref, run)) for ref, run in due]
        results = [task.result() for task in attempts]
        return Executed(attempts=results.count("finished"), contended=results.count("contended"))

    async def _attempt(self, ref: WorkspaceRef, run: Run, grace_over: asyncio.Event) -> _Attempt:
        rule = self._workspaces.rules.get(run.rule)
        if rule is not None and not rule.enabled:
            return "declined"  # its runs wait until it is enabled again
        storage = self._workspaces.storage
        key = run_lease(run.id)
        with untraced():
            if not await storage.acquire_lease(ref, key, self._holder, self._lease_ttl):
                return "contended"
        try:
            if rule is None:
                await self._cancel_orphan(ref, run)
                return "declined"
            finished = await self._attempt_leased(ref, run, rule, grace_over)
            return "finished" if finished else "declined"
        finally:
            with untraced():
                await storage.release_lease(ref, key, self._holder)

    async def _attempt_leased(
        self, ref: WorkspaceRef, due: Run, rule: Rule, grace_over: asyncio.Event
    ) -> bool:
        telemetry = self._workspaces.telemetry
        with telemetry.tracer.start_as_current_span(
            f"invoke_workflow {rule.name}",
            attributes={
                **workspace_attributes(ref.tenant_id, ref.workspace_id),
                **chain_attributes(due.correlation_id),
                a.OPERATION_NAME: "invoke_workflow",
                a.WORKFLOW_NAME: rule.name,
                a.RULE: rule.name,
                a.SCOPE: due.scope_key,
                a.RUN_ID: due.id,
            },
        ) as span:
            claimed = await self._claim(ref, due, rule)
            if claimed is None:
                return False
            run, events = claimed
            span.set_attribute(a.ATTEMPT, run.attempts)
            for envelope in events:
                linked = parse_traceparent(envelope.traceparent)
                if linked is not None:
                    span.add_link(linked, {a.EVENT_SEQ: envelope.seq})
            started = perf_counter()
            outcome = await self._run_action(ref, run, rule, events, grace_over)
            if outcome.kind == "failed":
                span.set_status(Status(StatusCode.ERROR, outcome.error))
            if outcome.kind == "interrupted":
                return False
            recorded = await self._complete(ref, run, rule, outcome)
            if recorded is None:
                return False
            self._record(ref, RUN_DURATION, perf_counter() - started, rule, recorded.status)
            return True

    async def _claim(
        self, ref: WorkspaceRef, due: Run, rule: Rule
    ) -> tuple[Run, tuple[Envelope, ...]] | None:
        """Start an attempt of a run, if it is still due and first in its scope."""
        async with self._workspaces.storage.transaction(ref) as transaction:
            run = await transaction.run(due.id)
            assert run is not None, "runs are never deleted"
            now = self._workspaces.clock()
            if run.status == "running":
                # Its executor stopped renewing the lease: the attempt counts as failed.
                failed, fact = fail(run, rule, now=now, error=ABANDONED, reason="abandoned")
                await self._save(transaction, failed, fact)
                self._record_status(ref, rule, failed)
                return None
            if run.status not in WAITING or run.next_attempt_at > now:
                return None
            scope_runs = await transaction.scope_runs(run.rule, run.scope_key)
            if run.id not in {r.id for r in runnable(scope_runs, rule, now=now)}:
                return None
            started, fact = start(run, now=now, trace_id=current_trace_id())
            await self._save(transaction, started, fact)
            events: list[Envelope] = []
            for seq in run.matched:
                events.extend(await transaction.read(after_seq=seq - 1, limit=1))
        self._record(ref, RUN_ATTEMPTS, 1, rule)
        self._record_status(ref, rule, started)
        return started, tuple(events)

    async def _run_action(
        self,
        ref: WorkspaceRef,
        run: Run,
        rule: Rule,
        events: tuple[Envelope, ...],
        grace_over: asyncio.Event,
    ) -> _Outcome:
        """Run the action while keeping the lease, and report how it ended."""
        workspace = await self._workspaces.open(
            ref.tenant_id,
            ref.workspace_id,
            actor=AgentActor(rule=rule.name, run_id=run.id, name=rule.then.action),
        )
        reaction = Reaction(
            workspace=workspace.caused_by(run.causation, correlation_id=run.correlation_id),
            run=run,
            rule=rule,
            events=events,
            deps=self._deps,
        )
        action = self._actions[rule.then.action]
        # The chain is the session: in baggage, an SDK's baggage processor can copy it onto
        # every span the action causes, such as its database and HTTP calls.
        token = context.attach(baggage.set_baggage(a.SESSION_ID, run.correlation_id))
        try:
            async with AsyncExitStack() as scope:
                if self._run_context is not None:
                    await scope.enter_async_context(self._run_context(reaction))
                return await self._supervise(ref, run, self._call(action, reaction), grace_over)
        finally:
            context.detach(token)

    async def _supervise(
        self,
        ref: WorkspaceRef,
        run: Run,
        attempt: Coroutine[Any, Any, _Outcome],
        grace_over: asyncio.Event,
    ) -> _Outcome:
        """Run an attempt while keeping its lease.

        The action is cancelled if the run is cancelled or its lease lost, or once the grace of
        a stop is over; the last makes the attempt an abandoned one.
        """
        task = asyncio.create_task(attempt)
        helpers = [
            asyncio.create_task(self._keep(ref, run, task)),
            asyncio.create_task(_cancel_when_set(grace_over, task)),
        ]
        try:
            return await task
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                raise  # the reactor itself is being cancelled
            if grace_over.is_set():
                return _Outcome("failed", error=ABANDONED, reason="abandoned")
            return _Outcome("interrupted")
        finally:
            for helper in helpers:
                helper.cancel()
            await asyncio.gather(*helpers, return_exceptions=True)

    @staticmethod
    async def _call(action: Action[D], reaction: Reaction[D]) -> _Outcome:
        limit = reaction.rule.timeout
        try:
            async with asyncio.timeout(limit.total_seconds() if limit else None):
                output = await action(reaction)
        except TimeoutError:
            return _Outcome("failed", error=f"the action timed out after {limit}", reason="timeout")
        except RunFailure as failure:
            return _Outcome(
                "failed", error=failure.message, reason=failure.reason, permanent=failure.permanent
            )
        except Exception as error:  # an action's failure is the run's, to retry
            return _Outcome("failed", error=f"{type(error).__name__}: {error}")
        try:
            value = output.model_dump(mode="json") if isinstance(output, BaseModel) else output
            return _Outcome("succeeded", output=_JSON.validate_python(value))
        except ValidationError:
            return _Outcome("failed", error="the action returned a value that is not JSON")

    async def _keep(self, ref: WorkspaceRef, run: Run, task: asyncio.Task[_Outcome]) -> None:
        """Renew the run's lease while its action runs.

        The action is stopped if the run was cancelled or the lease was lost. A storage error is
        logged and ends the renewals, so the lease lapses and another reactor can take the run.
        """
        storage = self._workspaces.storage
        try:
            with untraced():  # bookkeeping, not part of the attempt's trace
                while True:
                    await asyncio.sleep(self._lease_ttl.total_seconds() / 3)
                    kept = await storage.acquire_lease(
                        ref, run_lease(run.id), self._holder, self._lease_ttl
                    )
                    current = await storage.run(ref, run.id)
                    if not kept or current is None or current.status != "running":
                        task.cancel()
                        return
        except Exception:
            logger.exception("renewing the lease of run %s failed; it will lapse", run.id)

    async def _complete(
        self, ref: WorkspaceRef, run: Run, rule: Rule, outcome: _Outcome
    ) -> Run | None:
        """Record an attempt's outcome, unless the attempt is no longer the run's current one."""
        async with self._workspaces.storage.transaction(ref) as transaction:
            current = await transaction.run(run.id)
            assert current is not None, "runs are never deleted"
            if (current.status, current.attempts) != ("running", run.attempts):
                return None  # cancelled, or abandoned and attempted again elsewhere
            now = self._workspaces.clock()
            if outcome.kind == "succeeded":
                done, fact = succeed(current, now=now, output=outcome.output)
            else:
                done, fact = fail(
                    current,
                    rule,
                    now=now,
                    error=outcome.error,
                    reason=outcome.reason,
                    permanent=outcome.permanent,
                )
            await self._save(transaction, done, fact)
        self._record_status(ref, rule, done)
        return done

    async def _cancel_orphan(self, ref: WorkspaceRef, due: Run) -> None:
        """Cancel a run whose rule is no longer registered, so it stops being due."""
        async with self._workspaces.storage.transaction(ref) as transaction:
            run = await transaction.run(due.id)
            assert run is not None, "runs are never deleted"
            if run.status in FINISHED:
                return
            cancelled, fact = cancel(
                run, now=self._workspaces.clock(), reason="its rule is no longer registered"
            )
            await self._save(transaction, cancelled, fact)

    @staticmethod
    async def _save(transaction: Transaction, run: Run, fact: _Fact) -> None:
        await transaction.save_runs([run])
        entry = Entry(
            id=new_event_id(),
            actor=REACTOR,
            event=fact,
            causation=run.causation,
            correlation_id=run.correlation_id,
            traceparent=current_traceparent(),
        )
        await transaction.append([entry])

    def _record_status(self, ref: WorkspaceRef, rule: Rule, run: Run) -> None:
        extra: dict[str, AttributeValue] = {a.ACTOR_KIND: "system"}
        if run.status in ("retrying", "dead") and run.reason is not None:
            extra[a.RUN_REASON] = run.reason
        self._record(ref, RUNS, 1, rule, run.status, extra)
        if run.status == "dead":
            self._record(ref, DEAD_LETTERS, 1, rule)

    def _record(
        self,
        ref: WorkspaceRef,
        metric: Metric,
        value: float,
        rule: Rule,
        status: str | None = None,
        extra: Attributes | None = None,
    ) -> None:
        attributes: dict[str, AttributeValue] = {a.RULE: rule.name, **(extra or {})}
        if status is not None:
            attributes[a.RUN_STATUS] = status
        self._workspaces.telemetry.record(
            metric,
            value,
            tenant_id=ref.tenant_id,
            workspace_id=ref.workspace_id,
            attributes=attributes,
        )


async def _cancel_when_set(event: asyncio.Event, task: asyncio.Task[_Outcome]) -> None:
    """Cancel an action once ``event`` is set."""
    await event.wait()
    task.cancel()
