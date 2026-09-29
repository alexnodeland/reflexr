"""The reactor: the runtime that evaluates rules over workspaces' logs.

Deciding is exact (ADR-0005). One reactor at a time holds a workspace's evaluation lease, and
for each rule it reads the envelopes after the rule's cursor, lets core decide, and saves the
new state, the cursor, the firings' runs and reflexr's facts in one transaction. A crash before
the commit means the same envelopes are evaluated again against the same state.
"""

from datetime import timedelta
from time import perf_counter

from reflexr.core import (
    Evaluation,
    Fact,
    Rule,
    SystemActor,
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
)
from reflexr.telemetry.telemetry import Attributes
from reflexr.workspace.storage import Entry, Transaction, WorkspaceRef
from reflexr.workspace.workspace import Workspaces

EVALUATION_LEASE = "evaluate"
"""The lease key a reactor holds on a workspace while it evaluates it."""

REACTOR = SystemActor(name="reactor")
"""The actor of the facts the reactor records."""


class Reactor:
    """Evaluates every rule of a set of :class:`Workspaces`, in any number of processes.

    Args:
        workspaces: The workspaces, with their storage and rules.
        holder: This reactor's name in leases. Defaults to a new random id; give each process
            its own.
        batch_size: How many envelopes one transaction evaluates for one rule. The transaction
            holds the workspace's lock, so this bounds how long publishing can wait.
        lease_ttl: How long a lease lasts unless renewed. A reactor that dies holding one
            releases it when it lapses.
    """

    def __init__(
        self,
        workspaces: Workspaces,
        *,
        holder: str | None = None,
        batch_size: int = 500,
        lease_ttl: timedelta = timedelta(seconds=30),
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        self._workspaces = workspaces
        self._holder = holder or new_id("reactor")
        self._batch_size = batch_size
        self._lease_ttl = lease_ttl

    @property
    def holder(self) -> str:
        """This reactor's name in leases."""
        return self._holder

    async def evaluate(self, workspace: WorkspaceRef | None = None) -> int:
        """Evaluate every rule over new envelopes until each is caught up with the log.

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
        for name in self._workspaces.rules:
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
