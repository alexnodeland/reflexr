"""A feedback source for evalr: people's feedback in a workspace, as examples (ADR-0020).

Evaluators' verdicts are left out unless asked for, since a judge trained or calibrated on
its own verdicts learns nothing from them.
"""

from collections.abc import AsyncIterator, Awaitable, Callable, Collection
from dataclasses import dataclass
from inspect import isawaitable

from evalr.core import Example
from pydantic import BaseModel

from reflexr.core import (
    ChainTarget,
    Envelope,
    Feedback,
    FeedbackGiven,
    RunTarget,
    TargetKind,
)
from reflexr.evals.context import RunRecord, chain_events, run_record
from reflexr.workspace import Workspace


@dataclass(frozen=True)
class FeedbackContext[VerdictT: Feedback]:
    """One piece of feedback, with what it is about.

    Attributes:
        envelope: The ``feedback_given`` envelope: who gave it, and when.
        feedback: The feedback, validated as its type.
        run: The run and its events, for feedback on a run or a firing.
        chain: The causal chain's envelopes, for feedback on a chain.
    """

    envelope: Envelope
    feedback: VerdictT
    run: RunRecord | None = None
    chain: tuple[Envelope, ...] = ()


type BuildInput[InputT, VerdictT: Feedback] = Callable[
    [FeedbackContext[VerdictT]], InputT | Awaitable[InputT]
]
"""Turns a piece of feedback's context into an evaluator's input."""


class LogFeedbackSource[InputT: BaseModel, VerdictT: Feedback]:
    """Yields an example for each piece of one feedback type in a workspace's log.

    It is an evalr ``FeedbackSource``: the verdict is the feedback, and the input is built by
    the application from what the feedback is about, since only it knows what its evaluators
    judge. Example ids are the feedback's event ids, so they are stable, and a run's example
    carries the trace of its latest attempt.

    Args:
        workspace: The workspace whose log to read.
        feedback_type: The feedback type to collect: the verdict type.
        input_type: The evaluator's input type.
        input: Builds an input from a piece of feedback's context.
        targets: Only feedback on these kinds of target; every kind by default.
        include_evaluators: Whether to yield feedback an ``EvaluatorActor`` gave as well. By
            default only other actors' feedback is yielded, such as people's, because training
            or calibrating a judge on evaluators' verdicts, its own included, is circular.
    """

    def __init__(
        self,
        workspace: Workspace,
        *,
        feedback_type: type[VerdictT],
        input_type: type[InputT],
        input: BuildInput[InputT, VerdictT],
        targets: Collection[TargetKind] | None = None,
        include_evaluators: bool = False,
    ) -> None:
        self._workspace = workspace
        self._feedback_type = feedback_type
        self._input_type = input_type
        self._input = input
        self._targets = targets
        self._include_evaluators = include_evaluators

    @property
    def input_type(self) -> type[InputT]:
        """The evaluator's input type."""
        return self._input_type

    @property
    def verdict_type(self) -> type[VerdictT]:
        """The feedback type, which is the verdict type."""
        return self._feedback_type

    async def examples(self) -> AsyncIterator[Example[InputT, VerdictT]]:
        """Yield an example for each piece of the feedback type, oldest first."""
        for envelope in await self._workspace.read():
            event = envelope.event
            if not isinstance(event, FeedbackGiven):
                continue
            if event.feedback_type != self._feedback_type.feedback_type:
                continue
            if self._targets is not None and event.target.kind not in self._targets:
                continue
            if envelope.actor.kind == "evaluator" and not self._include_evaluators:
                continue
            yield await self._example(envelope, event)

    async def _example(self, envelope: Envelope, event: FeedbackGiven) -> Example[InputT, VerdictT]:
        feedback = self._feedback_type.model_validate(event.value)
        target = event.target
        context: FeedbackContext[VerdictT]
        if isinstance(target, ChainTarget):
            chain = await chain_events(self._workspace, target.correlation_id)
            context = FeedbackContext(envelope=envelope, feedback=feedback, chain=chain)
        else:
            run_id = target.run_id if isinstance(target, RunTarget) else target.firing_id
            run = await run_record(self._workspace, run_id)
            context = FeedbackContext(envelope=envelope, feedback=feedback, run=run)
        built = self._input(context)
        value = await built if isawaitable(built) else built
        trace = context.run.run.trace_ids[-1] if context.run and context.run.run.trace_ids else None
        return Example[InputT, VerdictT](
            id=envelope.id,
            input=self._input_type.model_validate(value),
            verdict=feedback,
            trace_id=trace if isinstance(target, RunTarget) else None,
            metadata={
                "tenant_id": self._workspace.tenant_id,
                "workspace_id": self._workspace.workspace_id,
                "target": target.kind,
                "given_by": envelope.actor.participant,
                "seq": envelope.seq,
            },
        )
