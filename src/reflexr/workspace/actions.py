"""Actions: what a firing runs, and the :class:`Reaction` every kind of action receives.

The action is a port (ADR-0025): an async callable over a ``Reaction``. Functions are actions
as they are; ``reflexr.agent`` adapts pydantic-ai agents and pydantic-graph graphs to it.
"""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol

from pydantic import JsonValue

from reflexr.core import Envelope, Event, Rule, Run, RunId, derived_event_id
from reflexr.workspace.workspace import Published, Workspace


class Reaction[D]:
    """Everything an action needs to respond to a firing.

    Attributes:
        workspace: The workspace, acting as the run's agent. Events published through it
            record the run as their cause and join its causal chain.
        run: The run: its rule, scope, matched ``seq``s, attempt and chain.
        rule: The rule that fired.
        events: The envelopes that made the rule fire, in order.
        deps: The application's dependencies, given to the reactor.
    """

    def __init__(
        self,
        *,
        workspace: Workspace,
        run: Run,
        rule: Rule,
        events: tuple[Envelope, ...],
        deps: D,
    ) -> None:
        self.workspace = workspace
        self.run = run
        self.rule = rule
        self.events = events
        self.deps = deps
        self._emitted = 0

    @property
    def run_id(self) -> RunId:
        """The run's id: the firing's id, and the action's idempotency key."""
        return self.run.id

    @property
    def attempt(self) -> int:
        """Which attempt this is, from 1."""
        return self.run.attempts

    @property
    def scope(self) -> dict[str, JsonValue]:
        """The values of the rule's scope fields for this firing."""
        return self.run.scope

    async def checkpoint(self, step: str, state: JsonValue) -> None:
        """Save the run's progress after ``step``, so a retry resumes after it.

        The saved state is ``reaction.run.checkpoint`` on the next attempt.

        Raises:
            InvalidState: If this attempt is no longer the run's current one; the action
                should stop.
        """
        self.run = await self.workspace.checkpoint_run(
            self.run.id, attempt=self.run.attempts, step=step, state=state
        )
        self._emitted = 0  # events after a checkpoint are numbered afresh, in its segment

    async def emit(self, event: Event) -> Published:
        """Publish an event caused by this run.

        Its id is derived from the run, its last checkpoint, and how many events it emitted
        since, so a retried attempt that emits the same events again adds nothing to the log,
        and a graph resumed after a checkpoint never reuses an earlier event's id.

        Raises:
            DepthExceeded: If the event would extend the causal chain beyond the limit.
        """
        event_id = derived_event_id(self.run.id, self._emitted, segment=self.run.checkpoints)
        self._emitted += 1
        return await self.workspace.publish(event, id=event_id)


class RunFailure(Exception):
    """Raise from an action, a tool or a capability to fail the attempt with a reason.

    The reason is a stable code, such as ``guardrail_blocked``, recorded on the run and its
    facts and counted in ``reflexr.runs``; the message is for people. A permanent failure is
    not retried, whatever the rule's policy, since retrying the same input would fail again.
    Other exceptions fail the attempt too, without a reason, and are retried.

    Args:
        message: What went wrong.
        reason: A stable code for why.
        permanent: Whether retrying cannot help.
    """

    def __init__(self, message: str, *, reason: str, permanent: bool = False) -> None:
        super().__init__(message)
        self.message = message
        self.reason = reason
        self.permanent = permanent


class Action[D](Protocol):
    """What a rule runs: an async callable over a :class:`Reaction`.

    It may return the run's output: JSON-compatible data or a Pydantic model. Raising fails
    the attempt, which is retried under the rule's policy.
    """

    async def __call__(self, reaction: Reaction[D], /) -> object:
        """Respond to a firing, returning the run's output or None."""
        ...


type RunContext = Callable[[Reaction[Any]], AbstractAsyncContextManager[object]]
"""A context entered around each run attempt, inside its span, given the attempt's reaction.

It is a port (ADR-0025): a backend that attributes a run in its own way, such as Langfuse's
propagated trace attributes, implements it, and the reactor stays free of the backend. It
matches artifactr's ``TurnContext``.
"""
