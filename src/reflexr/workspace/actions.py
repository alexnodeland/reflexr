"""Actions: what a firing runs, and the :class:`Reaction` every kind of action receives.

The action is a port (ADR-0025): an async callable over a ``Reaction``. Functions are actions
as they are; ``reflexr.agent`` adapts pydantic-ai agents and pydantic-graph graphs to it.
"""

from typing import Protocol

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

    async def emit(self, event: Event) -> Published:
        """Publish an event caused by this run.

        Its id is derived from the run and how many events the attempt emitted before it, so a
        retried attempt that emits the same events again adds nothing to the log.

        Raises:
            DepthExceeded: If the event would extend the causal chain beyond the limit.
        """
        event_id = derived_event_id(self.run.id, self._emitted)
        self._emitted += 1
        return await self.workspace.publish(event, id=event_id)


class Action[D](Protocol):
    """What a rule runs: an async callable over a :class:`Reaction`.

    It may return the run's output: JSON-compatible data or a Pydantic model. Raising fails
    the attempt, which is retried under the rule's policy.
    """

    async def __call__(self, reaction: Reaction[D], /) -> object:
        """Respond to a firing, returning the run's output or None."""
        ...
