"""Actions: what a firing runs, and the :class:`Reaction` every kind of action receives.

The action is a port (ADR-0025): an async callable over a ``Reaction``. Functions are actions
as they are; ``reflexr.agent`` adapts pydantic-ai agents and pydantic-graph graphs to it. An
action that takes params declares their model with a ``params`` attribute (RFC-0003), and
:func:`with_params` gives a function one.
"""

from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, JsonValue

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
        params: The rule's params, validated as the action's params model, or None if the
            action declares none. :meth:`params_as` returns them typed.
    """

    def __init__(
        self,
        *,
        workspace: Workspace,
        run: Run,
        rule: Rule,
        events: tuple[Envelope, ...],
        deps: D,
        params: BaseModel | None = None,
    ) -> None:
        self.workspace = workspace
        self.run = run
        self.rule = rule
        self.events = events
        self.deps = deps
        self.params = params
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

    def params_as[P: BaseModel](self, model: type[P]) -> P:
        """Return the params as ``model``, the action's params model.

        An agent's tools and prompt, and a graph's steps, read their params this way; a function
        given to :func:`with_params` gets them as its second argument.

        Raises:
            TypeError: If the params are not a ``model``: the action declares another model,
                or none.
        """
        if not isinstance(self.params, model):
            declared = "no params" if self.params is None else type(self.params).__name__
            raise TypeError(f"the action declares {declared}, not {model.__name__}")
        return self.params

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

    An action that takes params declares their model as a ``params`` attribute, as
    ``AgentAction``, ``GraphAction`` and :func:`with_params` do. The reactor validates each
    rule's params as that model when it is built, and again at each attempt, into the
    reaction's ``params``.
    """

    async def __call__(self, reaction: Reaction[D], /) -> object:
        """Respond to a firing, returning the run's output or None."""
        ...


@runtime_checkable
class _Parameterized(Protocol):
    """An action that declares a params model."""

    @property
    def params(self) -> type[BaseModel] | None:
        """The model of the params rules pass the action."""
        ...


def params_model(action: Action[Any]) -> type[BaseModel] | None:
    """Return the params model an action declares, or None if it declares none."""
    return action.params if isinstance(action, _Parameterized) else None


def with_params[D, P: BaseModel](
    fn: Callable[[Reaction[D], P], Awaitable[object]], params: type[P]
) -> Action[D]:
    """Adapt a function of the reaction and its params into an action that declares ``params``.

    pyright checks that the function's second parameter takes a ``params``::

        class NotifyParams(BaseModel):
            thread_id: str


        async def notify(reaction: Reaction[AppDeps], params: NotifyParams) -> None:
            await reaction.deps.bridge.notice(params.thread_id, reaction)


        actions = {"notify": with_params(notify, NotifyParams)}
    """
    return _WithParams(fn, params)


@dataclass(frozen=True)
class _WithParams[D, P: BaseModel]:
    fn: Callable[[Reaction[D], P], Awaitable[object]]
    params: type[P]

    async def __call__(self, reaction: Reaction[D], /) -> object:
        return await self.fn(reaction, reaction.params_as(self.params))


type RunContext = Callable[[Reaction[Any]], AbstractAsyncContextManager[object]]
"""A context entered around each run attempt, inside its span, given the attempt's reaction.

It is a port (ADR-0025): a backend that attributes a run in its own way, such as Langfuse's
propagated trace attributes, implements it, and the reactor stays free of the backend. It
matches artifactr's ``TurnContext``.
"""
