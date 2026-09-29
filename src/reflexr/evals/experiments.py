"""Experiment tasks: replay a firing's events against a candidate action, in isolation.

An experiment asks how a new agent, graph or model would have handled what happened. A
:func:`replay_task` is an evalr ``Task`` that, for each example, publishes its events into a
fresh in-memory workspace with one rule and the candidate action, lets the reactor settle, and
hands the result to the experiment's evaluators. Nothing touches the real workspaces.
"""

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

from evalr.core import Example
from evalr.core.experiments import Task
from pydantic import BaseModel

from reflexr.core import Envelope, Event, Rule, Run, SourceActor
from reflexr.workspace import Action, InMemoryStorage, Reactor, Workspaces


@dataclass(frozen=True)
class Replay:
    """What a replay produced: the rule's runs and the whole log.

    Attributes:
        runs: The rule's runs, oldest first.
        log: Every envelope of the isolated workspace, in order.
    """

    runs: tuple[Run, ...]
    log: tuple[Envelope, ...]

    @property
    def emitted(self) -> tuple[Envelope, ...]:
        """The events the rule's runs published."""
        ids = {run.id for run in self.runs}
        return tuple(
            envelope
            for envelope in self.log
            if envelope.causation is not None
            and envelope.causation.run_id in ids
            and envelope.actor.kind == "agent"
        )


def replay_task[D, InputT: BaseModel, VerdictT: BaseModel, OutputT: BaseModel](
    *,
    rule: Rule,
    action: Action[D],
    deps: D,
    events: Callable[[Example[InputT, VerdictT]], Sequence[Event]],
    output: Callable[[Replay], OutputT],
    event_types: Iterable[type[Event]] | None = None,
) -> Task[InputT, VerdictT, OutputT]:
    """Build an evalr task that replays each example's events against a candidate action.

    Args:
        rule: The rule under test; its action is the candidate.
        action: The candidate action: a new agent, graph, prompt or model.
        deps: The candidate's dependencies, such as fakes of the services it calls.
        events: The events to publish for an example, in order.
        output: What the experiment's evaluators judge, from the replay.
        event_types: The event types the isolated workspace accepts; every registered type by
            default.
    """

    async def task(example: Example[InputT, VerdictT]) -> OutputT:
        workspaces = Workspaces(InMemoryStorage(), events=event_types, rules=[rule])
        workspace = await workspaces.open(
            f"replay:{example.id}", "replay", actor=SourceActor(name="replay")
        )
        await workspace.publish_many(list(events(example)))
        await Reactor(workspaces, actions={rule.then.action: action}, deps=deps).settle()
        runs = tuple(reversed(await workspace.runs(rule=rule.name)))
        return output(Replay(runs=runs, log=tuple(await workspace.read())))

    return task
