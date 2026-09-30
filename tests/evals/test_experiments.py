"""Experiment tasks: replaying examples against a candidate action, in isolation."""

import pytest
from evalr.core import Dataset, Example, FunctionEvaluator
from evalr.memory import InMemoryExperimentTracker
from pydantic import BaseModel

from reflexr import Event, F, InvalidRule, Rule, by, on, run
from reflexr.core import EventRegistry
from reflexr.evals import Replay, replay_task
from reflexr.workspace import Reaction
from tests.event_types import Deploy, ServiceError


class Incident(BaseModel):
    service: str


class Rolled(BaseModel):
    """What the evaluators judge: whether the candidate rolled back, and what it answered."""

    rolled_back: bool
    answer: str


class Correct(BaseModel):
    correct: bool


rule = Rule(name="app:respond", when=on(ServiceError), scope=by(F.service), then=run("respond"))

TRIAL = EventRegistry()


class TrialEvent(Event, abstract=True, event_namespace="trial", registry=TRIAL):
    """An application whose events are in a registry of its own."""


class Probe(TrialEvent):
    service: str


async def candidate(reaction: Reaction[str]) -> str:
    await reaction.emit(Deploy(service=str(reaction.scope["service"]), version=reaction.deps))
    return f"rolled back to {reaction.deps}"


def incident(example: Example[Incident, Correct]) -> list[Event]:
    return [ServiceError(service=example.input.service, severity=9)]


def rolled_back(rolled: Rolled) -> Correct:
    return Correct(correct=rolled.rolled_back)


def replayed(replay: Replay) -> Rolled:
    [done] = replay.runs
    return Rolled(rolled_back=bool(replay.emitted), answer=str(done.output))


async def test_a_candidate_is_replayed_on_each_example_in_isolation() -> None:
    task = replay_task(
        rule=rule,
        action=candidate,
        deps="v41",
        events=incident,
        output=replayed,
        event_types=[ServiceError, Deploy],
    )
    dataset = Dataset(
        "incidents",
        [
            Example(id="e1", input=Incident(service="auth"), verdict=Correct(correct=True)),
            Example(id="e2", input=Incident(service="billing"), verdict=Correct(correct=True)),
        ],
        input_type=Incident,
        verdict_type=Correct,
    )
    judge = FunctionEvaluator(rolled_back, verdict_type=Correct, name="rolled")
    result = await InMemoryExperimentTracker().run_experiment(
        "rollbacks", dataset=dataset, task=task, evaluators=[judge]
    )
    assert [item.output for item in result.items] == [
        Rolled(rolled_back=True, answer="rolled back to v41"),
        Rolled(rolled_back=True, answer="rolled back to v41"),
    ]
    assert [v.value for v in result.verdicts("rolled").values()] == [Correct(correct=True)] * 2


async def test_a_replay_accepts_the_types_of_the_applications_registry() -> None:
    probed = Rule(name="trial:probed", when=on(Probe), then=run("respond"))

    async def answer(reaction: Reaction[None]) -> str:
        return f"probed {reaction.events[-1].data['service']}"

    def probes(example: Example[Incident, Correct]) -> list[Event]:
        return [Probe(service=example.input.service)]

    def answered(replay: Replay) -> Rolled:
        return Rolled(rolled_back=False, answer=str(replay.runs[0].output))

    example = Example(id="e1", input=Incident(service="auth"), verdict=Correct(correct=True))
    task = replay_task(
        rule=probed, action=answer, deps=None, events=probes, output=answered, registry=TRIAL
    )
    assert (await task(example)).answer == "probed auth"
    elsewhere = replay_task(rule=probed, action=answer, deps=None, events=probes, output=answered)
    with pytest.raises(InvalidRule, match="no event type 'trial:probe'"):
        await elsewhere(example)
