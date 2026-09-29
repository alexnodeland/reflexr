"""Experiment tasks: replaying examples against a candidate action, in isolation."""

from evalr.core import Dataset, Example, FunctionEvaluator
from evalr.memory import InMemoryExperimentTracker
from pydantic import BaseModel

import tests.event_types  # noqa: F401  (registers the test event types)
from reflexr import Event, F, Rule, by, on, run
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


rule = Rule(name="respond", when=on(ServiceError), scope=by(F.service), then=run("respond"))


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
