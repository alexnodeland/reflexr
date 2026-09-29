"""Evaluators as rules: an evalr evaluator as an action, recording its verdicts as feedback."""

from collections.abc import Awaitable, Callable
from dataclasses import KW_ONLY, dataclass

from evalr.core import Evaluator, HandOff
from pydantic import BaseModel, JsonValue

from reflexr.core import EvaluatorActor, Feedback, FeedbackTarget
from reflexr.workspace import Reaction

type Build[D, T] = Callable[[Reaction[D]], Awaitable[T]]
"""Builds something from the reaction, such as an evaluator's input or the target of its
verdict; async, since it usually reads the log."""


@dataclass
class EvaluatorAction[D, InputT: BaseModel, VerdictT: Feedback]:
    """Run an evalr evaluator when a rule fires, and record its verdict as feedback.

    Judging every triage run is a rule, sampled and throttled like any other::

        judge = EvaluatorAction(triage_judge, input=triage_input, target=judged_run)
        Rule(name="judge-triage", when=on(RunSucceeded).where(rule="triage"), then=run(judge))

    The verdict is given as feedback by an :class:`~reflexr.core.EvaluatorActor` with the
    evaluator's name and version, so people's and evaluators' judgements of the same target
    can be compared. An evaluator that hands off records nothing.

    Args:
        evaluator: The evaluator. Its verdict type must be a feedback type that can be given
            on the target.
        input: Builds the evaluator's input from the reaction.
        target: What the verdict is about, such as the run that succeeded.
        name: The name rules refer to the action by; defaults to the evaluator's name.
    """

    evaluator: Evaluator[InputT, VerdictT]
    _: KW_ONLY
    input: Build[D, InputT]
    target: Build[D, FeedbackTarget]
    name: str = ""

    def __post_init__(self) -> None:
        self.name = self.name or self.evaluator.name

    async def __call__(self, reaction: Reaction[D]) -> JsonValue:
        """Judge the input and record the verdict; the run's output is the verdict."""
        judged = await self.input(reaction)
        on = await self.target(reaction)
        try:
            verdict = await self.evaluator.evaluate(judged)
        except HandOff:
            return {"handed_off": self.evaluator.name}
        judge = EvaluatorActor(name=verdict.evaluator, version=verdict.version)
        await reaction.workspace.as_actor(judge).give_feedback(verdict.value, on=on)
        return verdict.model_dump(mode="json")
