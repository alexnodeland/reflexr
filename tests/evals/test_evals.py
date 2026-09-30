"""evalr for reflexr: feedback as examples, and evaluators as rules."""

from typing import Any

from evalr.contracts import check_feedback_source
from evalr.core import FunctionEvaluator, HandOff
from opentelemetry.sdk.trace import TracerProvider
from pydantic import BaseModel

from reflexr import EvaluatorActor, F, Feedback, Rule, SourceActor, UserActor, by, on, run
from reflexr.core import ChainTarget, FeedbackGiven, FiringTarget, RunSucceeded, RunTarget
from reflexr.evals import (
    EvaluatorAction,
    FeedbackContext,
    LogFeedbackSource,
    chain_events,
    run_record,
)
from reflexr.workspace import InMemoryStorage, Reaction, Reactor, Workspace, Workspaces
from tests.event_types import Deploy, ServiceError

triage = Rule(name="triage", when=on(ServiceError), scope=by(F.service), then=run("triage"))


class TriageQuality(Feedback, name="triage_quality", targets={"run", "firing", "chain"}):
    correct: bool


class Noise(Feedback, name="evals_noise", targets={"run"}):
    """Another feedback type, which a source of triage quality skips."""

    loud: bool


class TriageInput(BaseModel):
    service: str
    emitted: int


async def respond(reaction: Reaction[None]) -> str:
    await reaction.emit(Deploy(service=str(reaction.scope["service"]), version="rollback"))
    return "rolled back"


async def setup(*rules: Rule, actions: dict[str, Any] | None = None) -> Workspace:
    workspaces = Workspaces(
        InMemoryStorage(), rules=[triage, *rules], tracer_provider=TracerProvider()
    )
    workspace = await workspaces.open("acme", "prod", actor=SourceActor(name="monitor"))
    await workspace.publish(ServiceError(service="auth", severity=8))
    await Reactor(workspaces, actions={"triage": respond, **(actions or {})}).settle()
    return workspace.as_actor(UserActor(id="ada"))


def build(context: FeedbackContext[TriageQuality]) -> TriageInput:
    if context.run is None:
        return TriageInput(service="chain", emitted=len(context.chain))
    return TriageInput(
        service=str(context.run.run.scope["service"]), emitted=len(context.run.emitted)
    )


async def test_feedback_becomes_examples_with_the_context_it_is_about() -> None:
    workspace = await setup()
    [done] = await workspace.runs(rule="triage")
    on_run = await workspace.give_feedback(
        TriageQuality(correct=True), on=RunTarget(run_id=done.id)
    )
    await workspace.give_feedback(Noise(loud=True), on=RunTarget(run_id=done.id))
    await workspace.give_feedback(TriageQuality(correct=False), on=FiringTarget(firing_id=done.id))
    await workspace.give_feedback(
        TriageQuality(correct=True), on=ChainTarget(correlation_id=done.correlation_id)
    )
    source = LogFeedbackSource(
        workspace, feedback_type=TriageQuality, input_type=TriageInput, input=build
    )
    assert (source.input_type, source.verdict_type) == (TriageInput, TriageQuality)
    examples = [e async for e in source.examples()]
    chain = [e for e in await workspace.read() if e.correlation_id == done.correlation_id]
    assert await chain_events(workspace, done.correlation_id) == tuple(chain)
    assert [e.input for e in examples] == [
        TriageInput(service="auth", emitted=1),
        TriageInput(service="auth", emitted=1),
        TriageInput(service="chain", emitted=len(chain)),
    ]
    by_run = examples[0]
    assert (by_run.id, by_run.verdict, by_run.trace_id) == (
        on_run.id,
        TriageQuality(correct=True),
        done.trace_ids[-1],
    )
    assert by_run.metadata["given_by"] == "user:ada"
    assert [e.trace_id for e in examples[1:]] == [None, None]
    only_runs = LogFeedbackSource(
        workspace, feedback_type=TriageQuality, input_type=TriageInput, input=build, targets={"run"}
    )
    assert len([e async for e in only_runs.examples()]) == 1


async def test_input_builders_may_be_async_and_read_the_log() -> None:
    workspace = await setup()
    [done] = await workspace.runs(rule="triage")
    await workspace.give_feedback(TriageQuality(correct=True), on=RunTarget(run_id=done.id))

    async def reread(context: FeedbackContext[TriageQuality]) -> TriageInput:
        assert context.run is not None
        record = await run_record(workspace, context.run.run.id)
        return TriageInput(service=record.matched[0].data["service"], emitted=len(record.emitted))

    source = LogFeedbackSource(
        workspace, feedback_type=TriageQuality, input_type=TriageInput, input=reread
    )
    [example] = [e async for e in source.examples()]
    assert example.input == TriageInput(service="auth", emitted=1)


async def judged_input(reaction: Reaction[None]) -> TriageInput:
    record = await run_record(reaction.workspace, str(reaction.events[-1].data["run_id"]))
    return TriageInput(service=str(record.run.scope["service"]), emitted=len(record.emitted))


async def judged_run(reaction: Reaction[None]) -> RunTarget:
    return RunTarget(run_id=str(reaction.events[-1].data["run_id"]))


def judge_rule(action: EvaluatorAction[None, TriageInput, TriageQuality]) -> Rule:
    return Rule(
        name="judge-triage",
        when=on(RunSucceeded).where(rule="triage"),
        then=run(action),
    )


async def test_evaluators_run_as_rules_and_record_their_verdicts() -> None:
    def rollback_is_right(judged: TriageInput) -> TriageQuality:
        return TriageQuality(correct=judged.emitted > 0)

    judge = EvaluatorAction(
        FunctionEvaluator(
            rollback_is_right, verdict_type=TriageQuality, name="rollbacks", version="2"
        ),
        input=judged_input,
        target=judged_run,
    )
    workspace = await setup(judge_rule(judge), actions={judge.name: judge})
    [judged] = await workspace.runs(rule="judge-triage")
    assert judged.status == "succeeded"
    assert isinstance(judged.output, dict)
    assert judged.output["value"] == {"correct": True}
    [verdict] = [e for e in await workspace.read() if isinstance(e.event, FeedbackGiven)]
    assert verdict.actor == EvaluatorActor(name="rollbacks", version="2")
    [triaged] = await workspace.runs(rule="triage")
    assert verdict.event == FeedbackGiven(
        feedback_type="triage_quality", target=RunTarget(run_id=triaged.id), value={"correct": True}
    )
    # A source of examples leaves the evaluator's verdicts out unless asked for them.
    person = await workspace.give_feedback(
        TriageQuality(correct=False), on=RunTarget(run_id=triaged.id)
    )
    people = LogFeedbackSource(
        workspace, feedback_type=TriageQuality, input_type=TriageInput, input=build
    )
    assert [e.id async for e in people.examples()] == [person.id]
    everyone = LogFeedbackSource(
        workspace,
        feedback_type=TriageQuality,
        input_type=TriageInput,
        input=build,
        include_evaluators=True,
    )
    examples = [e async for e in everyone.examples()]
    assert [e.id for e in examples] == [verdict.id, person.id]
    assert examples[0].metadata["given_by"] == "evaluator:rollbacks@2"


async def test_an_evaluator_that_hands_off_records_nothing() -> None:
    def unsure(judged: TriageInput) -> TriageQuality:
        raise HandOff

    judge = EvaluatorAction(
        FunctionEvaluator(unsure, verdict_type=TriageQuality, name="unsure"),
        input=judged_input,
        target=judged_run,
        name="judge",
    )
    workspace = await setup(judge_rule(judge), actions={"judge": judge})
    [judged] = await workspace.runs(rule="judge-triage")
    assert judged.output == {"handed_off": "unsure"}
    assert not [e for e in await workspace.read() if isinstance(e.event, FeedbackGiven)]


async def test_the_source_passes_evalrs_feedback_source_contract() -> None:
    workspace = await setup()
    [done] = await workspace.runs(rule="triage")
    await workspace.give_feedback(TriageQuality(correct=True), on=RunTarget(run_id=done.id))
    await workspace.give_feedback(
        TriageQuality(correct=False), on=ChainTarget(correlation_id=done.correlation_id)
    )
    source = LogFeedbackSource(
        workspace, feedback_type=TriageQuality, input_type=TriageInput, input=build
    )
    await check_feedback_source(source)
