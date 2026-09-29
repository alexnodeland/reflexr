# Feedback and evaluation

To improve a workflow you need to know when it did the right thing. reflexr records judgements of its work as **typed feedback**: people's verdicts and evaluators', on runs, on firings and on whole causal chains ([ADR-0019](../adr/0019-typed-feedback-as-events.md)). Feedback is an event in the workspace's log, beside the work it judges, so it is attributed, replayable and something rules can watch, and it links to the traces of that work. This page covers feedback types, giving and reading feedback, feedback as scores in an evaluation backend, and evaluation with evalr.

## Feedback types

A feedback type is a Pydantic model that subclasses `Feedback`. It is registered by name, like an event type, and declares the targets it can be given on:

```python
from typing import Annotated, Literal

from pydantic import Field

from reflexr import Feedback


class TriageQuality(Feedback, name="triage_quality", targets={"run"}):
    correct: bool
    severity: Literal["low", "high", "critical"]
    note: str | None = None


class ShouldHaveFired(Feedback, targets={"firing"}):  # registered as "should_have_fired"
    fired_rightly: bool


class Resolution(Feedback, name="resolution", targets={"chain"}):
    resolved: bool
    minutes_to_mitigate: Annotated[int, Field(ge=0, le=1440)] | None = None
```

| Target | Use it for |
|---|---|
| `RunTarget(run_id)` | One run: a workflow's execution for one firing, over all its attempts. Was the triage right? |
| `FiringTarget(firing_id)` | One firing: should the rule have fired at all? A firing's id is its run's id |
| `ChainTarget(correlation_id)` | A causal chain: everything one triggering event led to, such as a whole incident |

A type must declare at least one target, or defining it raises `TypeError`; an intermediate base class passes `abstract=True` instead. Choose field types for how they will be scored ([Scores](#scores)): `bool` is yes or no, `Literal` and `Enum` are categories, bounded numbers are numeric, and `str` is free text.

## Giving feedback

Give feedback through a workspace handle, as the person or evaluator giving it:

```python
from reflexr import UserActor
from reflexr.core import RunTarget

ada = await workspaces.open("acme", "prod", actor=UserActor(id="ada"))
[latest] = await ada.runs(rule="error-spike", limit=1)
await ada.give_feedback(
    TriageQuality(correct=False, severity="critical", note="It missed the bad deploy."),
    on=RunTarget(run_id=latest.id),
)
```

The handle checks that the type can be given on the target's kind (`ValidationFailed` otherwise) and that the target exists (`NotFound`), then appends a `feedback_given` event with the validated value. Whoever gave it is the envelope's actor. The feedback joins the causal chain of what it is about, so it appears in that chain's session in your traces, and it is traced as `reflexr.feedback {type}` and counted in the `reflexr.feedback` metric ([Observability](observability.md)).

Every surface has it, as the `give_feedback` command. Over REST (`POST /v1/workspaces/{workspace_id}/commands`) and the WebSocket it is a command frame; over MCP it is the `give_feedback` tool, with the same fields:

```json
{"type": "command", "command_id": "fb_1", "command": {"type": "give_feedback", "feedback_type": "triage_quality", "target": {"kind": "run", "run_id": "fir_28fb78558a582bdc"}, "value": {"correct": true, "severity": "high"}}}
```

The server validates `value` against the registered type, and answers with a `recorded` outcome, the feedback's `seq` and `id` ([stream protocol](../protocol.md)).

### Evaluators

An evaluator's verdict is feedback of the same types, given by an `EvaluatorActor`:

```python
from reflexr import EvaluatorActor

evaluator = ada.as_actor(EvaluatorActor(name="triage-judge", version="2026-09-28"))
await evaluator.give_feedback(
    TriageQuality(correct=True, severity="high"), on=RunTarget(run_id=latest.id)
)
```

Each version of an evaluator is its own participant (`evaluator:triage-judge@2026-09-28`), so verdicts from a retrained judge never mix with its predecessor's. Because people and evaluators give the same types, comparing them is a query over the log. [Evaluators as rules](#evaluators-as-rules) runs evaluators automatically.

### Rules that watch feedback

Feedback is an event like any other, so a rule can react to it. This one escalates whenever someone says a triage was wrong:

```python
from reflexr import F, Rule, on, run
from reflexr.core import FeedbackGiven

wrong_triage = Rule(
    name="wrong-triage",
    when=on(FeedbackGiven).where(F.value.correct.eq(False), feedback_type="triage_quality"),
    then=run("escalate"),
)
```

### Reading feedback

Feedback is in the log with everything else:

```python
from reflexr.core import FeedbackGiven

verdicts = [
    (envelope.actor.participant, envelope.event.value)
    for envelope in await ada.read()
    if isinstance(envelope.event, FeedbackGiven)
    and envelope.event.feedback_type == "triage_quality"
]
```

```text
[('user:ada', {'correct': False, 'severity': 'critical', 'note': 'It missed the bad deploy.'}), ('evaluator:triage-judge@2026-09-28', {'correct': True, 'severity': 'high', 'note': None})]
```

## Scores

Evaluation backends see feedback as **scores**: one per field, named `{type}.{field}`, typed by the field, as in artifactr. The types above become:

| Score | Data type | Value |
|---|---|---|
| `triage_quality.correct` | `BOOLEAN` | 1 or 0 |
| `triage_quality.severity` | `CATEGORICAL` | `low`, `high` or `critical` |
| `triage_quality.note` | `TEXT` | The text, cut to 500 characters |
| `resolution.minutes_to_mitigate` | `NUMERIC` | Between 0 and 1440, from the field's bounds |

`Enum` fields are categorical too. Optional fields are scored when they have a value, and fields of other types (lists, nested models) are not scored. `score_configs(TriageQuality)` returns this mapping as `ScoreConfig`s.

`reflexr.scores.FeedbackMirror` follows a workspace's log and sends each piece of feedback's scores to a `ScoreSink`, attached to the trace the feedback is about:

```python
import asyncio

from reflexr.scores import FeedbackMirror

mirror = FeedbackMirror(workspace, sink)
task = asyncio.create_task(mirror.follow())  # until cancelled
```

| Feedback on | Scored on |
|---|---|
| A run | The trace of the run's latest attempt, from `Run.trace_ids` |
| A firing | The trace of the evaluation pass that recorded its `rule_fired` |
| A chain | The chain's session: the scores have a `session_id` and no trace |

Feedback about something that has no trace, because no SDK was configured when it ran, is scored on the session too. A mirror follows one workspace, so run one task per workspace you want mirrored; any actor's handle will do, since it only reads. Score ids are derived from the feedback's envelope, so a mirror can start over from the beginning of the log without duplicating anything, and feedback of a type the process does not register is skipped. Each score's metadata says where it came from: the tenant, workspace, feedback type, target kind, the participant who gave it, and the `seq`.

`sync_score_configs(store)` creates the score configs of every registered feedback type (or those you list) that a `ScoreConfigStore` lacks, and returns the names it created. The sink and the store are small protocols ([ADR-0025](../adr/0025-ports-and-adapters.md)), so any backend can implement them:

```python
from reflexr.scores import Score


class PrintingSink:
    async def send(self, score: Score) -> None:
        print(score.name, score.value, score.trace_id or score.session_id)
```

### Scores in Langfuse

With the `langfuse` extra, feedback becomes Langfuse scores beside the traces it judges. `LangfuseScores` is a `ScoreSink`, and `LangfuseScoreConfigs` a `ScoreConfigStore`:

```python
import asyncio

from reflexr.langfuse import LangfuseScoreConfigs, LangfuseScores
from reflexr.scores import FeedbackMirror, sync_score_configs

# From configure_telemetry(..., langfuse=True), or langfuse_client(...):
langfuse = telemetry.langfuse
await sync_score_configs(LangfuseScoreConfigs(langfuse))  # once, at startup
mirror = asyncio.create_task(FeedbackMirror(workspace, LangfuseScores(langfuse)).follow())
```

Score configs give Langfuse each score's type, range and categories, so its UI offers the same scales for annotation. Langfuse accepts config names of up to 35 characters; a longer `{type}.{field}` raises, and a shorter `name=` on the feedback type fixes it. Scores are queued by the Langfuse client and sent in the background, and a score sent again with the same id replaces the first; call `langfuse.flush()` before a short-lived process exits. [Langfuse](observability.md#langfuse) covers traces.

## Evaluators and datasets with evalr

[evalr](https://github.com/alexnodeland/evalr) is the family's evaluation library: evaluators that return typed verdicts (DSPy judges, decision models, plain functions), datasets, experiments and agreement metrics ([ADR-0020](../adr/0020-evalr-shared-eval-kit.md)). The `evals` extra connects reflexr to it. evalr is not on PyPI yet, so reflexr pins it to a GitHub revision, and an application installs it from GitHub alongside reflexr:

```bash
uv add "reflexr[evals] @ git+https://github.com/alexnodeland/reflexr" \
  "evalr @ git+https://github.com/alexnodeland/evalr"
```

### Evaluators as rules

An evaluator is an action like any other, so judging runs online is a rule. `EvaluatorAction` runs an evalr `Evaluator` when its rule fires and gives the verdict as feedback, from an `EvaluatorActor` with the evaluator's name and version. This one judges every successful `error-spike` run, at most twenty an hour:

```python
from datetime import timedelta

from evalr.core import FunctionEvaluator, HandOff
from pydantic import BaseModel

from reflexr.core import RunSucceeded, RunTarget
from reflexr.evals import EvaluatorAction, run_record


class TriageInput(BaseModel):
    service: str
    errors: int
    opened_incident: bool


def opened_an_incident(judged: TriageInput) -> TriageQuality:
    if judged.errors < 3:
        raise HandOff  # not enough to go on: leave it to a person
    return TriageQuality(correct=judged.opened_incident, severity="high")


triage_judge = FunctionEvaluator(
    opened_an_incident, verdict_type=TriageQuality, name="opened-incident", version="1"
)


async def judged_input(reaction: Reaction[AppDeps]) -> TriageInput:
    run_id = str(reaction.events[-1].data["run_id"])  # the run_succeeded that fired the judge
    record = await run_record(reaction.workspace, run_id)
    return TriageInput(
        service=str(record.run.scope["service"]),
        errors=len(record.matched),
        opened_incident=any(e.event_type == "incident.opened" for e in record.emitted),
    )


async def judged_run(reaction: Reaction[AppDeps]) -> RunTarget:
    return RunTarget(run_id=str(reaction.events[-1].data["run_id"]))


judge = EvaluatorAction(triage_judge, input=judged_input, target=judged_run)
judge_triage = Rule(
    name="judge-triage",
    when=on(RunSucceeded).where(rule="error-spike").at_most(20, per=timedelta(hours=1)),
    then=run(judge),
)
reactor = Reactor(workspaces, actions={"triage": triage, judge.name: judge}, deps=deps)
```

- `input` and `target` are async functions of the judge's reaction, since they usually read the log. `run_record(workspace, run_id)` loads a run with the envelopes that made its rule fire (`matched`) and the events it emitted (`emitted`); `chain_events(workspace, correlation_id)` loads a whole chain.
- The action's name defaults to the evaluator's, here `opened-incident`. The judge's run succeeds with the verdict as its output: the value, the evaluator's name and version, its confidence, latency and trace.
- An evaluator that raises `HandOff`, declining to judge, records nothing; its run's output is `{"handed_off": "opened-incident"}`.
- Real judges are evalr's DSPy judges or decision models, often a cheap decision model with a language-model judge behind it in evalr's `Fallback`. Throttles, filters and predicates sample and bound them like any rule ([Rules](rules.md)), and a judge's own runs are retried and dead-lettered like any other.

### Datasets from feedback

`LogFeedbackSource` is evalr's `FeedbackSource` over a workspace's log: each piece of one feedback type becomes an evalr example, whose verdict is the feedback and whose input your application builds from what the feedback is about, since only it knows what its evaluators judge:

```python
from evalr.core import collect

from reflexr.evals import FeedbackContext, LogFeedbackSource


def triage_input(context: FeedbackContext[TriageQuality]) -> TriageInput:
    assert context.run is not None  # triage_quality is given on runs
    record = context.run
    return TriageInput(
        service=str(record.run.scope["service"]),
        errors=len(record.matched),
        opened_incident=any(e.event_type == "incident.opened" for e in record.emitted),
    )


source = LogFeedbackSource(
    workspace, feedback_type=TriageQuality, input_type=TriageInput, input=triage_input
)
dataset = await collect("triage-quality", source)
```

- The context holds the `feedback_given` envelope and the validated feedback, with the run and its events (`context.run`, a `RunRecord`) for feedback on a run or a firing, or the chain's envelopes (`context.chain`) for feedback on a chain. The builder may be async.
- Example ids are the feedback's event ids, so they are stable across collections. A run's example carries the trace of its latest attempt, and every example's metadata names the tenant, workspace, target kind, `seq` and `given_by`, the participant who gave it.
- `targets={"run"}` keeps feedback on some kinds of target only. The source yields every piece of the type, whoever gave it, evaluators' verdicts included; to train or measure a judge against people only, keep the examples whose `given_by` starts with `user:`.

### Experiments

`replay_task` builds an evalr experiment task that asks how a candidate action (a new agent, prompt, graph or model) would have handled what happened. For each example it publishes the example's events into a fresh in-memory workspace with the rule under test and the candidate, lets a reactor settle, and hands the result to the experiment's evaluators. Nothing touches your real workspaces:

```python
from evalr.memory import InMemoryExperimentTracker

from reflexr.evals import Replay, replay_task


class Outcome(BaseModel):
    opened_incident: bool


def replayed(replay: Replay) -> Outcome:
    return Outcome(opened_incident=any(e.event_type == "incident.opened" for e in replay.emitted))


task = replay_task(
    rule=error_spike,
    action=candidate_triage,
    deps=deps,
    events=lambda example: [ServiceError(service=example.input.service, severity=9)] * 3,
    output=replayed,
    event_types=[ServiceError, IncidentOpened],
)
result = await InMemoryExperimentTracker().run_experiment(
    "candidate-triage", dataset=incidents, task=task, evaluators=[incident_judge]
)
```

A `Replay` holds the rule's `runs`, oldest first, the isolated workspace's whole `log`, and `emitted`, the events the runs published.

## End-to-end measures

Some measures need no judge, because the log already says what happened. `rule_outcomes` counts how each rule's runs went, and `time_to_resolution` times each chain from its first event to the event your application says resolves it:

```python
from reflexr.evals import rule_outcomes, time_to_resolution

outcomes = await rule_outcomes(workspace)
spike = outcomes["error-spike"]
print(spike.runs, spike.dead_letter_rate, spike.retry_rate, spike.intervention_rate)

resolved = await time_to_resolution(
    workspace, resolves=lambda envelope: envelope.event_type == "incident.resolved"
)  # {correlation_id: timedelta}, for each chain that was resolved
```

| Measure | What it counts |
|---|---|
| `runs`, `succeeded` | The runs the rule's firings created, and those that succeeded |
| `dead_letter_rate` | The share of runs dead-lettered at least once, whether they exhausted their retries or failed permanently |
| `retry_rate` | The share of runs that failed and were retried at least once |
| `intervention_rate` | The share of runs a person or an external agent retried, skipped or cancelled (the actor kinds in `OPERATORS`) |

A chain starts at the event that began it. For a rule that counts, the firing joins the chain of the event that completed the count, so the time to resolution runs from that event. Whether a chain achieved its goal is a judgement, not a count: give it as feedback on the chain, from a person or an evaluator.
