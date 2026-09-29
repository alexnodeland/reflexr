"""The reactor's execution: runs attempted at least once, under leases, in order per scope."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import pytest
from opentelemetry import baggage
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.trace import StatusCode
from pydantic import BaseModel

from reflexr import AgentActor, F, RetryPolicy, Rule, SourceActor, by, on, run
from reflexr.core import (
    InvalidRule,
    InvalidState,
    NotFound,
    Run,
    RunCancelled,
    RunDeadLettered,
    RunProgressed,
    RunRetrying,
    RunStarted,
    RunSucceeded,
    fail,
    start,
)
from reflexr.telemetry import attributes as a
from reflexr.telemetry import parse_traceparent
from reflexr.workspace import (
    REACTOR,
    InMemoryStorage,
    Reaction,
    Reactor,
    Settled,
    Storage,
    Workspace,
    WorkspaceRef,
    Workspaces,
    run_lease,
)
from tests.event_types import Deploy, ServiceError
from tests.workspace.conftest import START, Build, FakeClock, Telemetry
from tests.workspace.helpers import fired

ACME = WorkspaceRef("acme", "prod")
SECOND = timedelta(seconds=1)


@dataclass
class Deps:
    """What the test actions share: a record of calls and switches that steer them."""

    calls: list[Reaction["Deps"]] = field(default_factory=list[Reaction["Deps"]])
    failures: int = 0
    started: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)


async def respond(reaction: Reaction[Deps]) -> dict[str, object]:
    reaction.deps.calls.append(reaction)
    if reaction.deps.failures:
        reaction.deps.failures -= 1
        raise RuntimeError("the pager is down")
    return {"paged": reaction.scope["service"]}


async def blocked(reaction: Reaction[Deps]) -> None:
    reaction.deps.started.set()
    await reaction.deps.release.wait()


def rule(**changes: object) -> Rule:
    fields: dict[str, object] = {
        "name": "page",
        "when": on(Deploy),
        "scope": by(F.service),
        "then": run("respond"),
        "retry": RetryPolicy(max_attempts=2, backoff=SECOND),
    }
    return Rule.model_validate({**fields, **changes})


def reactor(
    workspaces: Workspaces,
    deps: Deps,
    *,
    lease_ttl: timedelta = timedelta(seconds=30),
    concurrency: int = 10,
) -> Reactor[Deps]:
    actions = {"respond": respond, "blocked": blocked}
    return Reactor(
        workspaces, actions=actions, deps=deps, lease_ttl=lease_ttl, concurrency=concurrency
    )


def trace_id(span: ReadableSpan) -> str:
    context = span.get_span_context()
    assert context is not None
    return f"{context.trace_id:032x}"


async def open_(workspaces: Workspaces) -> Workspace:
    return await workspaces.open("acme", "prod", actor=SourceActor(name="ci"))


async def test_a_run_succeeds_with_its_output_and_facts(build: Build) -> None:
    workspaces = build([rule()])
    deps = Deps()
    workspace = await open_(workspaces)
    deploy = (await workspace.publish(Deploy(service="auth"))).envelope
    assert await reactor(workspaces, deps).settle() == Settled(firings=1, attempts=1)
    [done] = await workspace.runs()
    assert (done.status, done.attempts, done.output) == ("succeeded", 1, {"paged": "auth"})
    [reaction] = deps.calls
    assert (reaction.run_id, reaction.attempt, reaction.events) == (done.id, 1, (deploy,))
    assert reaction.rule.name == "page"
    facts = [e for e in await workspace.read() if isinstance(e.event, RunStarted | RunSucceeded)]
    assert [type(e.event) for e in facts] == [RunStarted, RunSucceeded]
    assert {(e.actor, e.correlation_id, e.depth) for e in facts} == {(REACTOR, deploy.id, 1)}


async def test_failures_are_retried_then_dead_lettered(build: Build, clock: FakeClock) -> None:
    workspaces = build([rule()])
    deps = Deps(failures=2)
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    executor = reactor(workspaces, deps)
    assert await executor.settle() == Settled(firings=1, attempts=1)
    [waiting] = await workspace.runs()
    assert (waiting.status, waiting.error) == ("retrying", "RuntimeError: the pager is down")
    assert await executor.execute() == 0  # not due yet
    clock.advance(1)
    assert await executor.execute() == 1
    [dead] = await workspace.runs()
    assert (dead.status, dead.attempts) == ("dead", 2)
    kinds = [type(e.event) for e in await workspace.read()]
    assert RunRetrying in kinds
    assert RunDeadLettered in kinds


async def test_a_retried_run_does_not_emit_twice(build: Build, clock: FakeClock) -> None:
    async def announce(reaction: Reaction[Deps]) -> None:
        await reaction.emit(ServiceError(service="auth", message="deploy went out"))
        await respond(reaction)

    workspaces = build([rule(then=run("announce"))])
    deps = Deps(failures=1)
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    executor = Reactor(workspaces, actions={"announce": announce}, deps=deps)
    await executor.settle()
    clock.advance(1)
    await executor.settle()
    [done] = await workspace.runs()
    assert done.status == "succeeded"
    [emitted] = [e for e in await workspace.read() if isinstance(e.event, ServiceError)]
    assert emitted.actor == AgentActor(rule="page", run_id=done.id, name="announce")
    assert (emitted.causation, emitted.correlation_id) == (done.causation, done.correlation_id)


async def test_runs_of_a_scope_run_in_firing_order(build: Build, clock: FakeClock) -> None:
    workspaces = build([rule()])
    deps = Deps(failures=1)
    workspace = await open_(workspaces)
    for service in ("auth", "auth", "billing"):
        await workspace.publish(Deploy(service=service))
    executor = reactor(workspaces, deps)
    await executor.settle()
    # The first auth run failed and waits to retry; the second waits behind it.
    statuses = {(r.scope["service"], r.fired_seq): r.status for r in await workspace.runs()}
    assert statuses == {
        ("auth", 1): "retrying",
        ("auth", 2): "pending",
        ("billing", 3): "succeeded",
    }
    clock.advance(1)
    await executor.settle()
    assert {r.status for r in await workspace.runs()} == {"succeeded"}


async def test_timeouts_and_bad_outputs_fail_the_attempt(build: Build) -> None:
    class Paged(BaseModel):
        service: str

    async def slow(reaction: Reaction[None]) -> None:
        await asyncio.sleep(1)

    async def model(reaction: Reaction[None]) -> Paged:
        return Paged(service="auth")

    async def unserializable(reaction: Reaction[None]) -> object:
        return object()

    rules = [
        rule(name="slow", then=run("slow"), timeout=timedelta(milliseconds=10)),
        rule(name="model", then=run("model")),
        rule(name="bad", then=run("bad")),
    ]
    workspaces = build(rules)
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    actions = {"slow": slow, "model": model, "bad": unserializable}
    await Reactor(workspaces, actions=actions).settle()
    results = {r.rule: (r.status, r.error, r.output) for r in await workspace.runs()}
    assert results == {
        "slow": ("retrying", "the action timed out after 0:00:00.010000", None),
        "model": ("succeeded", None, {"service": "auth"}),
        "bad": ("retrying", "the action returned a value that is not JSON", None),
    }


async def test_abandoned_runs_count_as_failed_attempts(
    build: Build, storage: Storage, clock: FakeClock
) -> None:
    workspaces = build([rule()])
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    executor = reactor(workspaces, Deps())
    await executor.evaluate()
    [pending] = await workspace.runs()
    async with storage.transaction(ACME) as transaction:  # an executor that died mid-attempt
        await transaction.save_runs([start(pending, now=START)[0]])
    assert await executor.execute() == 0
    [abandoned] = await workspace.runs()
    assert (abandoned.status, abandoned.error) == (
        "retrying",
        "the attempt was abandoned: its executor stopped",
    )
    clock.advance(1)
    assert await executor.execute() == 1
    assert (await workspace.runs())[0].status == "succeeded"


async def test_a_stale_attempt_cannot_overwrite_a_newer_one(build: Build, storage: Storage) -> None:
    workspaces = build([rule(then=run("blocked"))])
    deps = Deps()
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    executor = reactor(workspaces, deps)
    await executor.evaluate()
    attempt = asyncio.create_task(executor.execute())
    await deps.started.wait()
    [first] = await workspace.runs()
    async with storage.transaction(ACME) as transaction:  # another executor took over
        failed, _ = fail(first, rule(), now=START, error="abandoned")
        await transaction.save_runs([start(failed, now=START)[0]])
    deps.release.set()
    assert await attempt == 0
    [current] = await workspace.runs()
    assert (current.status, current.attempts) == ("running", 2)


async def test_cancelling_a_run_stops_its_action(build: Build) -> None:
    workspaces = build([rule(then=run("blocked"))])
    deps = Deps()
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    executor = reactor(workspaces, deps, lease_ttl=timedelta(milliseconds=30))
    await executor.evaluate()
    attempt = asyncio.create_task(executor.execute())
    await deps.started.wait()
    [running] = await workspace.runs()
    await workspace.cancel_run(running.id, reason="false alarm")
    assert await asyncio.wait_for(attempt, timeout=1) == 0
    [cancelled] = await workspace.runs()
    assert cancelled.status == "cancelled"
    assert isinstance((await workspace.read())[-1].event, RunCancelled)


async def test_losing_the_lease_stops_the_action(
    build: Build, storage: Storage, clock: FakeClock
) -> None:
    workspaces = build([rule(then=run("blocked"))])
    deps = Deps()
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    executor = reactor(workspaces, deps, lease_ttl=timedelta(milliseconds=30))
    await executor.evaluate()
    attempt = asyncio.create_task(executor.execute())
    await deps.started.wait()
    [running] = await workspace.runs()
    clock.advance(60)
    assert await storage.acquire_lease(ACME, run_lease(running.id), "thief", timedelta(minutes=1))
    assert await asyncio.wait_for(attempt, timeout=1) == 0
    assert (await workspace.runs())[0].status == "running"  # for the new holder to recover


async def test_stopping_the_reactor_stops_the_action(build: Build) -> None:
    workspaces = build([rule(then=run("blocked"))])
    deps = Deps()
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    executor = reactor(workspaces, deps)
    await executor.evaluate()
    attempt = asyncio.create_task(executor.execute())
    await deps.started.wait()
    attempt.cancel()
    with pytest.raises(asyncio.CancelledError):
        await attempt


async def test_runs_leased_elsewhere_are_left_alone(build: Build, storage: Storage) -> None:
    workspaces = build([rule()])
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    executor = reactor(workspaces, Deps())
    await executor.evaluate()
    [pending] = await workspace.runs()
    await storage.acquire_lease(ACME, run_lease(pending.id), "elsewhere", timedelta(minutes=1))
    assert await executor.execute() == 0


class StaleDue(InMemoryStorage):
    """Storage whose due runs were read before other writers changed them."""

    stale: list[tuple[WorkspaceRef, Run]] | None = None

    async def due_runs(self, *, now: object, limit: int) -> list[tuple[WorkspaceRef, Run]]:
        return self.stale or []


async def test_runs_changed_since_they_were_found_due_are_skipped(clock: FakeClock) -> None:
    storage = StaleDue(clock=clock)
    blocking = rule(on_dead_letter="block", retry=RetryPolicy(max_attempts=1))
    workspaces = Workspaces(storage, rules=[blocking], clock=clock)
    first, second = fired("r1", rule="page"), fired("r2", rule="page", seq=2)
    orphan = fired("r3", rule="gone").model_copy(update={"status": "succeeded"})
    dead, _ = fail(start(first, now=START)[0], blocking, now=START, error="x")
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([dead, second, orphan])
    # Found due before the first run was dead-lettered and the orphan finished elsewhere.
    storage.stale = [(ACME, second), (ACME, first), (ACME, orphan)]
    assert await reactor(workspaces, Deps()).execute() == 0
    assert [r.status for r in await storage.runs(ACME)] == ["succeeded", "pending", "dead"]


async def test_runs_of_rules_no_longer_registered_are_cancelled(
    build: Build, storage: Storage
) -> None:
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([fired("r1", rule="retired")])
    workspaces = build([rule()])
    assert await reactor(workspaces, Deps()).execute() == 0
    orphan = await storage.run(ACME, "r1")
    assert orphan is not None
    assert (orphan.status, (await storage.read(ACME))[-1].event) == (
        "cancelled",
        RunCancelled(run_id="r1", rule="retired", reason="its rule is no longer registered"),
    )


async def test_settling_gives_up_on_endless_work(build: Build) -> None:
    workspaces = build([rule()])
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    with pytest.raises(RuntimeError, match="still busy after 1 rounds"):
        await reactor(workspaces, Deps()).settle(max_rounds=1)


async def test_serving_works_until_cancelled(build: Build) -> None:
    workspaces = build([rule()])
    workspace = await open_(workspaces)
    serving = asyncio.create_task(
        reactor(workspaces, Deps()).serve(poll_interval=timedelta(milliseconds=5))
    )
    await workspace.publish(Deploy(service="auth"))
    for _ in range(200):
        runs = await workspace.runs()
        if runs and runs[0].status == "succeeded":
            break
        await asyncio.sleep(0.005)
    serving.cancel()
    with pytest.raises(asyncio.CancelledError):
        await serving
    assert (await workspace.runs())[0].status == "succeeded"


async def test_the_reactor_checks_its_actions(build: Build) -> None:
    workspaces = build([rule()])
    with pytest.raises(InvalidRule, match="no action 'respond'"):
        Reactor(workspaces)
    with pytest.raises(ValueError, match="concurrency must be at least 1"):
        reactor(workspaces, Deps(), concurrency=0)


async def test_attempts_are_traced_and_linked_to_their_causes(
    build: Build, telemetry: Telemetry, clock: FakeClock
) -> None:
    workspaces = build([rule()])
    deps = Deps(failures=1)
    workspace = await open_(workspaces)
    deploy = (await workspace.publish(Deploy(service="auth"))).envelope
    executor = reactor(workspaces, deps)
    await executor.settle()
    clock.advance(1)
    await executor.settle()
    spans = [s for s in telemetry.spans.get_finished_spans() if s.name == "invoke_workflow page"]
    assert len(spans) == 2
    failed, succeeded = spans
    assert failed.status.status_code == StatusCode.ERROR
    attributes = dict(succeeded.attributes or {})
    [done] = await workspace.runs()
    assert attributes[a.OPERATION_NAME] == "invoke_workflow"
    assert (attributes[a.RUN_ID], attributes[a.ATTEMPT], attributes[a.SESSION_ID]) == (
        done.id,
        2,
        deploy.id,
    )
    cause = parse_traceparent(deploy.traceparent)
    assert cause is not None
    assert [link.context for link in succeeded.links] == [cause]
    assert done.trace_ids == tuple(trace_id(s) for s in spans)
    statuses = sorted(str(p[0][a.RUN_STATUS]) for p in telemetry.points("reflexr.runs"))
    assert statuses == ["pending", "retrying", "running", "succeeded"]
    assert telemetry.points("reflexr.run.attempts")[0][1] == 2
    assert telemetry.points("reflexr.run.duration")


def test_trace_contexts_parse_only_when_valid() -> None:
    assert parse_traceparent(None) is None
    assert parse_traceparent("not a traceparent") is None
    valid = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    context = parse_traceparent(valid)
    assert context is not None
    assert f"{context.trace_id:032x}" == "0af7651916cd43dd8448eb211c80319c"


async def test_long_actions_keep_their_lease_and_untraced_runs_record_no_traces(
    clock: FakeClock,
) -> None:
    async def lingers(reaction: Reaction[None]) -> None:
        await asyncio.sleep(0.05)  # several lease renewals

    workspaces = Workspaces(InMemoryStorage(clock=clock), rules=[rule(then=run("lingers"))])
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    executor = Reactor(
        workspaces, actions={"lingers": lingers}, lease_ttl=timedelta(milliseconds=15)
    )
    assert await executor.settle() == Settled(firings=1, attempts=1)
    [done] = await workspace.runs()
    assert (done.status, done.trace_ids) == ("succeeded", ())


async def test_actions_checkpoint_their_progress_for_the_next_attempt(
    build: Build, clock: FakeClock
) -> None:
    seen: list[object] = []

    async def stepwise(reaction: Reaction[None]) -> str:
        seen.append(reaction.run.checkpoint)
        if reaction.run.checkpoint is None:
            await reaction.checkpoint("fetch", {"done": ["fetch"]})
            raise RuntimeError("crashed after the first step")
        return "finished"

    workspaces = build([rule(then=run("stepwise"))])
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    executor = Reactor(workspaces, actions={"stepwise": stepwise})
    await executor.settle()
    [retrying] = await workspace.runs()
    assert (retrying.status, retrying.step, retrying.checkpoint) == (
        "retrying",
        "fetch",
        {"done": ["fetch"]},
    )
    progressed = [
        (e.event.step, e.depth)
        for e in await workspace.read()
        if isinstance(e.event, RunProgressed)
    ]
    assert progressed == [("fetch", 1)]
    clock.advance(1)
    await executor.settle()
    assert seen == [None, {"done": ["fetch"]}]
    assert (await workspace.runs())[0].output == "finished"


async def test_a_stale_attempt_cannot_checkpoint(build: Build, storage: Storage) -> None:
    workspaces = build([rule()])
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    await reactor(workspaces, Deps()).evaluate()
    [pending] = await workspace.runs()
    with pytest.raises(InvalidState, match=f"attempt 1 of run {pending.id} is no longer current"):
        await workspace.checkpoint_run(pending.id, attempt=1, step="s", state=None)
    with pytest.raises(NotFound, match="run nope"):
        await workspace.checkpoint_run("nope", attempt=1, step="s", state=None)


async def test_each_attempt_runs_inside_the_run_context_with_its_chain_in_baggage(
    build: Build,
) -> None:
    entered: list[tuple[str, int]] = []
    seen: list[object] = []

    @asynccontextmanager
    async def attribute(reaction: Reaction[Any]) -> AsyncIterator[None]:
        entered.append((reaction.run_id, reaction.attempt))
        yield

    async def look(reaction: Reaction[None]) -> None:
        seen.append(baggage.get_baggage(a.SESSION_ID))

    workspaces = build([rule(then=run("look"))])
    workspace = await open_(workspaces)
    deploy = (await workspace.publish(Deploy(service="auth"))).envelope
    executor = Reactor(workspaces, actions={"look": look}, run_context=attribute)
    await executor.settle()
    [done] = await workspace.runs()
    assert entered == [(done.id, 1)]
    assert seen == [deploy.id]
    assert baggage.get_baggage(a.SESSION_ID) is None  # detached after the attempt
