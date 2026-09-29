"""The reactor's execution: runs attempted at least once, under leases, in order per scope."""

import asyncio
from collections.abc import AsyncIterator, Iterable
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
    Envelope,
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
    EVALUATION_LEASE,
    REACTOR,
    InMemoryStorage,
    Reaction,
    Reactor,
    RunFailure,
    RunPolicy,
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


async def test_a_scopes_backlog_does_not_hold_up_other_scopes(build: Build) -> None:
    workspaces = build([rule()])
    deps = Deps()
    workspace = await open_(workspaces)
    await workspace.publish_many([Deploy(service="busy") for _ in range(150)])
    await workspace.publish(Deploy(service="quiet"))
    executor = reactor(workspaces, deps)
    await executor.evaluate()
    # More runs of busy wait than execute's limit (100), and quiet's one run fired last.
    assert await executor.execute() == 2
    assert [r.scope["service"] for r in deps.calls] == ["busy", "quiet"]


@pytest.mark.parametrize("depth", [2, 25])
async def test_each_pass_claims_one_run_per_scope_however_deep_the_backlog(
    build: Build, telemetry: Telemetry, depth: int
) -> None:
    workspaces = build([rule()])
    workspace = await open_(workspaces)
    services = ["auth", "billing"] * depth + ["db"]
    await workspace.publish_many([Deploy(service=service) for service in services])
    executor = reactor(workspaces, Deps())
    await executor.evaluate()

    def claims() -> int:
        spans = telemetry.spans.get_finished_spans()
        return sum(span.name == "invoke_workflow page" for span in spans)

    passes: list[int] = []
    while attempts := await executor.execute():
        assert claims() - sum(passes) == attempts  # every claim was an attempt
        passes.append(attempts)
    assert passes == [3] + [2] * (depth - 1)
    assert claims() == len(services)


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


async def test_settling_waits_for_a_run_another_reactor_holds(
    build: Build, storage: Storage
) -> None:
    workspaces = build([rule()])
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    executor = reactor(workspaces, Deps())
    await executor.evaluate()
    [pending] = await workspace.runs()
    key = run_lease(pending.id)
    await storage.acquire_lease(ACME, key, "elsewhere", timedelta(minutes=1))

    async def let_go() -> None:  # as a reactor that found it could not start the run would
        await asyncio.sleep(0.02)
        await storage.release_lease(ACME, key, "elsewhere")

    letting_go = asyncio.create_task(let_go())
    assert await executor.settle() == Settled(attempts=1)
    await letting_go
    assert [r.status for r in await workspace.runs()] == ["succeeded"]


async def test_settling_gives_up_on_a_run_held_elsewhere_for_good(
    build: Build, storage: Storage
) -> None:
    workspaces = build([rule()])
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    executor = reactor(workspaces, Deps())
    await executor.evaluate()
    [pending] = await workspace.runs()
    await storage.acquire_lease(ACME, run_lease(pending.id), "elsewhere", timedelta(minutes=1))
    with pytest.raises(RuntimeError, match="still busy after 4 rounds"):
        await executor.settle(max_rounds=4)


class SlowToLetGo(InMemoryStorage):
    """Storage on which the reactor named "slow" holds on to run leases until it is let go."""

    def __init__(self, clock: FakeClock) -> None:
        super().__init__(clock=clock)
        self.holding = asyncio.Event()
        self.let_go = asyncio.Event()

    async def release_lease(self, workspace: WorkspaceRef, key: str, holder: str) -> None:
        if holder == "slow" and key.startswith(run_lease("")):
            self.holding.set()
            await self.let_go.wait()
        await super().release_lease(workspace, key, holder)


async def first_waits(reaction: Reaction[Deps]) -> None:
    reaction.deps.calls.append(reaction)
    if reaction.run.fired_seq == 1:
        reaction.deps.started.set()
        await reaction.deps.release.wait()


async def test_reactors_settling_together_leave_nothing_to_do(clock: FakeClock) -> None:
    # Two reactors settle at once, as in #57. Before, while fast ran the scope's first run,
    # slow leased the two behind it, found they could not start, and was slow to let them go;
    # fast finished, found them leased, took that for nothing to do and settled, and so did
    # slow, leaving both pending. Now slow finds nothing it could start, and fast does it all.
    storage = SlowToLetGo(clock)
    workspaces = Workspaces(storage, events=[Deploy], rules=[rule()], clock=clock)
    workspace = await open_(workspaces)
    await workspace.publish_many([Deploy(service="auth") for _ in range(3)])
    deps = Deps()
    fast, slow = (
        Reactor(workspaces, actions={"respond": first_waits}, deps=deps, holder=holder)
        for holder in ("fast", "slow")
    )
    await fast.evaluate()
    async with asyncio.timeout(5):
        fast_settling = asyncio.create_task(fast.settle())
        await deps.started.wait()
        slow_settling = asyncio.create_task(slow.settle())
        holding = asyncio.create_task(storage.holding.wait())
        await asyncio.wait({holding, slow_settling}, return_when=asyncio.FIRST_COMPLETED)
        deps.release.set()
        settled = [await fast_settling]
        storage.let_go.set()
        settled.append(await slow_settling)
    holding.cancel()
    assert [r.status for r in await workspace.runs()] == ["succeeded"] * 3
    assert sum(s.attempts for s in settled) == len(deps.calls) == 3


class StaleDue(InMemoryStorage):
    """Storage whose due runs were read before other writers changed them."""

    stale: list[tuple[WorkspaceRef, Run]] | None = None

    async def due_runs(
        self, *, now: object, limit: int, policy: RunPolicy
    ) -> list[tuple[WorkspaceRef, Run]]:
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


async def test_runs_of_a_disabled_rule_wait_without_holding_others_up(build: Build) -> None:
    errors = rule(name="errors", when=on(ServiceError))
    workspaces = build([rule(), errors])
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    await workspace.publish(ServiceError(service="auth"))
    deps = Deps()
    await reactor(workspaces, deps).evaluate()
    paused = reactor(build([rule(enabled=False), errors]), deps)
    assert await paused.execute(limit=1) == 1  # the older run's rule is disabled, so it waits
    assert await paused.execute() == 0
    assert {r.rule: r.status for r in await workspace.runs()} == {
        "page": "pending",
        "errors": "succeeded",
    }
    assert await reactor(workspaces, deps).execute() == 1  # enabled again, it runs
    assert [r.status for r in await workspace.runs(rule="page")] == ["succeeded"]


async def test_a_disabled_rules_run_found_due_is_left_waiting(clock: FakeClock) -> None:
    storage = StaleDue(clock=clock)  # a storage that does not leave disabled rules' runs out
    workspaces = Workspaces(storage, rules=[rule(enabled=False)], clock=clock)
    waiting = fired("r1", rule="page")
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([waiting])
    storage.stale = [(ACME, waiting)]
    assert await reactor(workspaces, Deps()).execute() == 0
    assert [r.status for r in await storage.runs(ACME)] == ["pending"]


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


async def test_typed_failures_record_their_reason_and_permanent_ones_skip_retries(
    build: Build, telemetry: Telemetry
) -> None:
    async def throttled(reaction: Reaction[None]) -> None:
        raise RunFailure("slow down", reason="rate_limit")

    async def blocked(reaction: Reaction[None]) -> None:
        raise RunFailure("blocked by pii-mask", reason="guardrail_blocked", permanent=True)

    rules = [
        rule(name="throttled", then=run("throttled")),
        rule(name="blocked", then=run("blocked")),
    ]
    workspaces = build(rules)
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    await Reactor(workspaces, actions={"throttled": throttled, "blocked": blocked}).settle()
    runs = {r.rule: r for r in await workspace.runs()}
    assert (runs["throttled"].status, runs["throttled"].reason) == ("retrying", "rate_limit")
    assert (runs["blocked"].status, runs["blocked"].attempts, runs["blocked"].reason) == (
        "dead",
        1,
        "guardrail_blocked",
    )
    [dead] = [e.event for e in await workspace.read() if isinstance(e.event, RunDeadLettered)]
    assert (dead.reason, dead.error) == ("guardrail_blocked", "blocked by pii-mask")
    reasons = {
        (p[0].get(a.RUN_STATUS), p[0].get(a.RUN_REASON)) for p in telemetry.points("reflexr.runs")
    }
    assert {("retrying", "rate_limit"), ("dead", "guardrail_blocked")} <= reasons


async def test_events_emitted_after_a_resumed_checkpoint_are_not_mistaken_for_earlier_ones(
    build: Build, clock: FakeClock
) -> None:
    attempts: list[int] = []

    async def stepwise(reaction: Reaction[None]) -> None:
        attempts.append(reaction.attempt)
        if reaction.run.checkpoint is None:
            await reaction.emit(ServiceError(service="auth", message="step one"))
            await reaction.checkpoint("one", {"done": 1})
            raise RuntimeError("crashed in step two")
        published = await reaction.emit(ServiceError(service="auth", message="step two"))
        assert not published.duplicate

    workspaces = build([rule(then=run("stepwise"))])
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    executor = Reactor(workspaces, actions={"stepwise": stepwise})
    await executor.settle()
    clock.advance(1)
    await executor.settle()
    messages = [
        e.event.message for e in await workspace.read() if isinstance(e.event, ServiceError)
    ]
    assert (attempts, messages) == ([1, 2], ["step one", "step two"])
    [done] = await workspace.runs()
    assert (done.status, done.checkpoints) == ("succeeded", 1)


async def test_run_started_names_the_scope(build: Build) -> None:
    workspaces = build([rule()])
    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    await reactor(workspaces, Deps()).settle()
    [started] = [e.event for e in await workspace.read() if isinstance(e.event, RunStarted)]
    assert (started.scope, started.scope_key) == ({"service": "auth"}, '["auth"]')


async def test_serving_survives_a_failing_pass(
    build: Build, caplog: pytest.LogCaptureFixture
) -> None:
    workspaces = build([rule()])
    workspace = await open_(workspaces)
    executor = reactor(workspaces, Deps())
    real = executor.tick
    failures = [RuntimeError("the database blinked")]

    async def flaky() -> int:
        if failures:
            raise failures.pop()
        return await real()

    executor.tick = flaky
    serving = asyncio.create_task(executor.serve(poll_interval=timedelta(milliseconds=5)))
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
    assert "a reactor pass failed" in caplog.text


async def assert_let_go(storage: Storage, runs: Iterable[Run]) -> None:
    """Check that no lease is held on the workspace or its runs, and no transaction is open."""
    for key in [EVALUATION_LEASE, *(run_lease(r.id) for r in runs)]:
        assert await storage.acquire_lease(ACME, key, "next", timedelta(minutes=1)), key
    async with asyncio.timeout(5), storage.transaction(ACME) as transaction:
        await transaction.save_schedule("check", START)


async def test_a_stopped_reactor_lets_running_attempts_end_and_starts_no_more(
    build: Build, storage: Storage
) -> None:
    workspaces = build([rule(then=run("blocked"))])
    deps = Deps()
    workspace = await open_(workspaces)
    await workspace.publish_many([Deploy(service="auth"), Deploy(service="billing")])
    stop = asyncio.Event()
    serving = reactor(workspaces, deps, concurrency=1).serve(stop=stop, grace=timedelta(minutes=1))
    async with asyncio.timeout(5):
        served = asyncio.create_task(serving)
        await deps.started.wait()  # one scope's attempt runs, and the other's waits its turn
        stop.set()
        deps.release.set()
        assert await served is None
    runs = await workspace.runs()
    assert sorted((r.status, r.attempts) for r in runs) == [("pending", 0), ("succeeded", 1)]
    await assert_let_go(storage, runs)


async def test_a_stopped_reactor_abandons_attempts_still_running_after_the_grace(
    build: Build, storage: Storage
) -> None:
    workspaces = build([rule(then=run("stuck"))])
    running = asyncio.Barrier(3)  # both attempts, and the test

    async def stuck(reaction: Reaction[None]) -> None:
        await running.wait()
        await asyncio.Event().wait()

    workspace = await open_(workspaces)
    await workspace.publish_many([Deploy(service="auth"), Deploy(service="billing")])
    stop = asyncio.Event()
    executor = Reactor(workspaces, actions={"stuck": stuck})
    async with asyncio.timeout(5):
        served = asyncio.create_task(executor.serve(stop=stop, grace=timedelta(milliseconds=10)))
        await running.wait()
        stop.set()
        await served
    runs = await workspace.runs()
    assert {(r.status, r.attempts, r.reason, r.error) for r in runs} == {
        ("retrying", 1, "abandoned", "the attempt was abandoned: its executor stopped")
    }
    facts = [e.event for e in await workspace.read() if isinstance(e.event, RunRetrying)]
    assert [f.reason for f in facts] == ["abandoned", "abandoned"]
    await assert_let_go(storage, runs)


class HeldStorage(InMemoryStorage):
    """Storage whose next transaction or read, once armed, stays in flight until let go."""

    def __init__(self, clock: FakeClock) -> None:
        super().__init__(clock=clock)
        self.armed = False
        self.holding = asyncio.Event()
        self.let_go = asyncio.Event()

    async def hold(self) -> None:
        if self.armed:
            self.armed = False
            self.holding.set()
            await self.let_go.wait()

    @asynccontextmanager
    async def transaction(self, workspace: WorkspaceRef) -> AsyncIterator[Any]:
        async with super().transaction(workspace) as transaction:
            await self.hold()
            yield transaction

    async def read(self, workspace: WorkspaceRef, **window: Any) -> list[Envelope]:
        await self.hold()
        return await super().read(workspace, **window)


@pytest.mark.parametrize("call", ["checkpoint", "read"])
async def test_a_stopped_reactor_cancels_an_action_only_between_its_storage_calls(
    call: str, clock: FakeClock
) -> None:
    storage = HeldStorage(clock)
    workspaces = Workspaces(storage, rules=[rule(then=run("careful"))], clock=clock)
    finished: list[str] = []

    async def careful(reaction: Reaction[None]) -> None:
        storage.armed = True
        if call == "checkpoint":
            await reaction.checkpoint("first", {"done": 1})
        else:
            await reaction.workspace.read()
        finished.append(call)
        await asyncio.Event().wait()

    workspace = await open_(workspaces)
    await workspace.publish(Deploy(service="auth"))
    stop = asyncio.Event()
    executor = Reactor(workspaces, actions={"careful": careful})
    async with asyncio.timeout(5):
        served = asyncio.create_task(executor.serve(stop=stop, grace=timedelta(0)))
        await storage.holding.wait()  # the action is in the middle of the call
        stop.set()
        await asyncio.sleep(0.05)
        assert not served.done()  # the grace is over, but the call is in flight
        storage.let_go.set()
        await served
    assert finished == [call]  # the call completed, and the action was cancelled after it
    [abandoned] = await workspace.runs()
    assert (abandoned.status, abandoned.reason) == ("retrying", "abandoned")


async def test_a_stopped_reactor_stops_waiting_for_its_next_pass(build: Build) -> None:
    stop = asyncio.Event()
    serving = reactor(build(), Deps()).serve(poll_interval=timedelta(hours=1), stop=stop)
    async with asyncio.timeout(5):
        served = asyncio.create_task(serving)
        await asyncio.sleep(0.01)
        stop.set()
        await served
