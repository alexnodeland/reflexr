"""SQL storage across processes: locking, polling, leases and the values it stores.

The workspace behaviour suite runs on SQL storage too; these tests cover what only a shared
database can show. Each :class:`SqlStorage` built with :func:`process` has its own engine, as
it would in another process.
"""

import asyncio
from collections import Counter
from datetime import UTC, datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from reflexr import Rule, SourceActor, on, run
from reflexr.core import RuleProgress, ScopeState, UnknownEvent, start
from reflexr.sql import SqlStorage, migrate
from reflexr.workspace import (
    Clock,
    Entry,
    Reaction,
    Reactor,
    RunPolicy,
    WorkspaceRef,
    Workspaces,
    run_lease,
    utc_now,
)
from tests.databases import Database
from tests.event_types import Deploy, ServiceError
from tests.workspace.helpers import entry, fired

ACME = WorkspaceRef("acme", "prod")
MONITOR = SourceActor(name="monitor")
MINUTE = timedelta(minutes=1)
NOON = datetime(2026, 9, 28, 12, tzinfo=UTC)
WEST = timezone(timedelta(hours=-7))
EAST = timezone(timedelta(hours=5))


def process(
    database: Database, *, clock: Clock = utc_now, poll_interval: timedelta = MINUTE
) -> SqlStorage:
    """Storage on its own engine, as in another process."""
    return SqlStorage(database.engine(), clock=clock, poll_interval=poll_interval)


async def test_a_migrated_database_stores_workspaces(engine: AsyncEngine) -> None:
    await migrate(engine)
    workspace = await Workspaces(SqlStorage(engine), events=[Deploy]).open(
        "acme", "prod", actor=MONITOR
    )
    published = await workspace.publish(Deploy(service="auth"), id="d1")
    assert await workspace.read() == [published.envelope]


@pytest.mark.parametrize("workspace", ["new", "existing"])
async def test_a_transaction_waits_for_the_one_before_it(schema: Database, workspace: str) -> None:
    first, second = process(schema), process(schema)
    if workspace == "existing":
        async with first.transaction(ACME) as transaction:
            await transaction.save_progress("triage", RuleProgress())
    began, release = asyncio.Event(), asyncio.Event()

    async def append_when_released() -> None:
        async with first.transaction(ACME) as transaction:
            began.set()
            await release.wait()
            await transaction.append([entry("e1")])

    async def look() -> int:
        async with second.transaction(ACME) as transaction:
            return await transaction.head_seq()

    appender = asyncio.create_task(append_when_released())
    await began.wait()
    looker = asyncio.create_task(look())
    await asyncio.sleep(0.2)
    assert not looker.done(), "the second transaction waits for the first to end"
    release.set()
    await appender
    assert await asyncio.wait_for(looker, timeout=5) == 1, "and then sees its changes"


async def test_concurrent_appends_get_gap_free_seqs_and_ordered_times(schema: Database) -> None:
    storages = [process(schema) for _ in range(4)]

    async def append(storage: SqlStorage, event_id: str) -> None:
        async with storage.transaction(ACME) as transaction:
            await transaction.append([entry(event_id)])

    await asyncio.gather(*(append(s, f"e{i}") for i, s in enumerate(storages * 3)))
    log = await storages[0].read(ACME)
    assert [e.seq for e in log] == list(range(1, 13))
    assert [e.ts for e in log] == sorted(e.ts for e in log)
    assert {e.id for e in log} == {f"e{i}" for i in range(12)}
    assert await storages[1].head_seq(ACME) == 12


async def test_timestamps_never_go_backwards_across_processes(schema: Database) -> None:
    ahead = process(schema, clock=lambda: NOON.astimezone(EAST))
    behind = process(schema, clock=lambda: (NOON - MINUTE).astimezone(WEST))
    async with ahead.transaction(ACME) as transaction:
        [first] = await transaction.append([entry("e1")])
    async with behind.transaction(ACME) as transaction:
        [second] = await transaction.append([entry("e2")])
    assert first.ts == second.ts == NOON
    stored = await behind.read(ACME)
    assert stored == [first, second]
    assert stored[0].ts.utcoffset() == timedelta(hours=5), "an envelope keeps its ts as given"


async def test_subscriptions_poll_for_commits_made_elsewhere(schema: Database) -> None:
    here = process(schema, poll_interval=timedelta(milliseconds=20))
    elsewhere = process(schema)
    received: list[str] = []

    async def follow() -> None:
        async for envelope in here.subscribe(ACME):
            received.append(envelope.id)
            if len(received) == 2:
                return

    follower = asyncio.create_task(follow())
    await asyncio.sleep(0.05)  # the follower has read the empty log and is waiting
    for event_id in ("e1", "e2"):
        async with elsewhere.transaction(ACME) as transaction:
            await transaction.append([entry(event_id)])
    await asyncio.wait_for(follower, timeout=5)
    assert received == ["e1", "e2"]


async def test_one_of_many_concurrent_holders_gets_a_lease(schema: Database) -> None:
    storages = [process(schema) for _ in range(8)]
    taken = await asyncio.gather(
        *(s.acquire_lease(ACME, "k", f"holder {i}", MINUTE) for i, s in enumerate(storages))
    )
    assert sorted(taken) == [False] * 7 + [True]


async def test_leases_belong_to_their_workspace(schema: Database) -> None:
    storage = process(schema)
    assert await storage.acquire_lease(ACME, "k", "a", MINUTE)
    assert await storage.acquire_lease(WorkspaceRef("globex", "prod"), "k", "b", MINUTE)
    assert await storage.acquire_lease(WorkspaceRef("acme", "staging"), "k", "b", MINUTE)


async def test_lease_expiry_compares_instants_whatever_the_clocks_zone(schema: Database) -> None:
    now = NOON
    storage = process(schema, clock=lambda: now)
    ttl = timedelta(seconds=30)
    assert await storage.acquire_lease(ACME, "k", "a", ttl)
    now = (NOON + timedelta(seconds=29)).astimezone(WEST)
    assert not await storage.acquire_lease(ACME, "k", "b", ttl)
    now = (NOON + timedelta(seconds=31)).astimezone(EAST)
    assert await storage.acquire_lease(ACME, "k", "b", ttl)


async def test_due_runs_compare_instants_whatever_the_zone(schema: Database) -> None:
    storage = process(schema, clock=lambda: NOON)
    soon = fired("r1").model_copy(update={"next_attempt_at": (NOON + MINUTE).astimezone(EAST)})
    later = fired("r2").model_copy(update={"next_attempt_at": NOON + 2 * MINUTE})
    held, _ = start(fired("r3"), now=NOON)
    lapsed, _ = start(fired("r4"), now=NOON)
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([later, soon, held, lapsed])
    assert await storage.acquire_lease(ACME, run_lease("r3"), "executor", 3 * MINUTE)
    assert await storage.acquire_lease(ACME, run_lease("r4"), "executor", MINUTE)
    due = await storage.due_runs(
        now=(NOON + timedelta(seconds=90)).astimezone(WEST), limit=10, policy=RunPolicy()
    )
    assert [run.id for _, run in due] == ["r4", "r1"]
    due = await storage.due_runs(
        now=(NOON + 3 * MINUTE).astimezone(EAST), limit=10, policy=RunPolicy()
    )
    assert [run.id for _, run in due] == ["r3", "r4", "r1", "r2"]


async def test_schedule_times_come_back_as_the_same_instants(schema: Database) -> None:
    storage = process(schema)
    at = NOON.astimezone(EAST)
    async with storage.transaction(ACME) as transaction:
        await transaction.save_schedule("nightly", at)
    async with storage.transaction(ACME) as transaction:
        assert await transaction.schedule("nightly") == at
    [(name, stored)] = (await storage.schedules(ACME)).items()
    assert (name, stored, stored.tzinfo) == ("nightly", at, UTC)


async def test_many_scope_states_are_saved_and_read_together(schema: Database) -> None:
    storage = process(schema)
    states = {f'["s{i}"]': ScopeState(scope={"service": f"s{i}"}) for i in range(1234)}
    async with storage.transaction(ACME) as transaction:
        await transaction.save_states("triage", states)
    keys = [*reversed(states), '["missing"]']
    async with storage.transaction(ACME) as transaction:
        found = await transaction.states("triage", keys)
    assert found == states
    assert list(found) == keys[:-1], "in the order asked for"


async def test_stored_values_keep_their_key_order(schema: Database) -> None:
    storage = process(schema)
    extra: dict[str, object] = {"zeta": 1, "b": 2, "alpha_long": 3}
    scope: dict[str, object] = {"zeta": "z", "b": "b", "alpha_long": "a"}
    async with storage.transaction(ACME) as transaction:
        event = ServiceError(service="auth", extra=extra)
        await transaction.append([Entry(id="e1", actor=MONITOR, event=event)])
        await transaction.save_states("triage", {"k": ScopeState.model_validate({"scope": scope})})
    [envelope] = await storage.read(ACME)
    assert isinstance(envelope.event, ServiceError)
    assert list(envelope.event.extra) == list(extra)
    async with storage.transaction(ACME) as transaction:
        [state] = (await transaction.states("triage", ["k"])).values()
    assert list(state.scope) == list(scope)


async def test_events_of_types_this_process_does_not_know_round_trip(schema: Database) -> None:
    storage = process(schema)
    fields = {"unknown_type": "deploy.rolled_back", "service": "auth", "to": "1.2"}
    newer = UnknownEvent.model_validate(fields)
    async with storage.transaction(ACME) as transaction:
        [appended] = await transaction.append([Entry(id="e1", actor=MONITOR, event=newer)])
    [stored] = await storage.read(ACME)
    assert stored == appended
    assert isinstance(stored.event, UnknownEvent)
    assert stored.data == {"type": "deploy.rolled_back", "service": "auth", "to": "1.2"}


async def test_reactors_in_two_processes_fire_and_run_each_firing_once(schema: Database) -> None:
    calls: Counter[str] = Counter()

    async def note(reaction: Reaction[None]) -> None:
        calls[reaction.run.id] += 1

    rule = Rule(name="app:deploys", when=on(Deploy), then=run("note"))
    processes = [Workspaces(process(schema), events=[Deploy], rules=[rule]) for _ in range(2)]
    workspace = await processes[0].open("acme", "prod", actor=MONITOR)
    for i in range(10):
        await workspace.publish(Deploy(service=f"s{i}"))
    reactors = [Reactor(workspaces, actions={"note": note}) for workspaces in processes]
    settled = await asyncio.gather(*(reactor.settle() for reactor in reactors))
    assert sum(s.firings for s in settled) == 10
    runs = await workspace.runs()
    assert len(runs) == 10
    assert {r.status for r in runs} == {"succeeded"}
    assert calls == Counter({r.id: 1 for r in runs})
