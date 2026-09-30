"""A caller of SQL storage cancelled anywhere leaves no lock or connection behind.

A cancellation lands wherever its task is: a rule's timeout, a stopped or cancelled reactor, a
WebSocket that disconnects, or an anyio cancel scope, which cancels its task again until the task
ends. SQLAlchemy takes a statement cancelled part-way for a lost connection, so SQL storage awaits
every database call to its end and raises the cancellation after it.

The cancellations are seeded, not deterministic: where one lands depends on the database's timing.
"""

import asyncio
import contextlib
import random
from collections.abc import Awaitable, Callable
from datetime import timedelta

import pytest
from sqlalchemy.pool import QueuePool

from reflexr import SourceActor
from reflexr.core import new_event_id
from reflexr.sql import SqlStorage
from reflexr.sql.storage import _to_the_end
from reflexr.workspace import RunPolicy, WorkspaceRef, Workspaces, utc_now
from tests.databases import Database
from tests.event_types import Deploy
from tests.workspace.helpers import entry, fired

ACME = WorkspaceRef("acme", "prod")
ITERATIONS = 100
MINUTE = timedelta(minutes=1)
LOCK_TIMEOUT = timedelta(milliseconds=250)
"""How long the other process waits for a lock, so one left behind fails the test, not hangs it."""

type Operation = Callable[[], Awaitable[object]]
type Cancel = Callable[[Operation, random.Random], Awaitable[None]]


@pytest.mark.parametrize("ending", ["returns", "raises", "is cancelled"])
async def test_a_cancelled_call_ends_before_the_cancellation_is_raised(ending: str) -> None:
    release = asyncio.Event()
    ended: list[str] = []

    async def call() -> None:
        await release.wait()
        ended.append(ending)
        if ending == "raises":
            raise RuntimeError("the statement failed")
        if ending == "is cancelled":
            raise asyncio.CancelledError

    caller = asyncio.create_task(_to_the_end(call()))
    for _ in range(3):  # cancelled again at every await, as anyio does
        await asyncio.sleep(0)
        caller.cancel()
    await asyncio.sleep(0)
    assert not caller.done(), "the call is still running"
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await caller
    assert ended == [ending]


async def cancel_after_some_yields(operation: Operation, rng: random.Random) -> None:
    task = asyncio.ensure_future(operation())
    for _ in range(rng.randrange(40)):
        await asyncio.sleep(0)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    if not task.cancelled():
        task.result()


async def cancel_until_it_ends(operation: Operation, rng: random.Random) -> None:
    task = asyncio.ensure_future(operation())
    for _ in range(rng.randrange(40)):
        await asyncio.sleep(0)
    while not task.done():
        task.cancel()
        # Not at every turn of the loop, as anyio does: that starves aiosqlite's thread of the
        # GIL on Linux, and a call takes seconds to end.
        await asyncio.sleep(0.001)
    if not task.cancelled():
        task.result()


async def time_out(operation: Operation, rng: random.Random) -> None:
    with contextlib.suppress(TimeoutError):
        async with asyncio.timeout(rng.random() / 200):  # up to 5 ms
            await operation()


CANCELS: dict[str, Cancel] = {
    "after some yields": cancel_after_some_yields,
    "until it ends": cancel_until_it_ends,
    "by a timeout": time_out,
}


@pytest.mark.parametrize("cancel", CANCELS)
@pytest.mark.parametrize("operation", ["publish", "read", "acquire_lease", "due_runs"])
async def test_a_cancelled_call_leaves_the_workspace_usable(
    schema: Database, operation: str, cancel: str
) -> None:
    here = schema.engine()
    pool = here.pool
    assert isinstance(pool, QueuePool)
    storage = SqlStorage(here)
    # Another process, which gives up waiting for a lock rather than waiting on one never freed.
    elsewhere = SqlStorage(schema.engine(lock_timeout=LOCK_TIMEOUT))
    workspace = await Workspaces(storage, events=[Deploy]).open(
        "acme", "prod", actor=SourceActor(name="ci")
    )
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([fired("r1"), fired("r2", seq=2), fired("r3", scope="db")])
    ordered = RunPolicy(ordered=frozenset({"triage"}))
    operations: dict[str, Operation] = {
        "publish": lambda: workspace.publish(Deploy(service="auth")),
        "read": lambda: storage.read(ACME),
        "acquire_lease": lambda: storage.acquire_lease(ACME, "evaluate", "here", MINUTE),
        "due_runs": lambda: storage.due_runs(now=utc_now(), limit=10, policy=ordered),
    }
    rng = random.Random(f"{operation} {cancel}")
    for iteration in range(ITERATIONS):
        await CANCELS[cancel](operations[operation], rng)
        assert pool.checkedout() == 0, f"iteration {iteration} kept its connection"
        async with elsewhere.transaction(ACME) as other:
            await other.save_schedule("elsewhere", utc_now())
        async with storage.transaction(ACME) as ours:
            await ours.append([entry(new_event_id())])
    log = await storage.read(ACME)
    assert [envelope.seq for envelope in log] == list(range(1, len(log) + 1))
