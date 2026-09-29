"""The storage protocol's behaviour, which every implementation must share."""

import asyncio
from datetime import datetime, timedelta

import pytest

from reflexr import RetryPolicy, Rule, on, run
from reflexr.core import (
    EvaluationError,
    RuleProgress,
    ScopeState,
    cancel,
    fail,
    skip,
    start,
    succeed,
)
from reflexr.workspace import RunPolicy, Storage, WorkspaceRef, run_lease
from tests.event_types import ServiceError
from tests.workspace.conftest import START, FakeClock
from tests.workspace.helpers import entry, fired

ACME = WorkspaceRef("acme", "prod")
OTHER = WorkspaceRef("globex", "prod")

ANY_ORDER = RunPolicy()
"""No rule known: every due run is a candidate."""

ORDERED = RunPolicy(ordered=frozenset({"triage"}))
"""The runs of triage, which ``fired`` makes by default, run in order per scope."""


def triage(**changes: object) -> Rule:
    fields = {"name": "triage", "when": on(ServiceError), "then": run("respond")}
    return Rule.model_validate({**fields, **changes})


async def test_appends_assign_gap_free_seqs_and_default_chains(storage: Storage) -> None:
    async with storage.transaction(ACME) as transaction:
        first, second = await transaction.append([entry("e1"), entry("e2", chain="e1")])
    async with storage.transaction(ACME) as transaction:
        [third] = await transaction.append([entry("e3")])
    assert [e.seq for e in (first, second, third)] == [1, 2, 3]
    assert [e.correlation_id for e in (first, second, third)] == ["e1", "e1", "e3"]
    assert (first.workspace_id, first.ts) == ("prod", START)
    assert await storage.head_seq(ACME) == 3
    assert await storage.read(ACME) == [first, second, third]
    assert await storage.read(ACME, after_seq=1, limit=1) == [second]


async def test_timestamps_never_go_backwards(storage: Storage, clock: FakeClock) -> None:
    async with storage.transaction(ACME) as transaction:
        await transaction.append([entry("e1")])
    clock.advance(-60)
    async with storage.transaction(ACME) as transaction:
        [later] = await transaction.append([entry("e2")])
    assert later.ts == START


async def test_a_transaction_that_raises_rolls_back(storage: Storage) -> None:
    async def write_then_fail() -> None:
        async with storage.transaction(ACME) as transaction:
            await transaction.append([entry("e1")])
            await transaction.save_runs([fired("r1")])
            await transaction.save_progress("triage", RuleProgress(cursor=1))
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await write_then_fail()
    assert await storage.head_seq(ACME) == 0
    assert await storage.run(ACME, "r1") is None
    assert await storage.progress(ACME) == {}


async def test_a_transaction_reads_its_own_writes(storage: Storage) -> None:
    async with storage.transaction(ACME) as transaction:
        await transaction.append([entry("e1")])
    async with storage.transaction(ACME) as transaction:
        [pending] = await transaction.append([entry("e2")])
        assert await transaction.head_seq() == 2
        assert await transaction.envelope("e2") == pending
        assert (await transaction.envelope("e1")) is not None
        assert await transaction.envelope("missing") is None
        assert [e.id for e in await transaction.read(after_seq=0, limit=10)] == ["e1", "e2"]
        assert [e.id for e in await transaction.read(after_seq=0, limit=1)] == ["e1"]
        assert [e.id for e in await transaction.read(after_seq=1, limit=10)] == ["e2"]
        assert await transaction.read(after_seq=2, limit=10) == []


async def test_an_id_can_be_appended_once(storage: Storage) -> None:
    async with storage.transaction(ACME) as transaction:
        await transaction.append([entry("e1")])
    with pytest.raises(ValueError, match="already in the log"):
        async with storage.transaction(ACME) as transaction:
            await transaction.append([entry("e1")])
    with pytest.raises(ValueError, match="already in the log"):
        async with storage.transaction(ACME) as transaction:
            await transaction.append([entry("e2"), entry("e2")])
    assert await storage.head_seq(ACME) == 1


async def test_workspaces_are_isolated(storage: Storage) -> None:
    async with storage.transaction(ACME) as transaction:
        await transaction.append([entry("e1")])
        await transaction.save_runs([fired("r1")])
    assert await storage.read(OTHER) == []
    assert await storage.run(OTHER, "r1") is None
    async with storage.transaction(OTHER) as transaction:
        assert await transaction.envelope("e1") is None
        [envelope] = await transaction.append([entry("e1")])
    assert envelope.seq == 1
    assert set(await storage.workspaces()) == {ACME, OTHER}


async def test_rule_progress_and_scope_states(storage: Storage) -> None:
    auth = ScopeState(scope={"service": "auth"})
    billing = ScopeState(scope={"service": "billing"})
    async with storage.transaction(ACME) as transaction:
        assert await transaction.progress("triage") is None
        await transaction.save_progress("triage", RuleProgress(cursor=3))
        await transaction.save_states("triage", {'["auth"]': auth})
        assert await transaction.progress("triage") == RuleProgress(cursor=3)
        assert await transaction.states("triage", ['["auth"]', '["x"]']) == {'["auth"]': auth}
    async with storage.transaction(ACME) as transaction:
        await transaction.save_states("triage", {'["billing"]': billing})
        both = await transaction.states("triage", ['["auth"]', '["billing"]'])
        assert both == {'["auth"]': auth, '["billing"]': billing}
        assert await transaction.states("other", ['["auth"]']) == {}
    assert await storage.progress(ACME) == {"triage": RuleProgress(cursor=3)}


async def test_clearing_states_hides_committed_and_pending_ones(storage: Storage) -> None:
    auth = ScopeState(scope={"service": "auth"})
    billing = ScopeState(scope={"service": "billing"})
    async with storage.transaction(ACME) as transaction:
        await transaction.save_states("triage", {'["auth"]': auth})
        await transaction.save_states("paging", {'["auth"]': auth})
    async with storage.transaction(ACME) as transaction:
        await transaction.save_states("triage", {'["billing"]': billing})
        await transaction.clear_states("triage")
        assert await transaction.states("triage", ['["auth"]', '["billing"]']) == {}
        await transaction.save_states("triage", {'["billing"]': billing})
    async with storage.transaction(ACME) as transaction:
        keys = ['["auth"]', '["billing"]']
        assert await transaction.states("triage", keys) == {'["billing"]': billing}
        assert await transaction.states("paging", keys) == {'["auth"]': auth}


async def test_runs_are_saved_and_listed_newest_first(storage: Storage) -> None:
    first, second = fired("r1", seq=1), fired("r2", scope="billing", seq=2)
    third = fired("r3", rule="paging", seq=3)
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([first, second, third])
        assert await transaction.run("r1") == first
        assert await transaction.run("missing") is None
    running, _ = start(first, now=START)
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([running])
    assert await storage.run(ACME, "r1") == running
    assert await storage.runs(ACME) == [third, second, running]
    assert await storage.runs(ACME, rule="triage") == [second, running]
    assert await storage.runs(ACME, status="running") == [running]
    assert await storage.runs(ACME, scope_key='["billing"]') == [second]
    assert await storage.runs(ACME, limit=1) == [third]


async def test_scope_runs_are_unsucceeded_runs_in_firing_order(storage: Storage) -> None:
    later, earlier = fired("r2", seq=2), fired("r1", seq=1)
    done, _ = succeed(start(fired("r0", seq=0), now=START)[0], now=START)
    other = fired("r3", scope="billing", seq=3)
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([done, later, other])
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([earlier])
        assert await transaction.scope_runs("triage", '["auth"]') == [earlier, later]


async def test_due_runs_span_workspaces_oldest_first(storage: Storage) -> None:
    soon = fired("r1").model_copy(update={"next_attempt_at": START + timedelta(seconds=5)})
    now = fired("r2")
    later = fired("r3").model_copy(update={"next_attempt_at": START + timedelta(hours=1)})
    running, _ = start(fired("r4"), now=START)
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([soon, later, running])
    async with storage.transaction(OTHER) as transaction:
        await transaction.save_runs([now])
    await storage.acquire_lease(ACME, run_lease("r4"), "executor", timedelta(minutes=1))
    later_on = START + timedelta(seconds=10)
    due = await storage.due_runs(now=later_on, limit=10, policy=ANY_ORDER)
    assert due == [(OTHER, now), (ACME, soon)]
    assert await storage.due_runs(now=later_on, limit=1, policy=ANY_ORDER) == [(OTHER, now)]


async def test_due_runs_leave_out_the_runs_of_disabled_rules(storage: Storage) -> None:
    paging = fired("r1", rule="page")
    triage = fired("r2", scope="db")
    abandoned, _ = start(fired("r3", rule="page", scope="billing"), now=START)
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([paging, triage, abandoned])
    assert len(await storage.due_runs(now=START, limit=10, policy=ANY_ORDER)) == 3
    disabled = RunPolicy(disabled=frozenset({"page"}))
    assert await storage.due_runs(now=START, limit=10, policy=disabled) == [(ACME, triage)]
    assert await storage.due_runs(now=START, limit=1, policy=disabled) == [(ACME, triage)]


async def test_running_runs_are_due_when_their_lease_lapses(
    storage: Storage, clock: FakeClock
) -> None:
    held, _ = start(fired("r1"), now=START)
    orphaned, _ = start(fired("r2", scope="billing"), now=START)
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([held, orphaned])
    await storage.acquire_lease(ACME, run_lease("r1"), "executor", timedelta(seconds=30))
    assert await storage.due_runs(now=START, limit=10, policy=ANY_ORDER) == [(ACME, orphaned)]
    clock.advance(31)
    due = await storage.due_runs(now=clock(), limit=10, policy=ANY_ORDER)
    assert {run.id for _, run in due} == {"r1", "r2"}


def test_a_run_policy_describes_its_rules() -> None:
    rules = [
        triage(),
        triage(name="notify", ordering="none", on_dead_letter="block"),
        triage(name="page", on_dead_letter="block", enabled=False),
    ]
    assert RunPolicy.of(rules) == RunPolicy(
        disabled=frozenset({"page"}),
        ordered=frozenset({"triage", "page"}),
        blocking=frozenset({"page"}),
    )


async def due_ids(
    storage: Storage, policy: RunPolicy, *, now: datetime = START, limit: int = 10
) -> list[str]:
    return [run.id for _, run in await storage.due_runs(now=now, limit=limit, policy=policy)]


async def test_an_ordered_rules_due_runs_are_the_first_of_each_scope(storage: Storage) -> None:
    first, second = fired("r1", seq=1), fired("r2", seq=2)
    billing = fired("r3", scope="billing", seq=3)
    unordered = [fired("r4", rule="notify", seq=4), fired("r5", rule="notify", seq=5)]
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([first, second, billing, *unordered])
    async with storage.transaction(OTHER) as transaction:
        await transaction.save_runs([fired("r6", seq=6), fired("r7", seq=7)])
    assert await due_ids(storage, ORDERED) == ["r1", "r3", "r4", "r5", "r6"]
    assert await due_ids(storage, ANY_ORDER) == ["r1", "r2", "r3", "r4", "r5", "r6", "r7"]


async def test_a_scopes_backlog_takes_one_place_in_the_limit(storage: Storage) -> None:
    backlog = [fired(f"busy{seq}", scope="busy", seq=seq) for seq in range(1, 31)]
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([*backlog, fired("quiet", scope="quiet", seq=31)])
    assert await due_ids(storage, ORDERED, limit=2) == ["busy1", "quiet"]
    assert await due_ids(storage, ANY_ORDER, limit=2) == ["busy1", "busy2"]


async def test_finished_runs_do_not_hold_their_scope(storage: Storage) -> None:
    done, _ = succeed(start(fired("r1", seq=1), now=START)[0], now=START)
    cancelled, _ = cancel(fired("r2", seq=2), now=START)
    skipped, _ = skip(fired("r3", seq=3), now=START)
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([done, cancelled, skipped, fired("r4", seq=4)])
    assert await due_ids(storage, ORDERED) == ["r4"]


async def test_runs_waiting_to_retry_and_running_hold_their_scope(
    storage: Storage, clock: FakeClock
) -> None:
    retrying, _ = fail(start(fired("r1", seq=1), now=START)[0], triage(), now=START, error="x")
    running, _ = start(fired("r3", scope="billing", seq=3), now=START)
    behind = [fired("r2", seq=2), fired("r4", scope="billing", seq=4)]
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([retrying, running, *behind])
    await storage.acquire_lease(ACME, run_lease("r3"), "executor", timedelta(seconds=30))
    assert await due_ids(storage, ORDERED) == []
    clock.advance(31)  # the retry is due, and the running run's lease has lapsed
    assert await due_ids(storage, ORDERED, now=clock()) == ["r3", "r1"]


async def test_a_lapsed_attempt_is_due_wherever_it_is_in_its_scope(storage: Storage) -> None:
    # An operator retried an earlier run while a later one was running, and its executor died.
    running, _ = start(fired("r2", seq=2), now=START)
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([fired("r1", seq=1), running])
    assert await due_ids(storage, ORDERED) == ["r1", "r2"]


@pytest.mark.parametrize(("on_dead_letter", "due"), [("continue", ["r2"]), ("block", [])])
async def test_a_dead_lettered_run_holds_its_scope_if_its_rule_blocks(
    storage: Storage, on_dead_letter: str, due: list[str]
) -> None:
    rule = triage(on_dead_letter=on_dead_letter, retry=RetryPolicy(max_attempts=1))
    dead, _ = fail(start(fired("r1", seq=1), now=START)[0], rule, now=START, error="x")
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([dead, fired("r2", seq=2)])
    assert await due_ids(storage, RunPolicy.of([rule])) == due


async def test_runs_fired_at_the_same_seq_start_in_the_order_they_were_created(
    storage: Storage,
) -> None:
    # A rule replayed to fire again fires at seqs it already fired at, with new run ids.
    replayed, original = fired("r1", seq=5), fired("r2", seq=5)
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([original])
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([replayed, fired("r0", seq=4)])
        assert await transaction.scope_runs("triage", '["auth"]') == [
            fired("r0", seq=4),
            original,
            replayed,
        ]
    assert await due_ids(storage, ORDERED) == ["r0"]
    async with storage.transaction(ACME) as transaction:
        await transaction.save_runs([skip(fired("r0", seq=4), now=START)[0]])
    assert await due_ids(storage, ORDERED) == ["r2"]


async def test_dead_letters_are_kept_per_rule(storage: Storage) -> None:
    triage = EvaluationError(rule="triage", seq=1, error="no service")
    paging = EvaluationError(rule="paging", seq=2, error="boom")
    async with storage.transaction(ACME) as transaction:
        await transaction.dead_letter([triage, paging])
    assert await storage.dead_letters(ACME) == [triage, paging]
    assert await storage.dead_letters(ACME, rule="paging") == [paging]


async def test_subscribers_see_stored_then_new_envelopes(storage: Storage) -> None:
    async with storage.transaction(ACME) as transaction:
        await transaction.append([entry("e1"), entry("e2")])
    stream = storage.subscribe(ACME, after_seq=1)
    assert (await anext(stream)).id == "e2"
    waiting = asyncio.ensure_future(anext(stream))
    await asyncio.sleep(0)
    assert not waiting.done()
    async with storage.transaction(ACME) as transaction:
        await transaction.append([entry("e3")])
    assert (await asyncio.wait_for(waiting, timeout=1)).id == "e3"
    await stream.aclose()


async def test_leases_are_exclusive_until_they_lapse(storage: Storage, clock: FakeClock) -> None:
    ttl = timedelta(seconds=30)
    assert await storage.acquire_lease(ACME, "evaluate", "a", ttl)
    assert not await storage.acquire_lease(ACME, "evaluate", "b", ttl)
    assert await storage.acquire_lease(ACME, "evaluate", "a", ttl)  # a renewal
    assert await storage.acquire_lease(OTHER, "evaluate", "b", ttl)
    clock.advance(31)
    assert await storage.acquire_lease(ACME, "evaluate", "b", ttl)
    await storage.release_lease(ACME, "evaluate", "a")  # not a's any more: no effect
    assert not await storage.acquire_lease(ACME, "evaluate", "a", ttl)
    await storage.release_lease(ACME, "evaluate", "b")
    await storage.release_lease(ACME, "missing", "b")
    assert await storage.acquire_lease(ACME, "evaluate", "a", ttl)
