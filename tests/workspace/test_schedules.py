"""Schedules: ticks on a timetable, published once per time in each workspace they target."""

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from reflexr import F, Rule, SourceActor, by, on, run
from reflexr.core import Envelope, Tick
from reflexr.telemetry import attributes as a
from reflexr.workspace import (
    SCHEDULER,
    Entry,
    Reaction,
    Reactor,
    Schedule,
    ScheduleStatus,
    Settled,
    Storage,
    Workspace,
    WorkspaceRef,
    Workspaces,
    tick_id,
)
from tests.clock import START, FakeClock
from tests.event_types import Heartbeat
from tests.workspace.conftest import Telemetry

ACME = WorkspaceRef("acme", "prod")
every_30s = Schedule(name="heartbeat-check", every=timedelta(seconds=30))


async def noop(reaction: Reaction[None]) -> None:
    return None


def build(
    storage: Storage,
    clock: FakeClock,
    *schedules: Schedule,
    rules: tuple[Rule, ...] = (),
    telemetry: Telemetry | None = None,
) -> Workspaces:
    return Workspaces(
        storage,
        rules=rules,
        schedules=schedules,
        clock=clock,
        tracer_provider=telemetry.tracer_provider if telemetry else None,
        meter_provider=telemetry.meter_provider if telemetry else None,
    )


async def open_(workspaces: Workspaces) -> Workspace:
    return await workspaces.open("acme", "prod", actor=SourceActor(name="monitor"))


def ticks(envelopes: list[Envelope]) -> list[datetime]:
    return [e.event.at for e in envelopes if isinstance(e.event, Tick)]


# ─── the timetable ───────────────────────────────────────────────────────────


def test_a_schedule_has_one_valid_timetable() -> None:
    with pytest.raises(ValidationError, match="either every or cron"):
        Schedule(name="s")
    with pytest.raises(ValidationError, match="either every or cron"):
        Schedule(name="s", every=timedelta(seconds=1), cron="* * * * *")
    with pytest.raises(ValidationError, match="unknown time zone 'Mars/Olympus'"):
        Schedule(name="s", cron="* * * * *", timezone="Mars/Olympus")
    with pytest.raises(ValidationError, match="invalid cron expression '61 \\* \\* \\* \\*'"):
        Schedule(name="s", cron="61 * * * *")
    with pytest.raises(ValidationError):
        Schedule(name="s", every=timedelta(0))


def test_intervals_count_from_the_last_tick() -> None:
    times = every_30s.after(START)
    assert [next(times) for _ in range(2)] == [
        START + timedelta(seconds=30),
        START + timedelta(seconds=60),
    ]


def test_cron_follows_its_time_zone_across_daylight_saving() -> None:
    standup = Schedule(name="standup", cron="0 9 * * mon-fri", timezone="America/New_York")
    friday = datetime(2026, 3, 6, 14, 0, tzinfo=UTC)  # 09:00 in New York, before DST
    times = standup.after(friday)
    monday = next(times)
    assert monday == datetime(2026, 3, 9, 9, 0, tzinfo=ZoneInfo("America/New_York"))
    assert monday.astimezone(UTC).hour == 13  # after the clocks changed


def test_missed_ticks_are_skipped_or_caught_up() -> None:
    later = START + timedelta(seconds=95)
    assert every_30s.due(START, later) == [START + timedelta(seconds=90)]
    catch_up = every_30s.model_copy(update={"catch_up": "all", "max_catch_up": 2})
    assert catch_up.due(START, later) == [
        START + timedelta(seconds=60),
        START + timedelta(seconds=90),
    ]
    assert every_30s.due(START, START + timedelta(seconds=29)) == []
    assert every_30s.due(START, START - timedelta(seconds=60)) == []  # the clock went back
    standup = Schedule(name="standup", cron="0 9 * * *", catch_up="all")
    week = standup.due(START, START + timedelta(days=7))
    assert [at.day for at in week] == [1, 2, 3, 4, 5, 6, 7]
    assert Schedule(name="standup", cron="0 9 * * *").due(START, START + timedelta(days=7)) == [
        week[-1]
    ]


def test_tick_ids_name_the_schedule_and_the_moment() -> None:
    oslo = datetime(2026, 1, 1, 1, 0, tzinfo=ZoneInfo("Europe/Oslo"))
    assert tick_id("s", oslo) == tick_id("s", oslo.astimezone(UTC))
    assert tick_id("s", START) != tick_id("t", START)
    assert every_30s.targets("any", "where")
    only = Schedule(name="s", every=timedelta(seconds=1), workspaces=(("acme", "prod"),))
    assert (only.targets("acme", "prod"), only.targets("acme", "dev")) == (True, False)


# ─── publishing ticks ────────────────────────────────────────────────────────


async def test_ticks_start_on_the_first_check_and_publish_when_due(
    storage: Storage, clock: FakeClock
) -> None:
    workspaces = build(storage, clock, every_30s)
    workspace = await open_(workspaces)
    await workspace.publish(Heartbeat(service="auth"))
    reactor = Reactor(workspaces)
    assert await reactor.tick() == 0  # starts the timetable
    assert await storage.schedules(ACME) == {"heartbeat-check": START}
    clock.advance(29)
    assert await reactor.tick() == 0
    clock.advance(1)
    assert await reactor.tick() == 1
    clock.advance(95)
    assert await reactor.tick() == 1  # skipped the missed ones
    log = await workspace.read()
    assert ticks(log) == [START + timedelta(seconds=30), START + timedelta(seconds=120)]
    tick = log[1]
    assert (tick.actor, tick.id, tick.correlation_id) == (
        SCHEDULER,
        tick_id("heartbeat-check", START + timedelta(seconds=30)),
        tick.id,
    )


async def test_catching_up_publishes_every_missed_tick(storage: Storage, clock: FakeClock) -> None:
    catch_up = every_30s.model_copy(update={"catch_up": "all"})
    workspaces = build(storage, clock, catch_up)
    await (await open_(workspaces)).publish(Heartbeat(service="auth"))
    reactor = Reactor(workspaces)
    await reactor.tick()
    clock.advance(95)
    assert await reactor.tick() == 3


async def test_a_tick_already_in_the_log_is_not_published_again(
    storage: Storage, clock: FakeClock
) -> None:
    workspaces = build(storage, clock, every_30s)
    await (await open_(workspaces)).publish(Heartbeat(service="auth"))
    reactor = Reactor(workspaces)
    await reactor.tick()
    at = START + timedelta(seconds=30)
    async with storage.transaction(ACME) as transaction:  # another replica got there first
        tick = Tick(schedule="heartbeat-check", at=at)
        entry = Entry(id=tick_id("heartbeat-check", at), actor=SCHEDULER, event=tick)
        await transaction.append([entry])
    clock.advance(30)
    assert await reactor.tick() == 0
    assert await storage.schedules(ACME) == {"heartbeat-check": at}


async def test_a_schedule_can_target_workspaces_that_are_still_empty(
    storage: Storage, clock: FakeClock
) -> None:
    nightly = Schedule(name="nightly", cron="0 0 * * *", workspaces=(("acme", "prod"),))
    report = Rule(name="report", when=on(Tick).where(schedule="nightly"), then=run("noop"))
    workspaces = build(storage, clock, nightly, rules=(report,))
    reactor = Reactor(workspaces, actions={"noop": noop})
    await reactor.tick()
    clock.advance(24 * 3600)
    assert await reactor.settle() == Settled(ticks=1, firings=1, attempts=1)
    assert await storage.workspaces() == [ACME]


async def test_ticks_let_absence_rules_fire_in_quiet_workspaces(
    storage: Storage, clock: FakeClock
) -> None:
    quiet = Rule(
        name="quiet",
        when=on(Heartbeat).absent(within=timedelta(minutes=1)),
        scope=by(F.service),
        then=run("noop"),
    )
    workspaces = build(storage, clock, every_30s, rules=(quiet,))
    workspace = await open_(workspaces)
    await workspace.publish(Heartbeat(service="auth"))
    reactor = Reactor(workspaces, actions={"noop": noop})
    await reactor.settle()
    for _ in range(3):
        clock.advance(30)
        await reactor.settle()
    [paged] = await workspace.runs()
    assert (paged.rule, paged.status) == ("quiet", "succeeded")


async def test_a_workspace_reports_its_schedules_last_and_next_ticks(
    storage: Storage, clock: FakeClock
) -> None:
    elsewhere = Schedule(
        name="elsewhere", every=timedelta(minutes=5), workspaces=(("acme", "staging"),)
    )
    workspaces = build(storage, clock, every_30s, elsewhere)
    workspace = await open_(workspaces)
    assert await workspace.schedule_statuses() == [
        ScheduleStatus(schedule="heartbeat-check", last_tick=None, next_tick=None)
    ]
    await workspace.publish(Heartbeat(service="auth"))
    reactor = Reactor(workspaces)
    await reactor.tick()
    clock.advance(40)
    await reactor.tick()
    assert await workspace.schedule_statuses() == [
        ScheduleStatus(
            schedule="heartbeat-check",
            last_tick=START + timedelta(seconds=30),
            next_tick=START + timedelta(seconds=60),
        )
    ]


async def test_schedule_names_are_unique(storage: Storage, clock: FakeClock) -> None:
    with pytest.raises(ValueError, match="two schedules are named 'heartbeat-check'"):
        build(storage, clock, every_30s, every_30s)
    assert list(build(storage, clock, every_30s).schedules) == ["heartbeat-check"]


async def test_ticks_are_traced_and_counted(
    storage: Storage, clock: FakeClock, telemetry: Telemetry
) -> None:
    workspaces = build(storage, clock, every_30s, telemetry=telemetry)
    await (await open_(workspaces)).publish(Heartbeat(service="auth"))
    reactor = Reactor(workspaces)
    await reactor.tick()
    clock.advance(30)
    await reactor.tick()
    spans = [s for s in telemetry.spans.get_finished_spans() if s.name.startswith("reflexr.sch")]
    assert [dict(s.attributes or {})[a.EVENT_COUNT] for s in spans] == [0, 1]
    [(attributes, count)] = telemetry.points("reflexr.schedule.ticks")
    assert (attributes[a.SCHEDULE], count) == ("heartbeat-check", 1)
