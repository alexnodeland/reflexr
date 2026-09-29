# Schedules

A schedule publishes `tick` events into workspaces on a timetable: every so often, or on a cron expression in a time zone. Rules watch ticks like any other event, so a nightly report or a weekday reminder is a schedule and a rule. Ticks also move rules' clocks forward in workspaces where nothing else is happening, which is what lets an `absence` rule notice silence on time. This page covers defining schedules, watching their ticks, keeping time moving, and what happens after downtime ([ADR-0028](../adr/0028-schedules-and-cronsim.md)).

## Defining a schedule

A `Schedule` is validated data, registered on `Workspaces` next to the rules:

```python
from datetime import timedelta

from reflexr.workspace import InMemoryStorage, Schedule, Workspaces

heartbeat_check = Schedule(name="heartbeat-check", every=timedelta(seconds=30))
standup = Schedule(name="standup", cron="0 9 * * mon-fri", timezone="Europe/Oslo")

workspaces = Workspaces(
    InMemoryStorage(),
    events=[ServiceError, Deploy, Heartbeat, IncidentOpened],
    rules=[heartbeat_lost],
    schedules=[heartbeat_check, standup],
)
```

| Field | Default | Meaning |
|---|---|---|
| `name` | required | The schedule's name, which its ticks carry. Two schedules with the same name raise `ValueError` |
| `every` | | An interval: tick every `every` |
| `cron` | | A five-field cron expression, evaluated in `timezone`. Give `every` or `cron`, not both |
| `timezone` | `"UTC"` | An IANA time zone, such as `Europe/Oslo`, for cron expressions. Daylight saving is handled |
| `workspaces` | `"all"` | `"all"` for every workspace with a log, or a tuple of `(tenant_id, workspace_id)` pairs |
| `catch_up` | `"skip"` | After downtime, publish only the latest missed tick (`"skip"`), or each one (`"all"`) |
| `max_catch_up` | 100 | The most ticks one catch-up publishes |

A schedule without `every` or `cron`, with both, with an invalid cron expression or with an unknown time zone fails validation when it is created. Cron expressions are parsed by [cronsim](https://github.com/cuu508/cronsim). `schedule.after(moment)` yields the schedule's times after a moment, which is how to list what is coming:

```python
from datetime import UTC, datetime
from itertools import islice

print([t.isoformat() for t in islice(standup.after(datetime(2026, 9, 25, 12, 0, tzinfo=UTC)), 3)])
```

```text
['2026-09-28T09:00:00+02:00', '2026-09-29T09:00:00+02:00', '2026-09-30T09:00:00+02:00']
```

The [reactor](reactor.md) publishes ticks: `reactor.tick()` publishes whatever is due, and `settle()` and `serve()` call it on every round. A schedule's first check in a workspace starts its timetable there; its first tick comes at its next time after that. So the first tick of `heartbeat-check` comes 30 seconds after the reactor first sees the workspace, and the first tick of `standup` at the next weekday 09:00 in Oslo.

With `workspaces="all"`, a schedule ticks in every workspace that has a log, so a workspace that has never had an event gets no ticks until it has one. A schedule that lists `(tenant, workspace)` pairs ticks in exactly those, whether or not they have events yet.

## Watching ticks

A tick is one of reflexr's own events ([Events and envelopes](events.md#reflexrs-own-events)): a `Tick` with the schedule's `name` and the time it was due, `at`, published by `SystemActor(name="scheduler")`. Rules can always watch ticks, whatever the workspaces' allowlist says, and narrow them to one schedule:

```python
from reflexr import Rule, on, run
from reflexr.core import Tick
from reflexr.workspace import Reaction

standup_reminder = Rule(
    name="standup-reminder",
    when=on(Tick).where(schedule="standup"),
    then=run("remind"),
)


async def remind(reaction: Reaction[None]) -> None:
    tick = reaction.events[-1].event  # the Tick that made the rule fire
    ...
```

- **Each tick starts its own causal chain**, so every standup reminder is a separate session in traces.
- **A tick's id is derived from the schedule and its time** (`tick_id(name, at)`), the same in every process. Replicas that check the same schedule at the same time cannot publish a tick twice: the log already has its id.
- **Each workspace remembers each schedule's last tick**, saved in the transaction that appends the tick, so no lease is needed. `workspace.schedule_ticks()` returns them, and `workspace.schedule_statuses()`, REST's `GET /v1/workspaces/{workspace_id}/schedules` and the MCP `schedule_status` tool show each schedule's last and next tick ([REST endpoints](serving.md#rest-endpoints), [MCP tools](mcp.md#tools)).

## Keeping time moving

Rules measure time by the log, not by a clock ([Time](rules.md#time)). Every envelope moves a rule's clock forward, whether or not the rule's filter accepts it, but a quiet workspace has no envelopes. An `absence` rule there would notice that `auth` stopped sending heartbeats only when something else happened to be published.

A schedule fixes that. Its ticks move every rule's clock forward on a timetable, even though a tick is not a heartbeat and has no `service` field:

```python
from reflexr import F, by

heartbeat_lost = Rule(
    name="heartbeat-lost",
    when=on(Heartbeat).absent(within=timedelta(minutes=5)),
    scope=by(F.service),
    then=run("page"),
)
heartbeat_check = Schedule(name="heartbeat-check", every=timedelta(seconds=30))
```

With these two, a service whose last heartbeat was at 09:00:00 is paged at the first tick at or after 09:05:00, however quiet the workspace is otherwise. The firing matches the last heartbeat, not the tick, so it belongs to that heartbeat's causal chain. Choose the interval by how late a firing may be: here, at most 30 seconds.

## Catching up after downtime

When no reactor has run for a while, a schedule has missed ticks. The next check decides what to publish from `catch_up`:

| `catch_up` | Publishes | Suits |
|---|---|---|
| `"skip"` (the default) | Only the latest missed tick | Schedules that keep time moving, and anything where one late tick is as good as several |
| `"all"` | Each missed tick, in order, up to `max_catch_up` of the most recent | Schedules whose every tick matters, such as hourly reports |

Five minutes of downtime on a 30-second schedule publishes one tick with `"skip"`, and with `catch_up="all", max_catch_up=4`, the last four missed ticks: those at 09:14:00, 09:14:30, 09:15:00 and 09:15:30 for a reactor back at 09:15:30. Interval schedules count their missed ticks arithmetically, so even a long outage costs nothing to skip. A tick carries the time it was due in `at`, which may be earlier than the envelope's `ts`, when it was published.

Ticks add envelopes to every workspace a schedule targets, so pick the longest interval that keeps rules timely. A schedule removed from `Workspaces` simply stops ticking; its last tick stays in each workspace's state.
