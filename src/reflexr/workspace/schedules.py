"""Schedules: ticks published into workspaces on a timetable.

A schedule publishes :class:`~reflexr.core.Tick` events, every interval or on a cron
expression in a time zone. Rules watch ticks (``on(Tick).where(schedule="nightly")``), and
ticks move rules' clocks forward in quiet workspaces, so ``absence`` rules fire on time.

Each workspace remembers its last tick per schedule, updated in the transaction that appends
the tick, and each tick's id is derived from the schedule and its time: replicas cannot
publish a tick twice.
"""

import hashlib
from collections import deque
from collections.abc import Iterator
from datetime import datetime, timedelta
from itertools import takewhile
from typing import Literal, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from cronsim import CronSim, CronSimError
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from reflexr.core import EventId, SystemActor, TenantId, WorkspaceId

SCHEDULER = SystemActor(name="scheduler")
"""The actor of the ticks schedules publish."""


class Schedule(BaseModel):
    """When to publish ticks, and into which workspaces.

    Give ``every`` or ``cron``, not both::

        Schedule(name="heartbeat-check", every=timedelta(seconds=30))
        Schedule(name="weekday-standup", cron="0 9 * * mon-fri", timezone="Europe/Oslo")
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(min_length=1)
    every: timedelta | None = Field(default=None, gt=timedelta(0))
    cron: str | None = None
    """A five-field cron expression, evaluated in ``timezone``."""

    timezone: str = "UTC"
    workspaces: Literal["all"] | tuple[tuple[TenantId, WorkspaceId], ...] = "all"
    """``"all"`` for every workspace with a log, or ``(tenant, workspace)`` pairs."""

    catch_up: Literal["skip", "all"] = "skip"
    """After downtime, publish only the latest missed tick (``"skip"``), or every one."""

    max_catch_up: int = Field(default=100, ge=1)
    """The most ticks one catch-up publishes; older missed ticks are skipped."""

    @model_validator(mode="after")
    def _check(self) -> Self:
        if (self.every is None) == (self.cron is None):
            raise ValueError("give a schedule either every or cron")
        try:
            zone = ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ValueError(f"unknown time zone {self.timezone!r}") from error
        if self.cron is not None:
            try:
                CronSim(self.cron, datetime.now(zone))
            except CronSimError as error:
                raise ValueError(f"invalid cron expression {self.cron!r}: {error}") from error
        return self

    def after(self, moment: AwareDatetime) -> Iterator[datetime]:
        """Yield the schedule's times strictly after ``moment``, in order."""
        if self.every is not None:
            step = self.every
            at = moment + step
            while True:
                yield at
                at += step
        else:
            assert self.cron is not None
            yield from CronSim(self.cron, moment.astimezone(ZoneInfo(self.timezone)))

    def due(self, last: AwareDatetime, now: AwareDatetime) -> list[datetime]:
        """Return the ticks due after ``last`` and by ``now``, as ``catch_up`` allows."""
        keep = 1 if self.catch_up == "skip" else self.max_catch_up
        if self.every is not None:
            # Arithmetic, so a long outage costs nothing to skip.
            missed = (now - last) // self.every
            first = max(1, missed - keep + 1)
            return [last + step * self.every for step in range(first, missed + 1)]
        upcoming = takewhile(lambda at: at <= now, self.after(last))
        return list(deque(upcoming, maxlen=keep))

    def targets(self, tenant_id: TenantId, workspace_id: WorkspaceId) -> bool:
        """Whether the schedule ticks in a workspace."""
        return self.workspaces == "all" or (tenant_id, workspace_id) in self.workspaces


def tick_id(schedule: str, at: datetime) -> EventId:
    """Return the id of a schedule's tick at ``at``, the same in every replica."""
    stamp = at.astimezone(ZoneInfo("UTC")).isoformat()
    digest = hashlib.sha256(f"tick\x00{schedule}\x00{stamp}".encode()).hexdigest()
    return f"evt_{digest[:16]}"
