"""The rules and the schedule: what oncall watches for, as data.

Rules name their actions; :mod:`oncall.actions` builds the actions under those names.
"""

from datetime import timedelta

from oncall.events import AlertFired, Heartbeat, IncidentOpened
from reflexr import F, RetryPolicy, Rule, by, on, run
from reflexr.workspace import Schedule

SILENCE = timedelta(minutes=2)
"""How long a service may go without a heartbeat before it is paged."""

triage_rule = Rule(
    name="oncall:triage",
    description=(
        "Three or more alerts of severity 7 or more for one service within five minutes. "
        "Decide whether they are an incident, and open one if they are."
    ),
    when=on(AlertFired).where(F.severity >= 7).count(at_least=3, within=timedelta(minutes=5)),
    scope=by(F.service),
    then=run("triage"),
)

runbook_rule = Rule(
    name="oncall:runbook",
    description=(
        "An incident was opened. Roll back a recent deploy of the service, or page the person "
        "on call if there is none; verify the service, then resolve the incident."
    ),
    when=on(IncidentOpened),
    scope=by(F.service),
    then=run("runbook"),
    retry=RetryPolicy(max_attempts=3, backoff=timedelta(seconds=10)),
)

silence_rule = Rule(
    name="oncall:silence",
    description="A service stopped sending heartbeats for two minutes. Page the person on call.",
    when=on(Heartbeat).absent(within=SILENCE),
    scope=by(F.service),
    then=run("page"),
)

heartbeat_check = Schedule(name="heartbeat-check", every=timedelta(seconds=30))
"""Ticks move rules' clocks in a quiet workspace, so a silence is noticed on time."""

RULES = [triage_rule, runbook_rule, silence_rule]
SCHEDULES = [heartbeat_check]
