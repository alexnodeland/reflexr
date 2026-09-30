"""The events of an on-call workspace.

Producers publish the first three: monitoring fires alerts, CI reports deploys, and services
send heartbeats. The workflows publish the incidents: the triage agent opens one, and the
runbook resolves it.
"""

from pydantic import Field

from reflexr import Event


class OncallEvent(Event, abstract=True, event_namespace="oncall"):
    """Every oncall event, in the ``oncall`` namespace."""


class AlertFired(OncallEvent, name="alert.fired"):
    """Monitoring saw something wrong with a service."""

    service: str
    severity: int = Field(ge=1, le=10)
    """1 is informational, 10 is an outage."""

    message: str


class DeployCompleted(OncallEvent, name="deploy.completed"):
    """A new version of a service went live."""

    service: str
    version: str


class Heartbeat(OncallEvent, name="service.heartbeat"):
    """A service is alive. Services send one every minute or so."""

    service: str


class IncidentOpened(OncallEvent, name="incident.opened"):
    """Triage decided that a service's alerts are an incident."""

    service: str
    severity: int = Field(ge=1, le=10)
    summary: str


class IncidentResolved(OncallEvent, name="incident.resolved"):
    """The runbook mitigated an incident and verified the service."""

    service: str
    resolution: str


EVENTS: list[type[Event]] = [
    AlertFired,
    DeployCompleted,
    Heartbeat,
    IncidentOpened,
    IncidentResolved,
]
"""Every event type the workspaces accept: those producers publish and those workflows emit."""
