"""Event types used across the tests, in the ``app`` namespace.

Importing this module registers them.
"""

from pydantic import BaseModel, Field

from reflexr import Event


class AppEvent(Event, abstract=True, event_namespace="app"):
    """The tests' events, in the ``app`` namespace."""


class Labels(BaseModel):
    env: str = "prod"
    team: str | None = None


class ServiceError(AppEvent, name="service.error"):
    service: str
    severity: int = 5
    message: str = ""
    region: str = "eu"
    fingerprint: str = ""
    labels: Labels = Labels()
    owner: Labels | None = None
    tags: list[str] = Field(default_factory=list[str])
    extra: dict[str, object] = Field(default_factory=dict[str, object])


class Deploy(AppEvent, name="deploy.finished"):
    service: str
    version: str = "1"


class Heartbeat(AppEvent, name="heartbeat"):
    service: str


class Flag(AppEvent):
    """Registered under a derived name: ``app:flag``."""

    on: bool = True
