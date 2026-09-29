"""Event types used across the tests. Importing this module registers them."""

from pydantic import BaseModel, Field

from reflexr import Event


class Labels(BaseModel):
    env: str = "prod"
    team: str | None = None


class ServiceError(Event, name="service.error"):
    service: str
    severity: int = 5
    message: str = ""
    region: str = "eu"
    fingerprint: str = ""
    labels: Labels = Labels()
    owner: Labels | None = None
    tags: list[str] = Field(default_factory=list[str])
    extra: dict[str, object] = Field(default_factory=dict[str, object])


class Deploy(Event, name="deploy.finished"):
    service: str
    version: str = "1"


class Heartbeat(Event, name="heartbeat"):
    service: str


class Flag(Event):
    """Registered under a derived name: ``flag``."""

    on: bool = True
