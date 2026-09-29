"""Actors: who published an event or performed a command.

Every envelope is attributed to exactly one actor. The kinds match artifactr's, so a system that
uses both libraries can map one to the other, plus ``source`` for systems that publish events,
such as a monitoring service.
"""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from reflexr.core.ids import RuleName, RunId


class UserActor(BaseModel):
    """A person, identified by the host application's user id."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["user"] = "user"
    id: str
    name: str | None = None

    @property
    def display_name(self) -> str:
        """How this actor is named to people and agents."""
        return self.name or self.id


class AgentActor(BaseModel):
    """An agent or graph running an action for one firing of a rule."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["agent"] = "agent"
    rule: RuleName
    run_id: RunId
    name: str

    @property
    def display_name(self) -> str:
        """How this actor is named to people and agents."""
        return self.name


class ExternalAgentActor(BaseModel):
    """An agent outside reflexr, connected over MCP."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["external_agent"] = "external_agent"
    client_id: str
    name: str | None = None

    @property
    def display_name(self) -> str:
        """How this actor is named to people and agents."""
        return self.name or self.client_id


class SystemActor(BaseModel):
    """reflexr itself (the reactor, schedules), or the application acting on its own behalf."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["system"] = "system"
    name: str = "reflexr"

    @property
    def display_name(self) -> str:
        """How this actor is named to people and agents."""
        return self.name


class SourceActor(BaseModel):
    """A system that publishes events, such as a monitoring service or a webhook."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["source"] = "source"
    name: str

    @property
    def display_name(self) -> str:
        """How this actor is named to people and agents."""
        return self.name


Actor = Annotated[
    UserActor | AgentActor | ExternalAgentActor | SystemActor | SourceActor,
    Field(discriminator="kind"),
]
"""Any actor, discriminated by ``kind``."""
