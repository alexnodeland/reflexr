"""The workspace protocol: its commands, frames, handshake and resume rule.

Clients publish events, give feedback, and operate runs and rules with commands; they follow a
workspace's log over a WebSocket, resuming by ``seq``. The same commands are REST requests and
MCP tools, and every surface hands them to one handler, so they behave identically. The models
here are the protocol's source of truth; ``schemas/reflexr.v1.json`` is generated from them.
Its shape matches artifactr's thread protocol, so one client library can speak both.
"""

from typing import Annotated, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from reflexr.core.errors import UnsupportedProtocol
from reflexr.core.events import AnyEvent, Envelope
from reflexr.core.feedback import FeedbackTarget
from reflexr.core.ids import EventId, RuleName, RunId, WorkspaceId
from reflexr.core.runs import Run
from reflexr.core.state import RuleProgress

PROTOCOL: Final = "reflexr.v1"
"""The protocol version this library speaks."""


class _Model(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


# ─── commands ─────────────────────────────────────────────────────────────────


class Publish(_Model):
    """Publish an event. Publishing an ``id`` already in the log appends nothing."""

    type: Literal["publish"] = "publish"
    event: AnyEvent
    id: EventId | None = None
    correlation_id: str | None = None
    """The causal chain to join: the id of its first event. By default, a new chain."""


class GiveFeedback(_Model):
    """Give typed feedback on a run, a firing or a causal chain."""

    type: Literal["give_feedback"] = "give_feedback"
    feedback_type: str
    target: FeedbackTarget
    value: dict[str, JsonValue]


class RetryRun(_Model):
    """Make a run runnable now."""

    type: Literal["retry_run"] = "retry_run"
    run_id: RunId


class SkipRun(_Model):
    """Give up on a waiting or dead-lettered run."""

    type: Literal["skip_run"] = "skip_run"
    run_id: RunId
    reason: str | None = None


class CancelRun(_Model):
    """Cancel a run that has not finished, stopping its action if it is running."""

    type: Literal["cancel_run"] = "cancel_run"
    run_id: RunId
    reason: str | None = None


class ReplayRule(_Model):
    """Reset a rule to evaluate the log again, rebuilding its state or firing again."""

    type: Literal["replay_rule"] = "replay_rule"
    rule: RuleName
    from_seq: int = Field(default=0, ge=0)
    mode: Literal["rebuild", "refire"] = "rebuild"


type Command = Annotated[
    Publish | GiveFeedback | RetryRun | SkipRun | CancelRun | ReplayRule,
    Field(discriminator="type"),
]
"""Anything a client can ask a workspace to do."""


# ─── outcomes ─────────────────────────────────────────────────────────────────


class PublishedOutcome(_Model):
    """What publishing did: the event's position, and whether it was already there."""

    type: Literal["published"] = "published"
    seq: int
    id: EventId
    duplicate: bool = False


class RecordedOutcome(_Model):
    """Where a fact the command recorded, such as feedback, was appended."""

    type: Literal["recorded"] = "recorded"
    seq: int
    id: EventId


class RunOutcome(_Model):
    """A run after the command changed it."""

    type: Literal["run"] = "run"
    run: Run


class RuleOutcome(_Model):
    """A rule's progress after the command reset it."""

    type: Literal["rule"] = "rule"
    rule: RuleName
    progress: RuleProgress


type Outcome = Annotated[
    PublishedOutcome | RecordedOutcome | RunOutcome | RuleOutcome, Field(discriminator="type")
]
"""What a command did."""


# ─── the handshake ────────────────────────────────────────────────────────────


class Hello(_Model):
    """The first frame a client sends on a connection."""

    type: Literal["hello"] = "hello"
    protocol: str
    resume_after_seq: int = Field(default=0, ge=0)
    types: tuple[str, ...] | None = None
    """The event types to receive; ``None`` means every type. ``replay_complete`` always
    arrives, since it is decided on the whole log."""


class ResumePlan(_Model):
    """Where a connection's replay starts."""

    replay_after: int
    """Replay every event with a ``seq`` greater than this."""

    reset: bool = False
    """Whether the client must discard what it has and rebuild from the replay."""


def resume(hello: Hello, *, head_seq: int) -> ResumePlan:
    """Decide where replay starts for a connecting client.

    Args:
        hello: The client's hello frame.
        head_seq: The workspace log's latest ``seq`` (0 if it is empty).

    Raises:
        UnsupportedProtocol: If the client speaks another protocol version.
    """
    if hello.protocol != PROTOCOL:
        raise UnsupportedProtocol(
            f"this server speaks {PROTOCOL}; the client asked for {hello.protocol}"
        )
    if hello.resume_after_seq > head_seq:
        # The client has seen events this log does not have: it must start over.
        return ResumePlan(replay_after=0, reset=True)
    return ResumePlan(replay_after=hello.resume_after_seq)


# ─── frames ───────────────────────────────────────────────────────────────────


class CommandFrame(_Model):
    """A client's command, with an id that correlates it with its result.

    ``command_id`` is also an idempotency key: the server deduplicates repeated ids.
    """

    type: Literal["command"] = "command"
    command_id: str = Field(min_length=1)
    command: Command


type ClientFrame = Annotated[Hello | CommandFrame, Field(discriminator="type")]
"""Any frame a client sends."""


class Welcome(_Model):
    """The server's answer to ``hello``."""

    type: Literal["welcome"] = "welcome"
    protocol: str = PROTOCOL
    workspace_id: WorkspaceId
    head_seq: int
    reset: bool = False


class EventFrame(Envelope):
    """An envelope from the log."""

    type: Literal["event"] = "event"


class ReplayComplete(_Model):
    """Every event up to ``up_to_seq`` has been replayed; what follows is live."""

    type: Literal["replay_complete"] = "replay_complete"
    up_to_seq: int


class CommandResult(_Model):
    """The result of one command frame."""

    type: Literal["command_result"] = "command_result"
    command_id: str
    ok: bool
    outcome: Outcome | None = None
    rejection: dict[str, JsonValue] | None = None


class ErrorFrame(_Model):
    """A frame the server could not understand."""

    type: Literal["error"] = "error"
    message: str


type ServerFrame = Annotated[
    Welcome | EventFrame | ReplayComplete | CommandResult | ErrorFrame,
    Field(discriminator="type"),
]
"""Any frame the server sends."""
