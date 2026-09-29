"""What a rule remembers between envelopes, and what evaluating envelopes decides.

State is plain data: the host stores it as JSON per rule and scope, next to the rule's
:class:`RuleProgress`, and saves both in one transaction with the firings they produce.
"""

from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue

from reflexr.core.events import RuleErrored, RuleFired
from reflexr.core.ids import FiringId, RuleName, ScopeKey


class _State(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Match(_State):
    """An envelope that counted towards a pattern."""

    seq: int
    ts: AwareDatetime
    depth: int = 0


class DedupeState(_State):
    """When each recent key was last seen."""

    seen: dict[str, AwareDatetime] = {}


class EachState(_State):
    """The ``each`` pattern keeps nothing."""

    kind: Literal["each"] = "each"


class CountState(_State):
    """The envelopes counted within the window so far."""

    kind: Literal["count"] = "count"
    matched: tuple[Match, ...] = ()


class SequenceState(_State):
    """How far through its steps a sequence is, and the envelopes that matched them."""

    kind: Literal["sequence"] = "sequence"
    matched: tuple[Match, ...] = ()


class AbsenceState(_State):
    """The last envelope that passed; the rule's deadline for this scope follows from it."""

    kind: Literal["absence"] = "absence"
    last: Match | None = None


type PatternState = Annotated[
    EachState | CountState | SequenceState | AbsenceState, Field(discriminator="kind")
]
"""A pattern's state for one scope."""


class ThrottleState(_State):
    """When the scope fired recently."""

    fired: tuple[AwareDatetime, ...] = ()


class ScopeState(_State):
    """Everything a rule remembers about one scope."""

    scope: dict[str, JsonValue]
    """The scope's field values."""

    dedupe: DedupeState | None = None
    pattern: PatternState = EachState()
    throttle: ThrottleState | None = None


class RuleProgress(_State):
    """Where a rule is in a workspace's log, stored with its cursor."""

    cursor: int = Field(default=0, ge=0)
    """The ``seq`` of the last envelope evaluated."""

    generation: int = Field(default=0, ge=0)
    """Incremented when the rule's state is reset, so firing ids never repeat."""

    definition: str = ""
    """The :meth:`~reflexr.core.Rule.definition` the state belongs to."""

    deadlines: dict[ScopeKey, AwareDatetime] = {}
    """When each waiting scope's ``absence`` window ends."""


class Firing(_State):
    """A rule's condition held for one scope; its run is created with the same id."""

    id: FiringId
    rule: RuleName
    scope_key: ScopeKey
    scope: dict[str, JsonValue]
    seq: int
    """The envelope at which the rule fired."""

    at: AwareDatetime
    """That envelope's time."""

    matched: tuple[int, ...]
    """The envelopes that made the condition hold."""

    depth: int = 0
    """The deepest causation depth among the matched envelopes."""


class EvaluationError(_State):
    """A rule could not evaluate one envelope."""

    rule: RuleName
    seq: int
    error: str


class Evaluation(_State):
    """What evaluating a batch of envelopes decided, for the host to save in one transaction."""

    progress: RuleProgress
    states: dict[ScopeKey, ScopeState] = {}
    """The scopes whose state changed."""

    firings: tuple[Firing, ...] = ()
    errors: tuple[EvaluationError, ...] = ()
    events: tuple[RuleFired | RuleErrored, ...] = ()
    """The facts to append to the log, in the order they happened."""
