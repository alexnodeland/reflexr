"""Rejections: the ways a command can fail, and the error for an invalid rule.

Every rejection has a stable ``code``, which is what the stream protocol sends to clients in a
``command_result`` frame, and a :meth:`Rejection.payload` with its typed details.
"""

from typing import ClassVar

from pydantic import JsonValue


class Rejection(Exception):
    """Base class for every reason reflexr refuses a command."""

    code: ClassVar[str] = "rejected"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def details(self) -> dict[str, JsonValue]:
        """Return the rejection's typed fields; subclasses extend this."""
        return {}

    def payload(self) -> dict[str, JsonValue]:
        """Return the rejection as JSON-compatible data for the wire."""
        return {"type": self.code, "message": self.message, **self.details()}


class NotFound(Rejection):
    """Something the command refers to does not exist."""

    code: ClassVar[str] = "not_found"

    def __init__(self, entity: str, id: str) -> None:
        super().__init__(f"{entity} {id} does not exist")
        self.entity = entity
        self.id = id

    def details(self) -> dict[str, JsonValue]:
        """Return what was missing."""
        return {"entity": self.entity, "id": self.id}


class InvalidState(Rejection):
    """The command does not apply to the current state, such as retrying a finished run."""

    code: ClassVar[str] = "invalid_state"


class ValidationFailed(Rejection):
    """The command's data does not validate, such as an event that does not match its type."""

    code: ClassVar[str] = "validation_failed"

    def __init__(self, message: str, errors: list[JsonValue]) -> None:
        super().__init__(message)
        self.errors = errors

    def details(self) -> dict[str, JsonValue]:
        """Return Pydantic's validation errors."""
        return {"errors": self.errors}


class Forbidden(Rejection):
    """The actor may not perform the command."""

    code: ClassVar[str] = "forbidden"


class DepthExceeded(Rejection):
    """An event would extend a causal chain beyond the workspace's limit."""

    code: ClassVar[str] = "depth_exceeded"

    def __init__(self, depth: int, limit: int) -> None:
        super().__init__(
            f"the event would be at causation depth {depth}, beyond the limit of {limit}"
        )
        self.depth = depth
        self.limit = limit

    def details(self) -> dict[str, JsonValue]:
        """Return the depth and the limit."""
        return {"depth": self.depth, "limit": self.limit}


class UnsupportedProtocol(Rejection):
    """The client asked for a protocol version this server does not speak."""

    code: ClassVar[str] = "unsupported_protocol"


class InvalidRule(ValueError):
    """A rule refers to event types, fields, predicates or actions that do not exist.

    Raised when rules are registered, so a mistake fails at startup rather than silently never
    matching.
    """

    def __init__(self, rule: str, problems: list[str]) -> None:
        super().__init__(f"rule {rule!r} is invalid: " + "; ".join(problems))
        self.rule = rule
        self.problems = problems
