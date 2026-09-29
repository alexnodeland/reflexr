"""Error classification system for Reflex.

Provides structured error types for consistent API responses and error handling.

This module defines:
- ErrorCode: Enumeration of error codes for API responses
- ReflexError: Base exception class with structured error responses
- Specific error classes for different error categories
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class ErrorCode(StrEnum):
    """Error codes for API responses.

    These codes provide machine-readable error classification
    for clients to handle errors programmatically.
    """

    VALIDATION_ERROR = "VALIDATION_ERROR"
    EVENT_NOT_FOUND = "EVENT_NOT_FOUND"
    PUBLICATION_FAILED = "PUBLICATION_FAILED"
    RATE_LIMITED = "RATE_LIMITED"
    STORE_ERROR = "STORE_ERROR"
    AGENT_ERROR = "AGENT_ERROR"
    LOCK_ERROR = "LOCK_ERROR"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class ReflexError(Exception):
    """Base exception for Reflex errors.

    All Reflex-specific exceptions should inherit from this class.
    Provides structured error responses for API endpoints.

    Attributes:
        code: The error code for this exception type
        status_code: HTTP status code to return
        message: Human-readable error message
        details: Additional error details

    Example:
        raise ReflexError("Something went wrong", details={"field": "value"})
    """

    code: ErrorCode = ErrorCode.INTERNAL_ERROR
    status_code: int = 500

    def __init__(
        self,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the error.

        Args:
            message: Human-readable error message
            details: Additional error details (optional)
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_response(self) -> dict[str, Any]:
        """Convert to API response format.

        Returns:
            Dictionary suitable for JSON response
        """
        return {
            "error": {
                "code": self.code.value,
                "message": self.message,
                "details": self.details,
            }
        }


class ValidationError(ReflexError):
    """Invalid input data.

    Raised when request data fails validation.
    """

    code = ErrorCode.VALIDATION_ERROR
    status_code = 422


class EventNotFoundError(ReflexError):
    """Event not found.

    Raised when a requested event does not exist.
    """

    code = ErrorCode.EVENT_NOT_FOUND
    status_code = 404


class PublicationError(ReflexError):
    """Failed to publish event.

    Raised when event publication fails.
    """

    code = ErrorCode.PUBLICATION_FAILED
    status_code = 500


class RateLimitError(ReflexError):
    """Rate limit exceeded.

    Raised when a client exceeds the rate limit.
    """

    code = ErrorCode.RATE_LIMITED
    status_code = 429


class StoreError(ReflexError):
    """Event store operation failed.

    Raised when an event store operation fails.
    """

    code = ErrorCode.STORE_ERROR
    status_code = 500


class AgentError(ReflexError):
    """Agent execution failed.

    Raised when an agent fails during execution.
    """

    code = ErrorCode.AGENT_ERROR
    status_code = 500


class LockError(ReflexError):
    """Lock operation failed.

    Raised when a lock cannot be acquired or released.
    """

    code = ErrorCode.LOCK_ERROR
    status_code = 503
