"""Event API endpoints.

Provides endpoints for publishing events and managing the dead-letter queue.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, Path, Query, Request, status
from pydantic import BaseModel, Field

from reflex.api.deps import get_store
from reflex.api.rate_limiting import limiter
from reflex.config import get_settings
from reflex.core.errors import EventNotFoundError
from reflex.core.events import EventRegistry
from reflex.infra.store import EventStore

# Type alias for cleaner route signatures
StoreDep = Annotated[EventStore, Depends(get_store)]

router = APIRouter(prefix="/events", tags=["events"])


class PublishResponse(BaseModel):
    """Response model for event publication."""

    id: str = Field(description="The event ID")
    status: str = Field(description="Publication status")


class DLQListResponse(BaseModel):
    """Response model for DLQ listing."""

    count: int = Field(description="Number of events in DLQ")
    events: list[dict[str, Any]] = Field(description="List of DLQ events")


class DLQRetryResponse(BaseModel):
    """Response model for DLQ retry."""

    status: str = Field(description="Retry status")
    event_id: str = Field(description="The retried event ID")


def _get_rate_limit() -> str:
    """Get rate limit string from settings."""
    settings = get_settings()
    return f"{settings.rate_limit_requests}/{settings.rate_limit_window}second"


@router.post(
    "",
    response_model=PublishResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Publish an event",
    description="Publish a new event to the event store.",
    responses={
        201: {"description": "Event published successfully"},
        422: {"description": "Invalid event format"},
        429: {"description": "Rate limit exceeded"},
    },
)
@limiter.limit(_get_rate_limit)  # pyright: ignore[reportUntypedFunctionDecorator]
async def publish_event(
    request: Request,  # Required for rate limiter
    event_data: Annotated[dict[str, Any], Body()],
    store: StoreDep,
) -> PublishResponse:
    """Publish an event to the event store.

    The event must conform to one of the registered event types.
    Built-in types: WebSocketEvent, HTTPEvent, TimerEvent, LifecycleEvent.
    Custom types can be registered using @EventRegistry.register.

    Args:
        request: The incoming request (used for rate limiting)
        event_data: The event data as a dictionary
        store: The EventStore dependency

    Returns:
        The event ID and publication status
    """
    # Parse using EventRegistry to support custom event types
    event = EventRegistry.parse(event_data)
    await store.publish(event)
    return PublishResponse(id=event.id, status="published")


@router.get(
    "/dlq",
    response_model=DLQListResponse,
    summary="List dead-letter queue",
    description="List events in the dead-letter queue.",
)
async def list_dlq(
    store: StoreDep,
    limit: Annotated[int, Query(ge=1, le=1000, description="Maximum events to return")] = 100,
) -> DLQListResponse:
    """List events in the dead-letter queue.

    Returns events that have failed processing and exceeded
    the maximum retry attempts.

    Args:
        store: The EventStore dependency
        limit: Maximum number of events to return

    Returns:
        Count and list of DLQ events
    """
    events = await store.dlq_list(limit=limit)
    return DLQListResponse(
        count=len(events),
        events=[e.model_dump(mode="json") for e in events],
    )


@router.post(
    "/dlq/{event_id}/retry",
    response_model=DLQRetryResponse,
    summary="Retry a DLQ event",
    description="Move an event from the dead-letter queue back to pending for reprocessing.",
    responses={
        200: {"description": "Event moved to pending"},
        404: {"description": "Event not found in DLQ"},
    },
)
async def retry_dlq_event(
    event_id: Annotated[str, Path(description="The event ID to retry")],
    store: StoreDep,
) -> DLQRetryResponse:
    """Retry a dead-letter queue event.

    Moves the specified event from DLQ back to pending status
    for reprocessing.

    Args:
        event_id: The ID of the event to retry
        store: The EventStore dependency

    Returns:
        Retry status and event ID

    Raises:
        EventNotFoundError: If event is not found in DLQ
    """
    success = await store.dlq_retry(event_id)
    if not success:
        raise EventNotFoundError(
            f"Event {event_id} not found in dead-letter queue",
            details={"event_id": event_id},
        )
    return DLQRetryResponse(status="pending", event_id=event_id)
