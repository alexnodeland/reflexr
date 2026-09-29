"""Agent execution context and decision context.

Provides typed dependencies to agents during execution and
accumulates events for decision-making.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from reflex.core.events import BaseEvent, Event
    from reflex.infra.store import EventStore

# Type variable for event types
from typing import TypeVar

T = TypeVar("T", bound="BaseEvent")


def _create_derived_meta(
    causation_id: str,
    correlation_id: str | None,
    trace_id: str,
) -> Any:
    """Create EventMeta for a derived event."""
    from reflex.core.events import EventMeta

    return EventMeta(
        causation_id=causation_id,
        correlation_id=correlation_id or causation_id,
        trace_id=trace_id,
    )


def _empty_event_list() -> list[Event]:
    """Factory for empty event list with correct type annotation."""
    return []


@dataclass
class DecisionContext:
    """Context for accumulating and analyzing events.

    Provides helper methods for filtering, windowing, and summarizing
    events to support intelligent decision-making in triggers.

    Attributes:
        events: List of accumulated events
        last_action_time: Timestamp of the last action taken
    """

    events: list[Event] = field(default_factory=_empty_event_list)
    last_action_time: datetime | None = None

    def add(self, event: Event) -> None:
        """Append an event to the context.

        Args:
            event: The event to add
        """
        self.events.append(event)

    def window(self, seconds: float) -> list[Event]:
        """Get events within a time window.

        Args:
            seconds: Number of seconds to look back

        Returns:
            Events within the time window, oldest first
        """
        cutoff = datetime.now(UTC) - timedelta(seconds=seconds)
        return [e for e in self.events if e.timestamp >= cutoff]

    def of_type(self, *types: str) -> list[Event]:
        """Filter events by type.

        Args:
            *types: Event types to include

        Returns:
            Events matching any of the specified types
        """
        type_set = set(types)
        return [e for e in self.events if e.type in type_set]

    def since_last_action(self) -> list[Event]:
        """Get events since the last action was taken.

        Returns:
            Events after last_action_time, or all events if no action yet
        """
        if self.last_action_time is None:
            return list(self.events)
        return [e for e in self.events if e.timestamp > self.last_action_time]

    def count_by_type(self) -> dict[str, int]:
        """Get event counts grouped by type.

        Returns:
            Dictionary mapping event type to count
        """
        counts: dict[str, int] = {}
        for event in self.events:
            counts[event.type] = counts.get(event.type, 0) + 1
        return counts

    def summarize(self, max_events: int = 10) -> str:
        """Generate a human-readable summary for LLM context.

        Args:
            max_events: Maximum number of recent events to include

        Returns:
            Markdown-formatted summary string
        """
        lines: list[str] = []
        counts = self.count_by_type()

        lines.append(f"## Event Summary ({len(self.events)} total events)")
        lines.append("")
        lines.append("### Event Counts by Type")
        for event_type, count in sorted(counts.items()):
            lines.append(f"- {event_type}: {count}")

        lines.append("")
        lines.append(f"### Recent Events (last {max_events})")
        recent = self.events[-max_events:]
        for event in recent:
            timestamp = event.timestamp.isoformat()
            lines.append(f"- [{timestamp}] {event.type} from {event.source}")

        return "\n".join(lines)

    def clear(self) -> None:
        """Reset all state after an action is taken."""
        self.events.clear()
        self.last_action_time = datetime.now(UTC)

    def mark_action(self) -> None:
        """Mark that an action was taken without clearing events."""
        self.last_action_time = datetime.now(UTC)


@dataclass
class AgentContext:
    """Context provided to agents during execution.

    Contains the triggering event and typed dependencies for
    database access, event publishing, and external services.

    Attributes:
        event: The event that triggered this agent execution
        store: EventStore for persistence operations
        publish: Async function to publish new events
        scope: The scope key for this execution (for locking)
    """

    event: Event
    store: EventStore
    publish: Callable[[Any], Coroutine[Any, Any, None]]
    scope: str

    def derive_event(self, event_type: type[T], **kwargs: Any) -> T:
        """Create a new event derived from the current event.

        Automatically sets causation_id and correlation_id for tracing,
        and inherits the source from the current event.

        Args:
            event_type: The event class to instantiate
            **kwargs: Fields for the new event

        Returns:
            A new event instance with tracing metadata set

        Example:
            alert = ctx.derive_event(
                AlertEvent,
                title="Error Alert",
                severity="high",
            )
            await ctx.publish(alert)
        """
        # Set source from current event if not provided
        if "source" not in kwargs:
            kwargs["source"] = self.event.source

        # Create the event with tracing metadata
        meta = _create_derived_meta(
            causation_id=self.event.id,
            correlation_id=self.event.meta.correlation_id,
            trace_id=self.event.meta.trace_id,
        )
        return event_type(meta=meta, **kwargs)
