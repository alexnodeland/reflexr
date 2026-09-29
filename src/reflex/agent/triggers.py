"""Trigger system for event-to-agent mapping.

Triggers connect event filters to agents, determining which
agent should handle which events.

This module provides:
1. Trigger class - maps event filters to agents
2. TriggerRegistry - manages collections of triggers
3. Trigger functions - evaluate context and fire agents
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from reflex.agent.base import Agent
    from reflex.agent.filters import EventFilter
    from reflex.core.context import DecisionContext
    from reflex.core.deps import ReflexDeps
    from reflex.core.events import Event

# Type alias for trigger functions
TriggerFunc = Callable[["DecisionContext", "ReflexDeps"], Awaitable[Any | None]]


@dataclass
class Trigger:
    """Maps events to agents via filters.

    A trigger defines:
    - Which events to match (filter)
    - Which agent to execute (agent)
    - How to scope concurrent executions (scope_key)

    The scope_key function extracts a key from the event that
    determines the locking scope. Events with the same scope
    are processed serially to prevent race conditions.

    Example:
        trigger = Trigger(
            name="chat_handler",
            filter=TypeFilter(types=["ws.message"]),
            agent=ChatAgent(),
            scope_key=lambda e: f"user:{e.connection_id}",
        )
    """

    name: str
    filter: EventFilter
    agent: Agent
    scope_key: Callable[[Event], str] = field(default=lambda e: e.source)
    priority: int = 0  # Higher priority triggers execute first

    def matches(self, event: Event) -> bool:
        """Check if this trigger matches the event."""
        return self.filter.matches(event)

    def get_scope(self, event: Event) -> str:
        """Get the locking scope for this event."""
        return self.scope_key(event)


class TriggerRegistry:
    """Registry for managing triggers.

    Maintains a collection of triggers and provides efficient
    lookup for matching events.

    Example:
        registry = TriggerRegistry()
        registry.register(chat_trigger)
        registry.register(analytics_trigger)

        for trigger in registry.match(event):
            await trigger.agent.run(ctx)
    """

    def __init__(self) -> None:
        """Initialize empty registry."""
        self._triggers: list[Trigger] = []

    def register(self, trigger: Trigger) -> None:
        """Register a trigger.

        Args:
            trigger: The trigger to register
        """
        self._triggers.append(trigger)
        # Keep sorted by priority (descending)
        self._triggers.sort(key=lambda t: -t.priority)

    def unregister(self, name: str) -> bool:
        """Unregister a trigger by name.

        Args:
            name: The name of the trigger to remove

        Returns:
            True if trigger was found and removed, False otherwise
        """
        for i, trigger in enumerate(self._triggers):
            if trigger.name == name:
                del self._triggers[i]
                return True
        return False

    def match(self, event: Event) -> list[Trigger]:
        """Find all triggers matching the event.

        Returns triggers in priority order (highest first).

        Args:
            event: The event to match against

        Returns:
            List of matching triggers, sorted by priority
        """
        return [t for t in self._triggers if t.matches(event)]

    def get(self, name: str) -> Trigger | None:
        """Get a trigger by name.

        Args:
            name: The trigger name

        Returns:
            The trigger if found, None otherwise
        """
        for trigger in self._triggers:
            if trigger.name == name:
                return trigger
        return None

    @property
    def triggers(self) -> list[Trigger]:
        """Get all registered triggers."""
        return list(self._triggers)

    def clear(self) -> None:
        """Remove all triggers."""
        self._triggers.clear()


# Default registry instance (can be overridden for testing)
_default_registry: TriggerRegistry | None = None


def get_registry() -> TriggerRegistry:
    """Get the current trigger registry.

    Returns the configured registry instance, creating a default one if needed.
    This function should be used instead of accessing a global registry directly
    for better testability.

    Returns:
        The current TriggerRegistry instance
    """
    global _default_registry
    if _default_registry is None:
        _default_registry = TriggerRegistry()
    return _default_registry


def configure_registry(registry: TriggerRegistry | None) -> None:
    """Configure the trigger registry instance.

    Use this function to inject a custom registry, particularly useful for testing.
    Pass None to reset to default behavior.

    Args:
        registry: Custom TriggerRegistry instance, or None to reset

    Example:
        # In tests
        test_registry = TriggerRegistry()
        configure_registry(test_registry)
        try:
            # ... run tests
        finally:
            configure_registry(None)  # Reset
    """
    global _default_registry
    _default_registry = registry


def register_trigger(trigger: Trigger) -> None:
    """Register a trigger in the global registry."""
    get_registry().register(trigger)


def trigger(
    name: str,
    filter: EventFilter,
    agent: Agent | None = None,
    trigger_func: TriggerFunc | None = None,
    scope_key: Callable[[Any], str] | None = None,
    priority: int = 0,
) -> Callable[[type[Agent] | Callable[..., Any]], type[Agent] | Callable[..., Any]]:
    """Decorator to register a trigger.

    Can be used in two ways:

    1. Decorating an Agent class:
        @trigger("chat_handler", filter=type_filter("ws.message"))
        class ChatAgent(SimpleAgent):
            async def handle(self, ctx: AgentContext) -> None:
                ...

    2. With an agent instance (decorates a placeholder function):
        @trigger(
            name="error-alert",
            filter=type_filter("app.error"),
            agent=my_agent,
            trigger_func=error_threshold_trigger(threshold=5),
            priority=10,
        )
        def error_handler():
            pass

    Args:
        name: Unique name for the trigger
        filter: EventFilter to match events
        agent: Optional agent instance (if not decorating a class)
        trigger_func: Optional trigger function for complex trigger logic
        scope_key: Function to extract scope key from event (for locking)
        priority: Higher priority triggers execute first
    """

    def decorator(
        target: type[Agent] | Callable[..., Any],
    ) -> type[Agent] | Callable[..., Any]:
        # Determine the agent to use
        if agent is not None:
            # Agent was passed as argument
            agent_instance = agent
        elif isinstance(target, type):
            # Decorating an Agent class
            agent_instance = target()
        else:
            raise ValueError(
                "trigger() must either decorate an Agent class or be passed an agent= argument"
            )

        t = Trigger(
            name=name,
            filter=filter,
            agent=agent_instance,
            scope_key=scope_key or (lambda e: e.source),
            priority=priority,
        )
        register_trigger(t)

        # Store trigger_func on the trigger for later use if provided
        if trigger_func is not None:
            t.trigger_func = trigger_func  # type: ignore[attr-defined]

        return target

    return decorator


# --- Trigger Functions ---
#
# These are higher-order functions that create trigger evaluators.
# They take configuration and return an async function that evaluates
# the DecisionContext and returns an action (or None to skip).


def error_threshold_trigger(
    threshold: int,
    window_seconds: float = 60.0,
    error_types: tuple[str, ...] | list[str] = ("lifecycle",),
) -> TriggerFunc:
    """Create a trigger that fires when error count exceeds threshold.

    Monitors events of specified types within a time window and
    fires when the count exceeds the threshold.

    Args:
        threshold: Number of errors to trigger alert
        window_seconds: Time window to count errors in
        error_types: Event types to consider as errors

    Returns:
        Trigger function

    Example:
        trigger = error_threshold_trigger(
            threshold=5,
            window_seconds=60,
            error_types=("lifecycle",),
        )
        result = await trigger(ctx, deps)
        if result:
            # Alert was generated
    """

    async def _trigger(ctx: DecisionContext, deps: ReflexDeps) -> dict[str, Any] | None:
        # Get errors in window
        recent = ctx.window(window_seconds)
        errors = [e for e in recent if e.type in error_types]

        if len(errors) >= threshold:
            ctx.mark_action()
            return {
                "triggered": True,
                "error_count": len(errors),
                "threshold": threshold,
                "window_seconds": window_seconds,
                "summary": ctx.summarize(max_events=5),
            }
        return None

    return _trigger


def periodic_summary_trigger(
    event_count: int = 100,
    max_interval_seconds: float | None = None,
) -> TriggerFunc:
    """Create a trigger that fires after accumulating N events.

    Fires when the context accumulates `event_count` events since
    the last action, optionally with a max time interval.

    Args:
        event_count: Number of events to accumulate before firing
        max_interval_seconds: Max time between summaries (optional)

    Returns:
        Trigger function

    Example:
        trigger = periodic_summary_trigger(event_count=50)
        result = await trigger(ctx, deps)
        if result:
            # Summary was generated
    """
    from datetime import UTC, datetime

    async def _trigger(ctx: DecisionContext, deps: ReflexDeps) -> dict[str, Any] | None:
        events_since_action = ctx.since_last_action()

        # Check event count threshold
        should_fire = len(events_since_action) >= event_count

        # Check time interval if specified
        if not should_fire and max_interval_seconds is not None:
            if ctx.last_action_time is not None:
                elapsed = (datetime.now(UTC) - ctx.last_action_time).total_seconds()
                should_fire = elapsed >= max_interval_seconds and len(events_since_action) > 0

        if should_fire:
            summary = ctx.summarize()
            counts = ctx.count_by_type()
            ctx.clear()
            return {
                "triggered": True,
                "event_count": len(events_since_action),
                "counts_by_type": counts,
                "summary": summary,
            }
        return None

    return _trigger


def immediate_trigger() -> TriggerFunc:
    """Create a trigger that fires on every event.

    Useful for real-time processing where every event
    should invoke an agent.

    Returns:
        Trigger function

    Example:
        trigger = immediate_trigger()
        result = await trigger(ctx, deps)  # Always returns a result
    """

    async def _trigger(ctx: DecisionContext, deps: ReflexDeps) -> dict[str, Any]:
        events = ctx.since_last_action()
        ctx.mark_action()
        return {
            "triggered": True,
            "event_count": len(events),
            "latest_event": events[-1] if events else None,
        }

    return _trigger
