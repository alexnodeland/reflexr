"""Basic Reflex Example - Error Monitoring Agent.

This example demonstrates a simple error monitoring system that:
1. Receives error events via WebSocket or HTTP
2. Filters for high-severity errors
3. Triggers an alert agent after 3 errors in 60 seconds
4. Publishes alert events for downstream processing

Run with:
    python -m examples.basic.main

Or start the API server:
    uvicorn reflex.api.app:app --reload
"""

from __future__ import annotations

import asyncio
from typing import Literal

from pydantic_ai import Agent, RunContext

from reflex import (
    AgentContext,
    BaseEvent,
    EventRegistry,
    ReflexDeps,
    SimpleAgent,
    error_threshold_trigger,
    get_registry,
    source_filter,
    trigger,
    type_filter,
)

# =============================================================================
# Custom Event Types
# =============================================================================


@EventRegistry.register
class ErrorEvent(BaseEvent):
    """Application error event."""

    type: Literal["app.error"] = "app.error"
    service: str
    error_code: str
    message: str
    severity: int = 1  # 1-10 scale


@EventRegistry.register
class AlertEvent(BaseEvent):
    """Alert notification event."""

    type: Literal["alert.triggered"] = "alert.triggered"
    title: str
    description: str
    severity: str  # "low", "medium", "high", "critical"
    source_events: list[str]  # IDs of events that triggered the alert


# =============================================================================
# AI-Powered Alert Agent
# =============================================================================

alert_classifier = Agent(
    "anthropic:claude-sonnet-4-20250514",
    deps_type=ReflexDeps,
    system_prompt="""You are an error classification assistant.
    Given error events, determine the appropriate alert severity and compose
    a clear, actionable alert message for the operations team.

    Severity levels:
    - critical: Service is down or data loss possible
    - high: Major feature broken, affecting many users
    - medium: Minor feature broken, workaround available
    - low: Cosmetic issue or warning
    """,
)


@alert_classifier.tool
async def get_error_context(
    ctx: RunContext[ReflexDeps],
    service: str,
) -> str:
    """Get recent error history for a service."""
    # In a real app, this would query the event store
    return f"Service {service} has had 3 errors in the last minute."


async def classify_and_alert(ctx: AgentContext) -> dict:
    """Process errors and generate alerts using AI."""
    event = ctx.event

    # For demo purposes, use a simple rule-based approach
    # In production, you'd call the PydanticAI agent:
    # result = await alert_classifier.run(f"Classify: {event.message}", deps=ctx.deps)

    severity = "high" if event.severity >= 8 else "medium" if event.severity >= 5 else "low"

    # Create and publish alert event
    alert = ctx.derive_event(
        AlertEvent,
        title=f"Error Alert: {event.service}",
        description=f"Multiple errors detected: {event.message}",
        severity=severity,
        source_events=[event.id],
    )
    await ctx.publish(alert)

    return {
        "alert_id": alert.id,
        "severity": severity,
        "service": event.service,
    }


error_alert_agent = SimpleAgent(classify_and_alert)


# =============================================================================
# Trigger Registration
# =============================================================================


@trigger(
    name="error-threshold-alert",
    filter=type_filter("app.error") & source_filter("production-*"),
    trigger_func=error_threshold_trigger(
        threshold=3,
        window_seconds=60,
        error_types=["app.error"],
    ),
    agent=error_alert_agent,
    priority=10,
    scope_key=lambda e: f"service:{getattr(e, 'service', 'unknown')}",
)
def error_threshold_handler():
    """Trigger alerts after 3 errors in 60 seconds per service."""
    pass


# =============================================================================
# Simple Logging Agent (for demonstration)
# =============================================================================


async def log_alert(ctx: AgentContext) -> dict:
    """Log alert events."""
    event = ctx.event
    print(f"[ALERT] {event.severity.upper()}: {event.title}")
    print(f"        {event.description}")
    return {"logged": True}


alert_logger = SimpleAgent(log_alert)


@trigger(
    name="alert-logger",
    filter=type_filter("alert.triggered"),
    agent=alert_logger,
    priority=1,
)
def alert_log_handler():
    """Log all alert events."""
    pass


# =============================================================================
# Demo Runner
# =============================================================================


async def demo():
    """Run a simple demonstration."""
    print("Reflex Basic Example")
    print("=" * 50)

    # Show registered triggers
    registry = get_registry()
    print(f"\nRegistered triggers: {len(registry.triggers)}")
    for t in registry.triggers:
        print(f"  - {t.name} (priority: {t.priority})")

    # Show registered event types
    print(f"\nRegistered event types: {len(EventRegistry.type_names())}")
    for name in EventRegistry.type_names():
        print(f"  - {name}")

    # Create sample events
    print("\nCreating sample error events...")
    errors = [
        ErrorEvent(
            source="production-api",
            service="auth-service",
            error_code="AUTH_001",
            message="Failed to validate token",
            severity=7,
        ),
        ErrorEvent(
            source="production-api",
            service="auth-service",
            error_code="AUTH_002",
            message="Database connection timeout",
            severity=9,
        ),
        ErrorEvent(
            source="production-api",
            service="auth-service",
            error_code="AUTH_001",
            message="Failed to validate token",
            severity=7,
        ),
    ]

    for i, error in enumerate(errors, 1):
        print(f"\n  Event {i}: {error.type}")
        print(f"    ID: {error.id}")
        print(f"    Service: {error.service}")
        print(f"    Message: {error.message}")
        print(f"    Severity: {error.severity}")

    print("\n" + "=" * 50)
    print("To run the full system:")
    print("  1. Start PostgreSQL: docker-compose up -d")
    print("  2. Run migrations: alembic upgrade head")
    print("  3. Start API: uvicorn reflex.api.app:app --reload")
    print("  4. Publish events to /events endpoint")


if __name__ == "__main__":
    asyncio.run(demo())
