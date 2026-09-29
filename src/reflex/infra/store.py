"""Event persistence with real-time subscriptions.

This module provides the EventStore class for persisting events to PostgreSQL
and subscribing to new events via LISTEN/NOTIFY.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import logfire
from pydantic import TypeAdapter
from sqlalchemy import Index, text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlmodel import Field as SQLField
from sqlmodel import SQLModel

from reflex.config import settings

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    import asyncpg
    from sqlalchemy.ext.asyncio import async_sessionmaker
    from sqlmodel.ext.asyncio.session import AsyncSession


def _get_event_adapter() -> TypeAdapter[Any]:
    """Get a fresh Event TypeAdapter with all registered types.

    Creates a new TypeAdapter each time to ensure custom event types
    registered via @EventRegistry.register are included.

    Returns:
        TypeAdapter for parsing Event JSON including custom types
    """
    from reflex.core.events import get_event_union

    return TypeAdapter(get_event_union())


class EventRecord(SQLModel, table=True):
    """Persistent event storage.

    Status lifecycle:
        pending -> processing -> completed
                            \\-> failed (retried) -> pending
                                               \\-> dlq (max attempts exceeded)
    """

    __tablename__ = "events"  # type: ignore[assignment]

    id: str = SQLField(primary_key=True)
    type: str = SQLField(index=True)
    source: str = SQLField(index=True)
    timestamp: datetime = SQLField(
        index=True,
        sa_type=TIMESTAMP(timezone=True),  # pyright: ignore[reportArgumentType]
    )
    payload: str  # JSON-serialized event

    # Processing state
    status: str = SQLField(default="pending", index=True)
    attempts: int = SQLField(default=0)
    error: str | None = SQLField(default=None)

    # Timestamps (use timezone-aware TIMESTAMP for UTC datetimes)
    created_at: datetime = SQLField(
        default_factory=lambda: datetime.now(UTC),
        sa_type=TIMESTAMP(timezone=True),  # pyright: ignore[reportArgumentType]
    )
    processed_at: datetime | None = SQLField(
        default=None,
        sa_type=TIMESTAMP(timezone=True),  # pyright: ignore[reportArgumentType]
    )
    next_retry_at: datetime | None = SQLField(
        default=None,
        index=True,
        sa_type=TIMESTAMP(timezone=True),  # pyright: ignore[reportArgumentType]
    )

    __table_args__ = (Index("ix_events_status_timestamp", "status", "timestamp"),)


class EventStore:
    """Event persistence with real-time subscriptions.

    Uses PostgreSQL LISTEN/NOTIFY for immediate subscriber notification,
    avoiding expensive polling loops. Events are claimed using
    FOR UPDATE SKIP LOCKED to allow concurrent consumers.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        pool: asyncpg.Pool,
    ) -> None:
        """Initialize the event store.

        Args:
            session_factory: SQLAlchemy async session factory
            pool: Raw asyncpg pool for LISTEN/NOTIFY
        """
        self.session_factory = session_factory
        self.pool = pool
        self.max_attempts = settings.event_max_attempts
        self.retry_base_delay = settings.event_retry_base_delay
        self.retry_max_delay = settings.event_retry_max_delay

    async def publish(self, event: Any) -> None:
        """Persist event and notify subscribers.

        Args:
            event: Event object with id, type, source, timestamp attributes
                   and model_dump_json() method
        """
        with logfire.span("store.publish", event_id=event.id, event_type=event.type):
            async with self.session_factory() as session:
                record = EventRecord(
                    id=event.id,
                    type=event.type,
                    source=event.source,
                    timestamp=event.timestamp,
                    payload=event.model_dump_json(),
                )
                session.add(record)
                await session.commit()

            # Notify subscribers via LISTEN/NOTIFY
            async with self.pool.acquire() as conn:  # type: ignore[union-attr]
                await conn.execute(f"NOTIFY events, '{event.id}'")

            logfire.info("Event published", event_id=event.id)

    async def subscribe(
        self,
        event_types: list[str] | None = None,
        batch_size: int = 100,
    ) -> AsyncIterator[tuple[Any, str]]:
        """Subscribe to events, yielding (event, ack_token) pairs.

        Uses LISTEN/NOTIFY for real-time notification. Subscribers call
        ack() or nack() with the token to mark processing complete.

        Events are claimed with FOR UPDATE SKIP LOCKED, allowing multiple
        concurrent consumers without conflicts.

        Args:
            event_types: Optional list of event types to filter
            batch_size: Maximum number of events to claim per batch

        Yields:
            Tuple of (event, token) where token is used for ack/nack
        """
        adapter = _get_event_adapter()

        async with self.pool.acquire() as conn:  # type: ignore[union-attr]
            # Set up listener with asyncio Queue for notifications
            notify_queue: asyncio.Queue[str] = asyncio.Queue()

            def notification_handler(
                connection: object, pid: int, channel: str, payload: str
            ) -> None:
                notify_queue.put_nowait(payload)

            await conn.add_listener("events", notification_handler)

            while True:
                # Claim and fetch pending events
                async with self.session_factory() as session:
                    # Use parameterized query with PostgreSQL ANY() for type filtering
                    # This avoids SQL injection by never interpolating user input
                    # Note: Use CAST() instead of :: to avoid SQLAlchemy param conflicts
                    query = text("""
                        UPDATE events
                        SET status = 'processing', attempts = attempts + 1
                        WHERE id IN (
                            SELECT id FROM events
                            WHERE status = 'pending'
                                AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                                AND (CAST(:event_types AS text[]) IS NULL
                                     OR type = ANY(:event_types))
                            ORDER BY timestamp
                            LIMIT :batch_size
                            FOR UPDATE SKIP LOCKED
                        )
                        RETURNING id, payload
                    """)

                    result = await session.execute(  # pyright: ignore[reportDeprecated]
                        query,
                        {"batch_size": batch_size, "event_types": event_types},
                    )
                    rows = result.fetchall()
                    await session.commit()

                    for row in rows:
                        event = adapter.validate_json(row.payload)
                        yield event, row.id

                # If no events, wait for notification
                if not rows:
                    try:
                        # Wait for NOTIFY with timeout
                        event_id = await asyncio.wait_for(
                            notify_queue.get(),
                            timeout=5.0,
                        )
                        logfire.debug("Received notification", event_id=event_id)
                    except TimeoutError:
                        pass  # Continue loop, check for pending events

    async def ack(self, token: str) -> None:
        """Mark event as successfully processed.

        Args:
            token: Event ID returned from subscribe()
        """
        with logfire.span("store.ack", event_id=token):
            async with self.session_factory() as session:
                await session.execute(  # pyright: ignore[reportDeprecated]
                    text("""
                        UPDATE events
                        SET status = 'completed', processed_at = NOW()
                        WHERE id = :id
                    """),
                    {"id": token},
                )
                await session.commit()

    async def nack(self, token: str, error: str | None = None) -> None:
        """Mark event as failed with exponential backoff for retry.

        If attempts < max_attempts, event returns to pending for retry
        after an exponentially increasing delay. Otherwise, moves to
        dead-letter queue (status = 'dlq').

        Backoff formula: delay = min(base_delay * 2^attempts, max_delay)

        Args:
            token: Event ID returned from subscribe()
            error: Optional error message to store
        """
        with logfire.span("store.nack", event_id=token, error=error):
            async with self.session_factory() as session:
                # Calculate exponential backoff delay
                # We use attempts directly since it was already incremented in subscribe()
                await session.execute(  # pyright: ignore[reportDeprecated]
                    text("""
                        UPDATE events SET
                            status = CASE
                                WHEN attempts >= :max_attempts THEN 'dlq'
                                ELSE 'pending'
                            END,
                            error = :error,
                            next_retry_at = CASE
                                WHEN attempts >= :max_attempts THEN NULL
                                ELSE NOW() + (
                                    LEAST(:base_delay * POWER(2, attempts - 1), :max_delay)
                                    || ' seconds'
                                )::interval
                            END
                        WHERE id = :id
                    """),
                    {
                        "id": token,
                        "error": error,
                        "max_attempts": self.max_attempts,
                        "base_delay": self.retry_base_delay,
                        "max_delay": self.retry_max_delay,
                    },
                )
                await session.commit()

            logfire.warning("Event nacked", event_id=token, error=error)

    async def replay(
        self,
        start: datetime,
        end: datetime | None = None,
        event_types: list[str] | None = None,
    ) -> AsyncIterator[Any]:
        """Replay historical events for debugging.

        Events are yielded in timestamp order. Does not affect event status.

        Args:
            start: Start of time range (inclusive)
            end: End of time range (inclusive), defaults to now
            event_types: Optional list of event types to filter

        Yields:
            Event objects
        """
        adapter = _get_event_adapter()

        end_str = end.isoformat() if end else None
        with logfire.span("store.replay", start=start.isoformat(), end=end_str):
            async with self.session_factory() as session:
                conditions = ["timestamp >= :start"]
                params: dict[str, Any] = {"start": start}

                if end:
                    conditions.append("timestamp <= :end")
                    params["end"] = end

                if event_types:
                    conditions.append("type = ANY(:types)")
                    params["types"] = event_types

                where_clause = " AND ".join(conditions)
                query = text(
                    f"""
                    SELECT payload FROM events
                    WHERE {where_clause}
                    ORDER BY timestamp
                """  # noqa: S608 - where_clause uses parameterized conditions
                )

                result = await session.execute(query, params)  # pyright: ignore[reportDeprecated]
                count = 0
                for row in result:
                    yield adapter.validate_json(row.payload)
                    count += 1

                logfire.info("Replay completed", event_count=count)

    async def dlq_list(self, limit: int = 100) -> list[Any]:
        """List events in dead-letter queue for inspection.

        Args:
            limit: Maximum number of events to return

        Returns:
            List of events in DLQ
        """
        adapter = _get_event_adapter()

        async with self.session_factory() as session:
            result = await session.execute(  # pyright: ignore[reportDeprecated]
                text("""
                    SELECT payload, error, attempts, created_at
                    FROM events
                    WHERE status = 'dlq'
                    ORDER BY created_at DESC
                    LIMIT :limit
                """),
                {"limit": limit},
            )
            return [adapter.validate_json(row.payload) for row in result]

    async def dlq_retry(self, event_id: str) -> bool:
        """Move event from DLQ back to pending for retry.

        Args:
            event_id: ID of the event to retry

        Returns:
            True if event was found and moved, False otherwise
        """
        async with self.session_factory() as session:
            result = await session.execute(  # pyright: ignore[reportDeprecated]
                text("""
                    UPDATE events
                    SET status = 'pending', attempts = 0, error = NULL
                    WHERE id = :id AND status = 'dlq'
                """),
                {"id": event_id},
            )
            await session.commit()
            rowcount = int(result.rowcount or 0)  # type: ignore[arg-type]
            return rowcount > 0
