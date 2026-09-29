# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development Commands

```bash
# Development
make dev                  # Start with docker compose (includes PostgreSQL)
make dev-build            # Rebuild and start
make demo                 # Run interactive demo (server must be running)
make example EXAMPLE=basic  # Run an example (fraud_detection, log_anomaly, etc.)
make examples-list        # List available examples

# Testing
make test                 # Run all tests in Docker
pytest tests/             # Run all tests locally
pytest tests/test_file.py::test_name  # Run single test
pytest tests/ -m "not asyncio"        # Unit tests only (no DB required)
DATABASE_URL="..." pytest tests/test_store.py  # Integration tests

# Code Quality
make lint                 # ruff check src tests
make lint-fix             # Auto-fix lint issues
make format               # ruff format src tests
make type-check           # pyright src
make ci                   # Run full CI pipeline locally

# Database
make db-shell             # psql into PostgreSQL
make migrate              # Run migrations
```

## Architecture

Reflex is an event-driven AI agent framework using PostgreSQL LISTEN/NOTIFY for real-time pub/sub.

### Event Flow
1. Events arrive via WebSocket (`/ws`) or HTTP (`/events`)
2. `EventStore.publish()` persists to PostgreSQL and fires NOTIFY
3. Agent loop receives events via `EventStore.subscribe()` (uses `FOR UPDATE SKIP LOCKED`)
4. Triggers match events to agents via composable filters
5. Events are `ack()`'d on success or `nack()`'d for retry with exponential backoff

### Key Abstractions

**Events** (`src/reflex/core/events.py`):
- Use `@EventRegistry.register` decorator for custom events
- Must have `type: Literal["my.type"] = "my.type"` field
- Built-in: `WebSocketEvent`, `HTTPEvent`, `TimerEvent`, `LifecycleEvent`

**Filters** (`src/reflex/agent/filters.py`):
- Compose with `&` (and), `|` (or), `~` (not) operators
- Factory functions: `type_filter()`, `source_filter()`, `keyword_filter()`
- Stateful: `RateLimitFilter`, `DedupeFilter`

**Triggers** (`src/reflex/agent/triggers.py`):
- Connect filters to agents: `Trigger(name, filter, agent, scope_key)`
- Use `@trigger` decorator on agent classes
- `TriggerRegistry` manages trigger lookup

**Dependencies** (`src/reflex/core/deps.py`):
- `ReflexDeps`: Main container passed to PydanticAI tools via `RunContext[ReflexDeps]`
- Contains: `store` (EventStore), `http` (AsyncClient), `db` (AsyncSession), `scope`

### Module Organization

- `src/reflex/infra/` - Infrastructure (EventStore, database, observability) - keep stable
- `src/reflex/core/` - Core types (events, deps, errors) - extend carefully
- `src/reflex/agent/` - Agent logic (triggers, filters, agents) - primary extension point
- `src/reflex/api/` - FastAPI routes and middleware

### Testing Patterns

Unit tests use mock fixtures from `tests/conftest.py`:
```python
def test_something(mock_store, mock_deps):
    # mock_store is AsyncMock with publish/subscribe/ack/nack
    # mock_deps is ReflexDeps with all mocked dependencies
```

Integration tests require `DATABASE_URL` and use real fixtures (`store`, `real_deps`).

## Code Style

- Python 3.11+, strict pyright type checking
- ruff for linting (includes async, security, bugbear rules)
- Line length: 100 characters
- Use `from __future__ import annotations` for forward references
- Place runtime-only imports in `if TYPE_CHECKING:` blocks to avoid circular imports

## Claude Code Configuration

The `.claude/` directory contains commands, skills, and hooks that must stay in sync with the codebase:

```
.claude/
├── settings.json           # Hook configuration
├── hooks/
│   └── lint-format.sh      # Stop hook for auto-formatting
├── commands/               # Slash commands (/command-name)
│   ├── new-event.md        # Scaffold new event type
│   ├── new-agent.md        # Scaffold new agent
│   ├── new-filter.md       # Scaffold custom filter
│   ├── new-trigger.md      # Scaffold trigger
│   ├── check.md            # Run CI pipeline
│   ├── fix.md              # Auto-fix lint/format
│   ├── test.md             # Run tests
│   ├── api-test.md         # Run Bruno collection
│   ├── health.md           # Check system health
│   ├── dlq.md              # Inspect dead-letter queue
│   ├── replay.md           # Replay event
│   └── trace.md            # Trace event flow
└── skills/                 # Auto-applied contextual knowledge
    ├── reflex-events/      # Event system patterns
    ├── reflex-agents/      # Agent, filter, trigger patterns
    ├── reflex-database/    # PostgreSQL, EventStore, migrations
    └── reflex-testing/     # Test fixtures and patterns
```

**Important**: When modifying core patterns (events, agents, filters, triggers, testing), update the corresponding commands and skills to maintain parity. This ensures scaffolding commands generate correct code and skills provide accurate guidance.

## Documentation

The `docs/` directory contains user-facing documentation that must stay in sync with the codebase:

```
docs/
├── index.md              # Overview and quick start
├── getting-started.md    # Setup and first steps
├── architecture.md       # System design and event flow
├── extending.md          # Custom events, agents, and filters
├── configuration.md      # Environment variables
├── development.md        # Commands and testing
├── scaling.md            # Horizontal scaling
└── operations.md         # DLQ and observability
```

**Important**: When modifying APIs, configuration options, or architectural patterns, update the corresponding documentation. Key mappings:
- Event/agent/filter changes → `extending.md`
- Environment variables → `configuration.md`
- Make targets or dev workflow → `development.md`
- EventStore or database changes → `architecture.md`, `scaling.md`
- DLQ or observability → `operations.md`
