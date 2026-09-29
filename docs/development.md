# Development

Commands and workflows for developing with Reflex.

## 🛠️ Development Commands

=== "Docker"

    ```bash
    make dev          # Start with hot reload
    make dev-build    # Rebuild and start
    make dev-down     # Stop containers
    make logs         # Follow app logs
    make shell        # Shell into app container
    make db-shell     # psql into PostgreSQL
    ```

=== "Local"

    ```bash
    # Start PostgreSQL
    docker compose up db -d

    # Run the app with hot reload
    uv run uvicorn reflex.api.app:app --reload

    # Run the demo
    make demo
    ```

## 📦 Running Examples

Reflex includes several example applications demonstrating different use cases.

```bash
# List available examples
make examples-list

# Run a specific example
make example EXAMPLE=basic
make example EXAMPLE=fraud_detection
```

### Available Examples

| Example | Description |
|---------|-------------|
| `basic` | Error monitoring with alert triggers |
| `fraud_detection` | E-commerce order fraud detection with LLM |
| `incident_response` | Automated incident response system |
| `log_anomaly` | Log anomaly detection agent |
| `support_bot` | Customer support chatbot |
| `content_moderation` | Content moderation with AI classification |

!!! note "Requirements"
    Examples require:

    - A running database: `docker compose up db -d`
    - API keys: `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`

## 🧪 Testing

```bash
make test         # Run all tests in Docker
make test-cov     # Run with coverage report
```

### Running Tests Locally

=== "All Tests"

    ```bash
    pytest tests/
    ```

=== "Single Test"

    ```bash
    pytest tests/test_file.py::test_name
    ```

=== "Unit Tests Only"

    ```bash
    # No database required
    pytest tests/ -m "not asyncio"
    ```

=== "Integration Tests"

    ```bash
    # Requires DATABASE_URL
    DATABASE_URL="postgresql://..." pytest tests/test_store.py
    ```

### Test Fixtures

Tests use mock fixtures from `tests/conftest.py`:

```python
def test_something(mock_store, mock_deps):
    # mock_store: AsyncMock with publish/subscribe/ack/nack
    # mock_deps: ReflexDeps with all mocked dependencies
    pass
```

Integration tests use real fixtures:

```python
@pytest.mark.asyncio
async def test_integration(store, real_deps):
    # store: Real EventStore connected to PostgreSQL
    # real_deps: ReflexDeps with real dependencies
    pass
```

## ✅ Code Quality

```bash
make lint         # ruff check
make lint-fix     # Auto-fix lint issues
make format       # ruff format
make type-check   # pyright
make ci           # Run full CI pipeline locally
```

### CI Pipeline

The `make ci` command runs the same checks as GitHub Actions:

1. Lint check (`ruff check`)
2. Format check (`ruff format --check`)
3. Type check (`pyright`)
4. Tests (`pytest`)

## 📝 Code Style

!!! note "Style Guidelines"

    - Python 3.11+
    - Strict pyright type checking
    - ruff for linting (includes async, security, bugbear rules)
    - Line length: 100 characters

### Import Conventions

```python
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Type-only imports to avoid circular dependencies
    from reflex.core.deps import ReflexDeps
```

## 📚 Documentation

```bash
make docs         # Serve docs locally at http://localhost:8000
make docs-build   # Build static docs site
```
