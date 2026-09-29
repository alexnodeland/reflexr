.PHONY: dev test lint type-check format clean logs shell ci docs docs-build docs-deploy api-test api-test-health api-test-events example examples-list

# CI - Emulate GitHub Actions pipeline locally
ci:
	@echo "=== Running CI Pipeline ==="
	@echo "\n--- Lint Check ---"
	uv run ruff check src tests
	@echo "\n--- Format Check ---"
	uv run ruff format --check src tests
	@echo "\n--- Type Check ---"
	uv run pyright src
	@echo "\n--- Tests ---"
	uv run pytest -v
	@echo "\n=== CI Pipeline Complete ==="

# Development
dev:
	docker compose up

dev-build:
	docker compose up --build

dev-down:
	docker compose down

logs:
	docker compose logs -f app

shell:
	docker compose exec app bash

db-shell:
	docker compose exec db psql -U reflex

# Testing
test:
	docker compose run --rm app pytest -v

test-cov:
	docker compose run --rm app pytest --cov=reflex --cov-report=html

# Code quality
lint:
	uv run ruff check src tests

lint-fix:
	uv run ruff check --fix src tests

format:
	uv run ruff format src tests

type-check:
	uv run pyright src

check: lint type-check test

# Database
migrate:
	docker compose run --rm app python scripts/migrate.py

# Utilities
demo:
	uv run python scripts/demo.py

example:
ifndef EXAMPLE
	@uv run python scripts/run_example.py --list
else
	@uv run python scripts/run_example.py $(EXAMPLE)
endif

examples-list:
	@uv run python scripts/run_example.py --list

replay:
	docker compose run --rm app python scripts/replay.py $(ARGS)

dlq:
	docker compose run --rm app python scripts/dlq.py

# API Testing (Bruno)
api-test:
	bru run bruno --env docker

api-test-health:
	bru run bruno/health --env docker

api-test-events:
	bru run bruno/events --env docker

# Documentation
docs:
	uv run --extra docs mkdocs serve

docs-build:
	uv run --extra docs mkdocs build

docs-deploy:
	uv run --extra docs mkdocs gh-deploy --force

# Cleanup
clean:
	docker compose down -v
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	find . -type d -name .ruff_cache -exec rm -rf {} +
	find . -type d -name site -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
