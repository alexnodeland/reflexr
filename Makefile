# reflexr: everyday developer commands. `make` lists them.
#
# Everything runs through uv, so the versions used here are the ones in uv.lock.

.DEFAULT_GOAL := help
UV ?= uv

# The PostgreSQL that `make pg-up` starts for the SQL tests.
PG_CONTAINER ?= reflexr-postgres
PG_PORT ?= 54330
PG_URL ?= postgresql+asyncpg://postgres:reflexr@localhost:$(PG_PORT)/postgres

.PHONY: help install fmt lint typecheck test check schema pg-up pg-down test-pg changelog clean

help: ## List the available commands
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install every dependency group and extra, plus the git hooks
	$(UV) sync --all-groups --all-extras --all-packages
	$(UV) run pre-commit install --hook-type pre-commit --hook-type commit-msg

fmt: ## Format the code and apply safe lint fixes
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

lint: ## Check formatting and lint rules
	$(UV) run ruff format --check .
	$(UV) run ruff check .

typecheck: ## Type-check (strict for src/)
	$(UV) run pyright

test: ## Run the tests with the 100% branch-coverage gate
	$(UV) run pytest --cov --cov-report=term-missing

check: lint typecheck test ## Run everything CI runs

schema: ## Regenerate the rule and protocol JSON Schemas from the models
	$(UV) run python -m reflexr.core.schema rules > schemas/reflexr.rules.v1.json
	$(UV) run python -m reflexr.core.schema protocol > schemas/reflexr.v1.json

pg-up: ## Start a PostgreSQL container for the SQL tests (needs Docker)
	docker run --rm -d --name $(PG_CONTAINER) -p $(PG_PORT):5432 -e POSTGRES_PASSWORD=reflexr postgres:17
	@until docker exec $(PG_CONTAINER) pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; do sleep 1; done

pg-down: ## Stop the PostgreSQL container
	docker stop $(PG_CONTAINER)

test-pg: ## Run the tests on PostgreSQL as well as SQLite (after make pg-up)
	REFLEXR_TEST_POSTGRES_URL=$(PG_URL) $(UV) run pytest --cov --cov-report=term-missing

changelog: ## Regenerate CHANGELOG.md from conventional commits
	$(UV) run git-cliff --output CHANGELOG.md
	@$(UV) run python -c "import pathlib; p = pathlib.Path('CHANGELOG.md'); p.write_text(p.read_text().rstrip() + '\n')"

clean: ## Remove caches and build output
	rm -rf .pytest_cache .ruff_cache .coverage coverage.xml htmlcov dist site .cache
	find . -name __pycache__ -type d -prune -not -path './.venv/*' -exec rm -rf {} +
