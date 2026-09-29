# reflexr: everyday developer commands. `make` lists them.
#
# Everything runs through uv, so the versions used here are the ones in uv.lock.

.DEFAULT_GOAL := help
UV ?= uv

.PHONY: help install fmt lint typecheck test check schema changelog clean

help: ## List the available commands
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install every dependency group and extra, plus the git hooks
	$(UV) sync --all-groups --all-extras
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

changelog: ## Regenerate CHANGELOG.md from conventional commits
	$(UV) run git-cliff --output CHANGELOG.md
	@$(UV) run python -c "import pathlib; p = pathlib.Path('CHANGELOG.md'); p.write_text(p.read_text().rstrip() + '\n')"

clean: ## Remove caches and build output
	rm -rf .pytest_cache .ruff_cache .coverage coverage.xml htmlcov dist site .cache
	find . -name __pycache__ -type d -prune -not -path './.venv/*' -exec rm -rf {} +
