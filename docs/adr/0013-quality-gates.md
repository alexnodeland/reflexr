# ADR-0013: Quality gates

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

reflexr is a library other projects will build on, and its guarantees (exact decisions, isolated rules, at-least-once runs) are only as good as the tests that pin them. Reflex claimed strict typing while disabling one of its checks and carrying 37 inline suppressions (type ignores and lint exemptions), reached 67% coverage, and hid a broken integration behind mocks and broad `except` blocks. Trunk-based development ([ADR-0012](0012-trunk-based-development-with-rfcs-and-adrs.md)) only works if CI can be trusted.

## Decision

CI enforces these gates on every pull request and on `main`, the same as artifactr's:

| Gate | Rule |
|---|---|
| Coverage | **100% line and branch coverage** of `src/reflexr` and of the reference implementation |
| Types | pyright **strict** for `src/` and the reference implementation's code, standard for tests, with **no inline suppressions** |
| Lint and format | ruff, with Google-style docstrings on public API |
| Tests | pytest with **warnings as errors**, on Python 3.12, 3.13 and 3.14, plus a PostgreSQL job once SQL storage exists |
| Lockfile | `uv sync --locked` |
| Schemas | Generated protocol and rule schemas must match the checked-in files |
| Docs | The site builds in strict mode |

The only coverage exclusions are configured centrally (`if TYPE_CHECKING:`, `Protocol` bodies, `@overload`, `assert_never`, `...`). Inline `pragma: no cover` is not allowed. Where a third-party library is untyped, a stub file under `typings/` is preferred to a suppression.

## Options considered

| Option | Regression protection | Cost |
|---|---|---|
| **100% branch coverage, strict types without suppressions (chosen)** | High | Medium, paid per change |
| A threshold such as 90% | Medium: the gap is usually error handling | Lower |
| Keep Reflex's gates | Low, as the audit showed | Lowest |

## Consequences

- Easier: refactoring and upgrading dependencies with confidence; untested code cannot merge.
- Harder: defensive code needs a test or needs to go.

## Action items

1. [x] Configure the gates and CI (RFC-0001 phase 0).
2. [ ] Add the PostgreSQL job (phase 4), and the schema and docs checks (phases 5 and 7).
