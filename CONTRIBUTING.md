# Contributing to reflexr

Thanks for helping. This guide covers how to set up, how work flows into `main`, and what "done" means here.

## Set up

You need [uv](https://docs.astral.sh/uv/) and `make`. Everything else is installed from `uv.lock`.

```bash
git clone git@github.com:alexnodeland/reflexr.git
cd reflexr
make install        # every dependency group and extra, and the git hooks
make check          # lint, types and tests: the same gates as CI
```

Run `make` on its own to list every command:

| Command | What it does |
|---|---|
| `make fmt` | Format the code and apply safe lint fixes |
| `make lint` | Check formatting and lint rules |
| `make typecheck` | Type-check with pyright (strict for `src/`) |
| `make test` | Run the tests with the 100% branch-coverage gate |
| `make check` | Everything CI runs |
| `make pg-up`, `make test-pg`, `make pg-down` | Start PostgreSQL in Docker, run the tests on it as well as SQLite, and stop it |
| `make schema` | Regenerate `schemas/reflexr.rules.v1.json` from the rule models (a test fails if it drifts) |
| `make changelog` | Regenerate `CHANGELOG.md` from commit history |

## How work flows: trunk-based development

`main` is the trunk and is always releasable ([ADR-0012][adr-0012]).

1. Branch from the latest `main`. Keep branches short-lived: hours to a day or two, not weeks.
2. Keep pull requests small and focused on one change. Split large work into a sequence of PRs that each leave `main` green.
3. CI must pass before merging: lint, types, and tests at 100% coverage on every supported Python.
4. Pull requests are squash-merged, so the PR title becomes the commit on `main`. Write it as a [Conventional Commit](https://www.conventionalcommits.org/).
5. Delete the branch after merging. Don't stack branches on unmerged branches.

Unfinished features land behind unexported code paths or not at all; never on a long-lived branch.

### Commit messages

Commits and PR titles follow Conventional Commits, checked by a `commit-msg` hook:

```
feat(core): add the sequence pattern
fix(workspace): release a run's lease when the run is cancelled
docs(adr): record the cron parsing decision
```

Types: `feat`, `fix`, `docs`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`. Scopes are package or area names: `core`, `workspace`, `agent`, `sql`, `fastapi`, `mcp`, `examples`, `docs`, `adr`, `rfc`. Mark breaking changes with `!` (`feat(core)!: ...`) and a `BREAKING CHANGE:` footer. The changelog is generated from these messages.

## Dependencies

`pyproject.toml` states the **oldest** versions reflexr supports, as wide as correctness allows, so applications can resolve it alongside their own dependencies. `uv.lock` pins what CI and contributors run, and Dependabot keeps the lockfile (not the ranges) current. Raise a lower bound only when the code needs a newer feature or fix, in the same pull request as that code.

## Design: RFCs, ADRs and evergreen docs

| Document | When | Where |
|---|---|---|
| **RFC** | Before a substantial change: new public API, protocol changes, a new package, cross-cutting behaviour | [`docs/rfcs/`][rfcs] |
| **ADR** | When a decision is made, including decisions made while implementing an RFC | [`docs/adr/`][adrs] |
| **Architecture docs** | Updated in the same PR as the code they describe | [`docs/architecture.md`][architecture], [`docs/protocol.md`][protocol] |

An RFC proposes; ADRs record what was decided; the architecture docs describe what exists now. A PR that changes behaviour described in the architecture docs updates them in the same PR, never in a later cleanup. Accepted ADRs are not edited; a changed decision gets a new ADR that supersedes or amends the old one.

## Quality gates

These are enforced by CI and described in [ADR-0013][adr-0013]:

- **100% line and branch coverage** of `src/reflexr`. Code that cannot be reached by a test is usually code that should not exist. The only exclusions are configured in `pyproject.toml` (type-checking blocks, protocol stubs, overloads, `assert_never`).
- **pyright strict** for `src/`, standard for `tests/`, with no inline suppressions.
- **ruff** for formatting and linting, with Google-style docstrings on public API.
- **Warnings are errors** in the test suite.
- Core behaviour is specified by **conformance fixtures**; a change to core behaviour changes a fixture.

## Definition of done

- [ ] Tests cover the change, and `make check` passes locally.
- [ ] Public API has docstrings and type annotations.
- [ ] Architecture docs and the protocol spec reflect the change.
- [ ] New decisions have an ADR; substantial proposals had an RFC.
- [ ] The PR title is a Conventional Commit.

## Reporting bugs and proposing features

Use the issue templates. For security issues, follow [SECURITY.md][security] instead of opening a public issue.

## Code of conduct

This project follows the [Code of Conduct][code-of-conduct]. By participating, you agree to uphold it.

## License

By contributing, you agree that your contributions are licensed under the [MIT License][license].

<!-- Link targets live here so the documentation site can redefine them for its own layout. -->

[adr-0012]: docs/adr/0012-trunk-based-development-with-rfcs-and-adrs.md
[adr-0013]: docs/adr/0013-quality-gates.md
[adrs]: docs/adr/README.md
[architecture]: docs/architecture.md
[code-of-conduct]: CODE_OF_CONDUCT.md
[license]: LICENSE
[protocol]: docs/protocol.md
[rfcs]: docs/rfcs/README.md
[security]: SECURITY.md
