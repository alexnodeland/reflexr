# ADR-0012: Trunk-based development with RFCs, ADRs and evergreen docs

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

reflexr is rebuilt incrementally in the open, by a small team plus agents working in parallel, following the process artifactr uses. Reflex had no design records and no commit conventions, and its plans lived in files that the code drifted away from. Long-lived branches drift from each other and from `main`, and they end in painful merges. Design reasoning that lives only in pull request threads or chat is lost. Architecture documents that are written once and then left alone become wrong, and wrong documentation is worse than none.

## Decision

- **Trunk-based development.** `main` is the trunk and is always releasable. Work happens on short-lived branches cut from the latest `main` and merged within a day or two, as small pull requests. No stacked branches.
- **Squash merges with Conventional Commit titles**, so `main` has one commit per change and the changelog is generated from history with git-cliff.
- **CI gates every merge** ([ADR-0013](0013-quality-gates.md)).
- **RFCs before substantial changes.** A proposal is discussed as an RFC in [`docs/rfcs/`](../rfcs/README.md) before implementation. Multi-PR work is tracked in its RFC's checklist.
- **ADRs for decisions**, including decisions made while implementing an RFC. Accepted ADRs are immutable; a changed decision gets a new ADR that amends or supersedes the old one.
- **Evergreen architecture docs.** [`architecture.md`](../architecture.md) and [`protocol.md`](../protocol.md) describe the system as it is. A pull request that changes described behaviour updates them in the same pull request.

## Options considered

### Option A: Trunk-based development with RFCs, ADRs and evergreen docs (chosen)

| Dimension | Assessment |
|---|---|
| Complexity | Low day to day; needs discipline about PR size |
| Integration risk | Low: changes integrate continuously |
| Knowledge retention | High: proposals, decisions and current state each have a home |

**Pros:** small reviews; `main` is always usable; reasoning survives.
**Cons:** large features must be sliced into independently mergeable steps.

### Option B: Git flow with long-lived develop and feature branches

| Dimension | Assessment |
|---|---|
| Complexity | Medium |
| Integration risk | High: integration happens late |
| Knowledge retention | Depends on separate practices |

**Pros:** isolates unfinished work.
**Cons:** merge debt; `main` and `develop` diverge.

### Option C: ADRs only, no RFCs

| Dimension | Assessment |
|---|---|
| Complexity | Lowest |
| Integration risk | Unchanged |
| Knowledge retention | Decisions are recorded, but proposals are discussed without a document |

**Pros:** one fewer document type.
**Cons:** multi-PR plans have nowhere to live; decisions get recorded after the fact instead of reviewed before.

## Trade-off analysis

Trunk-based development keeps integration cheap, which matters most when several contributors (including agents working in parallel) touch the same package. RFCs and ADRs split two different jobs: discussing a proposal before building it, and recording a decision permanently. Evergreen docs keep the current state readable without replaying history.

## Consequences

- Easier: every commit on `main` is a reviewed, releasable change with a meaningful message.
- Easier: new contributors can read the RFCs, ADRs and architecture docs to understand why and what.
- Harder: every behaviour change carries a documentation obligation in the same PR.

## Action items

1. [x] Document the workflow in [CONTRIBUTING.md](https://github.com/alexnodeland/reflexr/blob/main/CONTRIBUTING.md).
2. [x] Add RFC and ADR templates.
3. [x] Enforce Conventional Commits with a `commit-msg` hook, and generate the changelog with git-cliff.
