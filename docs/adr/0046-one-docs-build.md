# ADR-0046: One docs build

**Status:** Accepted
**Date:** 2026-09-29
**Deciders:** Alex Nodeland

Amends [ADR-0032](0032-documentation-site.md), where the site is built, and [ADR-0033](0033-publishing-the-documentation-site.md), when the Docs workflow runs and how its runs queue.

## Context

The build's steps were written three times, in `make docs`, CI's Docs job and `docs.yml`, and it ran in two places: CI's Docs job on every pull request and push, and `docs.yml` on every push to `main`, so each push to `main` built the site twice. `make docs` left out the changelog, so it built a different site from either workflow. And `docs.yml` queued every run in one `pages` concurrency group: if pull requests ran it too, their builds would wait in the same queue as `main`'s deployments, where a waiting run is cancelled by the next one to arrive.

evalr and stackr had the same arrangement, and changed it in [evalr's ADR-0013](https://github.com/alexnodeland/evalr/blob/main/docs/adr/0013-docstrings-in-markdown-and-one-docs-build.md) and [stackr's ADR-0014](https://github.com/alexnodeland/stackr/blob/main/docs/adr/0014-one-docs-build.md). reflexr follows them, as artifactr does in [its ADR-0050](https://github.com/alexnodeland/artifactr/blob/main/docs/adr/0050-one-docs-build.md), so the family's sites are built the same way.

## Decision

- **`make docs` is the build**: it regenerates the changelog (`make changelog`), builds the site in strict mode, and checks its lists.
- **`docs.yml` runs `make docs`** on every pull request, on every push to `main` and by hand, in a job named Docs, as CI's was, and deploys only from `main`. CI's Docs job is removed.
- **Each branch's runs queue on their own.** The concurrency group is the ref's: runs on `main` wait their turn, so an older commit never deploys after a newer one, and a newer run on a pull request cancels the older one.

## Options considered

### Option A: One build, in `docs.yml` (chosen)

| Dimension | Assessment |
|---|---|
| Complexity | Low: one definition of the build |
| What a pull request checks | The build that deploys |

**Pros:** the site built locally, on a pull request and on `main` is built the same way, and each push to `main` builds it once.
**Cons:** `make docs` rewrites `CHANGELOG.md` in the working tree.

### Option B: Keep both jobs

| Dimension | Assessment |
|---|---|
| Complexity | A build written three times |
| What a pull request checks | A copy of the build that deploys |

**Pros:** no change.
**Cons:** the copies can drift, and every push to `main` builds the site twice.

## Trade-off analysis

One workflow that builds on pull requests and deploys from `main` checks exactly what will be published, and matches evalr's and stackr's. A concurrency group per ref keeps pull requests out of `main`'s queue, so their builds never hold up or take the place of a deployment.

## Consequences

- Easier: one build to change, and the same one in every sibling repository.
- Harder: `make docs` leaves a regenerated `CHANGELOG.md` behind; it is committed only before a release, by `make changelog`.

## Action items

1. [x] Build the site once, with `make docs`, in `docs.yml`, deploy from `main` one run at a time, and remove CI's Docs job.
