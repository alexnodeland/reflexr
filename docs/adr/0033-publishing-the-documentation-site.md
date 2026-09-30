# ADR-0033: Publishing the documentation site from main

**Status:** Accepted, amended by [ADR-0046](0046-one-docs-build.md)
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

[ADR-0032](0032-documentation-site.md) builds the documentation site. The maintainer has decided to publish it at `https://reflexr.alexnodeland.com`, beside artifactr's at `https://artifactr.alexnodeland.com`, and has set up the domain and GitHub Pages for the repository.

artifactr first built its site with a manual publishing step, then moved to publishing from `main` ([artifactr ADR-0026](https://github.com/alexnodeland/artifactr/blob/main/docs/adr/0026-publishing-the-documentation-site.md)), because documentation that changes in the same pull request as the code should reach readers when that pull request merges. A site that is deployed only when someone remembers to run a workflow falls behind `main`, and readers can't tell that it has.

## Decision

- **Every push to `main` deploys the site.** `.github/workflows/docs.yml` builds it in strict mode, as CI does, and deploys it to GitHub Pages on pushes to `main`, and when run by hand. Deployments queue in one `pages` concurrency group rather than overlap.
- **The custom domain is set in the repository's Pages settings**, with GitHub Actions as the source. Deployments from a workflow ignore a `CNAME` file, so the repository has none. `site_url` in `mkdocs.yml`, the project URLs in `pyproject.toml` and the README point at the custom domain.
- **The site shows `main`, not a release.** reflexr has no release yet; one version of the documentation is enough until there are several releases with differing APIs.

## Options considered

| Option | Site matches `main` | Effort per change |
|---|---|---|
| **Deploy on every push to `main` (chosen)** | Always | None |
| Deploy by hand | Only after someone runs it | A manual step each time |
| Deploy on releases only | At each release; ahead of released code in between | None, but there is no release yet, so no site |

## Consequences

- Easier: a merged documentation change is live within minutes, as artifactr's are.
- Harder: the site can describe features that are on `main` but in no release. Until the first release the site says so on its home page; revisit with versioned documentation (for example, one site per minor version) when that gap matters.

## Action items

1. [x] Deploy on pushes to `main`, and point `site_url`, the project URLs and the README at the custom domain.
2. [ ] Consider versioned documentation once there are several releases.
