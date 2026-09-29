# ADR-0002: The name reflexr

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

`reflex` is both the PyPI name and the import name of the Reflex web framework (reflex.dev, 0.9.12 on PyPI on this date). A library imported as `reflex` cannot be installed next to it, cannot be published under that name, and would be shadowed by, or shadow, a widely used package. The system that will combine this library with artifactr may well want a Python web UI.

## Decision

- The import package, the distribution and the GitHub repository are named **`reflexr`**. The name was free on PyPI on 2026-09-28 and mirrors `artifactr`.
- The repository moves from `alexnodeland/reflex` to `alexnodeland/reflexr`; GitHub redirects the old URLs.
- The documentation will live at `reflexr.alexnodeland.com`.

## Options considered

| Option | Coexists with reflex.dev | Publishable | Family resemblance |
|---|---|---|---|
| **`reflexr` (chosen)** | Yes | Yes | Mirrors `artifactr` |
| Keep `reflex` | No | No | None |
| Another distribution name, same import | No: the import still collides | Yes | Partial |

## Consequences

- Easier: installs alongside anything, including reflex.dev; the two libraries read as a family.
- Harder: existing references to `alexnodeland/reflex` rely on GitHub's redirect.

## Action items

1. [x] Rename the GitHub repository to `reflexr`.
2. [ ] Rename the package in phase 0.
