# ADR-0014: MIT license

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

Reflex is already published under the MIT license. The rebuild could change it, and the library should match its sibling, artifactr, which is MIT as well.

## Decision

reflexr stays under the **MIT license**, with the existing `LICENSE` file (copyright 2026 Alex Nodeland). Contributions are accepted under the same license.

## Options considered

| Option | Adoption | Protection |
|---|---|---|
| **MIT (chosen)** | Highest; matches artifactr and the Pydantic ecosystem | Minimal |
| Apache 2.0 | High | Explicit patent grant; differs from artifactr |
| A copyleft license | Lower for a library | Stronger |

## Consequences

- Easier: anyone can use reflexr in any application, and the two libraries can be combined without license questions.
- The license is declared in `pyproject.toml` with `license = "MIT"` and `license-files`.

## Action items

1. [x] Declare the license in the new packaging (RFC-0001 phase 0).
