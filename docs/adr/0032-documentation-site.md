# ADR-0032: The documentation site, and a brand shared by the family

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

RFC-0001 phase 7 asks for a documentation website with guides and an API reference, built in CI with no warnings, and a brand that is a sibling of artifactr's. The documentation already exists as Markdown under `docs/`: the architecture, the stream protocol, the ADRs and the RFCs. They are evergreen and read on GitHub as well as on a site, so the site has to use them as they are rather than copies.

The site needs:

- an API reference generated from the library's docstrings, which follow the Google style but cross-reference with Sphinx roles, in both the plain form (`:class:`Workspace``) and the titled form (`:meth:`Reaction.checkpoint <reflexr.workspace.Reaction.checkpoint>``)
- Mermaid diagrams, which the architecture, the protocol and the run lifecycle use
- search, code copy buttons, light and dark schemes, and the brand
- the repository's root files (`CONTRIBUTING.md`, `CHANGELOG.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, `LICENSE`) without duplicating them
- both generated JSON Schemas, of rules and of the protocol's frames
- a strict build that fails on broken links and anchors, so the site cannot rot silently

artifactr answered the same questions in its [ADR-0023](https://github.com/alexnodeland/artifactr/blob/main/docs/adr/0023-documentation-site.md): a spike showed that Zensical, the successor that Material for MkDocs' authors are building, renders mkdocstrings' reference, Mermaid and strict link checks well enough to start on, and found two limits (links inside included snippets are not checked, and Sphinx roles render literally). reflexr is artifactr's sibling ([ADR-0003](0003-independent-sibling-of-artifactr.md)), and a reader moving between the two sites should find the same structure and the same tools behind it.

The brand is the other half of the phase. evalr and stackr ([ADR-0020](0020-evalr-shared-eval-kit.md), [ADR-0023](0023-libraries-and-the-stackr-template.md)) will need identities too, so reflexr's is the first chance to turn artifactr's single brand into a system the family shares, rather than four brands that happen to look alike.

## Decision

- **The site is built exactly as artifactr's is.** Zensical builds it from `mkdocs.yml` with `docs_dir: docs` and the modern theme variant; mkdocstrings-python renders one reference page per package, grouped by concept and covering exactly each package's `__all__`, with `griffe-pydantic` for model fields; `pymdownx.snippets` includes the root files from small pages under `docs/project/`, whose reference-style link definitions replace the files' own; pages under `docs/` link to files outside it by GitHub URL; the tools are the `docs` dependency group, included in `dev`.
- **A Griffe extension converts Sphinx roles** when the docstrings are loaded (`scripts/griffe_sphinx_roles.py`), including the titled form, which reflexr's docstrings use and artifactr's do not. Each role is resolved in the scope of the object whose docstring contains it; one that cannot be resolved, or that names an internal module, becomes inline code. The script is held to the library's gates: pyright strict and the test that forbids inline suppressions both cover `scripts/`.
- **The build is strict everywhere.** `make docs` and CI's Docs job run `zensical build --strict --clean`, so a broken link, anchor or cross-reference fails the build.
- **The guides follow the code.** Every guide is written for a developer adopting the library, from the code and the design documents, and every snippet was run against the API when it was written. Where the architecture and the code have drifted, the guides describe the code.
- **One brand system for the family**, stated in [`docs/assets/brand/README.md`](../assets/brand/README.md): artifactr's typefaces, 64-unit grid, stroke and diagonal, and print-proofing logic, with each library printing in two of the three process inks (artifactr cyan and magenta, reflexr magenta and yellow, evalr cyan and yellow) whose overprint is its working colour, and stackr in key. reflexr's mark is artifactr's caret turned a quarter: a chevron whose point is where the rule fires. The files are hand-authored SVGs with outlined text, and the lockups and banners follow artifactr's layout to the unit.

## Options considered

### The toolchain

| Option | Matches artifactr | API reference | Strict build | Direction |
|---|---|---|---|---|
| **Zensical with mkdocstrings (chosen)** | Yes | mkdocstrings-python, verified in artifactr's spike | Yes, for links and anchors in page source | The successor its authors are developing |
| MkDocs 1.6 with Material for MkDocs | Nearly: the same configuration | mkdocstrings-python | Yes | Maintenance only |
| Sphinx with MyST | No | autodoc, which reads Sphinx roles natively | Yes (`-W`) | Active |

artifactr's ADR-0023 weighed these in full. For reflexr the deciding factor is the family: one toolchain means one set of conventions for contributors, one way to fix a rendering problem, and the option of a shared theme later.

### The brand

| Option | Reads as a family | Each library distinct | Scales to evalr and stackr |
|---|---|---|---|
| **One system, one ink pair per library (chosen)** | Yes: same grid, stroke, type and logic | Yes: its own mark and overprint | Yes, by rule |
| artifactr's brand, recoloured | Too much: the same caret everywhere | Only by colour | Yes, but the marks say nothing |
| An independent brand | No | Yes | Each library starts over |

## Consequences

- Easier: every guide, the reference and the design records are one site built from the repository, and the reference cannot drift from the code.
- Easier: a broken link or cross-reference fails CI, and both sites fail the same way.
- Easier: evalr's and stackr's sites and READMEs start from written rules and a drawn mark, not a blank page.
- Harder: the guides' snippets are verified when written, not in CI, so an API change must update the guides in the same pull request, as evergreen documentation requires ([ADR-0012](0012-trunk-based-development-with-rfcs-and-adrs.md)).
- Harder: root files must keep their links reference-style, and a new link in one of them needs a definition in its `docs/project/` page. Strict mode does not catch a missing one.
- Harder: a change to the brand system is a change to four repositories' brand READMEs.
- Revisit when Zensical reaches 1.0, or if its native configuration format replaces `mkdocs.yml`.

## Amendment (2026-09-29): lists render as they do on GitHub

Python-Markdown, which the site uses, needs four spaces to nest a list and a blank line before a list that follows a paragraph; GitHub needs neither. The repository's Markdown nests by two spaces, so most nested lists on the site were flat, and a few lists rendered as a paragraph of "- " text.

- **The `mdx_truly_sane_lists` extension** makes two spaces nest, as on GitHub. A nested item is indented by its parent's text: two spaces after `-`, three after `1.`.
- **A blank line goes before every list.** The pages that lacked one are fixed.
- **`scripts/check_site.py` checks the built site** in `make docs` and in CI: a paragraph or list item containing a line that starts with a list marker is a list that rendered as text, and fails the build.

## Action items

1. [x] Build the site, the brand and the branded README (RFC-0001 phase 7).
2. [x] Build the site in strict mode in CI.
3. [x] Publish the site ([ADR-0033](0033-publishing-the-documentation-site.md)).
4. [ ] Adopt the family system in evalr's and stackr's repositories.
