"""A Griffe extension that turns Sphinx cross-reference roles into mkdocstrings cross-references.

reflexr's docstrings follow the Google style but refer to other objects with Sphinx roles, such
as ``:class:`Workspace```, ``:meth:`~reflexr.workspace.Reactor.settle``` or
``:meth:`checkpoint <reflexr.workspace.Reaction.checkpoint>```. The API reference renders
docstrings as Markdown, where those roles would show up literally. This extension rewrites
each role into a Markdown cross-reference (``[`Workspace`][reflexr.workspace.Workspace]``),
resolved in the scope of the object whose docstring contains it, so the library's docstrings
stay as they are.

A role whose target cannot be resolved, or a module role naming an internal module, becomes
inline code instead of a broken link.
"""

import re
from typing import Any

import griffe

_ROLE = re.compile(
    r":(?P<role>class|meth|func|mod|attr|data|exc|obj):"
    r"`(?:(?P<title>[^<`]+?)\s*<(?P<titled>[\w.]+)>|(?P<tilde>~?)(?P<target>[\w.]+))`"
)

# The modules the reference renders a page for; other modules are internal.
_PUBLIC_MODULES = frozenset(
    {
        "reflexr.core",
        "reflexr.telemetry",
        "reflexr.workspace",
        "reflexr.agent",
        "reflexr.scores",
        "reflexr.evals",
        "reflexr.fastapi",
        "reflexr.mcp",
    }
)


class SphinxRoles(griffe.Extension):
    """Rewrite Sphinx roles in every docstring of a package once it is loaded."""

    def on_package(self, *, pkg: griffe.Module, **_kwargs: Any) -> None:
        """Rewrite the roles in the package's docstrings."""
        _rewrite(pkg, seen=set())


def _rewrite(obj: griffe.Object, *, seen: set[str]) -> None:
    if obj.path in seen:
        return
    seen.add(obj.path)
    if obj.docstring is not None:
        obj.docstring.value = _ROLE.sub(lambda match: _reference(obj, match), obj.docstring.value)
    for member in obj.members.values():
        if isinstance(member, griffe.Object):
            _rewrite(member, seen=seen)


def _reference(obj: griffe.Object, match: re.Match[str]) -> str:
    if match["titled"] is not None:
        target, label = match["titled"], " ".join(match["title"].split())
    else:
        target = match["target"]
        label = target.rsplit(".", 1)[-1] if match["tilde"] else target
    path = _resolve(obj, target)
    if path is None or (match["role"] == "mod" and path not in _PUBLIC_MODULES):
        return f"`{label}`"
    return f"[`{label}`][{path}]"


def _resolve(obj: griffe.Object, target: str) -> str | None:
    """Resolve a possibly dotted name in the object's scope, as Sphinx would."""
    if target.startswith("reflexr."):
        return target
    first, _, rest = target.partition(".")
    try:
        resolved = obj.resolve(first)  # looks in the object's scope, then its parents'
    except griffe.NameResolutionError:
        return None
    return f"{resolved}.{rest}" if rest else resolved
