"""The package layering in docs/architecture.md, enforced.

Each layer may import the standard library, the reflexr layers below it, and an explicit list of
third-party packages. Dependencies point one way, so each layer is usable without the ones above.
"""

import ast
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).parent.parent / "src" / "reflexr"

LAYERS: dict[str, tuple[set[str], set[str]]] = {
    # layer: (reflexr packages it may import, third-party packages it may import)
    "core": ({"reflexr.core"}, {"pydantic"}),
    "telemetry": ({"reflexr.core", "reflexr.telemetry"}, {"opentelemetry"}),
    "workspace": (
        {"reflexr.core", "reflexr.telemetry", "reflexr.workspace"},
        {"pydantic", "opentelemetry", "cronsim"},
    ),
    "fastapi": (
        {"reflexr.core", "reflexr.telemetry", "reflexr.workspace", "reflexr.fastapi"},
        {"pydantic", "opentelemetry", "fastapi", "starlette"},
    ),
    "mcp": (
        {"reflexr.core", "reflexr.telemetry", "reflexr.workspace", "reflexr.mcp"},
        {"pydantic", "opentelemetry", "mcp", "starlette"},
    ),
    "scores": (
        {"reflexr.core", "reflexr.telemetry", "reflexr.workspace", "reflexr.scores"},
        {"pydantic"},
    ),
    "evals": (
        {"reflexr.core", "reflexr.telemetry", "reflexr.workspace", "reflexr.evals"},
        {"pydantic", "evalr"},
    ),
    "otel": (
        {"reflexr.telemetry", "reflexr.otel", "reflexr.langfuse"},
        {"opentelemetry", "pydantic_ai", "fastapi", "sqlalchemy", "langfuse"},
    ),
    "langfuse": (
        {
            "reflexr.core",
            "reflexr.telemetry",
            "reflexr.workspace",
            "reflexr.scores",
            "reflexr.langfuse",
        },
        {"langfuse", "opentelemetry"},
    ),
    "litellm": (
        {"reflexr.core", "reflexr.telemetry", "reflexr.workspace", "reflexr.litellm"},
        {"pydantic_ai", "opentelemetry", "httpx2"},
    ),
    "agent": (
        {"reflexr.core", "reflexr.telemetry", "reflexr.workspace", "reflexr.agent"},
        {"pydantic", "pydantic_core", "opentelemetry", "pydantic_ai", "pydantic_graph"},
    ),
    "sql": (
        {"reflexr.core", "reflexr.telemetry", "reflexr.workspace", "reflexr.sql"},
        {"pydantic", "sqlalchemy", "alembic"},
    ),
}


def _imports(path: Path) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.add(node.module)
    return names


@pytest.mark.parametrize("layer", sorted(LAYERS))
def test_layer_imports_only_what_it_may(layer: str) -> None:
    own, third_party = LAYERS[layer]
    modules = sorted((SRC / layer).rglob("*.py"))
    assert modules, f"layer {layer} has no modules"
    for path in modules:
        for name in _imports(path):
            root = name.split(".")[0]
            where = f"{path.relative_to(SRC)} imports {name}"
            if root in sys.stdlib_module_names:
                continue
            if root == "reflexr":
                assert any(name == p or name.startswith(f"{p}.") for p in own), f"{where}: above"
            else:
                assert root in third_party, f"{where}: not a dependency of this layer"


EXAMPLE = Path(__file__).parent.parent / "examples" / "oncall" / "src" / "oncall"
PUBLIC = {"reflexr", *(f"reflexr.{layer}" for layer in LAYERS)}
"""What an application imports from: reflexr and its packages, never the modules inside them."""


def test_the_reference_implementation_uses_only_the_public_api() -> None:
    modules = sorted(EXAMPLE.rglob("*.py"))
    assert modules, "the reference implementation has no modules"
    for path in modules:
        for name in _imports(path):
            if name.split(".")[0] == "reflexr":
                assert name in PUBLIC, f"{path.name} imports {name}, not a public package"
