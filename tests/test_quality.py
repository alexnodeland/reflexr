"""Quality gates the linters cannot express (ADR-0013)."""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
SUPPRESSION = re.compile(r"#\s*(type:\s*ignore|pyright:\s*ignore|noqa|pragma:\s*no\s*cover)")


def _sources() -> list[Path]:
    return sorted(
        path
        for folder in ("src", "tests", "examples")
        for path in (ROOT / folder).rglob("*.py")
        if ".venv" not in path.parts
    )


@pytest.mark.parametrize("path", _sources(), ids=lambda path: str(path.relative_to(ROOT)))
def test_no_inline_suppressions(path: Path) -> None:
    # Restructure the code instead: suppressions hide the problems the gates exist to find.
    found = [
        f"{number}: {line.strip()}"
        for number, line in enumerate(path.read_text().splitlines(), start=1)
        if SUPPRESSION.search(line) and path.name != "test_quality.py"
    ]
    assert not found, "\n".join(found)
