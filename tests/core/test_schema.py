"""The checked-in rule schema matches the models."""

import json
import runpy
import sys
from pathlib import Path

import pytest

from reflexr.core.schema import RULES_SCHEMA_ID, rules_schema

SCHEMA = Path(__file__).parent.parent.parent / "schemas" / "reflexr.rules.v1.json"


def test_the_checked_in_schema_is_current() -> None:
    assert json.loads(SCHEMA.read_text()) == rules_schema(), "run `make schema`"
    assert rules_schema()["$id"] == RULES_SCHEMA_ID


@pytest.mark.filterwarnings("ignore:.*found in sys.modules:RuntimeWarning")
def test_the_module_prints_the_schema(capsys: pytest.CaptureFixture[str]) -> None:
    sys.modules.pop("reflexr.core.schema", None)
    runpy.run_module("reflexr.core.schema", run_name="__main__")
    assert json.loads(capsys.readouterr().out) == rules_schema()
