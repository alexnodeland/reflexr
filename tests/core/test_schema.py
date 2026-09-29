"""The checked-in rule schema matches the models."""

import json
import runpy
import sys
from pathlib import Path

import pytest

from reflexr.core.schema import (
    PROTOCOL_SCHEMA_ID,
    RULES_SCHEMA_ID,
    main,
    protocol_schema,
    rules_schema,
)

SCHEMAS = Path(__file__).parent.parent.parent / "schemas"


def test_the_checked_in_schemas_are_current() -> None:
    rules = json.loads((SCHEMAS / "reflexr.rules.v1.json").read_text())
    assert rules == rules_schema(), "run `make schema`"
    protocol = json.loads((SCHEMAS / "reflexr.v1.json").read_text())
    assert protocol == protocol_schema(), "run `make schema`"
    assert (rules["$id"], protocol["$id"]) == (RULES_SCHEMA_ID, PROTOCOL_SCHEMA_ID)
    assert set(protocol["properties"]) == {"client", "server"}


def test_either_schema_can_be_printed(capsys: pytest.CaptureFixture[str]) -> None:
    main(["protocol"])
    assert json.loads(capsys.readouterr().out) == protocol_schema()
    main(["rules"])
    assert json.loads(capsys.readouterr().out) == rules_schema()


@pytest.mark.filterwarnings("ignore:.*found in sys.modules:RuntimeWarning")
def test_the_module_prints_the_schema(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["schema"])
    sys.modules.pop("reflexr.core.schema", None)
    runpy.run_module("reflexr.core.schema", run_name="__main__")
    assert json.loads(capsys.readouterr().out) == rules_schema()
