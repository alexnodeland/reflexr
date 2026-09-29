"""The JSON Schema of rules, generated from the models.

``python -m reflexr.core.schema > schemas/reflexr.rules.v1.json`` regenerates the checked-in
schema, and a test fails if it drifts. Applications and agents that write rules as JSON can
validate them against it.
"""

import json
import sys
from typing import Any

from reflexr.core.rules import Rule

RULES_SCHEMA_ID = "https://github.com/alexnodeland/reflexr/schemas/reflexr.rules.v1.json"


def rules_schema() -> dict[str, Any]:
    """Return the JSON Schema of a rule."""
    schema = Rule.model_json_schema(mode="validation")
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": RULES_SCHEMA_ID,
        **schema,
    }


def main() -> None:
    """Print the rule schema as indented JSON."""
    sys.stdout.write(json.dumps(rules_schema(), indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
