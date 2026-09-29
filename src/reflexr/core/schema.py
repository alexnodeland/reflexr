"""The JSON Schemas of rules and of the workspace protocol, generated from the models.

Both are checked in under ``schemas/`` and a test fails if either drifts. Regenerate them with
``make schema``, or::

    python -m reflexr.core.schema rules > schemas/reflexr.rules.v1.json
    python -m reflexr.core.schema protocol > schemas/reflexr.v1.json

Applications and agents that write rules as JSON can validate them against the first; clients
generate their types from the second, for example with ``json-schema-to-typescript``.
"""

import json
import sys
from typing import Any

from pydantic import BaseModel

from reflexr.core.protocol import PROTOCOL, ClientFrame, ServerFrame
from reflexr.core.rules import Rule

RULES_SCHEMA_ID = "https://github.com/alexnodeland/reflexr/schemas/reflexr.rules.v1.json"
PROTOCOL_SCHEMA_ID = "https://github.com/alexnodeland/reflexr/schemas/reflexr.v1.json"


class _Frames(BaseModel):
    client: ClientFrame
    server: ServerFrame


def rules_schema() -> dict[str, Any]:
    """Return the JSON Schema of a rule."""
    schema = Rule.model_json_schema(mode="validation")
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": RULES_SCHEMA_ID,
        **schema,
    }


def protocol_schema() -> dict[str, Any]:
    """Return the JSON Schema of every frame a client sends and a server sends."""
    frames = _Frames.model_json_schema()
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": PROTOCOL_SCHEMA_ID,
        "title": PROTOCOL,
        "description": (
            f"Frames of the {PROTOCOL} workspace protocol: `client` is any frame a client "
            "sends, `server` any frame a server sends."
        ),
        "type": "object",
        "properties": frames["properties"],
        "$defs": frames["$defs"],
    }


def main(argv: list[str] | None = None) -> None:
    """Print a schema as indented JSON: ``rules`` (the default) or ``protocol``."""
    which = (argv if argv is not None else sys.argv[1:]) or ["rules"]
    schema = protocol_schema() if which[0] == "protocol" else rules_schema()
    sys.stdout.write(json.dumps(schema, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
