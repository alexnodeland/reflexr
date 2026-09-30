# JSON Schemas

reflexr publishes two JSON Schemas, generated from its Pydantic models, which are the source of truth, and checked in under [`schemas/`](https://github.com/alexnodeland/reflexr/tree/main/schemas):

| Schema | What it describes | Use it to |
|---|---|---|
| [`reflexr.rules.v1.json`](https://github.com/alexnodeland/reflexr/blob/main/schemas/reflexr.rules.v1.json) | A [rule](../guides/rules.md): its condition, scope, action and policies | Validate rules written as JSON, by people or by agents |
| [`reflexr.v1.json`](https://github.com/alexnodeland/reflexr/blob/main/schemas/reflexr.v1.json) | Every frame of the [stream protocol](../protocol.md): `client` is any frame a client sends, and `server` any frame a server sends; `$defs` holds every frame, command, outcome and envelope they refer to | Generate a client's types |

A test fails if either checked-in file drifts from the models. Regenerate both with `make schema`, which runs:

```bash
uv run python -m reflexr.core.schema rules > schemas/reflexr.rules.v1.json
uv run python -m reflexr.core.schema protocol > schemas/reflexr.v1.json
```

## Validating rules

A rule serializes to plain JSON, with durations as ISO 8601 strings. `Rule.model_validate` reads it back, and any JSON Schema validator can check it against `reflexr.rules.v1.json` first, which is how an application can accept rules from an agent or a form:

```python
from reflexr import Rule
from reflexr.core import DEFAULT_REGISTRY

rule = Rule.model_validate_json(text)  # raises pydantic.ValidationError if it does not fit
rule.check(events=DEFAULT_REGISTRY)  # raises InvalidRule for unknown types or fields
```

Names are qualified: an event type or a rule name without its namespace fails validation, and an event type's error names the qualified one it may mean.

## Generating client types

Frontends generate their types from the protocol schema rather than writing them by hand. For TypeScript, for example, with [json-schema-to-typescript](https://github.com/bcherny/json-schema-to-typescript):

```bash
npx json-schema-to-typescript schemas/reflexr.v1.json > src/reflexr.d.ts
```

Within `reflexr.v1`, changes are additive only: new optional fields and new event types. Clients should ignore fields they do not know, and keep an event of an unknown type as an opaque envelope, since it still carries a `seq`. Application events appear in the schema as objects with a `type` and any other fields, because the application defines them.

## The schemas

??? abstract "`schemas/reflexr.rules.v1.json`"

    ```json
    --8<-- "schemas/reflexr.rules.v1.json"
    ```

??? abstract "`schemas/reflexr.v1.json`"

    ```json
    --8<-- "schemas/reflexr.v1.json"
    ```
