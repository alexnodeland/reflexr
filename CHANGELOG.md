# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Features

- **agent**: Infer decisions' and forks' input types for graph checkpoints ([#52](https://github.com/alexnodeland/reflexr/pull/52))
- **deploy**: Grafana dashboards, tested against the metric registry ([#51](https://github.com/alexnodeland/reflexr/pull/51))
- **oncall**: Add the reference implementation, an incident-response app on the public API ([#40](https://github.com/alexnodeland/reflexr/pull/40))
- **litellm**: Route agents through a LiteLLM proxy, with guardrail blocks as permanent failures ([#42](https://github.com/alexnodeland/reflexr/pull/42))
- **workspace**: Typed run failures, recorded with a reason, permanent ones not retried ([#41](https://github.com/alexnodeland/reflexr/pull/41))
- **otel,langfuse**: Configure OpenTelemetry in one call, and file traces and feedback in Langfuse ([#39](https://github.com/alexnodeland/reflexr/pull/39))
- **evals**: Replay examples against a candidate action, as evalr experiment tasks ([#37](https://github.com/alexnodeland/reflexr/pull/37))
- **sql**: Store workspaces in PostgreSQL and SQLite, completing phase 4 ([#36](https://github.com/alexnodeland/reflexr/pull/36))
- **evals**: Measure workflows end to end from the log ([#35](https://github.com/alexnodeland/reflexr/pull/35))
- **evals**: Feedback as evalr examples, and evaluators as rules ([#34](https://github.com/alexnodeland/reflexr/pull/34))
- **workspace**: Enter a run context around each attempt, with its chain in baggage ([#33](https://github.com/alexnodeland/reflexr/pull/33))
- **scores**: Feedback as scores, a log mirror, and their ports ([#32](https://github.com/alexnodeland/reflexr/pull/32))
- **mcp**: Serve the workspace protocol to external agents over MCP ([#31](https://github.com/alexnodeland/reflexr/pull/31))
- **agent**: Run pydantic-graph graphs as actions, checkpointed at safe step boundaries ([#30](https://github.com/alexnodeland/reflexr/pull/30))
- **fastapi**: Serve the workspace protocol over REST and WebSocket ([#29](https://github.com/alexnodeland/reflexr/pull/29))
- **core**: Define the reflexr.v1 protocol, with one command handler ([#28](https://github.com/alexnodeland/reflexr/pull/28))
- **agent**: Run pydantic-ai agents as actions, with the EventContext capability ([#27](https://github.com/alexnodeland/reflexr/pull/27))
- **workspace**: Publish ticks on schedules, completing phase 2 ([#26](https://github.com/alexnodeland/reflexr/pull/26))
- **workspace**: Execute runs under leases, with the action port and Reaction ([#25](https://github.com/alexnodeland/reflexr/pull/25))
- **workspace**: Evaluate rules in the reactor, with replays and bounded depth ([#24](https://github.com/alexnodeland/reflexr/pull/24))
- **workspace**: Add the storage port, in-memory storage, workspace handles and telemetry ([#23](https://github.com/alexnodeland/reflexr/pull/23))
- **core**: Add typed feedback, evaluator actors and trace context ([#22](https://github.com/alexnodeland/reflexr/pull/22))
- **core**: Add the pure rules, conformance suite and run lifecycle ([#17](https://github.com/alexnodeland/reflexr/pull/17))

### Bug fixes

- **fastapi**: Show a tenant only its own schedule targets ([#59](https://github.com/alexnodeland/reflexr/pull/59))
- What the documentation found: disabled rules, filter checks, feedback sources, MCP authorization, chains ([#56](https://github.com/alexnodeland/reflexr/pull/56)) (**breaking**)
- **workspace**: What the oncall example found: emitted ids per checkpoint, run-only events ([#44](https://github.com/alexnodeland/reflexr/pull/44))

### Documentation

- Render lists on the site as GitHub does, and check them in the build ([#55](https://github.com/alexnodeland/reflexr/pull/55))
- Mark RFC-0002 Implemented, and list the docs targets in CONTRIBUTING ([#54](https://github.com/alexnodeland/reflexr/pull/54))
- The documentation site and the brand family ([#49](https://github.com/alexnodeland/reflexr/pull/49))
- **rfc**: RFC-0002 observability, feedback, evaluation and the LLM gateway ([#18](https://github.com/alexnodeland/reflexr/pull/18))
- **adr**: Give reflexr tenants and workspaces, like artifactr ([#16](https://github.com/alexnodeland/reflexr/pull/16))
- Add the reflexr design: architecture, protocol, RFC-0001 and ADRs ([#14](https://github.com/alexnodeland/reflexr/pull/14))

### Testing

- Register test event types once, with no inline suppressions anywhere ([#38](https://github.com/alexnodeland/reflexr/pull/38))

### Miscellaneous

- A contributor Compose stack and a Compose-based dev container ([#50](https://github.com/alexnodeland/reflexr/pull/50))
- Pin evalr at its v0.1, and check the feedback source against evalr's contract ([#43](https://github.com/alexnodeland/reflexr/pull/43))
- Lay the foundation for the reflexr library ([#15](https://github.com/alexnodeland/reflexr/pull/15))


