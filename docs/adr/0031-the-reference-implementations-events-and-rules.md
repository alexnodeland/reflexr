# ADR-0031: The reference implementation's events and rules

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

[ADR-0015](0015-reference-implementation-oncall.md) chose incident response for the reference implementation and sketched its events (`service.error`, `deploy.finished`, `heartbeat`, `incident.opened`, `page.sent`, `rollback.proposed`) and rules (`error-spike`, a `deploy-regression` sequence, `heartbeat-lost`). Building `examples/oncall` on the public API settled the details, and three of them differ from the sketch.

The event type registry is also process-wide: a type name belongs to one class. reflexr's own test fixtures register `deploy.finished` and `heartbeat`, and the example's tests run in the same pytest process, inside the same coverage gate, so the example cannot register those names too.

## Decision

- **Events.** Producers publish `alert.fired` (service, severity 1 to 10, message), `deploy.completed` (service, version) and `service.heartbeat` (service). The workflows emit `incident.opened` (service, severity, summary) and `incident.resolved` (service, resolution). There is no `page.sent` or `rollback.proposed`: pages and rollbacks are effects on the pager and deployer the actions receive in their deps, and the runbook records them in its checkpointed state and the resolution. `Workspaces` accepts these five types and no others; the workflows' types are among them because runs publish through the same allowlist as clients.
- **Rules.**
  - `triage`: three or more alerts of severity 7 or more for one service within five minutes runs a **triage agent** (pydantic-ai, with `EventContext(emit=[IncidentOpened])` and a structured verdict), which opens an incident if the alerts are one.
  - `runbook`: every `incident.opened` runs a **runbook graph** (pydantic-graph) for its service: diagnose from the log, decide, roll back a recent deploy or page a person, verify, and resolve.
  - `silence`: no heartbeat from a service for two minutes runs a **paging function**, which pages once per run and raises an alert about it. A `heartbeat-check` schedule ticks every 30 seconds so the silence is noticed in a quiet workspace.
- **The runbook runs on the incident, not on a sequence.** The agent decides whether alerts are an incident; the runbook then works any incident, whoever opened it: the agent, a person over REST, or an MCP client. The question the `deploy-regression` sequence asked, whether a deploy came just before, is the runbook's first step, which reads the log through its `Reaction`.
- **Storage** is in memory by default, and SQL (`reflexr.sql`) when `ONCALL_DATABASE_URL` is set. Authentication is a demo: the `x-user` header, one tenant.

## Options considered

### Chain the workflows through `incident.opened` (chosen)

**Pros:** each workflow is its own run, retried and replayed on its own, and all of them sit in the first alert's causal chain, so one session shows the incident from alert to resolution. Incidents opened by hand get the same runbook. The agent's job stays a judgement, not a pipeline step.
**Cons:** the example does not use `sequence`.

### A `deploy-regression` sequence rule, as ADR-0015 sketched

**Pros:** exercises `sequence`.
**Cons:** a sequence and a count over the same alerts start two workflows for one incident, and the runbook would never run for an incident a person opens.

### Keep ADR-0015's names, and rename the library's test fixtures

**Pros:** the example's names would match the architecture document's examples.
**Cons:** about ninety occurrences across seventeen test files, including the conformance fixtures, while other changes to the suite were in flight.

## Consequences

- Easier: the example exercises counts within windows, scopes, absence, schedules, an agent with emits, a checkpointed graph with a decision, a function, REST, the WebSocket stream, MCP and SQL storage, end to end with a scripted model.
- Harder: `sequence`, `distinct` and `at_most` are covered by the library's tests, not by the example.
- To revisit: if event types gain namespaces, or registries scoped to a `Workspaces`, the example can use the architecture document's names.

## Action items

1. [x] Build `examples/oncall` with these events and rules (RFC-0001 phase 6).
