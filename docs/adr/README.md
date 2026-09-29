# Architecture decision records

Each record captures one decision: the context that forced it, the options considered, and the consequences we accepted. Records are immutable once accepted; a changed decision gets a new record that amends or supersedes the old one. Proposals that precede decisions live in [`../rfcs/`](../rfcs/README.md).

| ADR | Title | Status |
|---|---|---|
| [0001](0001-library-with-a-sans-io-core.md) | A library with a sans-IO core, replacing the template | Accepted |
| [0002](0002-the-name-reflexr.md) | The name reflexr | Accepted |
| [0003](0003-independent-sibling-of-artifactr.md) | An independent sibling of artifactr, with aligned conventions | Accepted |
| [0004](0004-tenant-scoped-streams.md) | Tenant-scoped streams with one log each | Superseded by 0016 |
| [0005](0005-per-rule-cursors.md) | Per-rule cursors: decide exactly, act at least once | Accepted |
| [0006](0006-rules-as-typed-serializable-data.md) | Rules as typed, serializable data | Accepted |
| [0007](0007-rule-state-as-pure-reducers.md) | Rule state as pure reducers, with the log as the clock | Accepted |
| [0008](0008-actions-agents-graphs-and-functions.md) | Actions: agents, graphs and functions over one Reaction | Accepted |
| [0009](0009-graph-checkpoints.md) | Graph checkpoints at step boundaries | Accepted |
| [0010](0010-loop-and-spend-safety.md) | Loop and spend safety | Accepted |
| [0011](0011-surfaces.md) | Surfaces: ingest, REST, WebSocket, schedules and MCP | Accepted |
| [0012](0012-trunk-based-development-with-rfcs-and-adrs.md) | Trunk-based development with RFCs, ADRs and evergreen docs | Accepted |
| [0013](0013-quality-gates.md) | Quality gates | Accepted |
| [0014](0014-mit-license.md) | MIT license | Accepted |
| [0015](0015-reference-implementation-oncall.md) | Reference implementation: incident response | Accepted, amended by 0031 |
| [0016](0016-tenants-and-workspaces-like-artifactr.md) | Tenants and workspaces, like artifactr | Accepted |
| [0017](0017-the-cores-evaluation-contract.md) | The core's evaluation contract | Accepted |
| [0018](0018-opentelemetry-observability-with-langfuse.md) | OpenTelemetry-native observability, with Langfuse primary | Accepted |
| [0019](0019-typed-feedback-as-events.md) | Typed feedback as events, mirrored to Langfuse | Accepted |
| [0020](0020-evalr-shared-eval-kit.md) | evalr, a shared eval kit | Accepted |
| [0021](0021-contributor-compose-and-dev-containers.md) | Contributor Compose and dev containers here, infrastructure in stackr | Accepted |
| [0022](0022-litellm-proxy-first.md) | LiteLLM, proxy first, for routing and guardrails | Accepted |
| [0023](0023-libraries-and-the-stackr-template.md) | Libraries, and stackr as the infrastructure template | Accepted |
| [0024](0024-causal-chains-and-operator-actions.md) | Which chain a firing joins, and operator actions in the log | Accepted |
| [0025](0025-ports-and-adapters.md) | Ports and adapters | Accepted |
| [0026](0026-the-reactors-evaluation.md) | The reactor's evaluation: rules on workspaces, the depth of reflexr's facts, and rebuilds | Accepted |
| [0027](0027-executing-runs.md) | Executing runs | Accepted |
| [0028](0028-schedules-and-cronsim.md) | Schedules, with cronsim for cron expressions | Accepted |
| [0029](0029-metric-detail-through-sdk-views.md) | Metric detail through SDK views, and the OpenTelemetry and Langfuse adapters | Accepted |
| [0030](0030-sql-storage.md) | SQL storage with one dialect-neutral implementation | Accepted |
| [0031](0031-the-reference-implementations-events-and-rules.md) | The reference implementation's events and rules | Accepted |
| [0032](0032-documentation-site.md) | The documentation site, and a brand shared by the family | Accepted |
| [0033](0033-publishing-the-documentation-site.md) | Publishing the documentation site from main | Accepted |
| [0036](0036-typed-run-failures.md) | Typed run failures | Accepted |
| [0037](0037-joining-stackrs-network.md) | Joining stackr's network when it runs | Accepted |
| [0038](0038-dashboards-generated-tested-and-released.md) | Dashboards generated, tested and released | Accepted |
| [0039](0039-namespaced-event-types.md) | Namespaced event types | Accepted |
| [0040](0040-telemetry-that-composes-across-libraries.md) | Telemetry that composes across libraries, untraced polling and mirror cursors | Accepted |

To add a record, copy [`template.md`](template.md) to the next number and add a row above.
