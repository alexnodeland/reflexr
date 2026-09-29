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
| [0015](0015-reference-implementation-oncall.md) | Reference implementation: incident response | Accepted |
| [0016](0016-tenants-and-workspaces-like-artifactr.md) | Tenants and workspaces, like artifactr | Accepted |
| [0017](0017-the-cores-evaluation-contract.md) | The core's evaluation contract | Accepted |

To add a record, copy [`template.md`](template.md) to the next number and add a row above.
