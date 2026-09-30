# ADR-0047: Commands carried out once per id

**Status:** Accepted
**Date:** 2026-09-30
**Deciders:** Alex Nodeland

## Context

Clients retry commands they never heard back about, and agents retry tool calls. [ADR-0044](0044-surfaces.md) sent every command through one handler, `reflexr.workspace.execute`, which raised rejections, and let only REST and the WebSocket deduplicate by `command_id`: the router remembered their results itself, and MCP tools took no `command_id`. An MCP client retrying `give_feedback`, `retry_run` or a refiring `replay_rule` did it twice.

artifactr made the same choice for its surfaces in its [ADR-0048](https://github.com/alexnodeland/artifactr/blob/main/docs/adr/0048-surfaces-over-the-runner.md): one entry point, `Runner.execute(workspace, command, *, command_id) -> CommandResult`, which remembers results in a `CommandResults` port for every surface. This record makes reflexr's the same, superseding three parts of ADR-0044: its first decision's one handler, `reflexr.workspace.execute`; its decision that command ids deduplicate per participant over REST and the WebSocket; and its deliberate difference that command ids are REST's and the WebSocket's.

## Decision

- **One entry point: `Workspaces.execute(workspace, command, *, command_id) -> CommandResult`.** It carries out a command through the handle, as its actor, and never raises: a rejection is the result. REST and the WebSocket pass the frame's `command_id`, and MCP the tool's, or a new one. It replaces `reflexr.workspace.execute(workspace, command)` and the router's own memory of results.
- **A command is carried out once per id.** `Workspaces` remembers each result in a `CommandResults` port, `Workspaces(results=)`, keyed by a `CommandKey`: the tenant, the workspace, the actor's `participant` and the `command_id`. A repeated id returns the first result, an outcome or a rejection, whatever command it comes with, and carries nothing out. `InMemoryCommandResults`, the default, keeps the 10,000 most recent in the process, and forgets the oldest first. The names, the key and the semantics are artifactr's.
- **`Workspaces` holds the memory,** as artifactr's `Runner` does: it is the one object per process that every surface shares. A `Workspace` handle cannot reach it, since its keys span tenants and nothing on a handle may reach another tenant.
- **Every MCP tool that changes something takes an optional `command_id`:**
  - `publish_event` and `give_feedback`
  - the run commands, `retry_run`, `skip_run` and `cancel_run`
  - `replay_rule`
  - the stored-rule tools, `install_rule`, `update_rule` and `archive_rule`
- **An MCP call without a `command_id` gets a new one,** so it is carried out every time. The MCP request id cannot serve, since a retry is a new request. An empty `command_id` is refused, as REST refuses one, since a model that fills optional strings with `""` would otherwise get its first change's result back for every change. The server's instructions ask a client to give each change an id of its own, and the same one when it retries.
- **Beyond the memory, the log deduplicates what carries an id.** A publish with an event `id` appends once, whichever process it reaches, and a stored-rule change that would leave the rule as it is answers `duplicate`.

ADR-0044's other deliberate differences stand, batch publishing among them.

## Options considered

| Option | Assessment |
|---|---|
| **One `Workspaces.execute` that returns a `command_result` (chosen)** | Every surface calls the same method the same way, deduplication cannot be skipped, and it is artifactr's shape |
| `execute`, which raises, beside a router that remembers results (ADR-0044) | Two entry points, and MCP, which calls the first, deduplicates nothing |
| A free `execute(workspace, command, *, command_id)` that reaches the memory through the handle | A handle that could read another tenant's results |

## Consequences

- Easier: a client retries any change, on any surface, with the same id, and gets the first result.
- Easier: a system that uses artifactr and reflexr retries commands the same way on both.
- Harder: with the default port, deduplication is per process, and a retry that arrives while the first attempt is still being carried out runs again. A `CommandResults` over shared storage, with a claim on a command while it runs, would close both gaps.
- Breaking: `reflexr.workspace.execute` and `reflexr_router(remembered_commands=)` are gone; pass `Workspaces(results=InMemoryCommandResults(capacity=...))` to change how many results are remembered. `Stream` takes the `workspaces` whose `execute` carries out its commands, in place of an `execute` callback and a `telemetry`.

## Action items

1. [x] `CommandResults`, `InMemoryCommandResults` and `Workspaces.execute`, used by REST, the WebSocket and MCP.
2. [x] An optional `command_id` on every MCP tool that changes something.
3. [ ] Share command deduplication across processes.
