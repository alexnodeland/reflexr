# reflexr.workspace

::: reflexr.workspace
    options:
      members: false
      show_root_heading: false
      show_root_toc_entry: false

## Workspaces

Tenant-scoped handles. See [Workspaces and the log](../guides/workspaces.md).

::: reflexr.workspace.Workspaces

::: reflexr.workspace.Workspace

::: reflexr.workspace.Published

::: reflexr.workspace.RuleVersion

::: reflexr.workspace.WorkspaceRule

::: reflexr.workspace.RuleStatus

::: reflexr.workspace.ScheduleStatus

::: reflexr.workspace.Authorize

## The reactor

Evaluating rules and executing runs. See [The reactor](../guides/reactor.md).

::: reflexr.workspace.Reactor

::: reflexr.workspace.Settled

::: reflexr.workspace.EVALUATION_LEASE

::: reflexr.workspace.REACTOR

## Actions

The action port, and what every action receives. See [Actions](../guides/actions.md).

::: reflexr.workspace.Action

::: reflexr.workspace.Reaction

::: reflexr.workspace.with_params

::: reflexr.workspace.RunFailure

::: reflexr.workspace.RunContext

## Commands

Where `Workspaces.execute`, the one handler every surface hands commands to, remembers their results. See [Deduplication](../protocol.md#deduplication).

::: reflexr.workspace.CommandResults

::: reflexr.workspace.InMemoryCommandResults

::: reflexr.workspace.CommandKey

## Schedules

Ticks on a timetable. See [Schedules](../guides/schedules.md).

::: reflexr.workspace.Schedule

::: reflexr.workspace.SCHEDULER

::: reflexr.workspace.tick_id

## Storage

The storage protocol, and the in-memory implementation. See [Storage](../guides/storage.md).

::: reflexr.workspace.Storage

::: reflexr.workspace.Transaction

::: reflexr.workspace.Entry

::: reflexr.workspace.WorkspaceRef

::: reflexr.workspace.run_lease

::: reflexr.workspace.RUN_LEASE_PREFIX

::: reflexr.workspace.RunPolicy

::: reflexr.workspace.InMemoryStorage

::: reflexr.workspace.Clock

::: reflexr.workspace.utc_now
