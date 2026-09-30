# reflexr.fastapi

The `fastapi` extra. See [Serving over REST and WebSocket](../guides/serving.md).

::: reflexr.fastapi
    options:
      members: false
      show_root_heading: false
      show_root_toc_entry: false

## The router

::: reflexr.fastapi.reflexr_router

::: reflexr.fastapi.ResolveActor

::: reflexr.fastapi.Unauthorized

::: reflexr.fastapi.STATUS_CODES

## Bodies and responses

The router's `authorize` hook, [`Authorize`](workspace.md#reflexr.workspace.Authorize), the statuses it returns, [`RuleStatus`](workspace.md#reflexr.workspace.RuleStatus) and [`ScheduleStatus`](workspace.md#reflexr.workspace.ScheduleStatus), and the rule it reads, [`WorkspaceRule`](workspace.md#reflexr.workspace.WorkspaceRule), are the workspace layer's, shared by every surface.

::: reflexr.fastapi.PublishItem

::: reflexr.fastapi.PublishBatch

## The stream

One WebSocket connection, which the router serves at `/workspaces/{workspace_id}/stream`. It hands each command frame to [`Workspaces.execute`](workspace.md#reflexr.workspace.Workspaces.execute), as `POST .../commands` does, so a command is carried out once per `command_id` whichever of the two it arrives on.

::: reflexr.fastapi.Stream
