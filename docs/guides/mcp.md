# External agents over MCP

Agents outside your application, such as a coding assistant, an on-call copilot or another service, can feed and operate workspaces over the [Model Context Protocol](https://modelcontextprotocol.io). `reflexr.mcp` (the `mcp` extra) serves one: publishing, reading and administration are tools, runs are resources, and a run's progress arrives as resource-updated notifications. Every tool goes through the same command handler as REST and the WebSocket, so an external agent is an actor like any other, with the same rules and the same attribution ([ADR-0011](../adr/0011-surfaces.md)).

## Mounting the server

`ReflexrMcp` wraps the MCP SDK's server. Mount its Streamable HTTP app in your application, and run its lifespan inside the application's:

```python
import contextlib
from collections.abc import AsyncGenerator

from fastapi import FastAPI
from mcp.server.mcpserver import Context

from reflexr.core import ExternalAgentActor, TenantId
from reflexr.mcp import ReflexrMcp


async def resolve_client(ctx: Context) -> tuple[TenantId, ExternalAgentActor]:
    key = await api_keys.verify((ctx.headers or {}).get("authorization"))  # your authentication
    return key.tenant_id, ExternalAgentActor(client_id=key.id, name=key.name)


mcp = ReflexrMcp(workspaces, resolve=resolve_client)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    async with mcp.lifespan():
        yield


app = FastAPI(lifespan=lifespan)
app.mount("/mcp", mcp.http_app(streamable_http_path="/"))
```

Clients connect to `https://your-host/mcp/` with any MCP client that speaks Streamable HTTP. The server can share an application, and a `Workspaces`, with the [REST and WebSocket router](serving.md); if the application also runs the reactor in its lifespan, enter both there. `mcp.server` is the underlying `MCPServer`, to serve it another way, such as over stdio.

| `ReflexrMcp` argument | Meaning |
|---|---|
| `workspaces` | Opens tenant-scoped workspaces, and holds the rules the tools list and replay |
| `resolve` | Authenticates each request and returns the client's tenant and `ExternalAgentActor` |
| `authorize` | Whether a client may use a workspace of its tenant: the router's hook, asked on every tool call, resource read and resource subscription that names a workspace. `None`, the default, allows every one |
| `name` | The server's name, `"reflexr"` by default |
| `bus` | Where resource-updated notifications go: in process by default. Pass the MCP SDK's `SubscriptionBus` over a shared broker to fan them out across replicas. |

`resolve(ctx)` is the boundary. `ctx.headers` holds the HTTP request's headers (it is `None` for an in-process client), and they are the client's own claims until you have checked a credential. The tenant comes from `resolve`, never from a tool's arguments, so a client cannot reach another tenant by naming it. To refuse a client, raise; raising the SDK's `ToolError` (from `mcp.server.mcpserver.exceptions`) gives the client your message, such as `Error executing tool read_events: unknown API key`.

To decide which workspaces of its tenant a client may use, pass `authorize`, the same `reflexr.workspace.Authorize` hook the [router](serving.md#authentication) takes:

```python
from reflexr import Actor
from reflexr.core import WorkspaceId


async def authorize(tenant_id: TenantId, workspace_id: WorkspaceId, actor: Actor) -> bool:
    return await api_keys.may_use(actor, tenant_id, workspace_id)  # your access control


mcp = ReflexrMcp(workspaces, resolve=resolve_client, authorize=authorize)
```

It is asked on every tool call and resource read that names a workspace, before anything is read or written, and when a client subscribes to a run's changes. A refusal is a tool error carrying the `forbidden` rejection's message, `Error executing tool read_events: this workspace is not yours to use`. A refused resource read fails with the same message, and so does a `subscriptions/listen` request that names a run of a refused workspace. `list_rules` names no workspace, so it is not asked. Without `authorize`, a client may use every workspace of the tenant `resolve` returns.

## Tools

The server's instructions tell the client what it is talking to: event logs, one per workspace, watched by rules that run agents and workflows. Its tools cover everything the [stream protocol](../protocol.md#mcp) does:

| Tool | Does |
|---|---|
| `publish_event(workspace_id, event, id=None, correlation_id=None)` | Publishes an event, an object with its `type` and fields. An `id` already in the log adds nothing; `correlation_id` joins the causal chain that event started, and naming a later event of a chain is refused. |
| `read_events(workspace_id, after_seq=0, types=None, limit=50)` | Reads envelopes, oldest first, as JSON lines |
| `list_rules()` | The rules every workspace evaluates, as JSON. They are the application's, the same for every tenant, so every client sees them all ([Multi-tenancy and security](security.md#tenants-and-workspaces)) |
| `rule_status(workspace_id)` | Every registered rule, as `GET /v1/workspaces/{workspace_id}/rules` lists them: whether it is enabled, its cursor, how far it is behind the log, its generation and its number of dead letters. A rule that has not evaluated the workspace yet is at cursor 0. |
| `schedule_status(workspace_id)` | Each [schedule](schedules.md) that targets the workspace, as `GET /v1/workspaces/{workspace_id}/schedules` lists them: when it last ticked and when it ticks next, or that it has not started there yet |
| `replay_rule(workspace_id, rule, from_seq=0, mode="rebuild")` | Evaluates a rule again from `from_seq`: rebuilds its state quietly, or refires ([The reactor](reactor.md#replaying-a-rule)) |
| `list_runs(workspace_id, rule=None, scope_key=None, status=None, limit=20)` | Runs, newest first, as JSON lines, filtered as `GET /v1/workspaces/{workspace_id}/runs` filters them. `scope_key` holds the scope's values, such as `["auth"]`, or is a run's `scope_key` as the run has it. |
| `get_run(workspace_id, run_id)` | One run: its status, attempts, error, output and checkpoint |
| `retry_run(workspace_id, run_id)` | Makes a run runnable now, with a fresh retry budget if it had finished |
| `skip_run(workspace_id, run_id, reason=None)` | Gives up on a waiting or dead-lettered run, unblocking its scope |
| `cancel_run(workspace_id, run_id, reason=None)` | Cancels a run that has not finished, stopping it if it is running |
| `list_dead_letters(workspace_id, rule=None)` | The envelopes rules could not evaluate, as JSON lines |
| `give_feedback(workspace_id, feedback_type, target, value=None)` | Typed feedback on a run, a firing or a chain ([Feedback and evaluation](evaluation.md#giving-feedback)) |

Commands answer with their outcome as JSON, such as `{"type":"published","seq":1,"id":"alert-7","duplicate":false}`. A rejection is a tool error carrying its message: retrying a run that succeeded answers `Error executing tool retry_run: cannot retry run fir_ce4679d4c7453919, which is succeeded`, and publishing one of reflexr's own events answers `run_succeeded events are recorded by reflexr, not published`.

Events a client publishes are attributed to its `ExternalAgentActor`, so people, the reactor and other clients see who did what:

```json
{"kind": "external_agent", "client_id": "claude-code", "name": "Claude Code"}
```

The same rules apply as everywhere else: the `Workspaces` event allowlist decides what a client may publish, reflexr's own events are refused, and a publish is idempotent by its id.

## Run resources

Each run is a resource at `reflexr://{tenant_id}/{workspace_id}/runs/{run_id}`, whose content is the run's current JSON. `run_uri(tenant_id, workspace_id, run_id)` builds the URI. A client may read, and subscribe to, the runs of its own tenant only, in the workspaces `authorize` allows: reading another tenant's run fails with `runs of tenant acme are not available`, and so does a `subscriptions/listen` request that names one, with `INVALID_PARAMS`. The MCP SDK serves `subscriptions/listen` itself, so `ReflexrMcp` checks each run URI a listen request names, when the stream opens, as a read of it is checked; a request that names no run is not checked, and `resolve` is not called for it.

Once a client has used a workspace through a tool, the server follows that workspace's log, and every fact about a run from then on (`run_started`, `run_progressed`, `run_retrying`, `run_succeeded` and the rest) is published as a resource-updated notification for the run's URI. A client that listens for a run's URI learns when to read it again, which suits an agent that published an alert and wants to watch the triage it caused.

## Trying it

The MCP SDK's client connects in process, which is also how the library's own tests exercise the server. With a `resolve` that trusts a fixed identity (in process there are no headers), publish three severe errors, let the reactor fire, and watch the run finish:

```python
import json

from mcp import Client

from reflexr.mcp import run_uri

async with Client(mcp.server) as client:
    for n in range(3):
        await client.call_tool(
            "publish_event",
            {
                "workspace_id": "prod",
                "event": {"type": "service.error", "service": "auth", "severity": 8},
                "id": f"alert-{n}",
            },
        )
    await reactor.evaluate()  # error-spike fires: a pending run

    runs = await client.call_tool("list_runs", {"workspace_id": "prod"})
    run_id = json.loads(runs.content[0].text.splitlines()[0])["id"]

    async with client.listen(resource_subscriptions=[run_uri("acme", "prod", run_id)]) as updates:
        await reactor.execute()  # triage runs
        print(await anext(aiter(updates)))
```

```text
ResourceUpdated(uri='reflexr://acme/prod/runs/fir_ce4679d4c7453919')
```

In an application the reactor runs on its own, in the lifespan or in workers, and the client only publishes and listens. To connect over HTTP with a credential, give the SDK's Streamable HTTP transport a client with your headers:

```python
import httpx2
from mcp.client.streamable_http import streamable_http_client

http = httpx2.AsyncClient(headers={"authorization": "Bearer k_claude"})
async with Client(streamable_http_client("http://127.0.0.1:8000/mcp/", http_client=http)) as client:
    print(await client.call_tool("read_events", {"workspace_id": "prod"}))
```

The [reference implementation](../reference-implementation.md) serves MCP beside its REST and WebSocket endpoints.
