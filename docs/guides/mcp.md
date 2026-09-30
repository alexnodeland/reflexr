# External agents over MCP

Agents outside your application, such as a coding assistant, an on-call copilot or another service, can feed and operate workspaces over the [Model Context Protocol](https://modelcontextprotocol.io). `reflexr.mcp` (the `mcp` extra) serves one: publishing, reading and administration are tools, runs are resources, and a run's progress arrives as resource-updated notifications. Every tool goes through the same command handler as REST and the WebSocket, so an external agent is an actor like any other, with the same rules and the same attribution, and a change it retries is made once ([ADR-0044](../adr/0044-surfaces.md), [ADR-0047](../adr/0047-commands-carried-out-once-per-id.md)).

## Mounting the server

`ReflexrMcp` wraps the MCP SDK's server. Mount its Streamable HTTP app in your application, and run its lifespan inside the application's:

```python
import contextlib
from collections.abc import AsyncGenerator

from fastapi import FastAPI

from reflexr.core import ExternalAgentActor, TenantId
from reflexr.mcp import McpContext, ReflexrMcp


async def resolve_client(ctx: McpContext) -> tuple[TenantId, ExternalAgentActor]:
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

Clients connect to `https://your-host/mcp/` with any MCP client that speaks Streamable HTTP. The server can share an application, and a `Workspaces`, with the [REST and WebSocket router](serving.md); if the application also runs the reactor in its lifespan, enter both there. `mcp.server` is the underlying `MCPServer`, to serve it another way, such as over stdio. The SDK's server configures logging for the whole process as it is built, and `ReflexrMcp` undoes that, so logging stays the application's ([Logging and startup output](serving.md#logging-and-startup-output)).

| `ReflexrMcp` argument | Meaning |
|---|---|
| `workspaces` | Opens tenant-scoped workspaces, holds the rules the tools list and replay, and carries out every tool's command, once per `command_id` ([Retries](#retries)) |
| `resolve` | Authenticates each request and returns the client's tenant and `ExternalAgentActor` |
| `authorize` | Whether a client may use a workspace of its tenant: the router's hook, asked on every tool call, resource read and resource subscription that names a workspace. `None`, the default, allows every one |
| `name` | The server's name, `"reflexr"` by default |
| `bus` | Where resource-updated notifications go: in process by default. Pass the MCP SDK's `SubscriptionBus` over a shared broker to fan them out across replicas. |

`resolve(ctx)` is the boundary. `ctx.headers` holds the HTTP request's headers (it is `None` for an in-process client), and they are the client's own claims until you have checked a credential. The tenant comes from `resolve`, never from a tool's arguments, so a client cannot reach another tenant by naming it. To refuse a client, raise; raising the SDK's `ToolError` (from `mcp.server.mcpserver.exceptions`) gives the client your message, such as `Error executing tool read_events: unknown API key`.

`ctx` is an `McpContext`, the MCP SDK's `Context` with its request typed. Over HTTP, `ctx.request_context.request` is the Starlette `Request`, so an authenticator written for the router's `resolve_actor` can take it without a cast. Check it for `None` first: the in-process client that tests use has no HTTP request.

```python
from mcp.server.mcpserver.exceptions import ToolError


async def resolve_client(ctx: McpContext) -> tuple[TenantId, ExternalAgentActor]:
    request = ctx.request_context.request  # a starlette Request, or None
    if request is None:
        raise ToolError("connect over HTTP")
    user = await authenticate(request)  # the function resolve_actor uses
    return user.tenant_id, ExternalAgentActor(client_id=user.id, name=f"{user.name} (MCP)")
```

To decide which workspaces of its tenant a client may use, pass `authorize`, the same `reflexr.workspace.Authorize` hook the [router](serving.md#authentication) takes:

```python
from reflexr import Actor
from reflexr.core import WorkspaceId


async def authorize(tenant_id: TenantId, workspace_id: WorkspaceId, actor: Actor) -> bool:
    return await api_keys.may_use(actor, tenant_id, workspace_id)  # your access control


mcp = ReflexrMcp(workspaces, resolve=resolve_client, authorize=authorize)
```

It is asked on every tool call and resource read that names a workspace, before anything is read or written, and when a client subscribes to a run's changes. A refusal is a tool error carrying the `forbidden` rejection's message, `Error executing tool read_events: this workspace is not yours to use`. A refused resource read fails with the same message, and so does a `subscriptions/listen` request that names a run of a refused workspace, with `INVALID_PARAMS`; the error's data carries the URI and the `forbidden` rejection, as REST's error body does. `list_rules` names no workspace, so it is not asked. Without `authorize`, a client may use every workspace of the tenant `resolve` returns.

## Tools

The server's instructions tell the client what it is talking to: event logs, one per workspace, watched by rules that run agents and workflows. They also ask it to give each change a `command_id`. Its tools cover everything the [stream protocol](../protocol.md#mcp) does:

| Tool | Does |
|---|---|
| `publish_event(workspace_id, event, id=None, correlation_id=None)` | Publishes an event, an object with its `type` and fields. An `id` already in the log adds nothing; `correlation_id` joins the causal chain that event started, and naming a later event of a chain is refused. |
| `read_events(workspace_id, after_seq=0, before_seq=None, types=None, limit=None, last=None)` | Reads envelopes, oldest first, as JSON lines, as `GET /v1/workspaces/{workspace_id}/events` reads them: the window `after_seq < seq < before_seq`, of `types`, the first `limit` or the last `last`. Without either, the first 50. To read back from the latest events, give `last`, then `before_seq` the oldest `seq` returned ([Reading the log](workspaces.md#windows-types-and-the-tail)). |
| `list_rules()` | The rules registered in code, which every workspace evaluates, as JSON. They are the application's, the same for every tenant, so every client sees them all ([Multi-tenancy and security](security.md#tenants-and-workspaces)). A workspace's stored rules are not among them. |
| `rule_status(workspace_id)` | Each of the workspace's rules, code rules first, as `GET /v1/workspaces/{workspace_id}/rules` lists them: whether it is a code or a stored rule, a stored rule's version, whether it is enabled, its cursor, how far it is behind the log, its generation and its number of dead letters. A rule that has not evaluated the workspace yet is at cursor 0. |
| `get_rule(workspace_id, rule)` | One of the workspace's rules, as JSON, as `GET /v1/workspaces/{workspace_id}/rules/{rule}` returns it: its definition, its origin, and a stored rule's version and provenance |
| `schedule_status(workspace_id)` | Each [schedule](schedules.md) that targets the workspace, as `GET /v1/workspaces/{workspace_id}/schedules` lists them: when it last ticked and when it ticks next, or that it has not started there yet |
| `replay_rule(workspace_id, rule, from_seq=0, mode="rebuild")` | Evaluates a rule again from `from_seq`: rebuilds its state quietly, or refires ([The reactor](reactor.md#replaying-a-rule)) |
| `install_rule(workspace_id, rule, provenance=None)` | Installs a [stored rule](#stored-rules): version 1, or the next version of an archived rule |
| `update_rule(workspace_id, rule, expected_version=None, provenance=None)` | Replaces an active stored rule, named by `rule`'s name, with its next version |
| `archive_rule(workspace_id, rule, expected_version=None, reason=None)` | Archives a stored rule, named by `rule`, and cancels its unfinished runs |
| `list_runs(workspace_id, rule=None, scope_key=None, status=None, limit=20)` | Runs, newest first, as JSON lines, filtered as `GET /v1/workspaces/{workspace_id}/runs` filters them. `scope_key` holds the scope's values, such as `["auth"]`, or is a run's `scope_key` as the run has it. |
| `get_run(workspace_id, run_id)` | One run: its status, attempts, error, output and checkpoint |
| `retry_run(workspace_id, run_id)` | Makes a run runnable now, with a fresh retry budget if it had finished |
| `skip_run(workspace_id, run_id, reason=None)` | Gives up on a waiting or dead-lettered run, unblocking its scope |
| `cancel_run(workspace_id, run_id, reason=None)` | Cancels a run that has not finished, stopping it if it is running |
| `list_dead_letters(workspace_id, rule=None)` | The envelopes rules could not evaluate, as JSON lines |
| `give_feedback(workspace_id, feedback_type, target, value=None)` | Typed feedback on a run, a firing or a chain ([Feedback and evaluation](evaluation.md#giving-feedback)) |

Each tool that changes something, `publish_event`, `replay_rule`, `install_rule`, `update_rule`, `archive_rule`, `retry_run`, `skip_run`, `cancel_run` and `give_feedback`, also takes an optional `command_id` ([Retries](#retries)).

Commands answer with their outcome as JSON, such as `{"type":"published","seq":1,"id":"alert-7","duplicate":false}`. A rejection is a tool error carrying its message: retrying a run that succeeded answers `Error executing tool retry_run: cannot retry run fir_ce4679d4c7453919, which is succeeded`, and publishing one of reflexr's own events answers `reflexr:run_succeeded events are recorded by reflexr, not published`. A `validation_failed` rejection that lists problems has them after its message, as JSON, as REST's body has them in `errors`: publishing `{"type": "ops:service.error", "severity": 8}`, without its `service`, answers `invalid ops:service.error event: [{"loc": ["service"], "msg": "Field required", "type": "missing"}]`.

Events a client publishes are attributed to its `ExternalAgentActor`, so people, the reactor and other clients see who did what:

```json
{"kind": "external_agent", "client_id": "claude-code", "name": "Claude Code"}
```

The same rules apply as everywhere else: the `Workspaces` event allowlist decides what a client may publish, reflexr's own events are refused, and a publish is idempotent by its id.

## Retries

Agents retry tool calls, and transports drop replies. Give each call that changes something a `command_id` of the client's choosing, and the same id when retrying it: the server carries the command out once, and answers the retry with the first result, including a rejection. It is the `command_id` of REST's command frames, and one memory serves every surface, keyed by the tenant, the workspace, the client and the id ([Deduplication](../protocol.md#deduplication)). A call without a `command_id` is carried out every time, and one with an empty `command_id` is refused, as REST refuses it.

```python
feedback = {
    "workspace_id": "prod",
    "feedback_type": "triage_quality",
    "target": {"kind": "run", "run_id": run_id},
    "value": {"correct": True, "severity": "high"},
}
first = await client.call_tool("give_feedback", {**feedback, "command_id": "c_7"})
again = await client.call_tool("give_feedback", {**feedback, "command_id": "c_7"})  # given once
```

A publish with an event `id` is idempotent without a `command_id`, and so is a change to a stored rule that has already been made: both are deduplicated by the log, whichever process a retry reaches.

## Stored rules

When the application turns [stored rules](rules.md#stored-rules) on, a client can install, update and archive them in a workspace, as relayr does for a rule a person accepted in chat. The three tools are served only then: without `stored_rules` they could only refuse, so a client's list of tools doesn't carry them. `rule` is the whole rule, as the [rules schema](../reference/schema.md) has it, and the tool's input schema carries it, so a client can draft one; `provenance` is JSON that reflexr keeps with the rule and its `reflexr:rule_installed` fact, and does not read. Each tool answers with the command's `rule_version` outcome:

```json
{"type": "rule_version", "rule": "chat:prod-deploy-failures", "version": 1, "seq": 812, "duplicate": false}
```

The configuration's `allow` hook is asked about each change after `authorize`, and each change is checked as over REST: a rule the fixed limits refuse answers every problem at once, `rule chat:prod-deploy-failures cannot be stored: ["when.throttle is required", "timeout is required"]`, and a rule someone else changed since the `expected_version` a client gives answers `rule chat:prod-deploy-failures is at version 2, not 1`. A change retried with its `command_id` answers with its first result ([Retries](#retries)); one retried under a new `command_id`, or none, after it succeeded, changes nothing and answers `"duplicate": true`, whatever its `expected_version`. Give `expected_version` so a retry after someone else's change is refused rather than applied.

A stored rule belongs to the workspace it is installed in. `rule_status` lists it after the code rules, with its version, and `get_rule` returns it; another workspace, or another tenant's workspace of the same id, has no rule of that name. `list_rules` lists the code rules only.

```text
- ops:error-spike: code, enabled, cursor 812, 0 behind, generation 0, 0 dead letters
- chat:prod-deploy-failures: stored, version 1, enabled, cursor 812, 0 behind, generation 0, 0 dead letters
```

## Run resources

Each run is a resource at `reflexr://{tenant_id}/{workspace_id}/runs/{run_id}`, whose content is the run's current JSON. `run_uri(tenant_id, workspace_id, run_id)` builds the URI. A client may read, and subscribe to, the runs of its own tenant only, in the workspaces `authorize` allows: reading another tenant's run fails with `runs of tenant acme are not available`, and so does a `subscriptions/listen` request that names one, with `INVALID_PARAMS`. The MCP SDK serves `subscriptions/listen` itself, so `ReflexrMcp` checks each run URI a listen request names, when the stream opens, as a read of it is checked; a request that names no run is not checked, and `resolve` is not called for it.

Once a client has used a workspace through a tool, the server follows that workspace's log, and every fact about a run from then on (`reflexr:run_started`, `reflexr:run_progressed`, `reflexr:run_retrying`, `reflexr:run_succeeded` and the rest) is published as a resource-updated notification for the run's URI. A client that listens for a run's URI learns when to read it again, which suits an agent that published an alert and wants to watch the triage it caused.

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
                "event": {"type": "ops:service.error", "service": "auth", "severity": 8},
                "id": f"alert-{n}",
            },
        )
    await reactor.evaluate()  # ops:error-spike fires: a pending run

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
