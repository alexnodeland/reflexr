# Serving over REST and WebSocket

`reflexr.fastapi` (the `fastapi` extra) serves workspaces over HTTP: ingest for producers and webhooks, REST reads and administration for dashboards and operators, and the stream protocol over WebSocket for anything that follows a log live. All of it is one FastAPI router, and every command goes to one handler, `reflexr.workspace.execute`, so a command behaves the same over REST, the WebSocket and [MCP](mcp.md) ([ADR-0044](../adr/0044-surfaces.md)). The wire format is specified in the [stream protocol](../protocol.md); this page shows how to serve it.

## Mounting the router

Build the router from your `Workspaces` and your authentication, and include it under a prefix of your choosing. The router does not evaluate rules or run actions; the reactor does, so run it beside the application, here in FastAPI's lifespan:

```python
import asyncio
import contextlib
from collections.abc import AsyncGenerator

from fastapi import FastAPI
from starlette.requests import HTTPConnection

from reflexr import Actor, UserActor
from reflexr.core import TenantId
from reflexr.fastapi import Unauthorized, reflexr_router
from reflexr.workspace import InMemoryStorage, Reactor, Workspaces

workspaces = Workspaces(
    InMemoryStorage(), events=[ServiceError, Deploy, Heartbeat], rules=[error_spike]
)
reactor = Reactor(workspaces, actions={"triage": triage})


async def resolve_actor(connection: HTTPConnection) -> tuple[TenantId, Actor]:
    session = await sessions.load(connection.cookies.get("session"))  # your authentication
    if session is None:
        raise Unauthorized("sign in first")
    return session.tenant_id, UserActor(id=session.user_id, name=session.name)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    stop = asyncio.Event()
    serving = asyncio.create_task(reactor.serve(stop=stop))  # evaluate and execute
    yield
    stop.set()  # at shutdown: let running actions end, then let go of every lease
    await serving


app = FastAPI(lifespan=lifespan)
app.include_router(reflexr_router(workspaces, resolve_actor=resolve_actor), prefix="/v1")
```

Setting `stop` stops the reactor gracefully. It starts nothing new, gives the actions it is running `grace` to end (5 seconds by default), cancels those still running and records their attempts as abandoned, to be retried, and releases its leases before `serve` returns ([The reactor](reactor.md#running-the-reactor)). Keep `grace` within the time your platform gives a stopping process, less the rest of your shutdown: Docker allows 10 seconds, and Kubernetes `terminationGracePeriodSeconds`, 30 by default. Stop the reactor before anything it uses closes, such as the database engine.

Keep one `Workspaces` per process and share it between the router, the reactor and anything else, such as the [MCP server](mcp.md). The reactor can equally run in separate worker processes over the same [SQL storage](storage.md#sql-storage), since leases share the work between them ([The reactor](reactor.md#several-processes)).

## Authentication

Authentication is the application's. `resolve_actor` receives the `HTTPConnection` of each REST request, and of each WebSocket connection before its `hello`, and returns the tenant and the actor. Raise `Unauthorized` to refuse: REST answers 401, and the WebSocket is closed with code 4401. The tenant and actor never come from the request body or the client's frames, so a client cannot act as someone else or reach another tenant ([Multi-tenancy and security](security.md)).

Producers are actors too. A monitoring system that publishes alerts is a `SourceActor`, and a person in a browser a `UserActor`, so one resolver can tell them apart by their credentials:

```python
from reflexr import SourceActor


async def resolve_actor(connection: HTTPConnection) -> tuple[TenantId, Actor]:
    token = connection.headers.get("authorization", "").removeprefix("Bearer ")
    if token and (key := await api_keys.verify(token)) is not None:  # a monitoring system
        return key.tenant_id, SourceActor(name=key.source)
    session = await sessions.load(connection.cookies.get("session"))  # a person
    if session is None:
        raise Unauthorized("sign in first")
    return session.tenant_id, UserActor(id=session.user_id, name=session.name)
```

Browsers cannot set headers on a WebSocket, so a cookie, or a short-lived token in the query string, is usual there.

To decide which workspaces of its tenant an actor may use, pass `authorize`. A refusal answers 403, with the `forbidden` rejection as the body's `detail`, as every rejection has, or closes the WebSocket with 4403:

```python
from reflexr.core import WorkspaceId


async def authorize(tenant_id: TenantId, workspace_id: WorkspaceId, actor: Actor) -> bool:
    return await memberships.allows(tenant_id, workspace_id, actor)  # your access control


router = reflexr_router(workspaces, resolve_actor=resolve_actor, authorize=authorize)
```

Without `authorize`, any authenticated actor may use every workspace of its own tenant. `ReflexrMcp` takes the same hook, so one function decides for every surface ([External agents over MCP](mcp.md#mounting-the-server)).

## Publishing over HTTP

`POST /v1/workspaces/{workspace_id}/events` publishes one event, with an optional id:

```bash
curl -X POST localhost:8000/v1/workspaces/prod/events \
  -H 'authorization: Bearer k_grafana' -H 'content-type: application/json' \
  -d '{"event": {"type": "ops:service.error", "service": "auth", "severity": 8,
                 "message": "token check failed"}, "id": "alert-7"}'
```

```json
[{"type": "published", "seq": 1, "id": "alert-7", "duplicate": false}]
```

The id makes publishing idempotent: sending `alert-7` again appends nothing and answers with the logged event's `seq` and `"duplicate": true`, so a producer can retry until it hears back. Without an id the server generates one. A batch is published atomically and in order, all of it or none of it, and `correlation_id` puts every event in an existing causal chain, named by the id of its first event ([Workspaces and the log](workspaces.md#causal-chains)):

```json
{
  "events": [
    {"event": {"type": "ops:service.error", "service": "auth", "severity": 9}, "id": "alert-8"},
    {"event": {"type": "ops:service.error", "service": "auth", "severity": 9}, "id": "alert-9"}
  ],
  "correlation_id": "alert-7"
}
```

The response lists one `published` outcome per event. A request that cannot be published changes nothing:

| Response | When |
|---|---|
| 404 `not_found` | The event's type is not one the `Workspaces` accepts, or the `correlation_id` names no event |
| 403 `forbidden` | The event is one of reflexr's own, such as `reflexr:rule_fired`, which only reflexr records |
| 422 `validation_failed` | The `correlation_id` names a later event of a chain rather than its first; the message names the chain that event belongs to |
| 422 | The body does not validate, such as an event missing a field of its type; the detail lists Pydantic's errors |

Senders that cannot speak this format, such as a vendor's webhook, get a route of their own that turns their payload into an event. Use the sender's own id for the event, so a redelivery appends nothing:

```python
from typing import Any

from fastapi import Request


@app.post("/hooks/grafana")
async def grafana_alert(payload: dict[str, Any], request: Request) -> dict[str, int]:
    tenant_id, source = await verify_webhook(request)  # your signature check
    workspace = await workspaces.open(tenant_id, "prod", actor=source)
    published = await workspace.publish(
        ServiceError(service=payload["labels"]["service"], severity=8, message=payload["title"]),
        id=payload["fingerprint"],  # the sender's id, so a redelivery appends nothing
    )
    return {"seq": published.envelope.seq}
```

## Commands

Everything else a client can do is a command ([stream protocol](../protocol.md#commands)): `publish`, `give_feedback`, `retry_run`, `skip_run`, `cancel_run` and `replay_rule`, and, for [stored rules](#stored-rules), `install_rule`, `update_rule` and `archive_rule`. `POST /v1/workspaces/{workspace_id}/commands` takes the same command frame a WebSocket client sends, and answers with its `command_result`:

```json
{"type": "command", "command_id": "c_2",
 "command": {"type": "publish", "id": "deploy-1.4.2",
             "event": {"type": "ops:deploy.finished", "service": "auth", "version": "1.4.2"}}}
```

```json
{"type": "command_result", "command_id": "c_2", "ok": true, "outcome": {"type": "published", "seq": 7, "id": "deploy-1.4.2", "duplicate": false}, "rejection": null}
```

A rejected command answers with its status code (`STATUS_CODES`), and the body is still a `command_result`, with `ok: false` and the rejection's details:

```json
{"type": "command_result", "command_id": "c_1", "ok": false, "outcome": null, "rejection": {"type": "not_found", "message": "run fir_nope does not exist", "entity": "run", "id": "fir_nope"}}
```

| Rejection | Status |
|---|---|
| `not_found` | 404 |
| `forbidden` | 403 |
| `invalid_state` | 409 |
| `validation_failed`, `depth_exceeded` | 422 |
| `unsupported_protocol` | 400 |

`command_id` is chosen by the client and doubles as an idempotency key. The server remembers recent results (10,000 by default, per process), keyed by the tenant, the workspace, the actor's `participant` (so a changed display name does not matter) and the `command_id`, and answers a repeated id with the first result instead of running the command again. Retry a command that timed out with the same id.

## REST endpoints

Every path is relative to the router's prefix, `/v1` above:

| Method and path | Returns |
|---|---|
| `POST /workspaces/{workspace_id}/events` | Publishes one event or a batch; a list of `published` outcomes |
| `POST /workspaces/{workspace_id}/commands` | Runs one command frame; its `command_result` |
| `GET /workspaces/{workspace_id}/events?after_seq=&before_seq=&type=&limit=&last=` | A page of the log, as envelopes, oldest first: the window `after_seq < seq < before_seq`, of the event types `type` names (it may repeat), the first `limit` or the last `last` of them. Both `limit` and `last` is 422. |
| `GET /workspaces/{workspace_id}/runs?rule=&scope_key=&status=&limit=` | Runs, newest first |
| `GET /workspaces/{workspace_id}/runs/{run_id}` | One run: its status, attempts, error, output, checkpoint and each attempt's trace id |
| `GET /workspaces/{workspace_id}/dead-letters?rule=` | The envelopes rules could not evaluate, oldest first |
| `GET /workspaces/{workspace_id}/rules` | Each of the workspace's rules, code rules first: its `origin`, `code` or `stored`, and a stored rule's `version`; whether it is `enabled`; and its `cursor`, its `lag` behind the head of the log, its `generation`, and its number of `dead_letters` |
| `GET /workspaces/{workspace_id}/rules/{rule}` | One of the workspace's rules: its definition, its `origin`, and a stored rule's `version` and `provenance` |
| `GET /workspaces/{workspace_id}/schedules` | Each schedule that ticks in the workspace, with its `last_tick` and `next_tick` |
| `GET /rules` | The rules registered in code, as JSON: the same for every tenant, and visible to every authenticated caller ([Multi-tenancy and security](security.md#tenants-and-workspaces)) |
| `GET /schedules` | The registered schedules, likewise |

A dashboard follows a rule's health with `GET /v1/workspaces/prod/rules`:

```json
[{"rule": "ops:error-spike", "origin": "code", "version": null, "enabled": true, "cursor": 4, "lag": 3, "generation": 0, "dead_letters": 0}]
```

A lag that keeps growing means no reactor is evaluating the workspace, or it cannot keep up, unless the rule is [disabled](reactor.md#disabling-a-rule).

Page forwards through a long log with `after_seq`, starting from the last `seq` you have. To show the latest events first, as an incident timeline does, read the tail with `last`, then page backwards with `before_seq` set to the oldest `seq` you have:

```text
GET /v1/workspaces/prod/events?type=ops:service.error&last=20
GET /v1/workspaces/prod/events?type=ops:service.error&last=20&before_seq=4180
```

## Stored rules

When the application turns [stored rules](rules.md#stored-rules) on, a client installs, updates and archives them in a workspace with their commands, and each answers `rule_version`, the rule's new version and the `seq` of the fact that recorded the change:

```json
{"type": "command", "command_id": "install-prp_12",
 "command": {"type": "install_rule", "rule": {"name": "chat:prod-deploy-failures", "...": "..."},
             "provenance": {"source": "artifactr", "artifact": "art_rule_7"}}}
```

```json
{"type": "command_result", "command_id": "install-prp_12", "ok": true, "outcome": {"type": "rule_version", "rule": "chat:prod-deploy-failures", "version": 1, "seq": 812, "duplicate": false}, "rejection": null}
```

A change the configuration's `allow` hook refuses, or one to a code rule, is 403, a rule the fixed limits refuse is 422 with every problem in `errors`, a rule changed since the `expected_version` given is 409, and a name no stored rule has is 404. A change retried after it succeeded, even with a new `command_id`, changes nothing and answers `"duplicate": true`. Give `expected_version` so a retry after someone else's change is refused rather than applied.

A stored rule belongs to the workspace it is installed in, and is read there beside the code rules: `GET /v1/workspaces/prod/rules` lists it after them, and `GET /v1/workspaces/prod/rules/chat:prod-deploy-failures` returns it with its version and provenance:

```json
{"rule": {"name": "chat:prod-deploy-failures", "...": "..."}, "origin": "stored", "version": 1, "provenance": {"source": "artifactr", "artifact": "art_rule_7"}}
```

The same path in another workspace, or in another tenant's workspace of the same id, is 404, as is an archived rule. `GET /v1/rules` lists the code rules only.

## The WebSocket stream

A client connects to `/v1/workspaces/{workspace_id}/stream` with the subprotocol `reflexr.v1`, and sends `hello` with the last `seq` it has (0 the first time) and, optionally, the event types it wants:

```json
{"type": "hello", "protocol": "reflexr.v1", "resume_after_seq": 0, "types": ["ops:service.error", "reflexr:rule_fired", "reflexr:run_succeeded"]}
```

The server answers `welcome` with the head of the log, replays every event after `resume_after_seq`, and sends `replay_complete`. From then on the client receives each event as it is appended, and a `command_result` for each command frame it sends:

```text
{"type": "welcome", "protocol": "reflexr.v1", "workspace_id": "prod", "head_seq": 7, "reset": false}
{"seq": 1, "id": "alert-7", "ts": "2026-03-02T09:00:03Z", "workspace_id": "prod", "actor": {"kind": "source", "name": "grafana"}, "causation": null, "correlation_id": "alert-7", "traceparent": null, "event": {"type": "ops:service.error", "service": "auth", "severity": 8, "message": "token check failed"}, "type": "event"}
...
{"type": "replay_complete", "up_to_seq": 7}
```

- **Replay and live delivery are one subscription** to the log, so nothing falls between them. A client that reconnects says `hello` with its last `seq` and carries on. A client ahead of the log (the server lost events it had seen) gets `reset: true` and a replay from the beginning, and should discard what it has.
- **`from_head: true` starts at the head** instead, for a client that needs only what happens from now on, such as a dashboard that shows live alerts. Nothing is replayed: `welcome` carries the head, `replay_complete` follows at once, and then live events. Its position is then `welcome.head_seq`, and it reconnects with `resume_after_seq` from there, not with `from_head` again, so it does not miss what was appended while it was away. A `hello` with both `from_head` and a `resume_after_seq` is refused with 4400. To show some history first, read the tail over REST with `last`, then say `hello` with `resume_after_seq` set to the last `seq` it returned.
- **`types` filters** what the client receives; `null`, the default, sends everything. `replay_complete` is decided on the whole log, so it arrives even when the last replayed events were filtered out.
- **Frames that are not commands** get an `error` frame, and the connection stays open. A command runs as its own task, so a slow one never holds up the others.

The server closes a connection with a code that says what to do next:

| Code | Meaning | The client should |
|---|---|---|
| 4400 | The first frame was not a valid `hello`, asked for another protocol, or asked both to start at the head and to resume | Fix it and reconnect |
| 4401 | `resolve_actor` raised `Unauthorized` | Authenticate again |
| 4403 | `authorize` refused the workspace | Stop |
| 4408 | No `hello` arrived within `hello_timeout` | Reconnect |
| 4429 | The client did not read frames fast enough | Reconnect and resume from its last `seq` |

Every outgoing frame goes through one bounded outbox per connection. A client too slow to keep up is disconnected rather than allowed to hold events back or grow the server's memory, and it loses nothing, because it resumes from the log. The [stream protocol](../protocol.md#websocket) specifies every frame, and the [JSON Schema](../reference/schema.md) gives the types to generate a client from.

## Tuning

| `reflexr_router` option | Default | Meaning |
|---|---|---|
| `authorize` | `None` | Whether an actor may use a workspace of its tenant; `None` allows every one |
| `hello_timeout` | 10 seconds | How long a new connection has to send `hello` before it is closed (4408) |
| `outbox_size` | 1,000 frames | How far a connection may fall behind before it is closed (4429) |
| `remembered_commands` | 10,000 | Command results remembered for deduplication, per process |

Command deduplication is per process in v0.1: behind a load balancer, a retried command that reaches another process runs again. Publishing with an event id is idempotent everywhere, because the log itself remembers ids.

Each WebSocket connection is a `reflexr.stream` span, and counted in the `reflexr.stream.connections` and `reflexr.stream.disconnects` metrics. Its subscription reads the log untraced, so a connection that is only waiting makes no traces, however often SQL storage polls for it ([Observability](observability.md#polling)). When FastAPI is instrumented, as `configure_telemetry` does, each REST request's span is attributed to its tenant, workspace and actor too ([Observability](observability.md)).

## Logging and startup output

reflexr logs on its own loggers, `reflexr.reactor` for a failed reactor pass and `reflexr.fastapi` for a WebSocket command that failed. It sets no handlers, levels or environment variables unless the application asks, as `configure_telemetry` does when it adds its OTLP handler to the root logger ([Observability](observability.md)). Logging is the application's, as is what it prints at startup. Two of reflexr's dependencies would otherwise decide for it:

- **The MCP SDK configures logging as its server is built.** `MCPServer` calls `logging.basicConfig`, which, when the root logger has no handlers yet, sends the whole process's logs through a rich handler at INFO. `ReflexrMcp` puts the root logger's handlers and level back as they were once the server is built, so mounting [MCP](mcp.md) leaves logging as the application set it, or as Python's defaults leave it. An `MCPServer` you build yourself still does it, unless your application configures logging first.
- **pydantic-ai prints a banner on the first agent run** in a process, to a terminal or to a coding agent. It never prints one for an agent it instruments, such as one given `telemetry.capability()` ([Observability](observability.md)), nor under pytest or in CI. To turn it off, set `PYDANTIC_AI_NO_BANNER=1` in the environment, as in the service's container image or `.env`, or set `pydantic_ai.BANNER_ENABLED = False` in the application before its first agent run. reflexr sets neither, since both belong to the application.
