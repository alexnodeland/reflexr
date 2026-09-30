# oncall

reflexr's reference implementation: incident response. Monitoring fires alerts, CI reports
deploys, and services send heartbeats. Rules watch the log and run workflows. A burst of
severe alerts runs a triage agent, which opens an incident. An opened incident runs a runbook
graph, which rolls back a recent deploy or pages a person, then resolves the incident. A service
that stops sending heartbeats is paged.

It is a small, complete application built only on reflexr's public API:

| Piece | File | What it shows |
|---|---|---|
| Events | [`events.py`](src/oncall/events.py) | Five `Event` types: three that producers publish, two that the workflows emit |
| Rules | [`rules.py`](src/oncall/rules.py) | A count within a window, an event, and an absence, each scoped by service; a `Schedule` whose ticks keep time moving |
| Triage agent | [`triage.py`](src/oncall/triage.py) | A pydantic-ai `Agent` over `Reaction[OncallDeps]` with the `EventContext` capability, allowed to emit `oncall:incident.opened`, and a structured verdict |
| Runbook graph | [`runbook.py`](src/oncall/runbook.py) | A pydantic-graph `GraphBuilder` graph with a decision, checkpointed after every step, that reads the log and emits through the `Reaction` |
| Paging | [`actions.py`](src/oncall/actions.py) | A plain async function, idempotent by its run id |
| Services | [`services.py`](src/oncall/services.py) | The pager and deployer the workflows act on: in-memory fakes, given to every action as `reaction.deps` |
| Server | [`app.py`](src/oncall/app.py) | FastAPI with REST and the WebSocket stream at `/v1` and MCP at `/mcp`, running the `Reactor` in its lifespan, over in-memory or SQL storage, with OpenTelemetry, Langfuse and a LiteLLM proxy when configured |
| Terminal client | [`cli.py`](src/oncall/cli.py) | Publishes events, follows the stream live, and operates runs |

## How it fits together

```mermaid
graph LR
    alert[/"oncall:alert.fired"/] --> triage{{"triage<br/>3 alerts of severity 7+<br/>for a service in 5 min"}}
    triage --> agent["triage agent<br/>pydantic-ai"]
    agent -- emit_event --> opened[/"oncall:incident.opened"/]
    opened --> runbook{{"runbook<br/>each incident,<br/>per service"}}
    runbook --> graph["runbook graph<br/>pydantic-graph"]
    deploy[/"oncall:deploy.completed"/] -. read from the log .-> graph
    graph --> resolved[/"oncall:incident.resolved"/]
    heartbeat[/"oncall:service.heartbeat"/] --> silence{{"silence<br/>no heartbeat from<br/>a service for 2 min"}}
    tick[/"reflexr:tick, every 30 s"/] -. moves time .-> silence
    silence --> page["page<br/>function"]
    page --> alert
```

Events are slanted boxes, rules hexagons, and actions rectangles. Everything one alert leads to is
in its causal chain: the triage run, the incident, the runbook run and the resolution share its
`correlation_id`.

The runbook graph:

```mermaid
graph LR
    diagnose --> decide{recent deploy?}
    decide -->|yes| roll_back
    decide -->|no| escalate
    roll_back --> verify
    escalate --> verify
    verify --> resolve
```

`diagnose` looks in the log for a deploy of the service in the 30 minutes before the incident,
and the version before it. `verify` fails the attempt if the service is still unhealthy; the
retry resumes at `verify` from the checkpoint, so the service is not rolled back twice.

## Run it

From the repository root, with an Anthropic key. To use another provider, set `ONCALL_MODEL` to
any [pydantic-ai model name](https://ai.pydantic.dev/models/), such as `openai:<model>`, and set
that provider's key.

```sh
uv sync --all-packages
export ANTHROPIC_API_KEY=...
uv run oncall-serve
```

In a second terminal, watch the workspace:

```sh
uv run oncall watch
```

In a third, deploy twice, then fire three severe alerts:

```sh
uv run oncall --user ci deploy api --version v1
uv run oncall --user ci deploy api --version v2
uv run oncall --user monitor alert api --severity 8 --message "5xx rate above 5%"
uv run oncall --user monitor alert api --severity 9 --message "p99 latency above 2s"
uv run oncall --user monitor alert api --severity 8 --message "5xx rate above 5%"
```

The watch shows one line per event, with incidents, rule firings and runs highlighted (times are
UTC):

```text
Watching prod, 0 events so far.
── live after seq 0 ──
   1 09:00:01 deploy api v1 · ci
   2 09:00:04 deploy api v2 · ci
   3 09:00:09 alert api sev 8: 5xx rate above 5% · monitor
   4 09:00:12 alert api sev 9: p99 latency above 2s · monitor
   5 09:00:15 alert api sev 8: 5xx rate above 5% · monitor
   6 09:00:15 rule oncall:triage fired for service=api on seq 3, 4, 5 · reactor
   7 09:00:15 run fir_15e959a4fcfa3a8f of oncall:triage started, attempt 1 · reactor
   8 09:00:19 INCIDENT OPENED api sev 8: api v2 is failing 5% of requests · triage
   9 09:00:20 run fir_15e959a4fcfa3a8f of oncall:triage succeeded · reactor
  10 09:00:20 rule oncall:runbook fired for service=api on seq 8 · reactor
  11 09:00:21 run fir_5e173bcfb934d8d0 of oncall:runbook started, attempt 1 · reactor
  12 09:00:21 run fir_5e173bcfb934d8d0 finished step __start__ · runbook
  13 09:00:21 run fir_5e173bcfb934d8d0 finished step diagnose · runbook
  14 09:00:21 run fir_5e173bcfb934d8d0 finished step decide · runbook
  15 09:00:21 run fir_5e173bcfb934d8d0 finished step roll_back · runbook
  16 09:00:21 run fir_5e173bcfb934d8d0 finished step verify · runbook
  17 09:00:21 INCIDENT RESOLVED api: api v2 was deployed just before the incident; rolled api back to v1; api is healthy · runbook
```

Send a heartbeat (`uv run oncall heartbeat db`) and nothing else from `db` for two minutes, and
the silence rule pages the person on call and raises an alert about it. The in-memory pager and
deployer keep what they did; a real application passes its own clients in `OncallDeps`.

The server listens on `ONCALL_HOST` and `ONCALL_PORT` (default `127.0.0.1:8000`).

### Keep workspaces in a database

By default everything lives in memory and is gone when the server stops. Set
`ONCALL_DATABASE_URL` to keep it in SQLite or PostgreSQL. The server migrates the database to
reflexr's schema when it starts.

```sh
ONCALL_DATABASE_URL=sqlite+aiosqlite:///oncall.db uv run oncall-serve
ONCALL_DATABASE_URL=postgresql+asyncpg://user:password@localhost/oncall uv run oncall-serve
```

`Oncall(storage=...)` takes any reflexr `Storage` instead.

### Observe it, and route models through a gateway

With `OTEL_EXPORTER_OTLP_ENDPOINT` set, the server reports its traces, metrics and logs there
over OTLP (`reflexr.otel`): each run attempt is an `invoke_workflow {rule}` span, the triage
agent's model and tool calls are inside it, and requests and database queries are traced too. With
`LANGFUSE_PUBLIC_KEY` (and its secret key and host) set as well, each run is filed in Langfuse
under its rule, in its causal chain's session (`reflexr.langfuse`). Where a Collector sends
Langfuse the traces already, as stackr's does, set `ONCALL_LANGFUSE=scores` so oncall sends
Langfuse only each run's session, user and tags, not the spans a second time;
`compose.stackr.yaml` sets it. `ONCALL_ENVIRONMENT` names the deployment (`development` by
default).

With `ONCALL_LITELLM_URL` set, the triage agent calls that LiteLLM proxy instead of a provider
(`reflexr.litellm`): the model group `ONCALL_LITELLM_MODEL` (`claude-sonnet` by default), with the
key `ONCALL_LITELLM_KEY`, and every request carries the tenant, the causal chain and the trace.
[stackr](https://github.com/alexnodeland/stackr) runs a Collector, Langfuse and the proxy.

### In Docker

The repository's Compose file runs oncall on PostgreSQL, from `examples/oncall/Dockerfile`. From
the repository root:

```sh
make app-up     # docker compose --profile app up -d --build --wait
```

It passes `ONCALL_MODEL`, the providers' keys and the `ONCALL_LITELLM_*` settings through from
your environment. While [stackr](https://github.com/alexnodeland/stackr)'s stack runs, add the
overlay to send its telemetry to stackr's Collector:

```sh
docker compose -f compose.yaml -f compose.stackr.yaml --profile app up -d --build
```

### Client commands

| Command | Does |
|---|---|
| `oncall alert SERVICE --severity 1-10 --message TEXT` | Publishes `oncall:alert.fired`, as monitoring would |
| `oncall deploy SERVICE --version VERSION` | Publishes `oncall:deploy.completed`, as CI would |
| `oncall heartbeat SERVICE` | Publishes `oncall:service.heartbeat` |
| `oncall watch [--after SEQ] [--until TYPE]` | Replays the log after `SEQ`, then follows it live; `--until` stops after an event of that type, for scripts |
| `oncall runs [--rule R] [--status S]` | Lists runs, newest first, with where and why a stuck one stopped |
| `oncall retry RUN_ID` | Makes a run runnable now |
| `oncall skip RUN_ID [--reason TEXT]` | Gives up on a waiting run, unblocking its service |
| `oncall cancel RUN_ID [--reason TEXT]` | Cancels a run that has not finished |

Every command takes `--url` (default `http://127.0.0.1:8000`), `--workspace` (default `prod`) and
`--user` (default `guest`).

### Other surfaces

- **REST:** `curl -H 'x-user: alice' localhost:8000/v1/workspaces/prod/runs`. Opening an incident
  by hand runs the runbook too:

  ```sh
  curl -H 'x-user: alice' -H 'content-type: application/json' \
    localhost:8000/v1/workspaces/prod/events \
    -d '{"event": {"type": "oncall:incident.opened", "service": "api", "severity": 7, "summary": "checkout is slow"}}'
  ```

  See the [protocol](../../docs/protocol.md) for commands and reads.
- **MCP:** point an MCP client at `http://127.0.0.1:8000/mcp/`. It can publish events, read the
  log, and list, retry, skip and cancel runs, like any other participant.

Authentication is a demo: the server trusts the `x-user` header, and everyone shares one tenant.
Real applications resolve the actor from their own sessions in `resolve_actor`.

## Test it

The tests run the whole stack with a scripted model, so they need no API key. They drive the
reactor with `reactor.settle()` and a clock they move by hand, so nothing waits on timing:

```sh
uv run pytest examples/oncall/tests
```
