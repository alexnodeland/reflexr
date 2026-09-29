# ADR-0030: SQL storage with one dialect-neutral implementation

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

RFC-0001 phase 4 builds `SqlStorage`, the production adapter of the storage port ([ADR-0025](0025-ports-and-adapters.md)), on SQLAlchemy 2's asyncio extension and Alembic. The port and the in-memory adapter fix what storage must do, and the workspace behaviour suite is the contract: `SqlStorage` must pass it unchanged, on SQLite always and on PostgreSQL when one is configured. Building it settled how:

- A transaction is serialized with the others on its workspace when several processes share a database, on both PostgreSQL and SQLite, and how `seq` and `ts` are assigned under that serialization.
- The workspace's log, runs, rule progress, scope states, dead letters, schedules and leases are laid out in tables, given that events, states and checkpoints are the application's and the core's Pydantic models.
- A transaction reads its own writes, and saves insert or update rows, without dialect-specific upserts.
- Timestamps are stored, given that SQLite has no time zones and compares timestamps as text.
- A subscription learns about envelopes that other processes commit.
- The schema is created and upgraded next to an application's own tables and migrations.
- The 100% coverage gate ([ADR-0013](0013-quality-gates.md)) is met when the main test jobs have no PostgreSQL.

artifactr answered the same questions for its own `SqlStorage` (its ADR-0021). reflexr follows it, so that the siblings' storage reads the same ([ADR-0003](0003-independent-sibling-of-artifactr.md)), and records here where reflexr's port needed more.

## Decision

- **One dialect-neutral implementation.** Nothing in `SqlStorage` depends on the dialect, so there are no PostgreSQL-only or SQLite-only branches. What differs lives in engine setup: engines from `create_sqlite_engine` begin every transaction with `BEGIN IMMEDIATE`. The suite covers every line and branch on SQLite alone, and a CI job runs the whole suite on PostgreSQL 17 as well.
- **A transaction locks its workspace's row as it begins.** It creates the row if the workspace is new, then selects it `FOR UPDATE` before it reads anything. If two transactions create the same row at once, the losing insert fails inside a savepoint and that transaction locks the winner's row instead. The row holds the log's `head_seq` and `head_ts`, so `append` assigns `seq` and `ts = max(clock, head_ts)` under the lock without reading the log. SQLite ignores `FOR UPDATE`; `BEGIN IMMEDIATE` takes the database's write lock instead. PostgreSQL must run at its default `READ COMMITTED` isolation.
- **A SQLite engine keeps one connection.** SQLite runs one transaction at a time anyway, so `create_sqlite_engine` pools a single connection unless given another `poolclass`. A process's transactions queue for it and take turns in the order they begin, as on in-memory storage, instead of racing for the write lock through SQLite's busy handler, which neither keeps their order nor avoids its sleeps. Only other processes contend for the lock.
- **One table per kind of record, every primary key led by the tenant and the workspace,** and every query filters on both ([ADR-0016](0016-tenants-and-workspaces-like-artifactr.md)). Table names start with `reflexr_`:

  | Table | Primary key | Beside the JSON |
  |---|---|---|
  | `reflexr_workspaces` | tenant, workspace | `head_seq`, `head_ts`, `last_position` |
  | `reflexr_events` | tenant, workspace, `seq` | `event_id`, unique per workspace |
  | `reflexr_runs` | tenant, workspace, `id` | `position`, `rule`, `scope_key`, `status`, `fired_seq`, `next_attempt_at` |
  | `reflexr_rule_progress` | tenant, workspace, `rule` | `position` |
  | `reflexr_scope_states` | tenant, workspace, `rule`, `scope_key` | |
  | `reflexr_dead_letters` | tenant, workspace, `position` | `rule` |
  | `reflexr_schedules` | tenant, workspace, `name` | `position`, `at` |
  | `reflexr_leases` | tenant, workspace, `key` | `holder`, `expires_at` |

- **Records are JSON documents of the core's models, with the columns reads filter and order by beside them.** Envelopes, runs, rule progress, scope states and dead letters are stored as `model_dump(mode="json")` and read back with `model_validate`, so they round-trip exactly, and an event whose type the process does not know comes back as an `UnknownEvent`. The type is `JSON`, not PostgreSQL's `JSONB`, because `JSONB` reorders object keys. Lists keep the in-memory storage's order through a creation position taken from a counter on the locked workspace row: runs newest first, dead letters oldest first, and progress and schedules in the order they were first saved. `scope_runs` orders by `fired_seq`, then position.
- **Writes go to the session at once.** The session flushes them before each query, so a transaction reads its own writes, and a rollback discards them. A save reads the rows it touches and updates them or adds new ones; that is race-free without an upsert because every write to a workspace's rows holds its row's lock. Saving runs keeps the last version of each id, and `states` reads its keys 500 at a time. `append` looks each id up before adding it, so a duplicate raises `ValueError` just as in memory, before anything is added for it.
- **Timestamps in columns are stored and read in UTC.** A column type converts every value to UTC before it is stored, because SQLite compares timestamps as text, and marks values read back as UTC. What the models hold, such as an envelope's `ts` or a run's `next_attempt_at`, round-trips in its JSON exactly as given. What comes back from a column (a schedule's last tick, and the previous envelope's `ts` when the clock is behind it) is the same instant, expressed in UTC.
- **Due runs are one query across workspaces.** It selects pending and retrying runs due by `now`, and running runs whose lease, found by the key `run_lease(run.id)` builds, is missing or expired by `now`. It orders them by `next_attempt_at`, then tenant, workspace and creation position; the protocol leaves the order of runs due at the same moment in different workspaces open. `workspaces()` lists workspaces whose `head_seq` is above 0, by tenant and workspace.
- **Leases are rows, outside the workspace lock.** A lease is taken with one conditional `UPDATE` (the holder renewing it, or anyone once it has expired), or else an `INSERT`; an insert that collides with the primary key means another holder has it.
- **Subscriptions poll, and wake early for local commits.** `subscribe` reads the log a page at a time. Once caught up, it waits for a commit made through the same `SqlStorage`, or for `poll_interval` (0.5 s by default), whichever comes first, holding no connection while it waits. v0.1 does not use `LISTEN/NOTIFY`.
- **Migrations ship in the package.** `migrate(engine)` runs the packaged Alembic scripts to the latest revision, recording the version in `reflexr_alembic_version`, so they sit beside an application's own migrations. `create_schema(engine)` creates the tables from the models, for tests and prototypes. A test runs the migrations on both databases and checks that Alembic's autogenerate finds no difference from the models, then downgrades them.
- **Drivers are extras.** `reflexr[sql]` installs SQLAlchemy and Alembic; `reflexr[postgres]` adds asyncpg and `reflexr[sqlite]` adds aiosqlite. The library imports neither driver.

## Options considered

### Serializing the transactions on a workspace

| Option | Across processes | Dialect-specific code | Cost |
|---|---|---|---|
| **Lock the workspace row with `SELECT … FOR UPDATE` (chosen)** | Yes | None in storage; SQLite's engine begins immediately | One locked row per transaction |
| PostgreSQL advisory locks | Yes | PostgreSQL only | A second mechanism for SQLite |
| `SERIALIZABLE` isolation with retries | Yes | Retryable errors differ by driver | Every transaction must be ready to retry |
| An in-process lock | No | None | Wrong as soon as there are two processes |

### Connections to SQLite

| Option | Order of a process's transactions | Waiting |
|---|---|---|
| **One connection per engine (chosen)** | The order they begin | A queue in the process; the busy handler only for other processes |
| A pool of connections | Whichever wins the lock | SQLite's busy handler, which sleeps and retries, and fails after its timeout |

### Saving rows

| Option | Dialect-specific code | Queries |
|---|---|---|
| **Read the rows, then update or add them, under the workspace lock (chosen)** | None | One read per save (per 500 keys) |
| `INSERT … ON CONFLICT DO UPDATE` | A different construct for each dialect | One write per save |
| Buffer writes and apply them on commit, as in memory | None | Reads must merge the buffer with the database |

### Following the log from other processes

| Option | Latency across processes | Works on SQLite | Complexity |
|---|---|---|---|
| **Polling, plus an in-process wake-up (chosen)** | Up to `poll_interval` | Yes | Low: one query per interval for each caught-up subscription |
| PostgreSQL `LISTEN/NOTIFY` | Immediate | No, so polling is needed anyway | A dedicated connection, reconnection, and a dialect branch |
| An external broker | Immediate | Yes | A new infrastructure dependency |

## Trade-off analysis

A dialect-neutral implementation lets SQLite, which needs no server, exercise every line and branch, so `make check` meets the coverage gate on its own and the PostgreSQL job exists to prove correctness rather than to reach code. The price is leaving PostgreSQL-only features out of v0.1: `LISTEN/NOTIFY`, advisory locks, `JSONB` and upserts. None is needed for correctness. `LISTEN/NOTIFY` is the one with a visible benefit, lower latency for subscribers in other processes, and it can be added later behind the same `subscribe`, with polling kept as the fallback.

Holding the workspace row's lock from a transaction's first read, rather than only while assigning `seq`, is what keeps its reads valid until it commits, which the reactor's evaluation relies on to affect each rule exactly once ([ADR-0005](0005-per-rule-cursors.md)). Keeping `head_seq` and `head_ts` on that row means appending needs no read of the log.

The single SQLite connection costs nothing, since SQLite serializes transactions anyway, and buys the in-memory adapter's fairness within a process. Its risk is the same as the write lock's: code that holds a transaction open while it waits for another storage call on the same engine waits until the pool's timeout, where on a pool it would have failed after the busy timeout. Nothing in reflexr does that.

## Consequences

- Easier: one code path to reason about and test, and SQLite is a faithful stand-in for development and tests.
- Easier: event types, states and checkpoints change without migrations; a change affects validation, not the schema.
- Harder: a subscriber sees commits from another process up to `poll_interval` late, and each caught-up subscription queries the log once per interval.
- Harder: on SQLite every transaction, reads included, holds the database's write lock, so SQLite suits tests, development and single-process applications rather than busy ones.
- Applications that run Alembic's autogenerate on the same database should exclude the `reflexr_` tables (for example with `include_name`), so that they are not proposed for removal.
- Revisit `LISTEN/NOTIFY` if cross-process latency or polling load matters, and partial indexes for due runs if the runs table grows large.

## Action items

1. [x] `SqlStorage`, `create_sqlite_engine`, `migrate` and `create_schema`, with the initial migration (RFC-0001 phase 4).
2. [x] The workspace behaviour suite on in-memory storage, SQLite and PostgreSQL, with a PostgreSQL job in CI.
3. [ ] Wake subscribers with PostgreSQL `LISTEN/NOTIFY` if polling latency or load becomes a problem (after v0.1).
