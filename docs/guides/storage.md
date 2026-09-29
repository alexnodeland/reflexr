# Storage

A workspace keeps its log, each rule's progress and scope states, its runs, dead letters, schedule ticks, leases and its consumers' cursors in a storage. One protocol, `Storage`, covers all of it, and `Workspaces` takes any implementation: storage is a port, and the implementations are its adapters ([ADR-0025](../adr/0025-ports-and-adapters.md)). Application code builds a storage and hands it to `Workspaces`; after that it goes through workspace handles and the reactor, which hold the tenant and workspace scope.

Two implementations ship:

| Storage | Package | Use it for |
|---|---|---|
| `InMemoryStorage` | `reflexr.workspace` | Tests, examples and single-process prototypes. Nothing survives a restart. |
| `SqlStorage` | `reflexr.sql` (extras) | Everything else: PostgreSQL in production, or SQLite for development and single-process applications |

Both pass the same workspace behaviour suite, so an application that works on one works on the other.

## In-memory storage

`InMemoryStorage` keeps every workspace in the process:

```python
from reflexr.workspace import InMemoryStorage, Workspaces

workspaces = Workspaces(
    InMemoryStorage(), events=[ServiceError, Deploy, Heartbeat], rules=[error_spike]
)
```

It implements the whole protocol: transactions that roll back when their block raises, a live subscription to the log, leases and cursors. It keeps nothing across restarts and cannot be shared between processes, so run one reactor with it.

Its clock stamps envelopes and expires leases, and it is injectable, so tests control time. Give `Workspaces` the same clock, since it times runs:

```python
from datetime import UTC, datetime

now = datetime(2026, 1, 1, tzinfo=UTC)
workspaces = Workspaces(
    InMemoryStorage(clock=lambda: now), events=[ServiceError], clock=lambda: now
)
```

[Testing your application](testing.md#controlling-time) shows a clock that tests move by hand.

## SQL storage

`SqlStorage` keeps workspaces in PostgreSQL or SQLite through SQLAlchemy 2's asyncio extension, with the same code on both ([ADR-0030](../adr/0030-sql-storage.md)). Install the extra for your database:

| Extra | Database | Adds |
|---|---|---|
| `postgres` | PostgreSQL | SQLAlchemy, Alembic and asyncpg |
| `sqlite` | SQLite | SQLAlchemy, Alembic and aiosqlite |
| `sql` | Either, with a driver you install yourself | SQLAlchemy and Alembic |

### PostgreSQL

Create an async engine, bring the schema up to date with `migrate`, and give the storage to `Workspaces`:

```python
from sqlalchemy.ext.asyncio import create_async_engine

from reflexr.sql import SqlStorage, migrate
from reflexr.workspace import Workspaces

engine = create_async_engine("postgresql+asyncpg://app:secret@localhost/oncall")
await migrate(engine)  # creates or upgrades reflexr's tables
workspaces = Workspaces(
    SqlStorage(engine), events=[ServiceError, Deploy, Heartbeat], rules=[error_spike]
)
```

PostgreSQL, including a hosted one such as Supabase's, must run at its default `READ COMMITTED` isolation level.

### SQLite

SQLite has no row locks, so its engine must take the database's write lock as every transaction begins. `create_sqlite_engine` makes such an engine:

```python
from reflexr.sql import SqlStorage, create_sqlite_engine, migrate

engine = create_sqlite_engine("sqlite+aiosqlite:///oncall.db")
await migrate(engine)
workspaces = Workspaces(
    SqlStorage(engine), events=[ServiceError, Deploy, Heartbeat], rules=[error_spike]
)
```

Use a database file, not a plain `:memory:` database; for throwaway storage, use `InMemoryStorage`. Every transaction, reads included, holds the write lock, so transactions run one at a time. The engine keeps a single connection, so a process's transactions take turns in the order they begin, and only other processes wait on the lock itself, for up to the driver's timeout (5 seconds unless you pass `connect_args`). That suits tests, development and single-process applications rather than busy ones.

The storage does not own the engine: dispose of it when the application stops, with `await engine.dispose()`.

### How it behaves

- **One transaction per workspace at a time, across processes.** A transaction creates its workspace's row if the workspace is new, then locks it (`SELECT ... FOR UPDATE`) before it reads anything, and holds the lock until it commits. The row holds the head of the log, so `seq` and `ts` are assigned under the lock without reading the log. This is what lets the reactor evaluate each envelope for each rule exactly once ([ADR-0005](../adr/0005-per-rule-cursors.md)). On SQLite, `BEGIN IMMEDIATE` takes the database's write lock instead.
- **Subscriptions poll.** `subscribe` reads the log a page at a time. Once it has caught up, a commit made through the same `SqlStorage` wakes it at once, and a commit from another process is seen within `poll_interval` (half a second by default). `Workspace.subscribe` reads untraced, so the polls make no traces even with the database instrumented ([Observability](observability.md#polling)):

    ```python
    from datetime import timedelta

    storage = SqlStorage(engine, poll_interval=timedelta(seconds=0.1))
    ```

- **Records are JSON.** Envelopes, runs, rule progress, scope states and dead letters are stored as the JSON of their Pydantic models, beside the columns that queries filter and order by. An event whose type the process does not know comes back as an `UnknownEvent`. Adding a field with a default to an event type needs no migration.
- **Reads of the log filter in the database.** An event's type has a column of its own, indexed with its workspace and `seq`, so reading some types reads only their envelopes, and a read of the last so many reads the log backwards from the end.
- **Timestamps in columns are UTC.** They are stored and read back in UTC, because SQLite compares timestamps as text. Timestamps inside the JSON round-trip exactly as given.
- **Leases are rows,** taken with a conditional `UPDATE` or else an `INSERT`, and they expire by the storage's clock. `SqlStorage(engine, clock=...)` takes a clock, as `InMemoryStorage` does.
- **Cursors are rows,** each locked while a save compares it, so the furthest save wins.

Every table's name starts with `reflexr_`, and every primary key starts with the tenant and the workspace, so every row belongs to one tenant's workspace ([ADR-0016](../adr/0016-tenants-and-workspaces-like-artifactr.md)):

| Table | Holds |
|---|---|
| `reflexr_workspaces` | One row per workspace: the head of its log, and the lock transactions take |
| `reflexr_events` | The log, by `seq`, with each event id unique within its workspace and each event's type beside it |
| `reflexr_rule_progress` | Each rule's cursor, generation, definition and deadlines |
| `reflexr_scope_states` | What each rule remembers about each scope |
| `reflexr_runs` | Runs, with their status, attempts, checkpoint and output |
| `reflexr_dead_letters` | Envelopes rules could not evaluate |
| `reflexr_schedules` | Each schedule's last tick |
| `reflexr_leases` | Evaluation and run leases |
| `reflexr_cursors` | The cursors of the log's other consumers, such as feedback mirrors |

## Migrations

reflexr's Alembic migrations ship in the package. `migrate(engine)` upgrades the database to the latest schema, creating the tables if they do not exist, and leaves an up-to-date database alone, so it is safe to run on every start or deploy. The migrations record their version in their own table, `reflexr_alembic_version`, so they live beside your application's own tables and migrations in the same database without interfering.

If your application runs Alembic's autogenerate on the same database, it will propose dropping reflexr's tables, since they are not in your models. Exclude them in your `env.py`:

```python
def include_name(name, type_, parent_names):
    return not (type_ == "table" and name.startswith("reflexr_"))


context.configure(connection=connection, target_metadata=metadata, include_name=include_name)
```

`create_schema(engine)` creates the tables straight from the models, without Alembic. It suits tests and prototypes, but a database created this way records no schema version, and `migrate` fails on it later because the tables already exist. Use `migrate` for any database that has to last.

## Writing your own storage

To keep workspaces somewhere else, implement the `Storage` and `Transaction` protocols from `reflexr.workspace`; the [reference](../reference/workspace.md) lists every method. `InMemoryStorage` is the shortest complete example, and `SqlStorage` shows the same protocol over a database with locks.

Everything is scoped to a `WorkspaceRef`, a tenant and a workspace: the unit of isolation, ordering and locking. The rest of the library relies on these guarantees:

- **Transactions serialize per workspace from the moment they begin.** `storage.transaction(ref)` is an async context manager for a `Transaction`. Nothing the transaction reads can change before it commits, because no other transaction on that workspace runs meanwhile. Its writes are visible to its own later reads, commit together when the block exits normally, and roll back if it raises.
- **`append` assigns a gap-free `seq`, and a `ts` that never decreases:** the later of the clock and the previous envelope's `ts`. An entry without a `correlation_id` starts a new causal chain named by its own id. Appending an id that is already in the log raises `ValueError`, so callers look ids up first:

    ```python
    from reflexr import SourceActor
    from reflexr.workspace import Entry, WorkspaceRef

    ref = WorkspaceRef("acme", "prod")
    async with storage.transaction(ref) as transaction:
        if await transaction.envelope("alert-7") is None:  # appending an id twice raises
            await transaction.append(
                [
                    Entry(
                        id="alert-7",
                        actor=SourceActor(name="monitor"),
                        event=ServiceError(service="auth", severity=8),
                    )
                ]
            )
    ```

- **Within a transaction,** the host also loads and saves rule progress and scope states (and clears a rule's states when it is reset), saves runs, lists a scope's unfinished runs in firing order, dead-letters evaluation errors, and records schedule ticks.
- **`read(ref, after_seq=, before_seq=, types=, limit=, last=)` reads a window of the log,** the envelopes with `after_seq < seq < before_seq`, oldest first. `types` keeps those of some event types, `limit` the first so many that match, and `last` the last so many, still oldest first. Filter in the store rather than after reading, so that a tail read of a long log reads only its tail. `Workspace.read` checks the arguments first, so a storage never gets both `limit` and `last`, or a negative number.
- **`subscribe(ref, after_seq=...)` replays, then follows.** One iterator yields the stored envelopes after `after_seq` and then each new one as it commits, so nothing falls between catching up and following along. The WebSocket stream, MCP notifications and the feedback mirror all read the log this way.
- **Discovery spans workspaces.** `workspaces()` lists every workspace with a log, for the reactor to evaluate, and `due_runs(now=..., limit=..., policy=...)` returns the runs to attempt now that can start: pending and retrying runs that are due, and running runs whose lease (`run_lease(run_id)`) has lapsed. Storage does not see the rules, so the executor describes them with a `RunPolicy`, built by `RunPolicy.of(rules)`. A rule the policy does not name has every due run returned. The executor checks each run's place in its scope again when it claims it, so a storage that returns too much only costs claims that are declined, but one that returns too little leaves runs waiting: implement the filter exactly, with `RunPolicy.holds` saying which runs hold a scope. The policy names:
  - **`disabled`** rules, whose runs are left out, so they neither run nor take up the limit.
  - **`ordered`** rules, with `ordering="scope"`. A pending or retrying run of one is returned only if no earlier run of its scope, by `fired_seq` and then creation, holds the scope by being pending, retrying or running. So a scope's backlog takes one place in the limit however deep it is.
  - **`blocking`** rules, ordered ones with `on_dead_letter="block"`, whose dead-lettered runs hold their scope too.
- **Leases are exclusive and expire.** `acquire_lease(ref, key, holder, ttl)` takes or renews a lease and returns whether the holder has it; `release_lease` gives it up. The reactor holds one per workspace while it evaluates and one per run while it executes, so several processes can share the work ([ADR-0027](../adr/0027-executing-runs.md)).
- **Cursors only move forward.** `save_cursor(ref, name, seq)` records how far a named consumer of the log has got, and `cursor(ref, name)` reads it back, 0 if it has none. Saving a `seq` below the saved one leaves it, so a consumer that runs in several processes cannot move it back. A `FeedbackMirror` keeps one, as a rule keeps its progress, so a restarted mirror carries on where it was ([ADR-0040](../adr/0040-telemetry-that-composes-across-libraries.md)); `Workspace.cursor` and `Workspace.save_cursor` give an application's own consumers the same.

The workspace behaviour suite states the rest precisely, including rollback, isolation between workspaces, subscriptions that never miss an envelope, and lease expiry. It lives in the repository's [`tests/workspace/`](https://github.com/alexnodeland/reflexr/tree/main/tests/workspace) rather than in the package: its `storage` fixture runs every test on in-memory storage, SQLite and PostgreSQL, so copy the suite and add your storage to that fixture.
