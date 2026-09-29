# ADR-0039: Namespaced event types

**Status:** Proposed
**Date:** 2026-09-29
**Deciders:** Alex Nodeland

## Context

An event type's name is its identity everywhere: on the wire as `event.type`, in stored envelopes and their `event_type` column, in rules' `on` filters, and in every type filter. Names are flat, and one process-wide registry holds them (`_registry` in `core/events.py`). Defining a second class under a name that is taken raises `TypeError` at import. Nothing checks a name's form.

Flat names have cost us once, and would cost us again ([#45](https://github.com/alexnodeland/reflexr/issues/45)):

- **An application and the test suite.** oncall couldn't use `heartbeat` or `deploy.finished`, because reflexr's test fixtures register them in the same pytest process ([ADR-0031](0031-the-reference-implementations-events-and-rules.md)).
- **An application and a plugin**, or two applications, can't use the same names in one process.
- **A bridge and reflexr.** [stackr RFC-0002](https://github.com/alexnodeland/stackr/blob/main/docs/rfcs/0002-the-combined-system.md) adds relayr, which publishes artifactr's events into reflexr. artifactr's `feedback_given` and `run_started` are also names of reflexr's own facts. The RFC's decision D6 settles this issue before relayr's phase 1, so the bridged names are right the first time.

[#72](https://github.com/alexnodeland/reflexr/issues/72), which reserves a type to one publisher, may build on the same namespaces.

Where a name is used today:

| Where | What it does with the name |
|---|---|
| An `Event` subclass (`name=`, or the snake-cased class name) | Registers it, process-wide; `event_types()` and `get_event_type()` read the registry |
| `load_event` and `AnyEvent`, for every frame, body and stored envelope | Look the name up; an unregistered one becomes `UnknownEvent`, which round-trips |
| `Workspaces(events=, emitted=)` | Names the types clients may publish, and those only runs may. reflexr's facts are always `forbidden` |
| `on(...)`, the builder's field checks, `Rule.check` | `OnFilter.types` holds names, checked against `events=` (or the registry) and reflexr's facts |
| `matches` in `core/evaluation.py` | `envelope.event_type in types`: string equality |
| `Rule.definition()` | Hashes the condition, names included. A new hash resets the rule |
| `publish`, `hello.types`, REST's `type=`, MCP's `publish_event` and `read_events`, the agent's tools | Carry names as strings |
| SQL storage | The envelope's JSON, and the `event_type` column, indexed on `(tenant_id, workspace_id, event_type, seq)` (migration 0003) |
| The JSON Schemas | `OnFilter.types`, `Hello.types` and `event.type` are plain strings, with no pattern |
| MCP | `_RUN_FACTS` picks run facts by their `run_` prefix |
| Telemetry | `reflexr.publish {type}` spans, and `reflexr.event.type` on `reflexr.events.published`, which the workspace dashboard groups by |

reflexr's own facts have thirteen bare names: `rule_fired`, `rule_errored`, `rule_reset`, the eight `run_*` facts, `feedback_given` and `tick`. Feedback types have a registry of the same shape (`core/feedback.py`).

artifactr names events differently. Its own events are a closed union that only artifactr defines ([artifactr ADR-0005](https://github.com/alexnodeland/artifactr/blob/main/docs/adr/0005-one-event-log-per-workspace.md)). Applications' facts share one open type, `app_event`, told apart by a free-form `name` that nothing registers. pydantic-ai, whose `CustomEvent` naming reflexr follows, does namespace its capability events: `class CheckpointStartEvent(CapabilityEvent, namespace="checkpoint")`. Subclasses of an `abstract=True` base inherit its namespace, and a kind is the namespace and the name joined with a dot.

## Decision

**Proposed.** The maintainer decides each question below. Nothing is built until then. The recommendations, taken together:

- **Qualified names on the wire, in one registry.** A type in a namespace is `namespace:name`, such as `artifactr:message_posted`, wherever a name appears. `Workspaces(events=...)` still chooses which types a set of workspaces accepts.
- **A bare name is the application's.** Libraries, plugins and bridges declare a namespace. reflexr's own facts move to `reflexr:`.
- **An owner declares its namespace once**, on an abstract base its types subclass, as pydantic-ai does.
- **A clean break.** No aliases, one migration for the stored facts, and the protocol stays `reflexr.v1`, since nothing is released.
- **#72 reserves by namespace**, with per-type exceptions, as publishing policy on `Workspaces`.
- **relayr bridges artifactr's events as `artifactr:<type>`.**

| # | Question | Recommendation |
|---|---|---|
| 1a | Where namespaces live | Qualified names on the wire, in the one process-wide registry |
| 1b | How an owner declares one | `namespace=` on an abstract base, declared once |
| 2a | Separator and grammar | `:`, at most one; lowercase snake_case segments |
| 2b | Unqualified names | A bare name is the application's; no default namespace is filled in |
| 2c | reflexr's facts | `reflexr:rule_fired` and so on, with migration 0004 |
| 3 | Compatibility | A clean break, no aliases, still `reflexr.v1` |
| 4 | Reserved publishers (#72) | By namespace, with per-type exceptions, on `Workspaces` |
| 5 | relayr's names | `artifactr:message_posted` and so on |

## Options considered

The examples use three types: an application's `alert.fired`, relayr's bridge of artifactr's `feedback_given` (a name reflexr's own fact holds today), and reflexr's `run_dead_lettered`. Each shows a rule's filter, a `publish` command, and the stored envelope with its `event_type` column. `...` stands for fields that don't change.

### 1a. Where namespaces live

| Option | An application and a library in one `Workspaces` | Wire | Cost |
|---|---|---|---|
| Convention: flat names with agreed prefixes | By discipline | Unchanged | Nothing to build, and nothing enforced |
| A registry per `Workspaces`, the global one by default | Not solved: one log still needs one name per type | Unchanged | A registry threaded through body and frame parsing, storage reads, the builder and MCP |
| **Qualified names on the wire, in one registry (recommended)** | Solved | Names gain a namespace | The grammar, and a migration if reflexr's facts move (2c) |
| Both | Solved | As above | Both costs |

**Convention**, as RFC-0002 wrote its placeholders. Nothing says `artifactr.` is a namespace or who owns it, so #72 has nothing to hang on:

```text
rule      {"kind": "on", "types": ["artifactr.feedback_given"]}
publish   {"type": "publish", "event": {"type": "artifactr.feedback_given", "feedback_type": "rating", ...}}
envelope  {"seq": 12, "actor": {"kind": "source", "name": "artifactr"}, ..., "event": {"type": "artifactr.feedback_given", ...}}
column    event_type = 'artifactr.feedback_given'
```

**A registry per `Workspaces`.** Two applications in one process could each have a `heartbeat`. But relayr's types and reflexr's facts share one log, so relayr would still need a prefix by convention. And names are resolved where no `Workspaces` is in hand: `AnyEvent` validates bodies and frames on their own, storage validates envelopes as it reads them, and `on(...).where(...)` checks fields as it builds.

```text
declare   Workspaces(storage, types=EventTypes([AlertFired, FeedbackGiven]))   (hypothetical)
rule      {"kind": "on", "types": ["feedback_given"]}      reflexr's fact, or artifactr's?
publish   {"type": "publish", "event": {"type": "feedback_given", ...}}
envelope  {"seq": 12, ..., "event": {"type": "feedback_given", ...}}
column    event_type = 'feedback_given'
```

**Qualified names (recommended).** One mechanism covers all three collisions:

```text
declare   class ArtifactrEvent(Event, abstract=True, namespace="artifactr")
          class FeedbackGiven(ArtifactrEvent)                              artifactr:feedback_given
rule      {"kind": "on", "types": ["artifactr:feedback_given"]}
publish   {"type": "publish", "event": {"type": "artifactr:feedback_given", "feedback_type": "rating", ...}}
envelope  {"seq": 12, "actor": {"kind": "source", "name": "artifactr"}, ..., "event": {"type": "artifactr:feedback_given", ...}}
column    event_type = 'artifactr:feedback_given'
```

**Both.** The wire is as above, plus `Workspaces(types=...)`. Its only gain is two applications with the same bare names in one process, which a namespace also solves. Since qualified names are unambiguous, a registry per `Workspaces` can still be added later without touching the wire.

### 1b. How an owner declares its namespace

| Option | Declared | A typo | Precedent |
|---|---|---|---|
| In each name: `name="oncall:alert.fired"` | Per type | Quietly makes a new namespace for one type | None |
| **`namespace=` on an abstract base, declared once (recommended)** | Once per owner | Is in the base, so every type shows it | pydantic-ai's `CapabilityEvent` |
| Derived from the module: `oncall.events` gives `oncall` | Implicitly | Can't happen: nothing is typed | None, and moving a module renames its types on the wire |

The wire is the same under all three:

```text
in each name     class AlertFired(Event, name="oncall:alert.fired")
on a base        class OncallEvent(Event, abstract=True, namespace="oncall")
                 class AlertFired(OncallEvent, name="alert.fired")
from the module  class AlertFired(Event, name="alert.fired")                  in oncall/events.py

rule      {"kind": "on", "types": ["oncall:alert.fired"]}
publish   {"type": "publish", "event": {"type": "oncall:alert.fired", "service": "api", ...}}
envelope  {"seq": 7, ..., "event": {"type": "oncall:alert.fired", ...}}, event_type = 'oncall:alert.fired'
```

With the recommendation:

- Subclasses inherit the namespace. `name=` stays the local part, and a `:` in it is refused.
- A second class that declares the same namespace raises at import, naming both, as a duplicate name does today. pydantic-ai lets any class use a namespace, and refuses only duplicate kinds. Declaring once makes the owner something the registry knows, which #72 can rely on.
- A class without a namespace has a bare name (2b).

| Owner | Declares | Names |
|---|---|---|
| The application that hosts the process | Nothing | `alert.fired` |
| reflexr | `reflexr`, in core | `reflexr:run_started` |
| A library or plugin | Its own | `acme_pager:page.sent` |
| A bridge (relayr) | The source system's | `artifactr:message_posted` |
| An application that runs beside others, such as oncall in reflexr's tests | Its own | `oncall:alert.fired` |

### 2a. The separator and the grammar

| Option | `alert.fired` in `oncall` | Unambiguous | Notes |
|---|---|---|---|
| **`:` (recommended)** | `oncall:alert.fired` | Yes | The family already writes owners this way: RFC-0002's `reflexr:<rule>` client ids, and its `thread:` and `chain:` tags |
| `/` | `oncall/alert.fired` | Yes | Reads as a path and suggests nesting. It would need escaping if an endpoint ever took a type in its path |
| `.` | `oncall.alert.fired` | No | pydantic-ai's join. But local names already use dots (`service.error`), so only the registry could say where a namespace ends |
| A separate `namespace` field | `"namespace": "oncall"` beside `"type": "alert.fired"` | Yes | Every filter becomes a list of pairs, and `namespace` becomes a reserved field on every event |

```text
":"       rule      {"kind": "on", "types": ["oncall:alert.fired"]}
          publish   {"type": "publish", "event": {"type": "oncall:alert.fired", "service": "api", ...}}
          envelope  {"seq": 7, ..., "event": {"type": "oncall:alert.fired", ...}}, event_type = 'oncall:alert.fired'

"/"       rule      {"kind": "on", "types": ["oncall/alert.fired"]}
          publish   {"type": "publish", "event": {"type": "oncall/alert.fired", "service": "api", ...}}
          envelope  {"seq": 7, ..., "event": {"type": "oncall/alert.fired", ...}}, event_type = 'oncall/alert.fired'

"."       rule      {"kind": "on", "types": ["oncall.alert.fired"]}
          publish   {"type": "publish", "event": {"type": "oncall.alert.fired", "service": "api", ...}}
          envelope  {"seq": 7, ..., "event": {"type": "oncall.alert.fired", ...}}, event_type = 'oncall.alert.fired'

field     rule      {"kind": "on", "types": [{"namespace": "oncall", "name": "alert.fired"}]}
          publish   {"type": "publish", "event": {"namespace": "oncall", "type": "alert.fired", "service": "api", ...}}
          envelope  {"seq": 7, ..., "event": {"namespace": "oncall", "type": "alert.fired", ...}}, plus an event_namespace column
```

The recommended grammar:

- **A namespace** is lowercase letters, digits and underscores, starting with a letter, like a Python package name.
- **A local name** is one or more such segments joined by dots, as names are written today.
- **A name** is a local name, or a namespace, a `:` and a local name: `^([a-z][a-z0-9_]*:)?[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$`. Every name in the repository matches it, and so do derived names such as `service_error`.
- **Where it's checked:** when a type is defined, and in rules, `hello.types`, REST's `type=` and MCP's `types`. The JSON Schemas carry the pattern, so a rule drafted in chat is checked against it. Stored envelopes aren't checked again as they are read.
- **Room to grow:** `*` is outside the grammar, so a namespace wildcard (`artifactr:*`) could come later without clashing with a name. It isn't proposed here.
- **Storage** keeps the qualified name in the one `event_type` column. A namespace filter, if one comes, is a range on the existing index: `'artifactr:' <= event_type < 'artifactr;'`.

### 2b. How unqualified names resolve

| Option | Application types | Spellings of one type |
|---|---|---|
| Every name qualified: applications declare a namespace too | All change | One |
| **A bare name is the application's; nothing is filled in (recommended)** | Unchanged | One |
| A default namespace per `Workspaces` fills in bare names | Unchanged as sent, qualified as stored | Two |

```text
every name qualified
          rule      {"kind": "on", "types": ["oncall:alert.fired"]}
          publish   {"type": "publish", "event": {"type": "oncall:alert.fired", ...}}     a bare name is refused
          envelope  {"seq": 7, ..., "event": {"type": "oncall:alert.fired", ...}}, event_type = 'oncall:alert.fired'

a bare name is the application's (recommended)
          rule      {"kind": "on", "types": ["alert.fired", "artifactr:feedback_given"]}
          publish   {"type": "publish", "event": {"type": "alert.fired", ...}}
          envelope  {"seq": 7, ..., "event": {"type": "alert.fired", ...}}, event_type = 'alert.fired'

a default namespace per Workspaces
          declare   Workspaces(storage, namespace="oncall")                              (hypothetical)
          rule      {"kind": "on", "types": ["alert.fired"]}                             checked as oncall:alert.fired
          publish   {"type": "publish", "event": {"type": "alert.fired", ...}}
          envelope  {"seq": 7, ..., "event": {"type": "oncall:alert.fired", ...}}        not what was sent
```

A default namespace gives one type two spellings. Every input would have to normalize names (rules, `hello.types`, `type=`, MCP, the agent's tools), and the result would depend on which `Workspaces` it reached. When a bare name is the application's, what a client sends is what is stored and matched, and nothing resolves.

The cost: "libraries declare a namespace" is a convention reflexr can't enforce, since it can't tell a library from an application. The import error for a duplicate name will say to declare one.

### 2c. reflexr's own facts

| Option | Bare names left to the application | Stored data | Refusing a publish |
|---|---|---|---|
| **`reflexr:` (recommended)** | All | Migration 0004 rewrites thirteen names | The `reflexr` namespace is never published |
| Bare, as today | All but thirteen | Unchanged | `isinstance(event, SYSTEM_EVENTS)`, as today |

```text
reflexr: (recommended)
          rule      {"kind": "on", "types": ["reflexr:run_dead_lettered"]}
          publish   {"type": "publish", "event": {"type": "reflexr:run_dead_lettered", ...}}     forbidden, as today
          envelope  {"seq": 31, "actor": {"kind": "system", "name": "reflexr"}, ..., "event": {"type": "reflexr:run_dead_lettered", "run_id": "...", ...}}
          column    event_type = 'reflexr:run_dead_lettered'

bare
          rule      {"kind": "on", "types": ["run_dead_lettered"]}
          publish   {"type": "publish", "event": {"type": "run_dead_lettered", ...}}             forbidden
          envelope  {"seq": 31, "actor": {"kind": "system", "name": "reflexr"}, ..., "event": {"type": "run_dead_lettered", ...}}
          column    event_type = 'run_dead_lettered'
```

This is the closest call of the eight. With `reflexr:`, "a bare name is the application's" has no exceptions. An application can have its own `tick` or `run_started`, as a CI integration would want. A combined log reads `reflexr:feedback_given` beside `artifactr:feedback_given`. And reflexr's facts become the first reserved namespace (4). Keeping them bare costs nothing now, and matches artifactr, whose own events are bare. But then the application's names exclude thirteen that its authors have to know.

Migration 0004 rewrites `event_type`, and the envelope's `event.type`, for those thirteen names. It works in Python batches, to stay dialect-neutral ([ADR-0030](0030-sql-storage.md)), and its downgrade reverses it. Nothing else stores type names: dead letters keep a `seq`, and rule progress keeps a hash.

### 3. Compatibility

The libraries are pre-1.0 and pinned by GitHub revision. Nothing is released.

| Option | For | Against |
|---|---|---|
| **A clean break, no aliases, still `reflexr.v1` (recommended)** | One spelling from the first release | JSON rules and clients that name reflexr's facts change once |
| Aliases for a release: bare fact names accepted on input, then normalized | Old JSON rules and filters keep working | Normalizing at every input, as 2b's default namespace would, and code to remove later |
| `reflexr.v2`, served beside v1 | Follows the protocol's rule that v1 changes are additive | Two protocols before either has a user |

```text
clean break (recommended)
          rule      {"kind": "on", "types": ["run_dead_lettered"]}
                    fails at startup: no event type 'run_dead_lettered' (did you mean 'reflexr:run_dead_lettered'?)
          publish   {"type": "publish", "event": {"type": "alert.fired", ...}}           unchanged
          hello     {"type": "hello", "types": ["run_succeeded"], ...}                   matches nothing
          envelope  {"seq": 31, ..., "event": {"type": "reflexr:run_dead_lettered", ...}}, rewritten by migration 0004

aliases
          rule      {"kind": "on", "types": ["run_dead_lettered"]}                       accepted as reflexr:run_dead_lettered
          publish   {"type": "publish", "event": {"type": "alert.fired", ...}}           unchanged
          hello     {"type": "hello", "types": ["run_succeeded"], ...}                   treated as reflexr:run_succeeded
          envelope  {"seq": 31, ..., "event": {"type": "reflexr:run_dead_lettered", ...}}, always stored qualified

reflexr.v2 beside v1
          rule      {"kind": "on", "types": ["reflexr:run_dead_lettered"]}               rules are code, not protocol
          publish   {"type": "publish", "event": {"type": "alert.fired", ...}}           unchanged in both
          hello     {"type": "hello", "protocol": "reflexr.v1", ...}                     facts sent with bare names
          envelope  {"seq": 31, ..., "event": {"type": "reflexr:run_dead_lettered", ...}}, stored qualified, sent as stored in v2
```

What changes under the recommendation:

| What | Change |
|---|---|
| Application types without a namespace | Nothing |
| `Workspaces(events=, emitted=)` and `EventContext(emit=)` | Nothing: they take classes |
| Rules in Python, such as `on(RunDeadLettered)` | No change to the source. Their definition hash changes, so rules on facts reset once and start afresh at the head ([ADR-0026](0026-the-reactors-evaluation.md)) |
| Rules as JSON that name facts | Renamed. Until then, startup fails with the hint above |
| The JSON Schemas | A `pattern` on names in both, regenerated. The `$id`s stay |
| Clients that filter on facts (`hello.types`, `type=`, MCP) | Renamed. Old names match nothing. The only clients are oncall's CLI and tests |
| Stored envelopes | Migration 0004 |
| MCP | `_RUN_FACTS` comes from the classes, not the `run_` prefix |
| Dashboards | Legends show qualified names. The queries don't change |
| Names outside the grammar | Fail at import. The repository has none |
| oncall | Declares `oncall`, keeping ADR-0031's local names. Its CLI, runbook and tests use `oncall:` names |

The protocol document says v1 changed before its first release, and the commit is marked breaking (`feat(core)!:`).

### 4. Reserved publishers (#72)

Yes, a namespace can carry who may publish. It's the natural unit: a bridge owns a whole namespace, and reflexr owns its facts.

| Option | Declared by | Covers |
|---|---|---|
| On the namespace's declaration | The type's owner, in code | Whole namespaces only |
| Per type, as #72 is filed | The application | One type at a time |
| **Per namespace, with per-type exceptions, on `Workspaces` (recommended)** | The application | Both |

Only the application resolves clients to actors, and reserving a type to `SourceActor("artifactr")` means nothing if a client can be resolved to it. So the policy belongs on `Workspaces`, beside `events=` and `emitted=`, and a library exports its entry for the application to pass. The `reflexr` namespace stays reserved in code. Per-type entries cover the exceptions. For example, an application's projection of an artifactr `app_event` is in the application's namespace (5), but only relayr publishes it.

The cost: an application that forgets relayr's entry leaves forging open. relayr's wiring helper should pass it. #72 designs the policy's values (actors, sources, runs), and may fold `emitted=` into it. The declarations below are illustrations of shape, not proposals of API. The wire is the same under all three:

```text
declaration        class ArtifactrEvent(Event, abstract=True, namespace="artifactr", publishers=[SourceActor(name="artifactr")])
per type           Workspaces(storage, reserved={ProposalResolved: [SourceActor(name="artifactr")], ...})
per namespace      Workspaces(storage, publishers={"artifactr": [SourceActor(name="artifactr")], "plan_approved": [SourceActor(name="artifactr")]})

rule      {"kind": "on", "types": ["artifactr:proposal_resolved"]}
publish   {"type": "publish", "event": {"type": "artifactr:proposal_resolved", ...}}      from a user: forbidden
envelope  {"seq": 40, "actor": {"kind": "source", "name": "artifactr"}, ..., "event": {"type": "artifactr:proposal_resolved", ...}}
```

### 5. relayr's bridged names

| Option | `message_posted` becomes | For | Against |
|---|---|---|---|
| **`artifactr:` (recommended)** | `artifactr:message_posted` | Names where the fact happened. A different bridge would keep the names, and rules with them | The namespace isn't named after relayr, which declares it |
| `relayr:` | `relayr:message_posted` | Names the declarer | Names the plumbing. Rules would change if the bridge did |
| Flat prefixes, RFC-0002's placeholders | `artifactr.message_posted` | Works today | 1a's convention: no owner, nothing for #72 |

| artifactr event | Bridged as |
|---|---|
| `message_posted` | `artifactr:message_posted` |
| `artifact_created`, `artifact_changed`, `artifact_archived` | `artifactr:artifact_created` and so on |
| `proposal_created`, `proposal_resolved` | `artifactr:proposal_created`, `artifactr:proposal_resolved` |
| `run_ended` | `artifactr:turn_ended`, keeping RFC-0002's rename, so "run" means one thing to rule authors |
| `feedback_given` | `artifactr:feedback_given` |
| An application's `app_event`s and artifact kinds | The application's own types, in its namespace (bare by default), reserved to relayr type by type (4) |

relayr declares `artifactr` once, on its base, so nothing else can. RFC-0002's `install-rule`:

```text
artifactr: (recommended)
          rule      {"kind": "on", "types": ["artifactr:proposal_resolved"]}
          publish   {"type": "publish", "id": "<the artifactr envelope's id>", "event": {"type": "artifactr:proposal_resolved", "proposal_id": "prp_3", "decision": "accept", "author": {...}, ...}}
          envelope  {"seq": 40, "actor": {"kind": "source", "name": "artifactr"}, ..., "event": {"type": "artifactr:proposal_resolved", ...}}, event_type = 'artifactr:proposal_resolved'

relayr:   rule      {"kind": "on", "types": ["relayr:proposal_resolved"]}
          publish   {"type": "publish", "id": "...", "event": {"type": "relayr:proposal_resolved", ...}}
          envelope  {"seq": 40, ..., "event": {"type": "relayr:proposal_resolved", ...}}, event_type = 'relayr:proposal_resolved'

flat      rule      {"kind": "on", "types": ["artifactr.proposal_resolved"]}
          publish   {"type": "publish", "id": "...", "event": {"type": "artifactr.proposal_resolved", ...}}
          envelope  {"seq": 40, ..., "event": {"type": "artifactr.proposal_resolved", ...}}, event_type = 'artifactr.proposal_resolved'
```

## Trade-off analysis

Qualified names solve all three collisions with one mechanism. A registry per `Workspaces` solves one of them, and costs more, because names are resolved in places no `Workspaces` reaches. Making a bare name the application's leaves every application type unchanged, and gives every type one spelling, so nothing on the wire depends on configuration. `:` is the one separator that can't be confused with the dots names already use, without adding a field. Moving reflexr's facts costs one migration now, and leaves no exceptions later. The protocol hasn't shipped, so this is the cheapest moment for a break.

## What artifactr needs

No matching change.

- **Its event types can't collide.** artifactr's own events are a closed union that only artifactr defines, and nothing registers event types in an artifactr process.
- **The protocols share a shape, not a vocabulary.** `event.type` stays a string, and a client that speaks both protocols already treats it as an opaque one. A `:` in reflexr's names changes nothing for that client.
- **relayr does the renaming.** It maps artifactr's names to `artifactr:` names, so artifactr renames nothing.
- **Optional:** artifactr's docs could suggest the same `namespace:name` form for `app_event.name`, which no registry checks, when plugins share a process. It costs nothing, and relayr doesn't need it.
- **Separate:** artifactr's artifact kinds and feedback types, like reflexr's feedback types, sit in flat, process-wide registries too. relayr's `rule` artifact kind (RFC-0002's phase 5) could meet an application's own `rule`. If 2a is accepted, the same grammar can apply there, through an issue in each library. Phase 1 doesn't need it.

## Consequences

- Easier: libraries, plugins and bridges define types without agreeing on names, and every name on the wire says who owns it.
- Easier: relayr's names are final from phase 1, and #72 can reserve a namespace in one entry.
- Easier: oncall and reflexr's test suite share a process, whatever their local names.
- Harder: a name is bare or qualified, and readers learn which is whose. JSON rules on reflexr's facts get longer. One migration.
- To revisit:
  - a registry per `Workspaces`, if two applications ever need the same bare names in one process
  - namespace wildcards in `on`, `hello.types` and `type=`
  - feedback types (`feedback_given.feedback_type`), whose registry has the same shape; relayr's phase 6 registers some

## Action items

Once the questions are decided:

1. [ ] Core: `namespace=` on abstract bases, declared once; the grammar; `reflexr` for facts; a hint in `InvalidRule` and `not_found` for a bare fact name.
2. [ ] Schemas: the pattern on names; regenerate both.
3. [ ] SQL: migration 0004.
4. [ ] MCP: run facts from the classes.
5. [ ] oncall: the `oncall` namespace, keeping ADR-0031's local names.
6. [ ] Docs: the protocol's fact table and a note on v1, the architecture's events section and its table of conventions shared with artifactr, and the guides.
7. [ ] Then: #72 on namespace keys, relayr's phase 1 with `artifactr:` names, and RFC-0002's tracking item in stackr.
