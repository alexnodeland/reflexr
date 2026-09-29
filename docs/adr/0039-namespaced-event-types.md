# ADR-0039: Namespaced event types

**Status:** Accepted
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

The maintainer decided the eight questions on 2026-09-29, as recorded on #45. Two differ from the draft's recommendations: namespaces live both on the wire and in a registry per `Workspaces` (1a), and every name is qualified, applications' included (2b).

| # | Question | Decided |
|---|---|---|
| 1a | Where namespaces live | Both: qualified names on the wire, and a registry per `Workspaces`, the process-wide one by default |
| 1b | How an owner declares one | `namespace=` on an abstract base, declared once per registry |
| 2a | Separator and grammar | `:`, exactly once; lowercase snake_case segments |
| 2b | Unqualified names | None: every name is qualified, applications' included |
| 2c | reflexr's facts | `reflexr:rule_fired` and so on, with migration 0004 |
| 3 | Compatibility | A clean break, no aliases, still `reflexr.v1` |
| 4 | Reserved publishers (#72) | A policy on `Workspaces`, per namespace, with per-type exceptions |
| 5 | relayr's names | `artifactr:message_posted` and so on, and `artifactr:turn_ended` for `run_ended` |

### Every name is qualified

A name is a namespace, a `:` and a local name, everywhere a name appears: `oncall:alert.fired`, `reflexr:rule_fired`, `artifactr:message_posted`. There are no bare names.

- **A namespace** is lowercase letters, digits and underscores, starting with a letter, like a Python package name.
- **A local name** is one or more such segments joined by dots, as names are written today.
- **The pattern** is `^[a-z][a-z0-9_]*:[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$`. `*` and uppercase are outside it, so a namespace wildcard (`artifactr:*`) could come later without clashing with a name.
- **Where it's checked:** when a type is defined; in rules, including JSON rules as they load; in publishes; and in type filters (`hello.types`, REST's `type=`, MCP's `types`, `Workspace.read`). The JSON Schemas carry the pattern. Stored envelopes aren't checked again as they are read.
- **Storage** keeps the qualified name in the one `event_type` column.

### An owner declares its namespace once, on an abstract base

Applications declare theirs the same way as libraries, plugins and bridges:

```python
class OncallEvent(Event, abstract=True, namespace="oncall"):
    """Every oncall event."""


class AlertFired(OncallEvent, name="alert.fired"):  # oncall:alert.fired
    service: str
```

- Subclasses inherit the namespace. `name=` is the local part, or the snake-cased class name as before, and a `:` in it is refused.
- A concrete type with no namespace fails at definition.
- Declaring a namespace a second time in one registry fails at import, naming both declarations, as a duplicate name does today.
- reflexr declares `reflexr` for its facts, relayr `artifactr`, oncall `oncall`, and reflexr's tests `app`.

### A bare name fails with a hint

Old names fail loudly, and point at the new one. The hint lists each registered type with that local name; with none, it shows the form a name takes.

| Where | What happens |
|---|---|
| A class with no namespace | `TypeError`: `AlertFired has no namespace; subclass a base declared with namespace=..., such as class OncallEvent(Event, abstract=True, namespace="oncall")` |
| A rule, built or loaded from JSON | Invalid: `event type 'run_dead_lettered' has no namespace; did you mean 'reflexr:run_dead_lettered'?` |
| A `publish`, over any surface | `validation_failed`, with the same message |
| A type filter: REST's `type=`, MCP's `types`, `Workspace.read` | `validation_failed`, with the same message |
| `hello.types` | The stream closes with `4400`, as for any bad `hello` |

### A registry per `Workspaces`, the process-wide one by default

A registry's job is isolation. Namespaces keep owners apart within a registry; registries keep applications apart within a process. Two applications, or a test suite and an example, can each declare into a registry of their own. The same namespace and names can then exist in both as different types, and neither's workspaces accept the other's.

```python
ONCALL = EventRegistry()


class OncallEvent(Event, abstract=True, namespace="oncall", registry=ONCALL):
    """Every oncall event, kept out of the process-wide registry."""


workspaces = Workspaces(storage, registry=ONCALL, events=EVENTS)
```

- **What a registry holds:** types by qualified name, and which base declared each namespace. Every registry holds reflexr's facts.
- **`DEFAULT_REGISTRY`** holds every type whose namespace's base names no registry. `event_types()` and `get_event_type()` read it, as they read the one registry today.
- **Joining one:** a namespace's base passes `registry=`, and its types inherit it. `registry.add(*types)` includes types declared elsewhere, such as a library's.
- **What `Workspaces(registry=)` changes:** its rules are checked against the registry; `events=` and `emitted=` must be types in it; `events=None` accepts every type in it, rather than every type any import registered; and it resolves each event it publishes or reads through the registry. Resolving validates an event as the registry's type for its name, if it isn't one already. An unregistered name stays `UnknownEvent` on a read, and is `not_found` on a publish.
- **Everything else parses through the default registry, as today:** storage adapters, the surfaces' frames and bodies, and the builder's field checks. A type outside the default registry comes out of them as `UnknownEvent`, and its workspace resolves it. So its field errors are the workspace's `validation_failed`, rather than a parse error, and `Rule.check` checks its fields rather than the builder.
- **The rule this gives:** a name in the default registry means that type throughout the process. Applications that must not share names each use a registry of their own.
- **It is additive.** Without `registry=`, every type is in the default registry and every `Workspaces` uses it: the qualified-name design alone.

### reflexr's facts, compatibility, reserved publishers and relayr

- **reflexr's facts are `reflexr:*`.** The `reflexr` namespace is never published, which replaces the check on `SYSTEM_EVENTS`.
- **A clean break.** There are no aliases, and the protocol stays `reflexr.v1`, since nothing is released. Migration 0004 rewrites the thirteen facts' names in `event_type` and in the envelope's `event.type`, in Python batches to stay dialect-neutral ([ADR-0030](0030-sql-storage.md)); its downgrade reverses it. An application's stored types are its to rename, in its own migration. Rules on facts reset once, since their definition hash changes ([ADR-0026](0026-the-reactors-evaluation.md)).
- **Reserved publishers (#72)** will be a policy on `Workspaces`, beside `events=` and `emitted=`, keyed by namespace, with per-type exceptions. A library exports its entry for the application to pass. #72 designs the values, and may fold `emitted=` into it.
- **relayr bridges artifactr's events as `artifactr:<type>`,** and `run_ended` as `artifactr:turn_ended`. An application's projections of `app_event`s and artifact kinds are its own types, in its own namespace.

## Options considered

The examples use three types: an application's `alert.fired`, relayr's bridge of artifactr's `feedback_given` (a name reflexr's own fact holds today), and reflexr's `run_dead_lettered`. Each shows a rule's filter, a `publish` command, and the stored envelope with its `event_type` column. `...` stands for fields that don't change. The decided option is in bold.

### 1a. Where namespaces live

| Option | An application and a library in one `Workspaces` | Wire | Cost |
|---|---|---|---|
| Convention: flat names with agreed prefixes | By discipline | Unchanged | Nothing to build, and nothing enforced |
| A registry per `Workspaces`, the global one by default | Not solved: one log still needs one name per type | Unchanged | Names resolved in places no `Workspaces` reaches |
| Qualified names on the wire, in one registry | Solved | Names gain a namespace | The grammar, and a migration for reflexr's facts |
| **Both (decided)** | Solved | Names gain a namespace | Both, with a workspace resolving what the default registry doesn't hold |

**Convention**, as RFC-0002 wrote its placeholders. Nothing says `artifactr.` is a namespace or who owns it, so #72 has nothing to hang on:

```text
rule      {"kind": "on", "types": ["artifactr.feedback_given"]}
publish   {"type": "publish", "event": {"type": "artifactr.feedback_given", "feedback_type": "rating", ...}}
envelope  {"seq": 12, "actor": {"kind": "source", "name": "artifactr"}, ..., "event": {"type": "artifactr.feedback_given", ...}}
column    event_type = 'artifactr.feedback_given'
```

**A registry per `Workspaces`** alone. Two applications in one process could each have a `heartbeat`. But relayr's types and reflexr's facts share one log, so relayr would still need a prefix by convention:

```text
declare   registry.add(AlertFired, FeedbackGiven); Workspaces(storage, registry=registry)
rule      {"kind": "on", "types": ["feedback_given"]}      reflexr's fact, or artifactr's?
publish   {"type": "publish", "event": {"type": "feedback_given", ...}}
envelope  {"seq": 12, ..., "event": {"type": "feedback_given", ...}}
column    event_type = 'feedback_given'
```

**Qualified names** alone. One mechanism covers all three collisions, as long as each owner picks a different namespace:

```text
declare   class ArtifactrEvent(Event, abstract=True, namespace="artifactr")
          class FeedbackGiven(ArtifactrEvent)                              artifactr:feedback_given
rule      {"kind": "on", "types": ["artifactr:feedback_given"]}
publish   {"type": "publish", "event": {"type": "artifactr:feedback_given", "feedback_type": "rating", ...}}
envelope  {"seq": 12, "actor": {"kind": "source", "name": "artifactr"}, ..., "event": {"type": "artifactr:feedback_given", ...}}
column    event_type = 'artifactr:feedback_given'
```

**Both (decided).** The wire is the qualified names'. A registry adds isolation: the same namespace in two registries, and `events=None` scoped to one application's types. The draft recommended qualified names alone, adding a registry if it was ever needed. The maintainer chose isolation from the start, kept additive:

```text
declare   ONCALL = EventRegistry()
          class OncallEvent(Event, abstract=True, namespace="oncall", registry=ONCALL)
          Workspaces(storage, registry=ONCALL, events=EVENTS)
rule      {"kind": "on", "types": ["oncall:alert.fired"]}
publish   {"type": "publish", "event": {"type": "oncall:alert.fired", "service": "api", ...}}   resolved through ONCALL
envelope  {"seq": 7, ..., "event": {"type": "oncall:alert.fired", ...}}, event_type = 'oncall:alert.fired'
```

### 1b. How an owner declares its namespace

| Option | Declared | A typo | Precedent |
|---|---|---|---|
| In each name: `name="oncall:alert.fired"` | Per type | Quietly makes a new namespace for one type | None |
| **`namespace=` on an abstract base, declared once (decided)** | Once per owner | Is in the base, so every type shows it | pydantic-ai's `CapabilityEvent` |
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

pydantic-ai lets any class use a namespace, and refuses only duplicate kinds. Declaring once makes the owner something the registry knows, which #72 can rely on.

| Owner | Declares | Names |
|---|---|---|
| An application | Its own | `oncall:alert.fired` |
| reflexr | `reflexr`, in core | `reflexr:run_started` |
| A library or plugin | Its own | `acme_pager:page.sent` |
| A bridge (relayr) | The source system's | `artifactr:message_posted` |

### 2a. The separator and the grammar

| Option | `alert.fired` in `oncall` | Unambiguous | Notes |
|---|---|---|---|
| **`:` (decided)** | `oncall:alert.fired` | Yes | The family already writes owners this way: RFC-0002's `reflexr:<rule>` client ids, and its `thread:` and `chain:` tags |
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

### 2b. How unqualified names resolve

| Option | Application types | Spellings of one type |
|---|---|---|
| **Every name qualified: applications declare a namespace too (decided)** | All change | One |
| A bare name is the application's; nothing is filled in | Unchanged | One |
| A default namespace per `Workspaces` fills in bare names | Unchanged as sent, qualified as stored | Two |

```text
every name qualified (decided)
          rule      {"kind": "on", "types": ["oncall:alert.fired"]}
          publish   {"type": "publish", "event": {"type": "oncall:alert.fired", ...}}     a bare name fails, with a hint
          envelope  {"seq": 7, ..., "event": {"type": "oncall:alert.fired", ...}}, event_type = 'oncall:alert.fired'

a bare name is the application's
          rule      {"kind": "on", "types": ["alert.fired", "artifactr:feedback_given"]}
          publish   {"type": "publish", "event": {"type": "alert.fired", ...}}
          envelope  {"seq": 7, ..., "event": {"type": "alert.fired", ...}}, event_type = 'alert.fired'

a default namespace per Workspaces
          declare   Workspaces(storage, namespace="oncall")                              (hypothetical)
          rule      {"kind": "on", "types": ["alert.fired"]}                             checked as oncall:alert.fired
          publish   {"type": "publish", "event": {"type": "alert.fired", ...}}
          envelope  {"seq": 7, ..., "event": {"type": "oncall:alert.fired", ...}}        not what was sent
```

The draft recommended bare names for the application. That leaves application types unchanged, but rests on a convention reflexr can't enforce: that libraries declare a namespace and applications needn't. With every name qualified, a name always says its owner, there is no exception to remember, and a client or an agent drafting a rule never has to guess. A default namespace gives one type two spellings, so every input would have to normalize names.

### 2c. reflexr's own facts

| Option | Names left to applications | Stored data | Refusing a publish |
|---|---|---|---|
| **`reflexr:` (decided)** | All | Migration 0004 rewrites thirteen names | The `reflexr` namespace is never published |
| Bare, as today | All but thirteen | Unchanged | `isinstance(event, SYSTEM_EVENTS)`, as today |

```text
reflexr: (decided)
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

Once every name is qualified (2b), bare facts would be the only bare names, so this follows from 2b. A combined log reads `reflexr:feedback_given` beside `artifactr:feedback_given`, and reflexr's facts are the first reserved namespace (4).

### 3. Compatibility

The libraries are pre-1.0 and pinned by GitHub revision. Nothing is released.

| Option | For | Against |
|---|---|---|
| **A clean break, no aliases, still `reflexr.v1` (decided)** | One spelling from the first release | Every JSON rule, client and stored envelope changes once |
| Aliases for a release: old names accepted on input, then normalized | Old JSON rules and filters keep working | Normalizing at every input, as 2b's default namespace would, and code to remove later |
| `reflexr.v2`, served beside v1 | Follows the protocol's rule that v1 changes are additive | Two protocols before either has a user |

```text
clean break (decided)
          rule      {"kind": "on", "types": ["run_dead_lettered"]}
                    invalid: event type 'run_dead_lettered' has no namespace; did you mean 'reflexr:run_dead_lettered'?
          publish   {"type": "publish", "event": {"type": "alert.fired", ...}}           validation_failed, with the same hint
          envelope  {"seq": 31, ..., "event": {"type": "reflexr:run_dead_lettered", ...}}, rewritten by migration 0004

aliases
          rule      {"kind": "on", "types": ["run_dead_lettered"]}                       accepted as reflexr:run_dead_lettered
          publish   {"type": "publish", "event": {"type": "alert.fired", ...}}           accepted as oncall:alert.fired
          envelope  {"seq": 31, ..., "event": {"type": "reflexr:run_dead_lettered", ...}}, always stored qualified

reflexr.v2 beside v1
          rule      {"kind": "on", "types": ["reflexr:run_dead_lettered"]}               rules are code, not protocol
          publish   {"type": "publish", "event": {"type": "alert.fired", ...}}           accepted over v1 only
          envelope  {"seq": 31, ..., "event": {"type": "reflexr:run_dead_lettered", ...}}, sent bare over v1
```

What changes:

| What | Change |
|---|---|
| Application types | Declare a namespace on a base, and are published, filtered and stored by their qualified names |
| `Workspaces(events=, emitted=)` and `EventContext(emit=)` | Nothing: they take classes |
| Rules in Python, such as `on(RunDeadLettered)` | No change to the source. Their definition hash changes, so they reset once and start afresh at the head |
| Rules as JSON | Use qualified names. Old names fail with the hint |
| The JSON Schemas | The pattern on names in both, regenerated. The `$id`s stay |
| Clients | Publish and filter by qualified names. Old names fail with the hint |
| Stored envelopes | Migration 0004 renames reflexr's facts. Stored application types with old names read back as `UnknownEvent` until the application renames them |
| MCP | `_RUN_FACTS` comes from the classes, not the `run_` prefix |
| Dashboards | Legends show qualified names. The queries name no types, so they don't change |
| oncall | Declares `oncall`, keeping ADR-0031's local names |

The protocol document says v1 changed before its first release, and the change is marked breaking.

### 4. Reserved publishers (#72)

A namespace can carry who may publish. It's the natural unit: a bridge owns a whole namespace, and reflexr owns its facts.

| Option | Declared by | Covers |
|---|---|---|
| On the namespace's declaration | The type's owner, in code | Whole namespaces only |
| Per type, as #72 is filed | The application | One type at a time |
| **Per namespace, with per-type exceptions, on `Workspaces` (decided)** | The application | Both |

Only the application resolves clients to actors, and reserving a type to `SourceActor("artifactr")` means nothing if a client can be resolved to it. So the policy belongs on `Workspaces`, beside `events=` and `emitted=`, and a library exports its entry for the application to pass. The `reflexr` namespace stays reserved in code. Per-type entries cover the exceptions: an application's projection of an artifactr `app_event` is in the application's namespace (5), but only relayr publishes it.

The cost: an application that forgets relayr's entry leaves forging open, so relayr's wiring helper should pass it. #72 designs the policy's values (actors, sources, runs). The declarations below illustrate the shape, not an API, and the wire is the same under all three:

```text
declaration        class ArtifactrEvent(Event, abstract=True, namespace="artifactr", publishers=[SourceActor(name="artifactr")])
per type           Workspaces(storage, reserved={ProposalResolved: [SourceActor(name="artifactr")], ...})
per namespace      Workspaces(storage, publishers={"artifactr": [SourceActor(name="artifactr")], "plans:plan_approved": [SourceActor(name="artifactr")]})

rule      {"kind": "on", "types": ["artifactr:proposal_resolved"]}
publish   {"type": "publish", "event": {"type": "artifactr:proposal_resolved", ...}}      from a user: forbidden
envelope  {"seq": 40, "actor": {"kind": "source", "name": "artifactr"}, ..., "event": {"type": "artifactr:proposal_resolved", ...}}
```

### 5. relayr's bridged names

| Option | `message_posted` becomes | For | Against |
|---|---|---|---|
| **`artifactr:` (decided)** | `artifactr:message_posted` | Names where the fact happened. A different bridge would keep the names, and rules with them | The namespace isn't named after relayr, which declares it |
| `relayr:` | `relayr:message_posted` | Names the declarer | Names the plumbing. Rules would change if the bridge did |
| Flat prefixes, RFC-0002's placeholders | `artifactr.message_posted` | Works today | 1a's convention: no owner, nothing for #72 |

| artifactr event | Bridged as |
|---|---|
| `message_posted` | `artifactr:message_posted` |
| `artifact_created`, `artifact_changed`, `artifact_archived` | `artifactr:artifact_created` and so on |
| `proposal_created`, `proposal_resolved` | `artifactr:proposal_created`, `artifactr:proposal_resolved` |
| `run_ended` | `artifactr:turn_ended`, keeping RFC-0002's rename, so "run" means one thing to rule authors |
| `feedback_given` | `artifactr:feedback_given` |
| An application's `app_event`s and artifact kinds | The application's own types, in its namespace, reserved to relayr type by type (4) |

relayr declares `artifactr` once, on its base, so nothing else in its registry can. RFC-0002's `install-rule`:

```text
artifactr: (decided)
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

Qualified names solve all three collisions with one mechanism, and every name says who owns it. A registry per `Workspaces` adds what namespaces can't: two applications that use the same namespace in one process, and a `Workspaces` that accepts only its own application's types. It stays small by resolving in one place, the workspace, so storage adapters, surfaces and the builder don't change, and an application that never names a registry never sees one. The price is that a type outside the default registry is checked by its workspace rather than where it is parsed. Qualifying every name costs every application a base class and a rename, and buys names with no exceptions. `:` is the one separator that can't be confused with the dots names already use, without adding a field. The protocol hasn't shipped, so this is the cheapest moment for a break.

## What artifactr needs

No matching change.

- **Its event types can't collide.** artifactr's own events are a closed union that only artifactr defines, and nothing registers event types in an artifactr process.
- **The protocols share a shape, not a vocabulary.** `event.type` stays a string, and a client that speaks both protocols already treats it as an opaque one. A `:` in reflexr's names changes nothing for that client.
- **relayr does the renaming.** It maps artifactr's names to `artifactr:` names, so artifactr renames nothing.
- **Optional:** artifactr's docs could suggest the same `namespace:name` form for `app_event.name`, which no registry checks, when plugins share a process. It costs nothing, and relayr doesn't need it.
- **Separate:** artifactr's artifact kinds and feedback types, like reflexr's feedback types, sit in flat, process-wide registries too. relayr's `rule` artifact kind (RFC-0002's phase 5) could meet an application's own `rule`. The same grammar can apply there, through an issue in each library. Phase 1 doesn't need it.

## Consequences

- Easier: libraries, plugins, bridges and applications define types without agreeing on names, and every name on the wire says who owns it.
- Easier: two applications, or a test suite and an example, share a process, even with the same namespace, each in a registry of its own.
- Easier: relayr's names are final from phase 1, and #72 can reserve a namespace in one entry.
- Harder: every application declares a namespace, and every JSON rule, client and stored application envelope changes once.
- Harder: a name in the default registry must mean one type throughout the process, since storage and the surfaces parse through it.
- To revisit:
  - namespace wildcards in `on`, `hello.types` and `type=`
  - feedback types (`feedback_given.feedback_type`), whose registry has the same shape; relayr's phase 6 registers some
  - #72's policy on namespace keys

## Action items

1. [ ] Core: the grammar and its hints; `namespace=` and `registry=` on abstract bases; `EventRegistry` and `DEFAULT_REGISTRY`; reflexr's facts as `reflexr:*`.
2. [ ] Workspace: `Workspaces(registry=)`, resolving what it publishes and reads; type filters checked; MCP's run facts from the classes.
3. [ ] SQL: migration 0004, tested on SQLite and PostgreSQL.
4. [ ] Schemas: the pattern on names; regenerate both.
5. [ ] oncall: the `oncall` namespace, keeping ADR-0031's local names.
6. [ ] Docs: the protocol, the architecture, the guides and the getting-started page.
7. [ ] Then: #72 on namespace keys, relayr's phase 1 with `artifactr:` names, and RFC-0002's tracking item in stackr.
