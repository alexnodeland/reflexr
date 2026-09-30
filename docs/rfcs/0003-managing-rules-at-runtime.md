# RFC-0003: Managing rules at runtime

**Status:** Accepted
**Author:** Alex Nodeland
**Created:** 2026-09-29
**Discussion:** [#75](https://github.com/alexnodeland/reflexr/pull/75), from [issue #21][issue-21]; accepted on 2026-09-29, with the decisions in [Decisions](#decisions)
**Siblings:**

- [stackr RFC-0002][s-rfc-0002], the combined system. Its phase 5 installs rules that people accept in chat through the API designed here, and its D5 makes the artifact the reviewed record and reflexr's store the running definition.
- [ADR-0039](../adr/0039-namespaced-event-types.md) qualifies every event type (`oncall:deploy.completed`). This RFC qualifies every rule name the same way.

## Summary

Rules are typed, serializable data ([ADR-0006][adr-0006]), but they're registered in code, and every tenant's workspaces evaluate the same ones ([ADR-0016][adr-0016], as amended). This RFC adds **stored rules**: the same `Rule`, kept in storage for one workspace, installed, updated and archived through three commands, and seen by running reactors at their next batch.

It is the smallest design that serves stackr RFC-0002's rules from chat, and it reuses what reflexr has: the `Rule` model and `Rule.check`, core's `begin` and `reset`, generations and `reflexr:rule_reset`, `replay_rule`, the one command handler, and `due_runs` as it is. A simplicity review cut the draft's twenty-two questions to six choices, and the maintainer decided them on 2026-09-29.

## Motivation

`Workspaces(storage, rules=[...])` takes the rules at construction, so changing one means a deploy, and every authenticated client of any tenant can read every rule. ADR-0006 left runtime rule management for after v0.1.

stackr's RFC-0002, accepted on 2026-09-29, needs it. A person writes in a thread, "Whenever a deploy to production fails, tell me here." The chat agent drafts a `rule` artifact, a person accepts it, and relayr, the bridge, re-reads artifactr and installs the rule in reflexr with its provenance. That RFC requires:

- the artifact as the reviewed record, reflexr's store as the running definition, and drift noticed both ways (its D5)
- a tenant's rules never listed to other tenants
- a required throttle, no way to raise the depth limit, and only registered actions from an allowlist, never code

## Design

### An example

Installing the rule from the scenario over REST (`POST /v1/workspaces/prod/commands`). The WebSocket takes the same frame, and MCP's `install_rule` tool the same `rule` and `provenance`:

```json
{
  "type": "command",
  "command_id": "install-prp_12",
  "command": {
    "type": "install_rule",
    "rule": {
      "name": "chat:prod-deploy-failures",
      "description": "Whenever a deploy to production fails, tell me here.",
      "when": {
        "filter": {"kind": "all", "of": [
          {"kind": "on", "types": ["oncall:deploy.completed"]},
          {"kind": "where", "field": "environment", "op": "eq", "value": "production"},
          {"kind": "where", "field": "status", "op": "eq", "value": "failed"}
        ]},
        "throttle": {"at_most": 1, "per": "PT15M"}
      },
      "then": {"action": "notify", "params": {"thread_id": "thr_4"}},
      "ordering": "none",
      "timeout": "PT1M"
    },
    "provenance": {"source": "artifactr", "artifact": "art_rule_7", "artifact_version": 3, "proposal": "prp_12", "approved_by": "user:ada"}
  }
}
```

It answers `{"type": "rule_version", "rule": "chat:prod-deploy-failures", "version": 1, "seq": 812, "duplicate": false}`. In the same transaction the log gains the fact, attributed to relayr's `install-rule` run and continuing the chain of the acceptance that caused it:

```json
{
  "type": "event", "seq": 812,
  "actor": {"kind": "agent", "rule": "relayr:install-rule", "run_id": "fir_5c1e", "name": "install-rule"},
  "causation": {"firing_id": "fir_5c1e", "run_id": "fir_5c1e", "depth": 2},
  "event": {
    "type": "reflexr:rule_installed",
    "rule": "chat:prod-deploy-failures",
    "version": 1,
    "spec": {"name": "chat:prod-deploy-failures", "...": "..."},
    "provenance": {"source": "artifactr", "artifact": "art_rule_7", "...": "..."}
  }
}
```

The rule's cursor starts at 812, its own fact, so it acts only on deploys logged after it.

### Stored rules

- **One workspace.** A stored rule belongs to the workspace it's installed in, like everything else a rule has there: its cursor, state, runs and dead letters. A change takes the workspace's lock, so it's atomic with its fact and the rule's progress, and serialized with evaluation.
- **One row, with the log as history.** Storage keeps each rule's current version. Every change is a new version, numbered from 1 per workspace and name, and a `reflexr:rule_installed` fact carries the whole rule and its provenance. The facts are the history: the version that fired a run is the latest one before its firing, and rolling back is updating to an earlier version's rule.
- **Resets as code rules reset.** A new condition or scope (`Rule.definition()`) resets the rule. Core's `begin` and `reset` are called in the change's transaction, as `meet_rules` and `replay_rule` call them, so the rule starts or resets at its own fact, and `reflexr:rule_reset` follows `reflexr:rule_installed` in the log. Nothing published before a reactor's next pass is missed.
- **No replay on install.** `start` is `now`. The existing `replay_rule` rebuilds a rule's state if someone wants it warm.
- **A run executes the current version**, read when the run is claimed, as for code rules.
- **Archive is the only way to stop.** Stored rules are always enabled, run with `ordering: "none"`, and never block on a dead letter. So `due_runs`, `RunPolicy` and their schema stay as they are: a rule the policy doesn't name has all its due runs returned, which is right for an unordered, enabled rule. Archiving cancels the rule's unfinished runs with core's `cancel`, in its own transaction, and clears its scope states. Its progress row stays, so a rule installed again under the same name continues its generation and never reuses a firing id.
- **Watching is as for code rules.** A stored rule may watch any event type its `Workspaces` accepts, and `Rule.check` resolves its types and fields in that `Workspaces`' registry ([ADR-0039](../adr/0039-namespaced-event-types.md)).

### Configuration

Stored rules are off unless `Workspaces` is given one fixed configuration:

```python
async def allow(
    tenant_id: TenantId, workspace_id: WorkspaceId, actor: Actor, change: RuleChange
) -> bool:
    return isinstance(actor, AgentActor) and actor.rule == "relayr:install-rule"


workspaces = Workspaces(
    storage,
    rules=[...],
    stored_rules=StoredRules(
        # Shaped like authorize, with the change.
        allow=allow,
        # The fixed allowlist, with each action's parameters model.
        actions={"notify": NotifyParams},
        # The rule namespaces stored rules may use.
        namespaces={"chat"},
    ),
)
```

- **`allow`** is asked for every change, after the surfaces' `authorize`. `RuleChange` says which command, which rule, and the new rule if there is one. `False` is `forbidden`. Without `stored_rules`, every change is `forbidden`.
- **`actions`** is the only allowlist. A stored rule may name only these actions, and its parameters are checked against their models. An action without parameters maps to `None`.
- **`namespaces`** are the rule namespaces stored rules may use. `Workspaces` refuses a code rule in one of them at construction, so a stored rule and a code rule can never share a name.
- **Everything else is a constant in core**, the same for every tenant:

| Limit | Value |
|---|---|
| `throttle` | Required, at most 60 firings an hour |
| Scope fields, predicates, the `matches` operator | None |
| `start` | `now` |
| `enabled`, `ordering`, `on_dead_letter` | `true`, `"none"`, `"continue"` |
| `retry.max_attempts` | At most 5 |
| `timeout` | Required, at most 5 minutes |
| Windows: `within`, a dedupe window, a throttle's `per` | At most 1 day |
| `description` | At most 2,000 characters |
| The rule's JSON | At most 16 KiB |
| Provenance | At most 4 KiB |
| Active stored rules per workspace | At most 50 |

With no scope fields, the one throttle bounds the whole rule. `matches` is refused because Python's `re` has no timeout and evaluation holds the workspace's lock. Usage limits stay on the registered action, such as `AgentAction(usage_limits=...)`, and the tenant's gateway budget applies through `[litellm]`. The depth limit is `Workspaces(max_depth=)`, and rules have no field that could raise it.

**`check_stored(rule, config)`** is a pure function in core, beside `Rule.check`. It returns every problem with a rule against the config and the constants. Installing and updating run both, and report every problem together as `validation_failed`, so relayr's artifact type can run the same two checks on a draft before it's proposed. The count of active rules is checked in the change's transaction.

### Rule names

Every rule name is qualified, code rules included, symmetric with ADR-0039: `oncall:triage`, `chat:prod-deploy-failures`. The pattern is ADR-0039's namespace, a `:`, and today's rule name, at most 100 characters: `^[a-z][a-z0-9_]*:[a-z0-9][a-z0-9._-]*$`. Code rules use the application's namespaces, stored rules the config's `namespaces`, and `reflexr` is reserved. Rule namespaces aren't event namespaces, though they can share a word.

Renaming the code rules is a clean break before the first release, like ADR-0039's, and it can land together with #45's implementation, which renames event types in the same places:

- **Stored progress.** A rule's name keys its progress, scope states, runs, dead letters, firing ids and facts. A renamed rule is a new rule: it starts at the head of the log, and the executor cancels the old name's unfinished runs as it cancels any unregistered rule's. There's no migration, since only the application knows its names. The only deployments are oncall and applications generated in development.
- **Examples and docs.** oncall's `triage`, `runbook` and `silence` become `oncall:triage`, `oncall:runbook` and `oncall:silence`. The guides' and the architecture's examples change the same way.
- **Rules that name rules.** Filters on reflexr's facts, such as `where(rule="triage")` in `reflexr.evals` and the evaluation guide, name the qualified rule.
- **Tests.** The suite's rules, and the conformance fixtures, whose expected firing ids derive from rule names and are regenerated.
- **Dashboards.** Their queries group by `reflexr_rule` and don't change. Legends show the qualified names, and the dashboard test is unaffected.
- **Neighbours.** Span names (`invoke_workflow oncall:triage`), Langfuse trace names and `rule:` tags, and LiteLLM metadata carry the new names. stackr RFC-0002's actor, `reflexr:<rule>`, becomes `reflexr:oncall:triage`, an opaque string to artifactr. stackr's template renames its rules when it pins the new revision.

### Parameters

"Tell me here" needs the rule to name its thread, since the chain begins at a deploy, not in a thread. So an action may declare a parameters model, and a rule passes values for it:

```python
class NotifyParams(BaseModel):
    thread_id: str


async def notify(reaction: Reaction[AppDeps], params: NotifyParams) -> None:
    await reaction.deps.bridge.notice(params.thread_id, reaction)


reactor = Reactor(workspaces, actions={"notify": with_params(notify, NotifyParams)}, deps=deps)
rule = Rule(name="oncall:deploy-failed", when=..., then=run("notify", thread_id="thr_4"))
```

- **`ActionRef` gains `params`**, a JSON object, empty by default, which `run(action, **params)` fills. It isn't part of `Rule.definition()`, so changing it never resets the rule.
- **The action port doesn't change.** `Action[D]` is still an async callable over `Reaction[D]`, and plain functions without parameters are still actions. An action declares a model through a `params` attribute:
  - `AgentAction(..., params=Model)` and `GraphAction(..., params=Model)` gain the field
  - `with_params(fn, Model)` adapts a function of `(Reaction[D], P)` into an action, and pyright checks that `fn`'s second parameter is `Model`
  - the reactor reads the attribute where there is one, through a runtime-checkable protocol
- **`Reaction[D]` gains `params`**, the validated model or `None`, and `params_as(Model)`, a generic method that returns it typed as `Model`. Functions get it typed as their second argument. Agents' tools and prompts, and graphs' nodes, call `reaction.params_as(Model)`.
- **Checked at startup and at install.** At construction, the reactor validates each code rule's `params` against its action's model, and `InvalidRule` lists the problems, as for a missing action. It also confirms that each action in `StoredRules.actions` is registered and declares that model. A stored rule's `params` are checked by `check_stored` at install.
- **Validated at each attempt,** into `Reaction.params`. If a deploy changes a model so that a stored rule's parameters no longer validate, the attempt fails permanently, with reason `invalid_params`. A stored rule whose action a deploy removed fails its attempts permanently too, with reason `unknown_action`, until it's updated or archived.

The trade-offs, plainly:

- **No call site becomes generic.** `Reaction[D]` and `Action[D]` keep their one type parameter, so no existing action, capability or `RunContext` changes. The price is that an agent or graph names its model twice, in `params=` and in `params_as`, and a mismatch is a `TypeError` when it runs, not a type error. Only functions, through `with_params`, are checked end to end.
- **The fully typed alternative costs more.** It would be `Reaction[D, P]` with a default for `P`. A type parameter default needs PEP 696, whose syntax (`[D, P = None]`) needs Python 3.13. reflexr supports 3.12, so `Reaction` would move to `typing_extensions.TypeVar` and `Generic`. Every action type would gain a parameter: `Action`, `AgentAction[D, O]`, `GraphAction[D, S, I, O]` and `RunContext`.
- **The model is named twice for stored rules**, on the action and in `StoredRules.actions`, because installs are checked where no reactor is. The reactor's startup check keeps the two from disagreeing.

### Commands

Three commands go through `reflexr.workspace.execute`, like every other command, and are `Workspace` methods in process:

| Command | Fields | Does |
|---|---|---|
| `install_rule` | `rule`, `provenance?` | Version 1, or the next version of an archived rule. An active rule of that name is `invalid_state` |
| `update_rule` | `rule`, `expected_version?`, `provenance?` | A new version, reset if its definition changed |
| `archive_rule` | `rule` (the name), `expected_version?`, `reason?` | Archives it, and cancels its unfinished runs |

- **Each answers `rule_version`:** `{rule, version, seq, duplicate}`.
- **Duplicates.** A change whose result equals the current version creates nothing, appends nothing, and answers `duplicate: true`. That's checked before `expected_version`, so a retry of a change that succeeded is a duplicate, not a conflict.
- **`expected_version`** is optimistic concurrency. relayr passes the version it last installed, and `invalid_state`, naming the current version, tells it that someone else changed the rule.
- **Provenance** is an opaque JSON map, at most 4 KiB, stored on the row and on each fact. reflexr doesn't read it. Who made the change is the envelope's actor, which reflexr authenticated.
- **Rejections:**
  - `forbidden`: `allow` refused, there's no config, or the name is a code rule's
  - `not_found`: no stored rule has that name
  - `invalid_state`: the name is active, or the version isn't `expected_version`
  - `validation_failed`: `Rule.check` or `check_stored` found problems, each listed in `errors`, or the workspace has 50 active stored rules

### How reactors see changes

One seam: **`rules_in`** returns a workspace's rules, the code rules and its active stored rows. It reads through the storage call or the transaction at hand. The reactor's pass, each evaluation batch, each claim, `replay_rule` and `rule_statuses` use it and nothing else, so they all see the same rules.

- **Every pass, batch and claim reads them.** There is no cache, and no process can run a replaced or archived version. A change lands between two of the evaluator's batches, since both take the workspace's lock, and takes effect by the next pass: within `serve()`'s poll interval, one second by default.
- **No new lease.** A change takes the workspace's lock, as a publish does, and doesn't wait for the evaluation lease.
- **A rule doesn't see its own changes.** `reflexr:rule_installed` and `reflexr:rule_archived` are facts about a rule, so it never sees them, as with its other facts ([ADR-0010][adr-0010]). Other rules can watch them. relayr does, and proposes to the artifact any version whose provenance isn't its own. That, with `expected_version`, is D5's drift detection both ways.

### Listing and tenancy

Stored rules are keyed by tenant and workspace, so no read can reach another tenant's.

| Read | REST | MCP |
|---|---|---|
| Code rules, for every tenant | `GET /v1/rules`, unchanged | `list_rules`, unchanged |
| Each rule's status, which gains `origin` (`code` or `stored`) and `version` | `GET /v1/workspaces/{workspace_id}/rules` | `rule_status` |
| One rule's definition, and a stored rule's version and provenance | `GET /v1/workspaces/{workspace_id}/rules/{rule}` | `get_rule`, new |

A stored rule's name also reaches spans, Langfuse and LiteLLM metadata, which are operators' tools, not tenants'. The security guide says so.

### Metrics

Rule names are allowed as metric attributes "since code bounds them", and stored rules aren't bounded by code. Seven metrics carry `reflexr.rule`, at about 60 series per rule and workspace, so 1,000 workspaces with 20 stored rules each would add about 1.2 million series.

So a stored rule records `reflexr.rule` as its namespace, such as `chat:*`, which the config bounds. `reflexr.evaluation.lag` is a gauge, so for stored rules the reactor records the maximum lag among a namespace's rules. Code rules record their qualified names as today. The dashboards' queries don't change, and per-rule detail for stored rules is in traces, Langfuse and the status read.

### Storage and migrations

One table, keyed like every other ([ADR-0030][adr-0030]):

| Table | Primary key | Columns |
|---|---|---|
| `reflexr_rules` | tenant, workspace, `name` | `version`, `status` (`active` or `archived`), and the JSON of `rule` and `provenance` |

- **Port.** `Transaction` gains `stored_rule(name)`, `stored_rules()` and `save_stored_rule(stored)`, and `Storage` gains `stored_rules(workspace)`. Rules are listed by name, and the primary key serves every read. `InMemoryStorage` keeps them in a dictionary, and the behaviour suite covers both.
- **Migration 0006** creates the table, after ADR-0039's 0005. It changes no existing rows. Its downgrade drops the table, and with it every stored rule. Their facts stay in the logs.

### Protocol and JSON Schema

| Where | Change |
|---|---|
| Commands | `install_rule`, `update_rule`, `archive_rule` |
| Outcomes | `rule_version`: `{rule, version, seq, duplicate}` |
| reflexr's facts | `reflexr:rule_installed`: `rule`, `version`, `spec` (the rule), `provenance`. `reflexr:rule_archived`: `rule`, `version`, `reason?`, `cancelled` (how many runs) |
| Rule statuses | `origin` and `version` |
| Rules schema | Rule names are qualified. `ActionRef` gains `params` |
| REST and MCP | `GET /v1/workspaces/{workspace_id}/rules/{rule}`. The `install_rule`, `update_rule`, `archive_rule` and `get_rule` tools |

Qualified rule names are a clean break alongside ADR-0039's qualified event types, before the first release. The rest is additive, so the protocol stays `reflexr.v1`. Both schemas are regenerated with `make schema`.

### Security

- **Closed by default.** Without `stored_rules`, every change is `forbidden`. With it, the surfaces ask `authorize`, then `allow`.
- **Only registered code runs,** from the config's fixed allowlist, with checked parameters. No predicates and no `matches`.
- **Bounded cost.** One throttle over the whole rule, and the constants on retries, timeouts, windows, size and count, beside the action's usage limits and the tenant's gateway budget.
- **Bounded loops.** Facts about a firing are one step deeper than what fired ([ADR-0026][adr-0026]), and `reflexr:rule_installed` continues the chain of the run that installed it.
- **Untrusted text.** A description reaches agent prompts, so the rule's gateway guardrails apply as for any run.
- **Provenance is a claim.** reflexr records who made a change and what they said. relayr re-reads artifactr before installing.
- **Forged triggers.** relayr's `install-rule` fires on `artifactr:proposal_resolved`. [#72][issue-72]'s publish policy can reserve `artifactr` to relayr, and `allow` admits only relayr's run, so a forged event costs a re-read, not a rule.

## Decisions

The maintainer decided these on 2026-09-29, after a simplicity review. Each table marks the choice in bold.

### Configuration and authorization

| Option | For | Against |
|---|---|---|
| **One fixed `StoredRules` config: `allow`, the actions and the namespaces, with every other limit a constant in core (decided)** | One place, one hook shaped like `authorize`, and a pure `check_stored` for drafts | The same limits for every tenant |
| A per-change policy returning `RuleLimits` | Limits per tenant, plan or installer | A second, larger hook, for variation nothing needs yet |
| Reuse `authorize` | Nothing new | Anyone who may use a workspace could install behaviour that runs in it |

### Parameters

| Option | For | Against |
|---|---|---|
| None: one registered action per behaviour | Nothing to build | A chat rule can't register an action per thread |
| Free JSON, passed to the action | Little to build | A draft a person accepted fails only when it fires |
| **Typed through the action port, checked at install and at startup (decided, against the review's recommendation)** | Bad parameters fail before a rule runs, for stored and code rules alike | A `params` field and a function adapter, and an agent or graph names its model twice |

### Commands

| Option | For | Against |
|---|---|---|
| **Three: `install_rule`, `update_rule`, `archive_rule` (decided)** | Each change is what it is, on every surface | Changing a rule means sending it whole |
| Five, with `disable_rule` and `enable_rule` | Pause without archiving | Stopping state, and the parts of `due_runs` that serve it |
| One upsert | One command | An update meant for an existing rule silently creates one |
| REST resources | Familiar | A second path beside the commands |

### Stopping

| Option | For | Against |
|---|---|---|
| **Archive only: stored rules always enabled, unordered, never blocking (decided)** | `due_runs`, `RunPolicy` and their schema stay as they are | No pause, and a scope's runs may overlap |
| Disable and enable | Pause and resume | `due_runs` would join stored rules to learn which are disabled |
| Ordered stored rules | A scope's runs in firing order | The same join, to keep [#57][issue-57]'s fix |

### Watching

| Option | For | Against |
|---|---|---|
| **Any event type the workspace accepts, as for code rules (decided)** | Nothing new | A chat rule may watch reflexr's facts |
| Namespaces the config grants | Least privilege | A list to keep, with no present need |
| The namespaces the tenant may publish into | One policy | Watching isn't publishing: chat rules watch `artifactr:` types, which only relayr publishes |

### Rule names

| Option | For | Against |
|---|---|---|
| One flat name space, with clashes refused | Nothing renamed | A tenant can take a name a later deploy needs |
| Qualified stored names, bare code rules | Nothing renamed | Two grammars for rule names |
| **Every rule name qualified, code rules included (decided)** | Symmetric with ADR-0039, and a clash is impossible, since stored namespaces are the config's | Every code rule is renamed |
| A stored rule overrides the code rule of the same name | Tenants can tune a rule | The code rule quietly stops running |

### Settled as the design

The review settled these as the design rather than as questions:

- one workspace per rule, and one row per rule with the facts as history
- `begin` and `reset` in the change's transaction
- no replay on install, and `start=now`
- a run executes the current version, read at claim
- provenance as an opaque map of at most 4 KiB
- stored rules read on every pass, batch and claim, through one lookup
- nothing new for the depth limit
- `GET /v1/rules` unchanged, with `origin`, `version` and `get_rule` added
- the namespace as the metric label

## Deferred

Each comes back when something needs it:

- rules for a whole tenant, and a tenant-level listing
- pausing a stored rule, and ordered stored rules
- limits or namespace grants per tenant or installer
- grants of watched namespaces
- a table of every version, once retention compacts logs, and recording the fired version on each run
- typed provenance, and finding a rule from its source
- a throttle across a rule's scopes, and a lower depth limit for stored rules
- per-rule metrics for stored rules

## What the siblings need

- **relayr** (stackr RFC-0002, phase 5):
  - checks drafts with `Rule.check` and `check_stored`
  - installs with provenance, and updates with `expected_version`
  - archives the rule when its artifact is archived
  - proposes to the artifact any `reflexr:rule_installed` whose provenance isn't its own
- **stackr:**
  - RFC-0002's "Archiving the artifact disables the rule" should read "archives": [stackr#37][s-37]
  - RFC-0002's open question "How far does a chat rule reach?" is answered: one workspace
  - the template's `bridge` option passes `StoredRules`, and its rules take qualified names
- **artifactr:** nothing.

## Drawbacks

- **Tenants get behaviour that runs.** It's bounded by the allowlist, the constants, review and a config that is off by default, but it's a new surface to defend.
- **Every code rule is renamed**, and loses its state and unfinished runs once, before the first release.
- **More reads.** Every pass, batch and claim reads a workspace's stored rules.
- **Chat rules can't pause, order their runs, or throttle per scope.**
- **Less detail in metrics** for stored rules, which share a series per namespace.
- **An agent or graph names its parameters model twice.**
- **Downgrading loses stored rules.**

## Alternatives

- **Hot-reloading code rules** from a file or table: no per-tenant rules, no versions and no facts.
- **Rules only in the log**, with no table: the facts hold every version, but every pass would scan logs to find a workspace's rules.
- **reflexr reads rules from artifactr:** ruled out by stackr's D5 and reflexr's [ADR-0003][adr-0003].
- **Code in stored rules**, such as sandboxed Python or an expression language: ADR-0006 keeps rules as data.

## Unresolved questions

- **Turning a code rule off in one workspace.** `enabled` is still the code's, for every workspace.
- **A preview read.** relayr previews a draft by replaying the recent log through `core.evaluate`. A read-only `preview_rule` would serve any client, and could be added without changing anything here.
- **Counting what throttles drop.** A rule held at its throttle is worth an alert, and no metric shows it today.

## Phases

Each phase is a series of small pull requests to `main`.

| Phase | Deliverable | Exit criteria |
|---|---|---|
| 1. Names and core, with #45's implementation | Qualified rule names, and the code rules renamed in oncall, the tests, the fixtures and the docs. `ActionRef.params` and parameters on the action port, `StoredRules`, `RuleChange`, and `check_stored` and its constants | A bare rule name fails at construction. A table of bad stored rules, each refused with every problem listed |
| 2. Storage | `reflexr_rules`, in memory and in SQL, with migration 0006 | The behaviour suite passes on memory, SQLite and PostgreSQL, and the migrations don't drift |
| 3. Workspace and reactor | `Workspaces(stored_rules=)`, the three commands and `get_rule` on `Workspace`, the two facts they append (`reflexr:rule_installed` and `reflexr:rule_archived`), `rules_in`, `begin` and `reset` in the change's transaction, archiving, and the namespace as the metric label | A rule installed while two reactors serve fires on the next matching event, even one published before either's next pass. An archived rule's runs are cancelled, and a rule installed again never reuses a firing id |
| 4. Surfaces | The commands, outcome and facts in the protocol and its schema, the REST read and the MCP tools | Contract tests for each command on every surface. No surface shows a stored rule to another tenant or workspace |
| 5. Docs and oncall | ADRs for the decisions, and the architecture, protocol and guides. oncall installs a stored rule | `make docs` passes. oncall's smoke test installs a rule over REST and sees it fire |

## Tracking

- [x] Decided on 2026-09-29, and recorded on [#21][issue-21]
- [x] #45 decided, and ADR-0039 accepted (#74)
- [x] Phase 1, qualified rule names: landed with #45's implementation (#83)
- [x] Phase 1, the rest of core: `ActionRef.params`, `StoredRules`, `RuleChange` and `check_stored`
- [ ] Phase 2: storage and migration 0006
- [ ] Phase 3: workspace and reactor
- [ ] Phase 4: surfaces
- [ ] Phase 5: docs, ADRs and oncall
- [x] [stackr#37][s-37]: "archives", in RFC-0002
- [ ] stackr RFC-0002's prerequisite row for #21 marked done, unblocking its phase 5

[adr-0003]: ../adr/0003-independent-sibling-of-artifactr.md
[adr-0006]: ../adr/0006-rules-as-typed-serializable-data.md
[adr-0010]: ../adr/0010-loop-and-spend-safety.md
[adr-0016]: ../adr/0016-tenants-and-workspaces-like-artifactr.md
[adr-0026]: ../adr/0026-the-reactors-evaluation.md
[adr-0030]: ../adr/0030-sql-storage.md
[issue-21]: https://github.com/alexnodeland/reflexr/issues/21
[issue-57]: https://github.com/alexnodeland/reflexr/issues/57
[issue-72]: https://github.com/alexnodeland/reflexr/issues/72
[s-37]: https://github.com/alexnodeland/stackr/issues/37
[s-rfc-0002]: https://github.com/alexnodeland/stackr/blob/main/docs/rfcs/0002-the-combined-system.md
