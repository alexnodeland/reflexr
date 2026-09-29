# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Features

- **agent**: Infer decisions' and forks' input types for graph checkpoints ([#52](https://github.com/alexnodeland/reflexr/pull/52))
- **fastapi,mcp**: Show whether each rule is enabled in a workspace's rule status

### Bug fixes

- **core**: Check a filter's fields on the event types of their own conjunction, so a sequence step can filter on its own type's fields
- **workspace**: Join a causal chain only by its first event's id; a later event's id is `validation_failed`, naming its chain
- **evals**: Leave evaluators' verdicts out of `LogFeedbackSource` unless `include_evaluators=True`
- **mcp**: Ask an `authorize` hook, as the router does, on every tool call and resource read that names a workspace
- **fastapi**: List only the schedules that tick in the caller's tenant, with their targets narrowed to its workspaces
- **workspace**: Honour `Rule.enabled`: a disabled rule is not evaluated and its runs wait; `Storage.due_runs` takes the disabled rules (**breaking** for custom storage)

### Documentation

- **security**: Say plainly that every authenticated client of any tenant can read every rule and schedule definition, by design
- Add the reflexr design: architecture, protocol, RFC-0001 and ADRs ([#14](https://github.com/alexnodeland/reflexr/pull/14))
