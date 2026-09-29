# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Features

- **agent**: Infer decisions' and forks' input types for graph checkpoints ([#52](https://github.com/alexnodeland/reflexr/pull/52))

### Bug fixes

- **core**: Check a filter's fields on the event types of their own conjunction, so a sequence step can filter on its own type's fields

### Documentation

- Add the reflexr design: architecture, protocol, RFC-0001 and ADRs ([#14](https://github.com/alexnodeland/reflexr/pull/14))
