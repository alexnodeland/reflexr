# reflexr

A Python library for **reactive agent workflows**: rules watch the event log of each workspace (tenants and workspaces, as in artifactr), and when a rule's condition holds (three errors from one service within a minute, a deploy followed by a spike, a heartbeat that stops), it runs a workflow: a [pydantic-ai](https://ai.pydantic.dev) agent, a pydantic-graph graph, or a plain async function.

It is the sibling of [artifactr](https://github.com/alexnodeland/artifactr): artifactr is for live chats in which people and agents edit shared artifacts, and reflexr is for workflows that events start.

> **Status:** pre-release. reflexr is being rebuilt from the Reflex template in the phases tracked by [RFC-0001](docs/rfcs/0001-v0.1-implementation-plan.md). Work from before the rebuild is on the `archive/pre-rebuild` branch.

## Documentation

- [Architecture](docs/architecture.md): concepts, layers, rules, deciding and acting, actions, safety, tenancy and storage.
- [Stream protocol v1](docs/protocol.md): the REST, WebSocket and MCP contracts (draft).
- [Architecture decision records](docs/adr/README.md): why each part is the way it is.
- [RFCs](docs/rfcs/README.md): proposals and the v0.1 build plan.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, the trunk-based workflow, and the RFC and ADR process.

## License

[MIT](LICENSE)
