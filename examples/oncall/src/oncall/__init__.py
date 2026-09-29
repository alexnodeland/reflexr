"""oncall: incident response on reflexr.

Monitoring publishes alerts, CI publishes deploys and services send heartbeats. Rules watch the
log: a burst of severe alerts runs a triage agent, which opens an incident; an opened incident
runs a runbook graph, which rolls back a recent deploy or pages a person, then resolves it; and
a service that stops sending heartbeats is paged.

The reference implementation of reflexr. It uses only the library's public API.
"""
