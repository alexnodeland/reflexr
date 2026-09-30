"""Workspaces: tenant-scoped event logs, the storage protocol, and in-memory storage.

Open a :class:`Workspace` handle with :meth:`Workspaces.open`, and publish, read and operate
through it. Every surface hands commands to :meth:`Workspaces.execute`, which carries each out
through a handle, once per id. Rules are evaluated and runs executed by the ``Reactor``.
"""

from reflexr.workspace.actions import Action, Reaction, RunContext, RunFailure, with_params
from reflexr.workspace.memory import InMemoryStorage
from reflexr.workspace.reactor import EVALUATION_LEASE, REACTOR, Reactor, Settled
from reflexr.workspace.results import CommandKey, CommandResults, InMemoryCommandResults
from reflexr.workspace.schedules import SCHEDULER, Schedule, tick_id
from reflexr.workspace.storage import (
    RUN_LEASE_PREFIX,
    Clock,
    Entry,
    RunPolicy,
    Storage,
    Transaction,
    WorkspaceRef,
    run_lease,
    utc_now,
)
from reflexr.workspace.workspace import (
    Authorize,
    Published,
    RuleStatus,
    RuleVersion,
    ScheduleStatus,
    Workspace,
    WorkspaceRule,
    Workspaces,
)

__all__ = [
    "EVALUATION_LEASE",
    "REACTOR",
    "RUN_LEASE_PREFIX",
    "SCHEDULER",
    "Action",
    "Authorize",
    "Clock",
    "CommandKey",
    "CommandResults",
    "Entry",
    "InMemoryCommandResults",
    "InMemoryStorage",
    "Published",
    "Reaction",
    "Reactor",
    "RuleStatus",
    "RuleVersion",
    "RunContext",
    "RunFailure",
    "RunPolicy",
    "Schedule",
    "ScheduleStatus",
    "Settled",
    "Storage",
    "Transaction",
    "Workspace",
    "WorkspaceRef",
    "WorkspaceRule",
    "Workspaces",
    "run_lease",
    "tick_id",
    "utc_now",
    "with_params",
]
