"""Workspaces: tenant-scoped event logs, the storage protocol, and in-memory storage.

Open a :class:`Workspace` handle with :meth:`Workspaces.open`, and publish, read and operate
through it. Rules are evaluated and runs executed by the ``Reactor``.
"""

from reflexr.workspace.actions import Action, Reaction
from reflexr.workspace.memory import Clock, InMemoryStorage, utc_now
from reflexr.workspace.reactor import EVALUATION_LEASE, REACTOR, Reactor, Settled
from reflexr.workspace.storage import Entry, Storage, Transaction, WorkspaceRef, run_lease
from reflexr.workspace.workspace import Published, Workspace, Workspaces

__all__ = [
    "EVALUATION_LEASE",
    "REACTOR",
    "Action",
    "Clock",
    "Entry",
    "InMemoryStorage",
    "Published",
    "Reaction",
    "Reactor",
    "Settled",
    "Storage",
    "Transaction",
    "Workspace",
    "WorkspaceRef",
    "Workspaces",
    "run_lease",
    "utc_now",
]
