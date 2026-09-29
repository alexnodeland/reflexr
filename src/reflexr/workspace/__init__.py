"""Workspaces: tenant-scoped event logs, the storage protocol, and in-memory storage.

Open a :class:`Workspace` handle with :meth:`Workspaces.open`, and publish, read and operate
through it. Rules are evaluated and runs executed by the ``Reactor``.
"""

from reflexr.workspace.memory import Clock, InMemoryStorage, utc_now
from reflexr.workspace.storage import Entry, Storage, Transaction, WorkspaceRef
from reflexr.workspace.workspace import Published, Workspace, Workspaces

__all__ = [
    "Clock",
    "Entry",
    "InMemoryStorage",
    "Published",
    "Storage",
    "Transaction",
    "Workspace",
    "WorkspaceRef",
    "Workspaces",
    "utc_now",
]
