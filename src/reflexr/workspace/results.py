"""Command results, remembered so that a retried command is carried out once.

Every surface sends commands to :meth:`reflexr.workspace.Workspaces.execute` with the id the
client chose, and the workspaces remember each result in a :class:`CommandResults`. A client
that retries a command it never heard back about gets the first result, and the command is not
carried out twice (ADR-0047).
"""

from collections import OrderedDict
from dataclasses import dataclass
from typing import Protocol

from reflexr.core import CommandResult, TenantId, WorkspaceId


@dataclass(frozen=True)
class CommandKey:
    """What makes two commands the same: who sent them, where, and with which id."""

    tenant_id: TenantId
    """The tenant of the workspace the command was sent to."""

    workspace_id: WorkspaceId
    """The workspace it was sent to."""

    participant: str
    """The sender, as its actor's ``participant``: every action of one person or client."""

    command_id: str
    """The id the client chose for the command."""


class CommandResults(Protocol):
    """Where workspaces remember the results of commands: a port (ADR-0025).

    :class:`InMemoryCommandResults` remembers them in the process. An implementation over
    shared storage would deduplicate across processes too.
    """

    async def get(self, key: CommandKey) -> CommandResult | None:
        """Return the result remembered for a command, if there is one."""
        ...

    async def put(self, key: CommandKey, result: CommandResult) -> None:
        """Remember a command's result."""
        ...


class InMemoryCommandResults:
    """The most recent results, in this process: the default :class:`CommandResults`.

    Args:
        capacity: How many results to remember. Beyond it, the oldest is forgotten first, and
            a command whose result was forgotten is carried out again if it is repeated.
    """

    def __init__(self, capacity: int = 10_000) -> None:
        self._capacity = capacity
        self._results: OrderedDict[CommandKey, CommandResult] = OrderedDict()

    async def get(self, key: CommandKey) -> CommandResult | None:
        """Return the result remembered for a command, if it is still remembered."""
        return self._results.get(key)

    async def put(self, key: CommandKey, result: CommandResult) -> None:
        """Remember a command's result, forgetting the oldest beyond the capacity."""
        self._results[key] = result
        if len(self._results) > self._capacity:
            self._results.popitem(last=False)
