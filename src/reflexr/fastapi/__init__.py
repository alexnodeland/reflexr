"""FastAPI adapters: HTTP ingest, REST reads and administration, and the WebSocket stream.

Mount the router under a prefix of your choosing::

    app.include_router(reflexr_router(workspaces, resolve_actor=resolve_actor), prefix="/v1")

Every route speaks the workspace protocol in ``docs/protocol.md``, and every command goes
through :func:`reflexr.workspace.execute`, so REST, the WebSocket and MCP behave identically.
"""

from reflexr.fastapi.router import (
    STATUS_CODES,
    PublishBatch,
    PublishItem,
    ResolveActor,
    Unauthorized,
    reflexr_router,
)
from reflexr.fastapi.stream import Stream

# Defined with the workspaces, for every surface.
from reflexr.workspace import Authorize, RuleStatus, ScheduleStatus

__all__ = [
    "STATUS_CODES",
    "Authorize",
    "PublishBatch",
    "PublishItem",
    "ResolveActor",
    "RuleStatus",
    "ScheduleStatus",
    "Stream",
    "Unauthorized",
    "reflexr_router",
]
