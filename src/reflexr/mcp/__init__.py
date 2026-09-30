"""An MCP server, so external agents can publish into, read and operate workspaces.

Every tool goes through :func:`reflexr.workspace.execute`, the handler REST and the WebSocket
use, so it behaves the same whichever way it arrives.
"""

from reflexr.mcp.server import INSTRUCTIONS, McpContext, ReflexrMcp, ResolveClient, run_uri

__all__ = ["INSTRUCTIONS", "McpContext", "ReflexrMcp", "ResolveClient", "run_uri"]
