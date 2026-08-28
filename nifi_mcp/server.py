"""The shared FastMCP server instance. Tools register themselves against this."""

from __future__ import annotations

from fastmcp import FastMCP

mcp = FastMCP("nifi-diagnostics")
