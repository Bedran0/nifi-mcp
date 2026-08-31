"""
Entry point. Importing `tools` registers every @mcp.tool on the shared server,
then we start it with the configured transport.

Transport is chosen via the MCP_TRANSPORT environment variable:
  - "stdio" (default): the client launches this as a subprocess (single user).
  - "http": a long-lived network service multiple clients can connect to.
       Configure with MCP_HTTP_HOST / MCP_HTTP_PORT / MCP_HTTP_PATH.

Run with:   python -m nifi_mcp
"""

from __future__ import annotations

import sys

from .server import mcp
from . import config
from . import tools  # noqa: F401  (import registers the tools)


def main() -> None:
    if config.MCP_TRANSPORT == "http":
        # Long-lived network service. NOTE: this build ships without authentication,
        # so only expose it on a trusted LAN/VPN. Binding to 0.0.0.0 makes it reachable
        # from the network; 127.0.0.1 keeps it local to this machine.
        print(
            f"[nifi-mcp] Starting HTTP transport on "
            f"http://{config.MCP_HTTP_HOST}:{config.MCP_HTTP_PORT}{config.MCP_HTTP_PATH}",
            file=sys.stderr,
        )
        if config.MCP_HTTP_HOST == "0.0.0.0":
            print(
                "[nifi-mcp] WARNING: bound to 0.0.0.0 with no authentication - "
                "make sure this port is only reachable from a trusted network.",
                file=sys.stderr,
            )
        mcp.run(
            transport="streamable-http",
            host=config.MCP_HTTP_HOST,
            port=config.MCP_HTTP_PORT,
            path=config.MCP_HTTP_PATH,
        )
    else:
        mcp.run()  # stdio (default)


if __name__ == "__main__":
    main()
