"""
Entry point. Importing `tools` registers every @mcp.tool on the shared server,
then we start it over stdio.

Run with:   python -m nifi_mcp
"""

from __future__ import annotations

from .server import mcp
from . import tools  # noqa: F401  (import registers the tools)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
