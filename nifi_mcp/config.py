"""
Configuration for the NiFi diagnostics MCP server.

Everything here is read from environment variables at import time. Nothing in this
module is ever returned to the model — connection details stay on the server side.
"""

from __future__ import annotations

import os

# --- NiFi connection (read from the environment; never surfaced to the model) ---
BASE_URL = os.environ.get("NIFI_BASE_URL", "").rstrip("/")
USERNAME = os.environ.get("NIFI_USERNAME", "")
PASSWORD = os.environ.get("NIFI_PASSWORD", "")
CA_BUNDLE = os.environ.get("NIFI_CA_BUNDLE", "").strip()


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


# TLS verification: a CA bundle path if given, else a bool (default off for self-signed test).
VERIFY_SSL = CA_BUNDLE if CA_BUNDLE else _env_bool("NIFI_VERIFY_SSL", False)

# Whether to expose the parsed database NAME to the model (host/port/user always hidden).
EXPOSE_DB_NAME = _env_bool("NIFI_EXPOSE_DB_NAME", True)

# Controller-service types treated as "databases".
_DEFAULT_DB_TYPES = (
    "org.apache.nifi.dbcp.DBCPConnectionPool",
    "org.apache.nifi.dbcp.HikariCPConnectionPool",
)
_env_types = os.environ.get("NIFI_DB_SERVICE_TYPES", "").strip()
DB_SERVICE_TYPES = tuple(t.strip() for t in _env_types.split(",") if t.strip()) or _DEFAULT_DB_TYPES

# NiFi log directory (only needed by the log-based tools).
NIFI_LOG_DIR = os.environ.get("NIFI_LOG_DIR", "").strip()
NIFI_LOG_BASENAME = os.environ.get("NIFI_LOG_BASENAME", "nifi-app").strip()

# Base path of the NiFi REST API.
API = "/nifi-api"
