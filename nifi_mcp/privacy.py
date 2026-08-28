"""
Privacy helpers: extract only the harmless parts of a JDBC URL (DBMS type and
database name) and drop everything sensitive (host, port, user, password).
"""

from __future__ import annotations

import re
from typing import Optional

from . import config

_HOSTY = re.compile(r"^[0-9.]+$")


def parse_jdbc(url: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Extract ONLY (dbms_type, database_name) from a JDBC URL.
    Host, port, user, and password are NEVER returned.
    """
    if not url or not isinstance(url, str):
        return (None, None)
    u = url.strip()
    if not u.lower().startswith("jdbc:"):
        return (None, None)
    rest = u[5:]
    dbms = rest.split(":", 1)[0].split("//", 1)[0].lower() or None

    database: Optional[str] = None
    low = u.lower()
    m = re.search(r"databasename=([^;&]+)", low)  # SQL Server
    if m:
        database = m.group(1)
    else:
        core = u.split("?", 1)[0].split(";", 1)[0]
        if dbms == "oracle":
            tail = re.split(r"[:/]", core)[-1]
            database = tail or None
        elif "/" in core:
            database = core.rstrip("/").split("/")[-1] or None

    if database and ("@" in database or "." in database or _HOSTY.match(database)):
        database = None
    if not config.EXPOSE_DB_NAME:
        database = None
    return (dbms, database)


def db_label(name: Optional[str], dbms: Optional[str], database: Optional[str]) -> str:
    """Human-readable DB label (contains no connection details)."""
    label = name or "(unnamed DB service)"
    extras = [x for x in (dbms, database) if x]
    if extras:
        label += " [" + " · ".join(extras) + "]"
    return label
