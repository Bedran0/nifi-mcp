"""
All MCP tools, built on the helper modules. Every tool is read-only.

Tools:
  - check_connection      : verify connectivity/auth, return NiFi version
  - list_databases        : list DBCP pools with health counts
  - get_database_health   : per-DB errors (service + referencing processors)
  - get_flow_errors       : CURRENT failing processors (live bulletins), flat or tree
  - get_log_errors        : HISTORICAL errors from log files over the last N hours
"""

from __future__ import annotations

from collections import defaultdict
from typing import Optional

from .server import mcp
from . import config, nifi_api, log_reader
from .nifi_client import get

API = config.API


@mcp.tool
async def check_connection() -> dict:
    """
    Verify connectivity and authentication to NiFi; return the NiFi version.
    Does NOT return any credential (username/password/token). Read-only.
    """
    about = await get(f"{API}/flow/about")
    info = (about or {}).get("about", {}) or {}
    return {
        "connected": True,
        "nifi_version": info.get("version"),
        "title": info.get("title"),
        "read_only": True,
    }


@mcp.tool
async def list_databases() -> dict:
    """
    List all "databases" (DBCP / Hikari Connection Pool controller services) in NiFi.

    For each DB: name, id, state, validation status, DBMS type, (optional) database name,
    and error/warning counts (including errors on the processors that use it). Connection
    details (host/port/user/password/JDBC URL) are NEVER returned. Read-only.
    """
    services = await nifi_api.all_dbcp_services()
    bindex = await nifi_api.bulletin_index()
    dbs = []
    total_error_dbs = 0
    for s in services:
        r = nifi_api.build_db_report(s, bindex)
        if r["error_count"] > 0:
            total_error_dbs += 1
        dbs.append(
            {
                "id": r["id"],
                "name": r["name"],
                "label": r["label"],
                "dbms": r["dbms"],
                "database": r["database"],
                "state": r["state"],
                "validation_status": r["validation_status"],
                "error_count": r["error_count"],
                "warning_count": r["warning_count"],
                "referencing_processor_count": r["referencing_processor_count"],
                "healthy": r["healthy"],
            }
        )
    dbs.sort(key=lambda d: (-d["error_count"], d["name"] or ""))
    return {
        "total_databases": len(dbs),
        "databases_with_errors": total_error_dbs,
        "databases": dbs,
    }


@mcp.tool
async def get_database_health(database_id: Optional[str] = None) -> dict:
    """
    Show DB migration/connection errors: WHICH databases are failing and HOW MANY errors each.

    If database_id is given, return detail for just that DB; otherwise all DBs. Errors come from
    TWO sources: the DB service's own validation errors/bulletins, and bulletins on the processors
    that use this pool (e.g. PutDatabaseRecord). `summary_text` is human-readable. Connection
    details are NEVER returned. Read-only.
    """
    services = await nifi_api.all_dbcp_services()
    if database_id:
        services = [s for s in services if s.get("id") == database_id]
        if not services:
            return {"error": f"No DBCP service found with id '{database_id}'."}
    bindex = await nifi_api.bulletin_index()

    results = [nifi_api.build_db_report(s, bindex) for s in services]
    results.sort(key=lambda r: -r["error_count"])
    total_errors = sum(r["error_count"] for r in results)
    failing = [r for r in results if r["error_count"] > 0]

    lines = ["DB Migration/Connection Health", "=" * 32]
    lines.append(
        f"Total DBs: {len(results)}  |  Failing DBs: {len(failing)}  |  Total errors: {total_errors}"
    )
    lines.append("")
    if not failing:
        lines.append("OK - no failing databases.")
    for r in results:
        mark = "[X]" if r["error_count"] > 0 else "[OK]"
        lines.append(f"{mark} {r['label']}  ({r['error_count']} errors)")
        lines.append(
            f"    state: {r['state']} / {r['validation_status']}  "
            f"(uses {r['referencing_processor_count']} referencing processors)"
        )
        for e in r["service_validation_errors"]:
            lines.append(f"    |- [service/validation] {e}")
        for m in r["service_error_bulletins"]:
            lines.append(f"    |- [service/error] {m}")
        for m in r["service_warning_bulletins"]:
            lines.append(f"    |- [service/warn]  {m}")
        for rp in r["referencing_processors_with_issues"]:
            lines.append(f"    |- processor {rp['name']} [{rp['type']}] (id={rp['id']})")
            for m in rp["error_bulletins"]:
                lines.append(f"    |    |- [error] {m}")
            for m in rp["warning_bulletins"]:
                lines.append(f"    |    |- [warn]  {m}")

    return {
        "total_databases": len(results),
        "databases_with_errors": len(failing),
        "total_error_count": total_errors,
        "databases": results,
        "summary_text": "\n".join(lines),
    }


@mcp.tool
async def get_flow_errors(
    output: str = "tree",
    group_id: str = "root",
    include_warnings: bool = False,
    max_depth: int = 25,
) -> dict:
    """
    Show CURRENTLY failing / non-working processors (INVALID run status, or with an ERROR
    bulletin right now). This reflects the live NiFi state (bulletins cover ~5 minutes), which
    is different from get_log_errors (historical log files over hours).

    `output` controls the shape:
      - "tree" (default): grouped by process-group hierarchy, with a ready-to-show ASCII tree
        in `tree_text`. Healthy branches are pruned.
      - "flat": a flat list of failing processors (id, name, type, run status, error count,
        group path, error messages) sorted by error count.

    Set include_warnings=True to include WARNING-only processors. Read-only.
    """
    bindex = await nifi_api.bulletin_index()
    tree = await nifi_api.walk_group(group_id, bindex, include_warnings, True, 0, max_depth)

    if output == "flat":
        flat: list[dict] = []

        def collect(node: dict, path: str):
            p = f"{path} / {node['name']}" if path else node["name"]
            for proc in node.get("processors", []):
                flat.append({**proc, "group_path": p})
            for g in node.get("process_groups", []):
                collect(g, p)

        collect(tree, "")
        flat.sort(key=lambda x: -x["error_count"])
        return {
            "output": "flat",
            "total_failing_processors": len(flat),
            "total_error_count": tree["error_count"],
            "total_warning_count": tree["warning_count"],
            "failing_processors": flat,
        }

    # default: tree
    body = nifi_api.render_tree(tree, "", True)
    header = (
        f"NiFi Error Tree - root: {tree['name']}  "
        f"(total {tree['error_count']} errors, {tree['warning_count']} warnings)"
    )
    tree_text = header + "\n" + "\n".join(body) if body else header + "\n\\- OK: no errors found."
    return {
        "output": "tree",
        "total_error_count": tree["error_count"],
        "total_warning_count": tree["warning_count"],
        "tree": tree,
        "tree_text": tree_text,
    }


@mcp.tool
async def list_affected_processors(database_id: str) -> dict:
    """
    List ALL processors that use (reference) a given DBCP connection pool, with each one's
    scheduled state (RUNNING / STOPPED / DISABLED) and process-group path. Unlike
    get_database_health (which shows only FAILING processors), this shows every processor a
    connection change would affect, plus a state summary (how many RUNNING vs STOPPED).

    Use this before changing a connection to see its blast radius — e.g. "pool X is used by
    526 processors, 188 RUNNING / 338 STOPPED". Pass the pool's id (from list_databases).
    Read-only: it only reads state, never stops/starts/edits anything.
    """
    services = await nifi_api.all_dbcp_services()
    match = [s for s in services if s.get("id") == database_id]
    if not match:
        return {"error": f"No DBCP service found with id '{database_id}'."}

    service = match[0]
    comp = service.get("component", {}) or {}
    procs = nifi_api.collect_referencing_processors(comp)

    # Resolve each processor's group path (nice tree label) from the flow map.
    pid_map = await nifi_api.pid_to_group_path()
    for p in procs:
        info = pid_map.get(p["id"], {})
        p["group_path"] = info.get("group_path") or "(not found in current flow)"

    # State summary — normalize NiFi's state strings into RUNNING / STOPPED / DISABLED / other.
    summary: dict[str, int] = defaultdict(int)
    for p in procs:
        state = (p.get("state") or "UNKNOWN").upper()
        summary[state] += 1

    procs.sort(key=lambda p: (p.get("state") or "", p.get("name") or ""))

    # Human-readable text
    lines = [
        f"Processors affected by connection: {comp.get('name')} (id={database_id})",
        "=" * 40,
        f"Total: {len(procs)}  |  "
        + "  ".join(f"{k}: {v}" for k, v in sorted(summary.items())),
        "",
    ]
    by_group: dict[str, list] = defaultdict(list)
    for p in procs:
        by_group[p["group_path"]].append(p)
    for group, items in by_group.items():
        lines.append(f"[PG] {group}")
        for p in items:
            flags = ""
            if p.get("validation_errors"):
                flags = "  [INVALID]"
            lines.append(
                f"   - {p['name']} [{p['type']}] - {p.get('state')}"
                f" (id={p['id']}){flags}"
            )

    return {
        "database_id": database_id,
        "database_name": comp.get("name"),
        "total_affected": len(procs),
        "state_summary": dict(summary),
        "processors": procs,
        "summary_text": "\n".join(lines),
    }


@mcp.tool
async def get_log_errors(hours: float = 12.0, detailed: bool = False) -> dict:
    """
    Report processors that logged ERRORs in the last `hours` hours, read from NiFi's log files
    (nifi-app.log + hourly archives), each mapped to its process-group tree.

    Unlike get_flow_errors (live, ~5 min), this looks back over real historical logs. Each
    processor: id, name, type, group path, total error count, first/last error time. If
    detailed=True, also breaks errors into distinct TYPES (normalized) with counts, time ranges,
    and a few cleaned example lines.

    Requires NIFI_LOG_DIR. Read-only (only reads log files).
    """
    scan = log_reader.scan_logs(hours, levels=("ERROR",))
    by_pid = scan["by_pid"]
    pid_map = await nifi_api.pid_to_group_path()

    processors = []
    for pid, rec in by_pid.items():
        info = pid_map.get(pid, {})
        entry = {
            "id": pid,
            "name": info.get("name") or rec.get("type"),
            "type": rec.get("type"),
            "group_path": info.get("group_path") or "(not found in current flow)",
            "error_count": rec["count"],
            "first_error": rec["first_ts"].isoformat(sep=" "),
            "last_error": rec["last_ts"].isoformat(sep=" "),
            "distinct_error_types": len(rec["message_types"]),
        }
        if detailed:
            types = []
            for norm, mt in sorted(rec["message_types"].items(), key=lambda kv: -kv[1]["count"]):
                types.append(
                    {
                        "error_type": norm,
                        "count": mt["count"],
                        "first_error": mt["first_ts"].isoformat(sep=" "),
                        "last_error": mt["last_ts"].isoformat(sep=" "),
                        "examples": mt["examples"],
                    }
                )
            entry["error_types"] = types
        processors.append(entry)

    processors.sort(key=lambda p: -p["error_count"])
    total_errors = sum(p["error_count"] for p in processors)

    lines = [
        f"NiFi Log Errors - last {hours} hour(s)",
        "=" * 40,
        f"Processors with errors: {len(processors)}  |  "
        f"Total error lines: {total_errors}  |  Files scanned: {scan['files_scanned']}",
        "",
    ]
    if not processors:
        lines.append("OK - no ERROR lines found in the window.")
    by_group: dict[str, list] = defaultdict(list)
    for p in processors:
        by_group[p["group_path"]].append(p)
    for group, procs in by_group.items():
        lines.append(f"[PG] {group}")
        for p in procs:
            lines.append(
                f"   [X] {p['name']} [{p['type']}] (id={p['id']}) - "
                f"{p['error_count']} errors, {p['distinct_error_types']} type(s), "
                f"{p['first_error']} -> {p['last_error']}"
            )
            if detailed:
                for t in p.get("error_types", []):
                    lines.append(f"      - [{t['count']}x] {t['error_type'][:100]}")

    return {
        "hours": hours,
        "detailed": detailed,
        "processors_with_errors": len(processors),
        "total_error_count": total_errors,
        "files_scanned": scan["files_scanned"],
        "processors": processors,
        "summary_text": "\n".join(lines),
    }
