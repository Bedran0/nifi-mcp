"""
Shared helpers that fetch and shape data from the NiFi REST API: the bulletin
index, DBCP service discovery, per-DB health reports, recursive process-group
walking, and a processor-id -> group-path map. Tools build on these.
"""

from __future__ import annotations

from typing import Optional

from . import config
from .nifi_client import get
from .privacy import parse_jdbc, db_label

API = config.API


async def bulletin_index() -> dict[str, list[dict]]:
    """
    Fetch the bulletin board and return a map of sourceId (component id) -> bulletins.
    Each bulletin: {level, message, category, timestamp, sourceName}.
    """
    data = await get(f"{API}/flow/bulletin-board")
    index: dict[str, list[dict]] = {}
    bulletins = (data or {}).get("bulletinBoard", {}).get("bulletins", []) or []
    for entry in bulletins:
        src = entry.get("sourceId") or entry.get("groupId")
        b = entry.get("bulletin", {}) or {}
        if not src:
            continue
        index.setdefault(src, []).append(
            {
                "level": (b.get("level") or "").upper(),
                "message": b.get("message"),
                "category": b.get("category"),
                "timestamp": b.get("timestamp"),
                "sourceName": b.get("sourceName") or entry.get("sourceName"),
            }
        )
    return index


def count_levels(bulletins: list[dict]) -> tuple[int, int]:
    errors = sum(1 for x in bulletins if x.get("level") == "ERROR")
    warns = sum(1 for x in bulletins if x.get("level") in ("WARN", "WARNING"))
    return errors, warns


async def all_dbcp_services() -> list[dict]:
    """Return all DBCP (incl. Hikari) controller services across every group in one call."""
    data = await get(
        f"{API}/flow/process-groups/root/controller-services",
        params={
            "includeAncestorGroups": "false",
            "includeDescendantGroups": "true",
            # true: processors that USE each pool come back inline (no extra per-DB call).
            "includeReferencingComponents": "true",
        },
    )
    services = (data or {}).get("controllerServices", []) or []
    out = []
    for s in services:
        comp = s.get("component", {}) or {}
        ctype = comp.get("type", "") or ""
        is_db = ctype in config.DB_SERVICE_TYPES or (
            ctype.endswith("ConnectionPool")
            and "dbcp" in ctype.lower()
            and not ctype.endswith("Lookup")
        )
        if is_db:
            out.append(s)
    return out


def collect_referencing_processors(comp: dict) -> list[dict]:
    """Recursively collect processors (any depth) that reference this controller service.

    Each entry includes the processor's scheduled state (RUNNING/STOPPED/DISABLED),
    active thread count, and any validation errors — enough to answer "who is affected
    by this connection and what state are they in" without extra calls.
    """
    out: list[dict] = []
    seen: set[str] = set()

    def walk(refs):
        for rc in refs or []:
            rcc = rc.get("component", {}) or {}
            rid = rcc.get("id") or rc.get("id")
            rtype = rcc.get("referenceType")
            if rtype == "Processor" and rid and rid not in seen:
                seen.add(rid)
                out.append(
                    {
                        "id": rid,
                        "name": rcc.get("name"),
                        "type": (rcc.get("type") or "").split(".")[-1],
                        "state": rcc.get("state"),
                        "active_thread_count": rcc.get("activeThreadCount"),
                        "validation_errors": list(rcc.get("validationErrors", []) or []),
                        "group_id": rcc.get("groupId"),
                    }
                )
            walk(rcc.get("referencingComponents", []))

    walk(comp.get("referencingComponents", []))
    return out


def build_db_report(service: dict, bindex: dict[str, list[dict]]) -> dict:
    """
    Full health picture for one DB (DBCP service): the service's own validation errors
    and bulletins, plus bulletins on the processors that use this pool. Connection
    details are never included.
    """
    sid = service.get("id")
    comp = service.get("component", {}) or {}
    props = comp.get("properties", {}) or {}
    dbms, database = parse_jdbc(props.get("Database Connection URL"))

    svc_val_errors = list(comp.get("validationErrors", []) or [])
    svc_bl = bindex.get(sid, [])
    svc_err = [b["message"] for b in svc_bl if b.get("level") == "ERROR"]
    svc_warn = [b["message"] for b in svc_bl if b.get("level") in ("WARN", "WARNING")]

    ref_procs = collect_referencing_processors(comp)
    ref_reports = []
    ref_err_total = 0
    ref_warn_total = 0
    for rp in ref_procs:
        bl = bindex.get(rp["id"], [])
        e = [b["message"] for b in bl if b.get("level") == "ERROR"]
        w = [b["message"] for b in bl if b.get("level") in ("WARN", "WARNING")]
        ref_err_total += len(e)
        ref_warn_total += len(w)
        if e or w:
            ref_reports.append({**rp, "error_bulletins": e, "warning_bulletins": w})

    error_count = len(svc_val_errors) + len(svc_err) + ref_err_total
    warning_count = len(svc_warn) + ref_warn_total

    return {
        "id": sid,
        "name": comp.get("name"),
        "label": db_label(comp.get("name"), dbms, database),
        "dbms": dbms,
        "database": database,
        "state": comp.get("state"),
        "validation_status": comp.get("validationStatus"),
        "service_validation_errors": svc_val_errors,
        "service_error_bulletins": svc_err,
        "service_warning_bulletins": svc_warn,
        "referencing_processor_count": len(ref_procs),
        "referencing_processors_with_issues": ref_reports,
        "error_count": error_count,
        "warning_count": warning_count,
        "healthy": error_count == 0 and comp.get("validationStatus") == "VALID",
    }


async def processor_validation_errors(pid: str) -> list[str]:
    """Fetch validation error messages for a single processor when it is INVALID."""
    try:
        data = await get(f"{API}/processors/{pid}")
    except Exception:
        return []
    comp = (data or {}).get("component", {}) or {}
    return list(comp.get("validationErrors", []) or [])


async def walk_group(
    group_id: str,
    bindex: dict[str, list[dict]],
    include_warnings: bool,
    fetch_validation: bool,
    depth: int,
    max_depth: int,
) -> dict:
    """Recursively walk a process group and return a health-annotated tree node."""
    data = await get(f"{API}/flow/process-groups/{group_id}")
    pgf = (data or {}).get("processGroupFlow", {}) or {}
    flow = pgf.get("flow", {}) or {}
    breadcrumb = pgf.get("breadcrumb", {}) or {}
    group_name = (breadcrumb.get("breadcrumb", {}) or {}).get("name") or group_id

    node = {
        "id": group_id,
        "name": group_name,
        "type": "process_group",
        "processors": [],
        "process_groups": [],
        "error_count": 0,
        "warning_count": 0,
    }

    for pe in flow.get("processors", []) or []:
        pid = pe.get("id")
        comp = pe.get("component", {}) or {}
        status = pe.get("status", {}) or {}
        run_status = (
            status.get("runStatus")
            or status.get("aggregateSnapshot", {}).get("runStatus")
            or comp.get("state")
        )
        bl = bindex.get(pid, [])
        b_err, b_warn = count_levels(bl)
        is_invalid = (run_status or "").upper() == "INVALID"

        val_errors: list[str] = list(comp.get("validationErrors", []) or [])
        if is_invalid and not val_errors and fetch_validation:
            val_errors = await processor_validation_errors(pid)

        err_count = b_err + (len(val_errors) if val_errors else (1 if is_invalid else 0))
        is_failing = err_count > 0 or is_invalid
        if not is_failing and not (include_warnings and b_warn > 0):
            continue

        node["processors"].append(
            {
                "id": pid,
                "name": comp.get("name"),
                "type": (comp.get("type") or "").split(".")[-1],
                "run_status": run_status,
                "error_count": err_count,
                "warning_count": b_warn,
                "validation_errors": val_errors,
                "error_messages": [b["message"] for b in bl if b.get("level") == "ERROR"],
                "warning_messages": [
                    b["message"] for b in bl if b.get("level") in ("WARN", "WARNING")
                ]
                if include_warnings
                else [],
            }
        )
        node["error_count"] += err_count
        node["warning_count"] += b_warn

    if depth < max_depth:
        for ge in flow.get("processGroups", []) or []:
            child_id = ge.get("id")
            if not child_id:
                continue
            child = await walk_group(
                child_id, bindex, include_warnings, fetch_validation, depth + 1, max_depth
            )
            if child["error_count"] > 0 or child["processors"] or child["process_groups"] or (
                include_warnings and child["warning_count"] > 0
            ):
                node["process_groups"].append(child)
                node["error_count"] += child["error_count"]
                node["warning_count"] += child["warning_count"]

    return node


def render_tree(node: dict, prefix: str = "", is_last: bool = True) -> list[str]:
    lines = []
    connector = "\\- " if is_last else "|- "
    if node["type"] == "process_group":
        head = f"{node['name']}  ({node['error_count']} errors"
        if node.get("warning_count"):
            head += f", {node['warning_count']} warnings"
        head += ")"
        lines.append(prefix + connector + "[PG] " + head)
    child_prefix = prefix + ("   " if is_last else "|  ")

    procs = node.get("processors", [])
    groups = node.get("process_groups", [])
    items = [("proc", p) for p in procs] + [("grp", g) for g in groups]
    for i, (kind, item) in enumerate(items):
        last = i == len(items) - 1
        if kind == "proc":
            conn = "\\- " if last else "|- "
            mark = "[X]" if item["error_count"] > 0 else "[.]"
            line = (
                f"{child_prefix}{conn}{mark} {item['name']} "
                f"[{item['type']}] - {item['run_status']} "
                f"(id={item['id']}, {item['error_count']} errors)"
            )
            lines.append(line)
            detail_prefix = child_prefix + ("   " if last else "|  ")
            for e in item.get("validation_errors", []):
                lines.append(f"{detail_prefix}|- [validation] {e}")
            for m in item.get("error_messages", []):
                lines.append(f"{detail_prefix}|- [error] {m}")
            for m in item.get("warning_messages", []):
                lines.append(f"{detail_prefix}|- [warn]  {m}")
        else:
            lines.extend(render_tree(item, child_prefix, last))
    return lines


async def pid_to_group_path() -> dict[str, dict]:
    """Map processor id -> {name, group_path} by walking the flow (for log tools)."""
    result: dict[str, dict] = {}

    async def walk(group_id: str, path: str, depth: int):
        if depth > 30:
            return
        data = await get(f"{API}/flow/process-groups/{group_id}")
        pgf = (data or {}).get("processGroupFlow", {}) or {}
        flow = pgf.get("flow", {}) or {}
        breadcrumb = pgf.get("breadcrumb", {}) or {}
        name = (breadcrumb.get("breadcrumb", {}) or {}).get("name") or group_id
        here = f"{path} / {name}" if path else name
        for pe in flow.get("processors", []) or []:
            comp = pe.get("component", {}) or {}
            pid = pe.get("id") or comp.get("id")
            if pid:
                result[pid] = {"name": comp.get("name"), "group_path": here}
        for ge in flow.get("processGroups", []) or []:
            cid = ge.get("id")
            if cid:
                await walk(cid, here, depth + 1)

    await walk("root", "", 0)
    return result
