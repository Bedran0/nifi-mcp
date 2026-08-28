"""
Log-reading infrastructure. NiFi's bulletin board only keeps ~5 minutes, but every
ERROR is also written to nifi-app.log (plus hourly archives that go back much
further). This module selects the relevant files, parses lines, and scans them by
timestamp. Log files are only READ here, never modified.
"""

from __future__ import annotations

import glob
import gzip
import os
import re
from datetime import datetime, timedelta

from . import config

# A log line looks like:
#   2026-08-26 10:17:40,244 ERROR [Timer-Driven Process Thread-3] \
#     o.a.nifi.processors.standard.ExecuteSQL ExecuteSQL[id=<uuid>] <message>
LOG_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d{3}\s+"
    r"(?P<level>[A-Z]+)\s+"
    r"\[[^\]]*\]\s+"
    r"(?P<logger>\S+)\s+"
    r"(?P<body>.*)$"
)
LOG_ID_RE = re.compile(r"\[id=([0-9a-fA-F-]{36})\]")
UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)


def normalize_message(msg: str) -> str:
    """Collapse the variable parts of a message so repeats group into one 'error type'."""
    m = UUID_RE.sub("<uuid>", msg)
    m = re.sub(r"StandardFlowFileRecord\[[^\]]*\]", "StandardFlowFileRecord[...]", m)
    m = re.sub(r"\boffset=\d+", "offset=<n>", m)
    m = re.sub(r"\bsize=\d+", "size=<n>", m)
    return m.strip()


def clean_example(msg: str) -> str:
    """Trim per-flowfile noise so an example reads cleanly (logs are only read, never changed)."""
    cleaned = re.sub(r"\s*for StandardFlowFileRecord\[[^\]]*\]", "", msg)
    cleaned = UUID_RE.sub("<uuid>", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def log_files_for_window(hours: float) -> list[str]:
    """The live log plus the hourly archives whose hour falls in the last `hours` hours."""
    if not config.NIFI_LOG_DIR:
        raise RuntimeError(
            "NIFI_LOG_DIR is not set; point it at NiFi's logs directory "
            "(e.g. /opt/nifi-2.11.0/logs) to use the log-based tools."
        )
    if not os.path.isdir(config.NIFI_LOG_DIR):
        raise RuntimeError(
            f"NIFI_LOG_DIR does not exist or is not a directory: {config.NIFI_LOG_DIR}"
        )

    cutoff = datetime.now() - timedelta(hours=hours)
    files: list[str] = []

    pattern = os.path.join(config.NIFI_LOG_DIR, f"{config.NIFI_LOG_BASENAME}_*.log*")
    arch_re = re.compile(
        re.escape(config.NIFI_LOG_BASENAME) + r"_(\d{4}-\d{2}-\d{2})_(\d{2})\.\d+\.log(?:\.gz)?$"
    )
    for path in glob.glob(pattern):
        m = arch_re.search(os.path.basename(path))
        if not m:
            continue
        try:
            file_hour = datetime.strptime(f"{m.group(1)} {m.group(2)}", "%Y-%m-%d %H")
        except ValueError:
            continue
        if file_hour + timedelta(hours=1) >= cutoff:
            files.append(path)

    live = os.path.join(config.NIFI_LOG_DIR, f"{config.NIFI_LOG_BASENAME}.log")
    if os.path.isfile(live):
        files.append(live)

    files.sort()
    return files


def _open_log(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "r", encoding="utf-8", errors="replace")


def scan_logs(hours: float, levels: tuple[str, ...] = ("ERROR",)) -> dict:
    """
    Read the relevant log files and collect lines at the given `levels` from the last
    `hours` hours, grouped by processor id. Also records which processor ids were seen
    at ALL (any level) so callers can reason about "silent" processors.

    Returns:
      {
        "by_pid": { pid: {id, type, count, first_ts, last_ts, message_types{...}} },
        "seen_pids": { pid: {type, count} },   # every pid seen at any level
        "files_scanned": int,
      }
    """
    cutoff = datetime.now() - timedelta(hours=hours)
    wanted = tuple(l.upper() for l in levels)
    by_pid: dict[str, dict] = {}
    seen_pids: dict[str, dict] = {}
    files = log_files_for_window(hours)
    scanned = 0

    for path in files:
        try:
            fh = _open_log(path)
        except Exception:
            continue
        with fh:
            scanned += 1
            for line in fh:
                m = LOG_LINE_RE.match(line)
                if not m:
                    continue
                try:
                    ts = datetime.strptime(m.group("ts"), "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
                if ts < cutoff:
                    continue
                body = m.group("body")
                idm = LOG_ID_RE.search(body)
                pid = idm.group(1) if idm else None
                if not pid:
                    continue
                ptype = m.group("logger").split(".")[-1]

                # Track every processor seen at any level (for "silent" analysis).
                sp = seen_pids.get(pid)
                if sp is None:
                    seen_pids[pid] = {"type": ptype, "count": 1}
                else:
                    sp["count"] += 1

                if m.group("level") not in wanted:
                    continue

                msg = body.split("]", 1)[1].strip() if "]" in body else body
                norm = normalize_message(msg)

                rec = by_pid.get(pid)
                if rec is None:
                    rec = {
                        "id": pid,
                        "type": ptype,
                        "count": 0,
                        "first_ts": ts,
                        "last_ts": ts,
                        "message_types": {},
                    }
                    by_pid[pid] = rec
                rec["count"] += 1
                rec["first_ts"] = min(rec["first_ts"], ts)
                rec["last_ts"] = max(rec["last_ts"], ts)

                mt = rec["message_types"].get(norm)
                if mt is None:
                    mt = {"count": 0, "first_ts": ts, "last_ts": ts, "examples": []}
                    rec["message_types"][norm] = mt
                mt["count"] += 1
                mt["first_ts"] = min(mt["first_ts"], ts)
                mt["last_ts"] = max(mt["last_ts"], ts)
                if len(mt["examples"]) < 3:
                    mt["examples"].append(
                        {"timestamp": ts.isoformat(sep=" "), "message": clean_example(msg)}
                    )

    return {"by_pid": by_pid, "seen_pids": seen_pids, "files_scanned": scanned}
