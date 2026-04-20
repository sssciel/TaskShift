import json
import re
from pathlib import Path


TASKSHIFT_LOG_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} [0-9:.,]+) \| (?P<level>[A-Z]+) \| (?P<source>[^|]+) \| (?P<message>.*)$"
)


def get_logs_root() -> Path:
    return Path("logs").resolve()


def normalize_log_limit(limitValue, default: int = 300, maximum: int = 2000) -> int:
    try:
        limit = int(limitValue)
    except (TypeError, ValueError):
        return default

    return max(1, min(limit, maximum))


def normalize_page(pageValue, default: int = 1) -> int:
    try:
        page = int(pageValue)
    except (TypeError, ValueError):
        return default

    return max(1, page)


def normalize_status_filters(statusValues) -> list[str]:
    normalized = []
    for value in statusValues or []:
        if value is None:
            continue
        for part in str(value).split(","):
            token = part.strip().upper()
            if token and token not in normalized:
                normalized.append(token)

    return normalized


def normalize_job_log_status(statusValue) -> str:
    normalized = str(statusValue or "").strip().upper()
    if normalized == "ATTEMPTED":
        return "LAUNCH_ATTEMPTED"
    if normalized == "FAILED":
        return "LAUNCH_FAILED"
    if normalized in {"SUCCEEDED", "SUCCESS"}:
        return "LEFT_PENDING_QUEUE"
    return normalized or "UNKNOWN"


def build_taskshift_log_payload(
    *,
    query: str = "",
    statuses: list[str] | None = None,
    page: int = 1,
    limit: int = 100,
) -> dict:
    logPath = get_logs_root() / "taskshift.log"
    selectedStatuses = normalize_status_filters(statuses)
    entries = []
    currentEntry = None

    if logPath.exists():
        for rawLine in logPath.read_text(encoding="utf-8", errors="replace").splitlines():
            match = TASKSHIFT_LOG_PATTERN.match(rawLine)
            if match:
                if currentEntry is not None:
                    entries.append(currentEntry)
                currentEntry = {
                    "timestamp": match.group("timestamp"),
                    "level": match.group("level"),
                    "source": match.group("source").strip(),
                    "message": match.group("message"),
                    "raw": rawLine,
                }
                continue

            if currentEntry is None:
                currentEntry = {
                    "timestamp": None,
                    "level": "OTHER",
                    "source": "unstructured",
                    "message": rawLine,
                    "raw": rawLine,
                }
                continue

            currentEntry["message"] += f"\n{rawLine}"
            currentEntry["raw"] += f"\n{rawLine}"

        if currentEntry is not None:
            entries.append(currentEntry)

    availableStatuses = sorted({entry["level"] for entry in entries})
    normalizedQuery = query.strip().lower()
    filteredEntries = []
    for entry in entries:
        if selectedStatuses and entry["level"] not in selectedStatuses:
            continue

        if normalizedQuery:
            haystack = "\n".join(
                [
                    entry.get("timestamp") or "",
                    entry.get("level") or "",
                    entry.get("source") or "",
                    entry.get("message") or "",
                ]
            ).lower()
            if normalizedQuery not in haystack:
                continue

        filteredEntries.append(entry)

    filteredEntries = list(reversed(filteredEntries))
    filteredEntriesTotal = len(filteredEntries)
    pageSize = max(1, limit)
    totalPages = max(1, (filteredEntriesTotal + pageSize - 1) // pageSize)
    currentPage = min(max(1, page), totalPages)
    pageStart = (currentPage - 1) * pageSize
    pageEntries = filteredEntries[pageStart:pageStart + pageSize]
    statusCounts = {}
    for entry in filteredEntries:
        statusCounts[entry["level"]] = statusCounts.get(entry["level"], 0) + 1

    return {
        "file": str(logPath),
        "query": query,
        "selected_statuses": selectedStatuses,
        "available_statuses": availableStatuses,
        "total_entries": len(entries),
        "filtered_entries": filteredEntriesTotal,
        "shown_entries": len(pageEntries),
        "page": currentPage,
        "page_size": pageSize,
        "total_pages": totalPages,
        "has_prev_page": currentPage > 1,
        "has_next_page": currentPage < totalPages,
        "status_counts": statusCounts,
        "entries": pageEntries,
    }


def build_job_logs_payload(
    *,
    query: str = "",
    jobId: str = "",
    statuses: list[str] | None = None,
    page: int = 1,
    limit: int = 100,
) -> dict:
    logPath = get_logs_root() / "job_launches.jsonl"
    selectedStatuses = []
    for status in normalize_status_filters(statuses):
        normalizedStatus = normalize_job_log_status(status)
        if normalizedStatus not in selectedStatuses:
            selectedStatuses.append(normalizedStatus)
    normalizedQuery = query.strip().lower()
    normalizedJobId = str(jobId).strip()
    entries = []

    if logPath.exists():
        for rawLine in logPath.read_text(encoding="utf-8", errors="replace").splitlines():
            if not rawLine.strip():
                continue
            try:
                payload = json.loads(rawLine)
            except json.JSONDecodeError:
                continue

            payload["status"] = normalize_job_log_status(payload.get("status"))
            payload["raw"] = rawLine
            entries.append(payload)

    availableStatuses = sorted(
        {
            str(entry.get("status", "")).upper()
            for entry in entries
            if entry.get("status")
        }
    )
    filteredEntries = []
    for entry in entries:
        entryStatus = str(entry.get("status", "")).upper()
        if selectedStatuses and entryStatus not in selectedStatuses:
            continue

        if normalizedJobId and normalizedJobId not in str(entry.get("job_id", "")):
            continue

        if normalizedQuery:
            haystack = "\n".join(
                [
                    str(entry.get("job_id", "")),
                    str(entry.get("job_name", "")),
                    str(entry.get("status", "")),
                    str(entry.get("partition", "")),
                    str(entry.get("feature", "")),
                    ", ".join(entry.get("nodes", []) or []),
                    str(entry.get("reason", "")),
                ]
            ).lower()
            if normalizedQuery not in haystack:
                continue

        filteredEntries.append(entry)

    filteredEntries = list(reversed(filteredEntries))
    filteredEntriesTotal = len(filteredEntries)
    pageSize = max(1, limit)
    totalPages = max(1, (filteredEntriesTotal + pageSize - 1) // pageSize)
    currentPage = min(max(1, page), totalPages)
    pageStart = (currentPage - 1) * pageSize
    pageEntries = filteredEntries[pageStart:pageStart + pageSize]
    statusCounts = {}
    for entry in filteredEntries:
        entryStatus = str(entry.get("status", "")).upper() or "UNKNOWN"
        statusCounts[entryStatus] = statusCounts.get(entryStatus, 0) + 1

    return {
        "file": str(logPath),
        "query": query,
        "job_id": normalizedJobId,
        "selected_statuses": selectedStatuses,
        "available_statuses": availableStatuses,
        "total_entries": len(entries),
        "filtered_entries": filteredEntriesTotal,
        "shown_entries": len(pageEntries),
        "page": currentPage,
        "page_size": pageSize,
        "total_pages": totalPages,
        "has_prev_page": currentPage > 1,
        "has_next_page": currentPage < totalPages,
        "status_counts": statusCounts,
        "entries": pageEntries,
    }
