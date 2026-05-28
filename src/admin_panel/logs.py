import json
import re
from collections import deque
from pathlib import Path

from config.logger import (
    FORECAST_RUNTIME_LOG_FILE_NAME,
    JOB_RUNTIME_LOG_FILE_NAME,
    SCHEDULER_RUNTIME_LOG_FILE_NAME,
)

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
    normalizedQuery = query.strip().lower()
    totalEntries = 0
    filteredEntriesTotal = 0
    availableStatuses = set()
    statusCounts = {}

    for entry in _iter_taskshift_log_entries(logPath):
        totalEntries += 1
        availableStatuses.add(entry["level"])
        if not _taskshift_entry_matches(
            entry,
            normalizedQuery=normalizedQuery,
            selectedStatuses=selectedStatuses,
        ):
            continue
        filteredEntriesTotal += 1
        statusCounts[entry["level"]] = statusCounts.get(entry["level"], 0) + 1

    pageSize = max(1, limit)
    totalPages = max(1, (filteredEntriesTotal + pageSize - 1) // pageSize)
    currentPage = min(max(1, page), totalPages)
    pageEntries = _collect_paginated_entries(
        entries=_iter_taskshift_log_entries(logPath),
        matcher=lambda entry: _taskshift_entry_matches(
            entry,
            normalizedQuery=normalizedQuery,
            selectedStatuses=selectedStatuses,
        ),
        currentPage=currentPage,
        pageSize=pageSize,
    )

    return {
        "file": str(logPath),
        "query": query,
        "selected_statuses": selectedStatuses,
        "available_statuses": sorted(availableStatuses),
        "total_entries": totalEntries,
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
    totalEntries = 0
    filteredEntriesTotal = 0
    availableStatuses = set()
    statusCounts = {}

    for entry in _iter_job_log_entries(logPath):
        totalEntries += 1
        entryStatus = str(entry.get("status", "")).upper()
        if entryStatus:
            availableStatuses.add(entryStatus)
        if not _job_log_entry_matches(
            entry,
            normalizedQuery=normalizedQuery,
            normalizedJobId=normalizedJobId,
            selectedStatuses=selectedStatuses,
        ):
            continue
        filteredEntriesTotal += 1
        normalizedStatus = entryStatus or "UNKNOWN"
        statusCounts[normalizedStatus] = statusCounts.get(normalizedStatus, 0) + 1

    pageSize = max(1, limit)
    totalPages = max(1, (filteredEntriesTotal + pageSize - 1) // pageSize)
    currentPage = min(max(1, page), totalPages)
    pageEntries = _collect_paginated_entries(
        entries=_iter_job_log_entries(logPath),
        matcher=lambda entry: _job_log_entry_matches(
            entry,
            normalizedQuery=normalizedQuery,
            normalizedJobId=normalizedJobId,
            selectedStatuses=selectedStatuses,
        ),
        currentPage=currentPage,
        pageSize=pageSize,
    )

    return {
        "file": str(logPath),
        "query": query,
        "job_id": normalizedJobId,
        "selected_statuses": selectedStatuses,
        "available_statuses": sorted(availableStatuses),
        "total_entries": totalEntries,
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


def build_scheduler_runtime_log_payload(
    *,
    query: str = "",
    statuses: list[str] | None = None,
    page: int = 1,
    limit: int = 100,
) -> dict:
    return _build_structured_runtime_log_payload(
        fileName=SCHEDULER_RUNTIME_LOG_FILE_NAME,
        query=query,
        statuses=statuses,
        page=page,
        limit=limit,
    )


def build_forecast_runtime_log_payload(
    *,
    query: str = "",
    statuses: list[str] | None = None,
    page: int = 1,
    limit: int = 100,
) -> dict:
    return _build_structured_runtime_log_payload(
        fileName=FORECAST_RUNTIME_LOG_FILE_NAME,
        query=query,
        statuses=statuses,
        page=page,
        limit=limit,
    )


def build_job_runtime_log_payload(
    *,
    query: str = "",
    statuses: list[str] | None = None,
    page: int = 1,
    limit: int = 100,
) -> dict:
    logPath = get_logs_root() / JOB_RUNTIME_LOG_FILE_NAME
    return _build_structured_runtime_log_payload(
        fileName=JOB_RUNTIME_LOG_FILE_NAME,
        query=query,
        statuses=statuses,
        page=page,
        limit=limit,
        duplicateFailedPoolKeys=_collect_failed_attempt_keys(logPath),
    )


def _iter_taskshift_log_entries(logPath: Path):
    if not logPath.exists():
        return

    currentEntry = None
    with open(logPath, "r", encoding="utf-8", errors="replace") as file:
        for rawLine in file:
            line = rawLine.rstrip("\r\n")
            match = TASKSHIFT_LOG_PATTERN.match(line)
            if match:
                if currentEntry is not None:
                    yield currentEntry
                currentEntry = {
                    "timestamp": match.group("timestamp"),
                    "level": match.group("level"),
                    "source": match.group("source").strip(),
                    "message": match.group("message"),
                    "raw": line,
                }
                continue

            if currentEntry is None:
                currentEntry = {
                    "timestamp": None,
                    "level": "OTHER",
                    "source": "unstructured",
                    "message": line,
                    "raw": line,
                }
                continue

            currentEntry["message"] += f"\n{line}"
            currentEntry["raw"] += f"\n{line}"

    if currentEntry is not None:
        yield currentEntry


def _iter_job_log_entries(logPath: Path):
    if not logPath.exists():
        return

    with open(logPath, "r", encoding="utf-8", errors="replace") as file:
        for rawLine in file:
            line = rawLine.rstrip("\r\n")
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue

            payload["status"] = normalize_job_log_status(payload.get("status"))
            payload["raw"] = line
            yield payload


def _build_structured_runtime_log_payload(
    *,
    fileName: str,
    query: str,
    statuses: list[str] | None,
    page: int,
    limit: int,
    duplicateFailedPoolKeys: set[tuple[str, str]] | None = None,
) -> dict:
    logPath = get_logs_root() / fileName
    selectedStatuses = normalize_status_filters(statuses)
    normalizedQuery = query.strip().lower()
    totalEntries = 0
    filteredEntriesTotal = 0
    availableStatuses = set()
    statusCounts = {}
    runIds = set()

    for entry in _iter_structured_runtime_log_entries(logPath):
        if _is_duplicate_failed_pool_entry(entry, duplicateFailedPoolKeys):
            continue
        totalEntries += 1
        entryStatus = str(entry.get("status") or "UNKNOWN").upper()
        availableStatuses.add(entryStatus)
        if not _structured_runtime_log_entry_matches(
            entry,
            normalizedQuery=normalizedQuery,
            selectedStatuses=selectedStatuses,
        ):
            continue
        filteredEntriesTotal += 1
        statusCounts[entryStatus] = statusCounts.get(entryStatus, 0) + 1
        if entry.get("run_id"):
            runIds.add(str(entry["run_id"]))

    pageSize = max(1, limit)
    totalPages = max(1, (filteredEntriesTotal + pageSize - 1) // pageSize)
    currentPage = min(max(1, page), totalPages)
    pageEntries = _collect_paginated_entries(
        entries=_iter_structured_runtime_log_entries(logPath),
        matcher=lambda entry: (
            not _is_duplicate_failed_pool_entry(entry, duplicateFailedPoolKeys)
            and _structured_runtime_log_entry_matches(
                entry,
                normalizedQuery=normalizedQuery,
                selectedStatuses=selectedStatuses,
            )
        ),
        currentPage=currentPage,
        pageSize=pageSize,
    )

    return {
        "file": str(logPath),
        "query": query,
        "selected_statuses": selectedStatuses,
        "available_statuses": sorted(availableStatuses),
        "total_entries": totalEntries,
        "filtered_entries": filteredEntriesTotal,
        "shown_entries": len(pageEntries),
        "page": currentPage,
        "page_size": pageSize,
        "total_pages": totalPages,
        "has_prev_page": currentPage > 1,
        "has_next_page": currentPage < totalPages,
        "status_counts": statusCounts,
        "run_ids": sorted(runIds),
        "run_count": len(runIds),
        "entries": pageEntries,
    }


def _iter_structured_runtime_log_entries(logPath: Path):
    if not logPath.exists():
        return

    with open(logPath, "r", encoding="utf-8", errors="replace") as file:
        for rawLine in file:
            line = rawLine.rstrip("\r\n")
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue

            normalized = {
                "timestamp": payload.get("timestamp"),
                "timestamp_unix": payload.get("timestamp_unix"),
                "level": str(payload.get("level") or "INFO").upper(),
                "status": str(payload.get("status") or payload.get("event_type") or "UNKNOWN").upper(),
                "event_type": str(payload.get("event_type") or payload.get("status") or "UNKNOWN").upper(),
                "source": str(payload.get("source") or payload.get("category") or "runtime"),
                "category": payload.get("category"),
                "message": str(payload.get("message") or ""),
                "run_id": payload.get("run_id"),
                "job_id": payload.get("job_id"),
                "job_name": payload.get("job_name"),
                "trigger": payload.get("trigger"),
                "raw": line,
                "data": payload,
            }
            yield normalized


def _collect_failed_attempt_keys(logPath: Path) -> set[tuple[str, str]]:
    keys = set()
    for entry in _iter_structured_runtime_log_entries(logPath):
        if str(entry.get("status") or "").upper() != "LAUNCH_FAILED":
            continue
        key = _job_runtime_dedupe_key(entry)
        if key is not None:
            keys.add(key)
    return keys


def _is_duplicate_failed_pool_entry(
    entry: dict,
    duplicateFailedPoolKeys: set[tuple[str, str]] | None,
) -> bool:
    if not duplicateFailedPoolKeys:
        return False
    if str(entry.get("status") or "").upper() != "BLOCKED_FAILED_POOL":
        return False
    key = _job_runtime_dedupe_key(entry)
    return key in duplicateFailedPoolKeys


def _job_runtime_dedupe_key(entry: dict) -> tuple[str, str] | None:
    runId = entry.get("run_id")
    jobId = entry.get("job_id")
    if runId in {None, ""} or jobId in {None, ""}:
        return None
    return str(runId), str(jobId)


def _taskshift_entry_matches(entry: dict, *, normalizedQuery: str, selectedStatuses: list[str]) -> bool:
    if selectedStatuses and entry["level"] not in selectedStatuses:
        return False

    if not normalizedQuery:
        return True

    haystack = "\n".join(
        [
            entry.get("timestamp") or "",
            entry.get("level") or "",
            entry.get("source") or "",
            entry.get("message") or "",
        ]
    ).lower()
    return normalizedQuery in haystack


def _job_log_entry_matches(
    entry: dict,
    *,
    normalizedQuery: str,
    normalizedJobId: str,
    selectedStatuses: list[str],
) -> bool:
    entryStatus = str(entry.get("status", "")).upper()
    if selectedStatuses and entryStatus not in selectedStatuses:
        return False

    if normalizedJobId and normalizedJobId not in str(entry.get("job_id", "")):
        return False

    if not normalizedQuery:
        return True

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
    return normalizedQuery in haystack


def _structured_runtime_log_entry_matches(
    entry: dict,
    *,
    normalizedQuery: str,
    selectedStatuses: list[str],
) -> bool:
    entryStatus = str(entry.get("status") or "UNKNOWN").upper()
    if selectedStatuses and entryStatus not in selectedStatuses:
        return False

    if not normalizedQuery:
        return True

    haystack = "\n".join(
        [
            str(entry.get("timestamp") or ""),
            str(entry.get("level") or ""),
            str(entry.get("status") or ""),
            str(entry.get("event_type") or ""),
            str(entry.get("source") or ""),
            str(entry.get("message") or ""),
            str(entry.get("run_id") or ""),
            str(entry.get("job_id") or ""),
            str(entry.get("job_name") or ""),
            json.dumps(entry.get("data") or {}, ensure_ascii=False, sort_keys=True),
        ]
    ).lower()
    return normalizedQuery in haystack


def _collect_paginated_entries(*, entries, matcher, currentPage: int, pageSize: int) -> list[dict]:
    if pageSize <= 0:
        return []

    keepCount = currentPage * pageSize
    if keepCount <= 0:
        return []

    pageBuffer = deque(maxlen=keepCount)
    for entry in entries:
        if matcher(entry):
            pageBuffer.append(entry)

    pageStart = (currentPage - 1) * pageSize
    reversedEntries = list(reversed(pageBuffer))
    return reversedEntries[pageStart:pageStart + pageSize]
