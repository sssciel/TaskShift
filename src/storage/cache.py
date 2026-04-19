import json
from datetime import datetime
from pathlib import Path

from .models import RawHistoricalJobRow
from .timeutils import parse_time_value


def load_state(statePath: Path) -> dict:
    if not statePath.exists():
        return {}

    with open(statePath, "r", encoding="utf-8") as file:
        return json.load(file)


def save_state(statePath: Path, state: dict):
    with open(statePath, "w", encoding="utf-8") as file:
        json.dump(state, file, indent=2, ensure_ascii=False)


def load_cached_historical_job_rows(rawJobsPath: Path) -> list[RawHistoricalJobRow]:
    if not rawJobsPath.exists():
        return []

    with open(rawJobsPath, "r", encoding="utf-8") as file:
        payload = json.load(file)

    return [RawHistoricalJobRow.from_dict(item) for item in payload]


def save_cached_historical_job_rows(rawJobsPath: Path, rows: list[RawHistoricalJobRow]):
    with open(rawJobsPath, "w", encoding="utf-8") as file:
        json.dump([row.to_dict() for row in rows], file, indent=2, ensure_ascii=False)


def resolve_history_start(historyStart, state: dict) -> int | None:
    if state.get("history_start") is not None:
        return state["history_start"]

    return parse_time_value(historyStart)


def build_state_payload(
    previousState: dict,
    mergedRows: list[RawHistoricalJobRow],
    historyStartTimestamp: int | None,
    modifiedUntilTimestamp: int | None,
) -> dict:
    lastModTime = max(
        (row.mod_time for row in mergedRows),
        default=previousState.get("last_mod_time", 0) if previousState else 0,
    )
    return {
        "history_start": historyStartTimestamp,
        "modified_until": modifiedUntilTimestamp,
        "last_mod_time": lastModTime,
        "job_count": len(mergedRows),
        "last_sync_at": datetime.now().isoformat(),
    }
