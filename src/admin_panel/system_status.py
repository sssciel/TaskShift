from copy import deepcopy
from datetime import datetime
from pathlib import Path

from config import getSchedulerConfig
from scheduler.cron import SCHEDULER_INTERVAL_MINUTES, get_scheduler_service_status
from scheduler.runtime_state import SchedulerRuntimeStateStore


def build_scheduler_system_status_payload(
    projectRoot: str | Path,
    schedulerController=None,
) -> dict:
    projectPath = Path(projectRoot).resolve()
    stateStore = SchedulerRuntimeStateStore(projectPath, SCHEDULER_INTERVAL_MINUTES)
    runtimeState = stateStore.read()
    serviceStatus = get_scheduler_service_status(projectPath)
    schedulerConfig = getSchedulerConfig()
    payload = deepcopy(runtimeState)

    payload["runtime_file"] = str(stateStore.filePath.resolve())
    payload["service"]["running"] = bool(serviceStatus["running"])
    payload["service"]["pid"] = serviceStatus["pid"]
    payload["service"]["pid_file"] = serviceStatus["pid_file"]
    payload["service"]["log_file"] = serviceStatus["log_file"]
    payload["service"]["interval_minutes"] = SCHEDULER_INTERVAL_MINUTES
    payload["service"]["manual_run_available"] = bool(
        schedulerController is not None and schedulerController.can_run_now()
    )

    if not serviceStatus["running"]:
        payload["service"]["next_run_at"] = None

    nextRunAt = _parse_iso_timestamp(payload["service"].get("next_run_at"))
    if serviceStatus["running"] and nextRunAt is not None:
        payload["service"]["countdown_seconds"] = max(
            0,
            int((nextRunAt - datetime.now()).total_seconds()),
        )
    else:
        payload["service"]["countdown_seconds"] = None

    payload["service"]["status"] = payload["last_run"].get("status") or "idle"
    if not serviceStatus["running"]:
        payload["service"]["status"] = "inactive"
    elif payload["last_run"].get("status") == "running":
        payload["service"]["status"] = "running"
    elif payload["last_run"].get("error_kind") == "db_connection":
        payload["service"]["status"] = "db_connection_error"
    elif payload["last_run"].get("status") == "error":
        payload["service"]["status"] = "error"
    else:
        payload["service"]["status"] = "active"

    payload["controls"] = {
        "default_max_launched_jobs": schedulerConfig.max_launched_jobs,
        "can_run_now": bool(schedulerController is not None and schedulerController.can_run_now()),
    }
    return payload


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None
