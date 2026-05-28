from copy import deepcopy
from datetime import datetime
from pathlib import Path

from config import getSchedulerConfig
from config.timezone import now_in_timezone
from scheduler.attempt_cache import get_failed_job_pool_cleanup_status
from scheduler.cron import SCHEDULER_INTERVAL_MINUTES, get_scheduler_service_status
from scheduler.runtime_state import SchedulerRuntimeStateStore


def build_scheduler_system_status_payload(
    projectRoot: str | Path,
    schedulerController=None,
) -> dict:
    projectPath = Path(projectRoot).resolve()
    schedulerConfig = getSchedulerConfig()
    stateStore = SchedulerRuntimeStateStore(
        projectPath,
        SCHEDULER_INTERVAL_MINUTES,
        timezoneName=schedulerConfig.timezone,
    )
    runtimeState = stateStore.read()
    serviceStatus = get_scheduler_service_status(projectPath)
    failedJobPoolCleanup = get_failed_job_pool_cleanup_status()
    payload = deepcopy(runtimeState)

    payload["runtime_file"] = str(stateStore.filePath.resolve())
    payload["service"]["running"] = bool(serviceStatus["running"])
    payload["service"]["pid"] = serviceStatus["pid"]
    payload["service"]["pid_file"] = serviceStatus["pid_file"]
    payload["service"]["log_file"] = serviceStatus["log_file"]
    payload["service"]["interval_minutes"] = SCHEDULER_INTERVAL_MINUTES
    payload["service"]["timezone"] = schedulerConfig.timezone
    payload["service"]["manual_run_available"] = bool(
        schedulerController is not None and schedulerController.can_run_now()
    )

    if not serviceStatus["running"]:
        payload["service"]["next_run_at"] = None

    nextRunAt = _parse_iso_timestamp(payload["service"].get("next_run_at"))
    if serviceStatus["running"] and nextRunAt is not None:
        currentTime = now_in_timezone(schedulerConfig.timezone)
        if nextRunAt.tzinfo is None:
            currentTime = currentTime.replace(tzinfo=None)
        payload["service"]["countdown_seconds"] = max(
            0,
            int((nextRunAt - currentTime).total_seconds()),
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
        "can_reset_failed_job_pool": bool(schedulerController is not None),
        "failed_job_pool_cleanup_interval_seconds": failedJobPoolCleanup["cleanup_interval_seconds"],
        "failed_job_pool_next_cleanup_at": failedJobPoolCleanup["next_cleanup_at"],
    }
    return payload


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None
