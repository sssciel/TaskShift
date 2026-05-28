import json
import logging
import threading
import time
import traceback
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from scheduler.attempt_cache import reset_failed_job_pool
from config.timezone import now_in_timezone

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info


SCHEDULER_RUNTIME_STATE_FILE_NAME = "scheduler_runtime_state.json"


def _isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None

    return value.isoformat(timespec="seconds")


def classify_scheduler_error(error: Exception) -> str:
    errorModule = type(error).__module__.lower()
    errorMessage = str(error).lower()
    if (
        errorModule.startswith("mysql.connector")
        or "mysql" in errorModule
        or "slurmdb" in errorMessage
        or "mysql-connector-python" in errorMessage
        or "access denied" in errorMessage
        or "database does not exist" in errorMessage
        or "can't connect to mysql" in errorMessage
        or "can not connect to mysql" in errorMessage
    ):
        return "db_connection"

    return "scheduler_error"


class SchedulerRuntimeStateStore:
    def __init__(self, projectRoot: str | Path, intervalMinutes: int, timezoneName: str = "Europe/Moscow"):
        self.projectRoot = Path(projectRoot).resolve()
        self.intervalMinutes = int(intervalMinutes)
        self.timezoneName = timezoneName
        self.filePath = self.projectRoot / "logs" / SCHEDULER_RUNTIME_STATE_FILE_NAME
        self.filePath.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        if not self.filePath.exists():
            self.write(self._build_default_state())

    def _build_default_state(self) -> dict:
        return {
            "updated_at": _isoformat(now_in_timezone(self.timezoneName)),
            "service": {
                "running": False,
                "pid": None,
                "interval_minutes": self.intervalMinutes,
                "next_run_at": None,
                "manual_run_available": False,
                "timezone": self.timezoneName,
            },
            "last_run": {
                "status": "idle",
                "trigger": None,
                "started_at": None,
                "finished_at": None,
                "duration_seconds": None,
                "requested_max_launched_jobs": None,
                "effective_max_launched_jobs": None,
                "error_kind": None,
                "error_message": None,
                "error_traceback": None,
                "pending_job_count": 0,
                "running_job_count": 0,
                "launched_count": 0,
                "skipped_by_timelimit": 0,
                "skipped_by_resources": 0,
                "skipped_by_forecast": 0,
                "skipped_by_failed_attempt_pool": 0,
                "failed_job_pool_size": 0,
                "attempted_job_ids": [],
                "pending_jobs": [],
            },
        }

    def read(self) -> dict:
        shouldRewrite = False
        with self._lock:
            try:
                payload = json.loads(self.filePath.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                payload = self._build_default_state()
                shouldRewrite = True

        if not isinstance(payload, dict):
            payload = self._build_default_state()
            shouldRewrite = True

        payload.setdefault("service", {})
        payload.setdefault("last_run", {})
        payload["service"].setdefault("interval_minutes", self.intervalMinutes)
        payload["service"].setdefault("running", False)
        payload["service"].setdefault("pid", None)
        payload["service"].setdefault("next_run_at", None)
        payload["service"].setdefault("manual_run_available", False)
        payload["service"].setdefault("timezone", self.timezoneName)
        payload["last_run"].setdefault("status", "idle")
        payload["last_run"].setdefault("skipped_by_forecast", 0)
        payload["last_run"].setdefault("attempted_job_ids", [])
        payload["last_run"].setdefault("pending_jobs", [])
        if shouldRewrite:
            self.write(payload)
        return payload

    def write(self, payload: dict):
        normalizedPayload = dict(payload)
        normalizedPayload["updated_at"] = _isoformat(now_in_timezone(self.timezoneName))
        with self._lock:
            self.filePath.write_text(
                json.dumps(normalizedPayload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    def mutate(self, mutator):
        payload = self.read()
        mutator(payload)
        self.write(payload)
        return payload


class SchedulerControlPlane:
    def __init__(self, projectRoot: str | Path, intervalMinutes: int, timezoneName: str = "Europe/Moscow"):
        self.projectRoot = Path(projectRoot).resolve()
        self.intervalMinutes = int(intervalMinutes)
        self.timezoneName = timezoneName
        self.stateStore = SchedulerRuntimeStateStore(self.projectRoot, self.intervalMinutes, timezoneName=timezoneName)
        self._scheduler = None
        self._schedulerJobId = None
        self._jobRunner = None
        self._runLock = threading.Lock()

    def close(self):
        self.mark_service_stopped()

    def set_job_runner(self, jobRunner):
        self._jobRunner = jobRunner

    def bind_scheduler(self, scheduler, schedulerJobId: str, pid: int, nextRunAt: datetime | None = None):
        self._scheduler = scheduler
        self._schedulerJobId = schedulerJobId
        self.stateStore.mutate(
            lambda payload: payload["service"].update(
                {
                    "running": True,
                    "pid": pid,
                    "interval_minutes": self.intervalMinutes,
                    "next_run_at": _isoformat(nextRunAt),
                    "manual_run_available": True,
                    "timezone": self.timezoneName,
                }
            )
        )

    def mark_service_stopped(self):
        self.stateStore.mutate(
            lambda payload: payload["service"].update(
                {
                    "running": False,
                    "pid": None,
                    "next_run_at": None,
                    "manual_run_available": False,
                }
            )
        )

    def get_state(self) -> dict:
        return self.stateStore.read()

    def can_run_now(self) -> bool:
        return self._jobRunner is not None and self._scheduler is not None

    def run_scheduled_tick(self):
        return self._execute(trigger="scheduled", maxLaunchedJobs=None, failIfBusy=False)

    def run_startup_tick(self):
        return self._execute(trigger="startup", maxLaunchedJobs=None, failIfBusy=False)

    def request_manual_run(self, maxLaunchedJobs: int | None = None):
        if not self.can_run_now():
            raise RuntimeError("Manual scheduler run is unavailable because the scheduler service is not active.")

        if not self._runLock.acquire(blocking=False):
            raise RuntimeError("Scheduler run is already in progress.")

        worker = threading.Thread(
            target=self._run_manual_tick,
            args=(maxLaunchedJobs,),
            daemon=True,
            name="taskshift-manual-scheduler-run",
        )
        worker.start()
        return self.get_state()

    def reset_failed_job_cache(self):
        if not self._runLock.acquire(blocking=False):
            raise RuntimeError("Failed-job cache cannot be reset while a scheduler run is already in progress.")

        try:
            reset_failed_job_pool()
            self.stateStore.mutate(
                lambda payload: payload["last_run"].update(
                    {
                        "failed_job_pool_size": 0,
                    }
                )
            )
            return self.get_state()
        finally:
            self._runLock.release()

    def _run_manual_tick(self, maxLaunchedJobs: int | None):
        try:
            self._execute(
                trigger="manual",
                maxLaunchedJobs=maxLaunchedJobs,
                failIfBusy=True,
                lockAlreadyHeld=True,
            )
        except Exception as error:
            logger.warning(f"Manual scheduler run failed: {error}")

    def _execute(self, *, trigger: str, maxLaunchedJobs: int | None, failIfBusy: bool, lockAlreadyHeld: bool = False):
        if not lockAlreadyHeld and not self._runLock.acquire(blocking=False):
            if failIfBusy:
                raise RuntimeError("Scheduler run is already in progress.")
            return None

        startedAt = now_in_timezone(self.timezoneName)
        startedMonotonic = time.monotonic()
        self.stateStore.mutate(
            lambda payload: payload["last_run"].update(
                {
                    "status": "running",
                    "trigger": trigger,
                    "started_at": _isoformat(startedAt),
                    "finished_at": None,
                    "duration_seconds": None,
                    "requested_max_launched_jobs": maxLaunchedJobs,
                    "error_kind": None,
                    "error_message": None,
                    "error_traceback": None,
                }
            )
        )

        try:
            summary = self._jobRunner(maxLaunchedJobs=maxLaunchedJobs, trigger=trigger) or {}
        except Exception as error:
            finishedAt = now_in_timezone(self.timezoneName)
            self.stateStore.mutate(
                lambda payload: payload["last_run"].update(
                    {
                        "status": "error",
                        "trigger": trigger,
                        "finished_at": _isoformat(finishedAt),
                        "duration_seconds": round(time.monotonic() - startedMonotonic, 3),
                        "error_kind": classify_scheduler_error(error),
                        "error_message": str(error),
                        "error_traceback": traceback.format_exc(),
                        "effective_max_launched_jobs": maxLaunchedJobs,
                        "pending_job_count": 0,
                        "running_job_count": 0,
                        "launched_count": 0,
                        "skipped_by_timelimit": 0,
                        "skipped_by_resources": 0,
                        "skipped_by_forecast": 0,
                        "skipped_by_failed_attempt_pool": 0,
                        "failed_job_pool_size": 0,
                        "attempted_job_ids": [],
                        "pending_jobs": [],
                    }
                )
            )
            logger.exception(f"Scheduler pass failed | trigger={trigger}: {error}")
            return None
        else:
            finishedAt = now_in_timezone(self.timezoneName)
            self.stateStore.mutate(
                lambda payload: payload["last_run"].update(
                    {
                        "status": "success",
                        "trigger": trigger,
                        "finished_at": _isoformat(finishedAt),
                        "duration_seconds": round(time.monotonic() - startedMonotonic, 3),
                        "effective_max_launched_jobs": summary.get("effective_max_launched_jobs"),
                        "pending_job_count": summary.get("pending_job_count", 0),
                        "running_job_count": summary.get("running_job_count", 0),
                        "launched_count": summary.get("launched_count", 0),
                        "skipped_by_timelimit": summary.get("skipped_by_timelimit", 0),
                        "skipped_by_resources": summary.get("skipped_by_resources", 0),
                        "skipped_by_forecast": summary.get("skipped_by_forecast", 0),
                        "skipped_by_failed_attempt_pool": summary.get("skipped_by_failed_attempt_pool", 0),
                        "failed_job_pool_size": summary.get("failed_job_pool_size", 0),
                        "attempted_job_ids": summary.get("attempted_job_ids", []),
                        "pending_jobs": summary.get("pending_jobs", []),
                        "error_kind": None,
                        "error_message": None,
                        "error_traceback": None,
                    }
                )
            )
            return summary
        finally:
            self._reschedule_next_run()
            self._runLock.release()

    def _reschedule_next_run(self):
        nextRunAt = now_in_timezone(self.timezoneName) + timedelta(minutes=self.intervalMinutes)
        if self._scheduler is not None and self._schedulerJobId is not None:
            try:
                self._scheduler.modify_job(self._schedulerJobId, next_run_time=nextRunAt)
            except Exception as error:
                logger.warning(f"Failed to reschedule next scheduler tick: {error}")

        self.stateStore.mutate(
            lambda payload: payload["service"].update(
                {
                    "next_run_at": _isoformat(nextRunAt),
                    "interval_minutes": self.intervalMinutes,
                    "manual_run_available": self.can_run_now(),
                }
            )
        )
        return nextRunAt
