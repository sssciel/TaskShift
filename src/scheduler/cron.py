import logging
import os
import signal
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info


LEGACY_TASKSHIFT_CRON_MARKER = "# taskshift-scheduler"
SCHEDULER_INTERVAL_MINUTES = 15
SCHEDULER_JOB_ID = "taskshift-scheduler"


def start_scheduler_service_process(
    projectRoot: str,
    withoutForecast: bool = False,
    maxLaunchedJobs: int | None = None,
) -> int:
    projectPath = Path(projectRoot).resolve()
    ensure_scheduler_runtime_dirs(projectPath)
    remove_legacy_scheduler_cron()

    status = get_scheduler_service_status(projectPath)
    if status["running"]:
        return status["pid"]

    command = build_scheduler_service_command(
        projectRoot=projectPath,
        withoutForecast=withoutForecast,
        maxLaunchedJobs=maxLaunchedJobs,
    )

    logFile = get_scheduler_log_file(projectPath)
    with open(logFile, "a", encoding="utf-8") as stream:
        process = subprocess.Popen(
            command,
            cwd=str(projectPath),
            stdout=stream,
            stderr=stream,
            start_new_session=True,
        )

    write_scheduler_pid_file(projectPath, process.pid)
    return process.pid


def stop_scheduler_service_process(projectRoot: str) -> int | None:
    projectPath = Path(projectRoot).resolve()
    pid = read_scheduler_pid(projectPath)
    if pid is None:
        remove_stale_scheduler_pid_file(projectPath)
        return None

    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        remove_stale_scheduler_pid_file(projectPath)
        return None
    except PermissionError as error:
        raise RuntimeError(f"Failed to stop scheduler service PID {pid}: {error}") from error

    deadline = time.time() + 10.0
    while time.time() < deadline:
        if not is_process_running(pid):
            remove_stale_scheduler_pid_file(projectPath)
            return pid
        time.sleep(0.2)

    try:
        os.kill(pid, signal.SIGKILL)
    except PermissionError as error:
        raise RuntimeError(f"Failed to force-stop scheduler service PID {pid}: {error}") from error
    remove_stale_scheduler_pid_file(projectPath)
    return pid


def get_scheduler_service_status(projectRoot: str | Path) -> dict[str, int | bool | str | None]:
    projectPath = Path(projectRoot).resolve()
    pid = read_scheduler_pid(projectPath)
    running = pid is not None and is_process_running(pid)
    if not running:
        remove_stale_scheduler_pid_file(projectPath)

    return {
        "running": running,
        "pid": pid if running else None,
        "pid_file": str(get_scheduler_pid_file(projectPath)),
        "log_file": str(get_scheduler_log_file(projectPath)),
    }


def run_scheduler_service_loop(
    jobRunner,
    projectRoot: str,
    runImmediately: bool = False,
    schedulerController=None,
    backgroundJobs: list[dict] | None = None,
):
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.triggers.interval import IntervalTrigger
    except ModuleNotFoundError as error:
        raise ModuleNotFoundError("apscheduler is required to run the background scheduler service.") from error

    projectPath = Path(projectRoot).resolve()
    pid = os.getpid()
    ensure_scheduler_runtime_dirs(projectPath)
    write_scheduler_pid_file(projectPath, pid)

    scheduler = BlockingScheduler()
    nextRunAt = datetime.now() + timedelta(minutes=SCHEDULER_INTERVAL_MINUTES)
    scheduledJobRunner = jobRunner
    if schedulerController is not None:
        scheduledJobRunner = schedulerController.run_scheduled_tick
        schedulerController.bind_scheduler(
            scheduler=scheduler,
            schedulerJobId=SCHEDULER_JOB_ID,
            pid=pid,
            nextRunAt=nextRunAt,
        )

    scheduler.add_job(
        scheduledJobRunner,
        trigger=IntervalTrigger(minutes=SCHEDULER_INTERVAL_MINUTES),
        id=SCHEDULER_JOB_ID,
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=SCHEDULER_INTERVAL_MINUTES * 60,
        next_run_time=nextRunAt,
    )

    for backgroundJob in backgroundJobs or []:
        jobKind = backgroundJob.get("kind", "cron")
        if jobKind == "cron":
            scheduler.add_job(
                backgroundJob["runner"],
                trigger=CronTrigger(
                    day_of_week=backgroundJob["day_of_week"],
                    hour=backgroundJob.get("hour", 0),
                    minute=backgroundJob.get("minute", 0),
                ),
                id=backgroundJob["id"],
                replace_existing=True,
                coalesce=True,
                max_instances=1,
                misfire_grace_time=backgroundJob.get("misfire_grace_time"),
            )
            continue

        raise ValueError(f"Unsupported background job kind: {jobKind}")

    def handle_stop(signum, frame):
        logger.info("Stopping TaskShift scheduler service")
        scheduler.shutdown(wait=False)

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    try:
        logger.info(
            f"TaskShift scheduler service started with PID {pid}. "
            f"Interval: every {SCHEDULER_INTERVAL_MINUTES} minutes"
        )
        if runImmediately:
            if schedulerController is not None:
                schedulerController.run_startup_tick()
            else:
                jobRunner()
        scheduler.start()
    finally:
        if schedulerController is not None:
            schedulerController.mark_service_stopped()
        cleanup_scheduler_pid_file(projectPath, pid)


def ensure_scheduler_runtime_dirs(projectRoot: str | Path):
    projectPath = Path(projectRoot).resolve()
    get_scheduler_log_file(projectPath).parent.mkdir(parents=True, exist_ok=True)
    get_scheduler_pid_file(projectPath).parent.mkdir(parents=True, exist_ok=True)


def build_scheduler_service_command(
    projectRoot: str | Path,
    withoutForecast: bool = False,
    maxLaunchedJobs: int | None = None,
) -> list[str]:
    projectPath = Path(projectRoot).resolve()
    taskshiftPath = projectPath / "taskshift"
    command = ["/bin/sh", str(taskshiftPath), "schedule"]

    if withoutForecast:
        command.append("--without-forecast")

    if maxLaunchedJobs is not None:
        command.append(f"--max-launched-jobs={int(maxLaunchedJobs)}")

    return command


def remove_legacy_scheduler_cron():
    try:
        existingLines = read_current_crontab_lines()
    except Exception as error:
        logger.warning(f"Failed to inspect existing crontab while removing legacy scheduler entry: {error}")
        return

    filteredLines = [line for line in existingLines if LEGACY_TASKSHIFT_CRON_MARKER not in line]
    if filteredLines == existingLines:
        return

    try:
        write_crontab_lines(filteredLines)
        logger.info("Removed legacy TaskShift crontab entry")
    except Exception as error:
        logger.warning(f"Failed to remove legacy TaskShift crontab entry: {error}")


def read_current_crontab_lines() -> list[str]:
    result = subprocess.run(
        ["crontab", "-l"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = (result.stderr or "").lower()
        if "no crontab" in stderr:
            return []

        raise RuntimeError(f"Failed to read current crontab: {result.stderr.strip()}")

    return [line for line in result.stdout.splitlines() if line.strip()]


def write_crontab_lines(lines: list[str]):
    payload = "\n".join(lines).strip()
    if payload:
        payload += "\n"

    subprocess.run(
        ["crontab", "-"],
        input=payload,
        text=True,
        check=True,
    )


def read_scheduler_pid(projectRoot: str | Path) -> int | None:
    pidFile = get_scheduler_pid_file(projectRoot)
    if not pidFile.exists():
        return None

    try:
        return int(pidFile.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def write_scheduler_pid_file(projectRoot: str | Path, pid: int):
    pidFile = get_scheduler_pid_file(projectRoot)
    pidFile.write_text(str(pid), encoding="utf-8")


def cleanup_scheduler_pid_file(projectRoot: str | Path, expectedPid: int):
    pidFile = get_scheduler_pid_file(projectRoot)
    if not pidFile.exists():
        return

    currentPid = read_scheduler_pid(projectRoot)
    if currentPid != expectedPid:
        return

    pidFile.unlink(missing_ok=True)


def remove_stale_scheduler_pid_file(projectRoot: str | Path):
    get_scheduler_pid_file(projectRoot).unlink(missing_ok=True)


def is_process_running(pid: int) -> bool:
    if pid <= 0:
        return False

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True

    return True


def get_default_project_root() -> str:
    return str(Path(__file__).resolve().parents[2])


def get_scheduler_pid_file(projectRoot: str | Path) -> Path:
    return Path(projectRoot).resolve() / "logs" / "scheduler_service.pid"


def get_scheduler_log_file(projectRoot: str | Path) -> Path:
    return Path(projectRoot).resolve() / "logs" / "scheduler_service.log"
