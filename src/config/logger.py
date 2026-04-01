import logging
import os
import sys
import json
from datetime import datetime
from pathlib import Path

try:
    from loguru import logger
except ModuleNotFoundError:
    logger = None

file_logger = None
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
LOGURU_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {name}:{function}:{line} | {message}"
LOG_FILE_NAME = "taskshift.log"
JOB_LAUNCH_LOG_FILE_NAME = "job_launches.jsonl"


def _get_log_file_path() -> Path:
    projectRoot = Path(__file__).resolve().parents[2]
    logDir = projectRoot / "logs"
    logDir.mkdir(parents=True, exist_ok=True)
    return logDir / LOG_FILE_NAME


def _get_job_launch_log_file_path() -> Path:
    projectRoot = Path(__file__).resolve().parents[2]
    logDir = projectRoot / "logs"
    logDir.mkdir(parents=True, exist_ok=True)
    return logDir / JOB_LAUNCH_LOG_FILE_NAME


def _configure_standard_logging(logFilePath: Path):
    formatter = logging.Formatter(LOG_FORMAT)

    streamHandler = logging.StreamHandler(sys.stderr)
    streamHandler.setLevel(logging.DEBUG)
    streamHandler.setFormatter(formatter)

    fileHandler = logging.FileHandler(logFilePath, mode="a", encoding="utf-8")
    fileHandler.setLevel(logging.DEBUG)
    fileHandler.setFormatter(formatter)

    rootLogger = logging.getLogger()
    rootLogger.handlers.clear()
    rootLogger.setLevel(logging.DEBUG)
    rootLogger.addHandler(streamHandler)
    rootLogger.addHandler(fileHandler)


def setup_logger():
    global file_logger
    logFilePath = _get_log_file_path()
    _configure_standard_logging(logFilePath)
    sessionMarker = (
        f"========== TaskShift start | pid={os.getpid()} | "
        f"time={datetime.now().isoformat(timespec='seconds')} =========="
    )

    if logger is None:
        file_logger = logging.getLogger("taskshift.file")
        file_logger.info(sessionMarker)
        return

    logger.remove()
    logger.add(sys.stderr, level="DEBUG", format=LOGURU_FORMAT)
    logger.add(str(logFilePath), level="DEBUG", format=LOGURU_FORMAT, mode="a", enqueue=False)

    file_logger = logger
    logger.info(sessionMarker)


def build_job_launch_event(
    *,
    job,
    placement,
    launchTimestamp: int | None = None,
    status: str = "ATTEMPTED",
):
    timestamp = launchTimestamp or int(datetime.now().timestamp())
    return {
        "event": "job_launch",
        "status": status,
        "job_id": job.getID(),
        "job_name": job.jobName,
        "partition": job.partition,
        "feature": placement.featureName,
        "nodes": placement.nodeNames,
        "requested_cpus": job.getRequestedCpus(),
        "requested_gpus": job.getRequestedGpus(),
        "requested_nodes": job.getRequestedNodes(),
        "timelimit_minutes": job.getTimelimit(),
        "launched_at_unix": timestamp,
        "launched_at": datetime.fromtimestamp(timestamp).isoformat(timespec="seconds"),
    }


def append_job_launch_event(event: dict):
    logFilePath = _get_job_launch_log_file_path()
    with open(logFilePath, "a", encoding="utf-8") as file:
        json.dump(event, file, ensure_ascii=False)
        file.write("\n")
