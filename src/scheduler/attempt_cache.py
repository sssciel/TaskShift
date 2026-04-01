import json
from pathlib import Path


ATTEMPT_CACHE_FILE_NAME = "job_launch_attempts.json"


def _get_attempt_cache_file_path() -> Path:
    projectRoot = Path(__file__).resolve().parents[2]
    logDir = projectRoot / "logs"
    logDir.mkdir(parents=True, exist_ok=True)
    return logDir / ATTEMPT_CACHE_FILE_NAME


def load_launch_attempts() -> list[dict]:
    cacheFilePath = _get_attempt_cache_file_path()
    if not cacheFilePath.exists():
        return []

    try:
        with open(cacheFilePath, "r", encoding="utf-8") as file:
            payload = json.load(file)
    except (OSError, json.JSONDecodeError):
        return []

    if not isinstance(payload, list):
        return []

    return [item for item in payload if isinstance(item, dict)]


def save_launch_attempts(attempts: list[dict]):
    cacheFilePath = _get_attempt_cache_file_path()
    with open(cacheFilePath, "w", encoding="utf-8") as file:
        json.dump(attempts, file, ensure_ascii=False, indent=2)
