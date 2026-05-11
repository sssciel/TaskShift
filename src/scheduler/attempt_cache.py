import time

_launch_attempts: list[dict] = []
_failed_job_pool: set[int] = set()
_initialized_at: float | None = None
CLEANUP_INTERVAL_SECONDS = 12 * 60 * 60  # 12 hours


def _maybe_cleanup():
    global _initialized_at, _launch_attempts, _failed_job_pool

    if _initialized_at is None:
        _initialized_at = time.time()
        return

    if time.time() - _initialized_at >= CLEANUP_INTERVAL_SECONDS:
        _launch_attempts = []
        _failed_job_pool = set()
        _initialized_at = time.time()


def reset_cache():
    global _initialized_at, _launch_attempts, _failed_job_pool

    _launch_attempts = []
    _failed_job_pool = set()
    _initialized_at = None


def load_launch_attempts() -> list[dict]:
    _maybe_cleanup()
    return list(_launch_attempts)


def save_launch_attempts(attempts: list[dict]):
    _maybe_cleanup()
    global _launch_attempts
    _launch_attempts = list(attempts)


def load_failed_job_pool() -> set[int]:
    _maybe_cleanup()
    return set(_failed_job_pool)


def save_failed_job_pool(jobIds: set[int] | list[int]):
    _maybe_cleanup()
    global _failed_job_pool
    _failed_job_pool = {int(x) for x in jobIds}
