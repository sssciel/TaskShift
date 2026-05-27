import logging
import threading
from pathlib import Path

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info

from .models import SchedulerConfig
from .paths import schedulerConfigFile


HOT_RELOADABLE_SCHEDULER_FIELDS = (
    "timelimit",
    "max_launched_jobs",
    "forecast_enabled",
    "forecast_data_dir",
    "forecast_model_dir",
    "forecast_skip_startup_training",
    "cluster_config_snapshot_interval_hours",
)


class SchedulerRuntimeConfig:
    DEFAULT_POLL_INTERVAL_SECONDS = 2.0

    def __init__(
        self,
        configPath: str | Path = schedulerConfigFile,
        initialConfig: SchedulerConfig | None = None,
        pollIntervalSeconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    ):
        self.configPath = Path(configPath).resolve()
        self.pollIntervalSeconds = float(pollIntervalSeconds)
        self._lock = threading.RLock()
        self._stopEvent = threading.Event()
        self._thread = None
        self._config = (initialConfig or self._load_config()).copy()
        self._lastSignature = self._read_signature()

    def get_config(self) -> SchedulerConfig:
        with self._lock:
            return self._config.copy()

    def hot_reload_enabled(self) -> bool:
        with self._lock:
            return bool(self._config.hot_reload_enabled)

    def start_background(self):
        if not self.hot_reload_enabled():
            logger.info("Scheduler hot reload is disabled in scheduler.yaml")
            return self

        with self._lock:
            if self._thread is not None:
                return self

            self._stopEvent.clear()
            self._thread = threading.Thread(
                target=self._watch_loop,
                daemon=True,
                name="taskshift-scheduler-config-watchdog",
            )
            self._thread.start()

        logger.info(
            f"Scheduler hot reload watchdog started for {self.configPath} "
            f"(poll interval: {self.pollIntervalSeconds:.1f}s)"
        )
        return self

    def reload_if_changed(self) -> list[str]:
        currentSignature = self._read_signature()
        if currentSignature == self._lastSignature:
            return []

        loadedConfig = self._load_config()
        changedFields = []
        ignoredFields = []

        with self._lock:
            previousConfig = self._config
            nextConfig = previousConfig.copy()
            for fieldName in HOT_RELOADABLE_SCHEDULER_FIELDS:
                previousValue = getattr(previousConfig, fieldName)
                nextValue = getattr(loadedConfig, fieldName)
                if previousValue != nextValue:
                    setattr(nextConfig, fieldName, nextValue)
                    changedFields.append(fieldName)

            if previousConfig.web_panel_enabled != loadedConfig.web_panel_enabled:
                ignoredFields.append("web_panel_enabled")
            if previousConfig.hot_reload_enabled != loadedConfig.hot_reload_enabled:
                ignoredFields.append("hot_reload_enabled")
            if previousConfig.cluster_config_refresh_command != loadedConfig.cluster_config_refresh_command:
                ignoredFields.append("cluster_config_refresh_command")
            if previousConfig.connector_mserver_url != loadedConfig.connector_mserver_url:
                ignoredFields.append("connector.mserver_url")
            if previousConfig.connector_api_token != loadedConfig.connector_api_token:
                ignoredFields.append("TASKSHIFT_MSERVER_API_TOKEN")
            if previousConfig.connector_timeout_seconds != loadedConfig.connector_timeout_seconds:
                ignoredFields.append("connector.timeout_seconds")
            if previousConfig.connector_target_qos != loadedConfig.connector_target_qos:
                ignoredFields.append("connector.target_qos")

            self._config = nextConfig
            self._lastSignature = currentSignature

        if changedFields:
            logger.info(
                "Applied hot-reloaded scheduler config fields: "
                + ", ".join(changedFields)
            )
        if ignoredFields:
            logger.info(
                "Ignored scheduler config fields that require restart: "
                + ", ".join(ignoredFields)
            )

        return changedFields

    def close(self):
        self._stopEvent.set()
        with self._lock:
            thread = self._thread
            self._thread = None
        if thread is not None:
            thread.join(timeout=max(self.pollIntervalSeconds * 2.0, 1.0))

    def _watch_loop(self):
        while not self._stopEvent.wait(self.pollIntervalSeconds):
            try:
                self.reload_if_changed()
            except Exception as error:
                logger.warning(f"Scheduler hot reload watchdog failed to refresh config: {error}")

    def _load_config(self) -> SchedulerConfig:
        return SchedulerConfig().loadConfig(str(self.configPath))

    def _read_signature(self) -> tuple[int, int] | None:
        if not self.configPath.exists():
            return None

        stat = self.configPath.stat()
        return (stat.st_mtime_ns, stat.st_size)
