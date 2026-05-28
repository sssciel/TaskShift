import argparse
import logging
import signal
from datetime import datetime
from pathlib import Path

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info

from admin_panel import AdminPanelServer
from config import (
    SchedulerRuntimeConfig,
    clusterConfigFile,
    getSchedulerConfig,
    refreshClusterConfig,
    setSchedulerForecastDataDir,
)
from config.timezone import now_in_timezone, resolve_timezone
from config.logger import (
    append_forecast_runtime_event,
    append_job_runtime_event,
    append_scheduler_runtime_event,
    build_runtime_log_event,
    build_scheduler_run_id,
    setup_logger,
)
from forecast.training import (
    DEFAULT_FORECAST_PREDICTION_HORIZON_HOURS,
    DEFAULT_MODEL_UPDATE_INTERVAL_HOURS,
    load_artifact,
    resolve_artifact_refresh_due_at,
    resolve_data_dir,
    resolve_model_dir,
    train_gradient_boosting_forecast,
)
from scheduler import Scheduler, SlurmConnector
from scheduler.cron import (
    SCHEDULER_INTERVAL_MINUTES,
    get_default_project_root,
    run_scheduler_service_loop,
)
from scheduler.runtime_state import SchedulerControlPlane
from storage import slurmStorage


class GracefulInterrupt(Exception):
    pass


ACTIVE_RESOURCES = []


def _get_numeric_config_value(config, fieldName: str, defaultValue):
    value = getattr(config, fieldName, defaultValue)
    if isinstance(value, bool):
        return defaultValue
    if isinstance(value, (int, float)):
        return value
    return defaultValue


def register_resource(resource):
    ACTIVE_RESOURCES.append(resource)
    return resource


def unregister_resource(resource):
    if resource in ACTIVE_RESOURCES:
        ACTIVE_RESOURCES.remove(resource)


def handle_sigint(signum, frame):
    raise GracefulInterrupt()


def cleanup_active_resources():
    while ACTIVE_RESOURCES:
        resource = ACTIVE_RESOURCES.pop()
        close = getattr(resource, "close", None)
        if close is None:
            continue

        try:
            close()
        except Exception as error:
            logger.warning(f"Failed to close resource during shutdown: {error}")


def build_parser():
    parser = argparse.ArgumentParser(description="TaskShift command line interface")
    subparsers = parser.add_subparsers(dest="command")

    scheduleParser = subparsers.add_parser(
        "schedule", help="Run scheduler loop in foreground every 15 minutes"
    )
    add_scheduler_run_arguments(scheduleParser)
    scheduleParser.add_argument(
        "--with-web-panel",
        action="store_true",
        help="Start the admin web panel alongside the scheduler loop",
    )
    scheduleParser.add_argument(
        "--without-web-panel",
        action="store_true",
        help="Disable the admin web panel even if it is enabled in scheduler.yaml",
    )

    runOnceParser = subparsers.add_parser(
        "run-scheduler-once", help="Run one scheduler pass immediately"
    )
    add_scheduler_run_arguments(runOnceParser)

    webPanelParser = subparsers.add_parser(
        "serve-web-panel",
        help="Run the admin web panel for cluster inspection and config editing",
    )

    refreshClusterParser = subparsers.add_parser(
        "refresh-cluster-config",
        help="Refresh cluster nodes and partitions config from /etc/slurm/slurm.conf",
    )
    refreshClusterParser.add_argument(
        "--output-file",
        default=clusterConfigFile,
        help="Path where the parsed cluster config YAML should be written",
    )

    setForecastParser = subparsers.add_parser(
        "set-forecast-data-dir",
        help="Persist default forecast data directory in scheduler.yaml",
    )
    setForecastParser.add_argument(
        "path",
        help="Directory with exported utilization data. Accepts either the export root or its series subdirectory",
    )

    exportParser = subparsers.add_parser(
        "export",
        help="Incrementally export historical utilization series into separate files",
    )
    exportParser.add_argument(
        "--output-dir",
        default=None,
        help="Export directory with raw cache, state, and series files. Defaults to scheduler forecast_data_dir",
    )
    exportParser.add_argument(
        "--interval-minutes",
        type=int,
        default=15,
        help="Aggregation step in minutes",
    )
    exportParser.add_argument(
        "--history-start",
        default=None,
        help="Initial lower bound for mod_time on the first sync. Accepts unix timestamp or ISO datetime",
    )
    exportParser.add_argument(
        "--modified-until",
        default=None,
        help="Optional upper bound for mod_time. Accepts unix timestamp or ISO datetime",
    )
    exportParser.add_argument(
        "--now-timestamp",
        default=None,
        help="Optional end timestamp for currently running jobs. Accepts unix timestamp or ISO datetime",
    )

    rebuildParser = subparsers.add_parser(
        "rebuild-series",
        help="Rebuild historical utilization series only from local raw cache without MySQL access",
    )
    rebuildParser.add_argument(
        "--output-dir",
        default=None,
        help="Export directory containing raw_job_rows.json and state.json. Defaults to scheduler forecast_data_dir",
    )
    rebuildParser.add_argument(
        "--interval-minutes",
        type=int,
        default=15,
        help="Aggregation step in minutes",
    )
    rebuildParser.add_argument(
        "--now-timestamp",
        default=None,
        help="Optional end timestamp for currently running jobs. Accepts unix timestamp or ISO datetime",
    )

    trainForecastParser = subparsers.add_parser(
        "train-forecast-model",
        help="Refresh exported utilization data and train the GPU forecast model artifact",
    )
    trainForecastParser.add_argument(
        "--data-dir",
        default=None,
        help="Directory with exported historical utilization data. Defaults to scheduler forecast_data_dir",
    )
    trainForecastParser.add_argument(
        "--model-dir",
        default=None,
        help="Directory where the trained forecast model artifact should be stored. Defaults to scheduler forecast_model_dir",
    )
    trainForecastParser.add_argument(
        "--skip-export",
        action="store_true",
        help="Train the model from the existing export without refreshing historical utilization first",
    )

    return parser


def add_scheduler_run_arguments(parser):
    parser.add_argument(
        "--max-launched-jobs",
        type=int,
        default=None,
        help="Maximum number of jobs to launch during one scheduler pass",
    )
    parser.add_argument(
        "--without-forecast",
        action="store_true",
        help="Disable forecast usage and treat forecast-constrained resources as fully available",
    )


def resolve_forecast_data_dir(args, schedulerConfig=None) -> str | None:
    if getattr(args, "without_forecast", False):
        return None

    effectiveSchedulerConfig = schedulerConfig or getSchedulerConfig()
    if not effectiveSchedulerConfig.forecast_enabled:
        return None

    return str(resolve_data_dir(get_default_project_root()))


def resolve_export_output_dir(args, schedulerConfig=None) -> str | None:
    if getattr(args, "data_dir", None):
        return str(resolve_data_dir(get_default_project_root(), args.data_dir))

    if getattr(args, "output_dir", None):
        return str(resolve_data_dir(get_default_project_root(), args.output_dir))

    return str(resolve_data_dir(get_default_project_root()))


def resolve_forecast_model_dir(args=None, schedulerConfig=None) -> str | None:
    if args is not None and getattr(args, "model_dir", None):
        return resolve_project_path(args.model_dir)

    effectiveSchedulerConfig = schedulerConfig or getSchedulerConfig()
    return resolve_project_path(effectiveSchedulerConfig.forecast_model_dir)


def resolve_cluster_refresh_command(schedulerConfig=None) -> list[str]:
    effectiveSchedulerConfig = schedulerConfig or getSchedulerConfig()
    return list(effectiveSchedulerConfig.cluster_config_refresh_command)


def resolve_project_path(pathValue: str | None) -> str | None:
    if not pathValue:
        return None

    path = Path(pathValue)
    if path.is_absolute():
        return str(path)

    return str((Path(get_default_project_root()) / path).resolve())


def run_scheduler_once(
    args,
    schedulerConfig=None,
    maxLaunchedJobsOverride: int | None = None,
    runId: str | None = None,
    trigger: str = "scheduled",
    bootstrapForecast: bool = True,
):
    effectiveSchedulerConfig = schedulerConfig or getSchedulerConfig()
    effectiveRunId = runId or build_scheduler_run_id(trigger=trigger)
    if bootstrapForecast:
        bootstrap_forecast_runtime(
            args=args,
            schedulerConfig=effectiveSchedulerConfig,
            startupReason=f"{trigger}_scheduler_pass",
        )

    logger.debug("Creating MySQL connector")
    storage = register_resource(slurmStorage().create())

    try:
        logger.info("Get pending jobs")
        connector = SlurmConnector(
            mserverUrl=effectiveSchedulerConfig.connector_mserver_url,
            apiToken=effectiveSchedulerConfig.connector_api_token,
            targetQos=effectiveSchedulerConfig.connector_target_qos,
            timeoutSeconds=effectiveSchedulerConfig.connector_timeout_seconds,
            jobRuntimeEventWriter=append_job_runtime_event,
        )
        return Scheduler(
            storage,
            connector,
            schedulerConfig=effectiveSchedulerConfig,
            forecastDataDir=resolve_forecast_data_dir(args, effectiveSchedulerConfig),
            jobRuntimeEventWriter=append_job_runtime_event,
            schedulerRuntimeEventWriter=append_scheduler_runtime_event,
        ).schedule(
            maxLaunchedJobs=(
                maxLaunchedJobsOverride
                if maxLaunchedJobsOverride is not None
                else getattr(args, "max_launched_jobs", None)
            ),
            runId=effectiveRunId,
            trigger=trigger,
        )
    finally:
        unregister_resource(storage)
        storage.close()


def run_schedule(args):
    webPanelServer = None
    schedulerRuntimeConfig = None
    schedulerControlPlane = None
    try:
        schedulerConfig = getSchedulerConfig()
        schedulerControlPlane = register_resource(
            SchedulerControlPlane(
                projectRoot=get_default_project_root(),
                intervalMinutes=SCHEDULER_INTERVAL_MINUTES,
                timezoneName=schedulerConfig.timezone,
            )
        )
        schedulerRuntimeConfig = register_resource(
            SchedulerRuntimeConfig(initialConfig=schedulerConfig)
        )
        schedulerRuntimeConfig.start_background()
        shouldRunWebPanel = schedulerConfig.web_panel_enabled
        if getattr(args, "with_web_panel", False):
            shouldRunWebPanel = True
        if getattr(args, "without_web_panel", False):
            shouldRunWebPanel = False

        if shouldRunWebPanel:
            webPanelServer = register_resource(
                AdminPanelServer(
                    get_default_project_root(),
                    schedulerController=schedulerControlPlane,
                ).start_background()
            )
            logger.info(f"Admin web panel is available at {webPanelServer.base_url}")

        bootstrap_forecast_runtime(
            args=args,
            schedulerConfig=schedulerRuntimeConfig.get_config(),
            startupReason="service_startup",
        )
        run_scheduler_loop(
            args,
            schedulerRuntimeConfig=schedulerRuntimeConfig,
            schedulerControlPlane=schedulerControlPlane,
        )
    finally:
        if webPanelServer is not None:
            unregister_resource(webPanelServer)
            webPanelServer.close()
        if schedulerRuntimeConfig is not None:
            unregister_resource(schedulerRuntimeConfig)
            schedulerRuntimeConfig.close()
        if schedulerControlPlane is not None:
            unregister_resource(schedulerControlPlane)
            schedulerControlPlane.close()


def run_scheduler_loop(args, schedulerRuntimeConfig=None, schedulerControlPlane=None):
    def execute_scheduler_pass(maxLaunchedJobs=None, trigger="scheduled"):
        effectiveSchedulerConfig = (
            schedulerRuntimeConfig.get_config()
            if schedulerRuntimeConfig is not None
            else getSchedulerConfig()
        )
        startedAt = now_in_timezone(effectiveSchedulerConfig.timezone)
        runId = build_scheduler_run_id(trigger=trigger, startedAt=startedAt)
        logger.info(f"Scheduler tick started | trigger={trigger} | run_id={runId}")
        append_scheduler_runtime_event(
            build_runtime_log_event(
                category="scheduler_runtime",
                status="RUN_STARTED",
                eventType="RUN_STARTED",
                message=f"Scheduler tick started with trigger '{trigger}'.",
                timestamp=int(startedAt.timestamp()),
                source="cli.run_scheduler_loop",
                run_id=runId,
                trigger=trigger,
                requested_max_launched_jobs=maxLaunchedJobs,
            )
        )
        try:
            summary = run_scheduler_once(
                args,
                schedulerConfig=effectiveSchedulerConfig,
                maxLaunchedJobsOverride=maxLaunchedJobs,
                runId=runId,
                trigger=trigger,
                bootstrapForecast=False,
            )
        except Exception as error:
            append_scheduler_runtime_event(
                build_runtime_log_event(
                    category="scheduler_runtime",
                    status="RUN_FAILED",
                    level="ERROR",
                    eventType="RUN_FAILED",
                    message=f"Scheduler tick failed with trigger '{trigger}': {error}",
                    source="cli.run_scheduler_loop",
                    run_id=runId,
                    trigger=trigger,
                    error=str(error),
                )
            )
            raise

        logger.info(f"Scheduler tick finished | trigger={trigger} | run_id={runId}")
        if isinstance(summary, dict):
            summary["run_id"] = runId
        return summary

    def train_forecast_model_job():
        effectiveSchedulerConfig = (
            schedulerRuntimeConfig.get_config()
            if schedulerRuntimeConfig is not None
            else getSchedulerConfig()
        )
        if getattr(args, "without_forecast", False):
            logger.info("Scheduled forecast model training skipped because forecast is disabled by CLI flag")
            append_forecast_runtime_event(
                build_runtime_log_event(
                    category="forecast_runtime",
                    status="TRAINING_SKIPPED",
                    eventType="TRAINING_SKIPPED",
                    message="Scheduled forecast model training skipped because forecast is disabled by CLI flag.",
                    source="cli.run_scheduler_loop",
                    reason="forecast_disabled_by_cli_flag",
                )
            )
            return None
        if not effectiveSchedulerConfig.forecast_enabled:
            logger.info("Scheduled forecast model training skipped because forecast is disabled in scheduler config")
            append_forecast_runtime_event(
                build_runtime_log_event(
                    category="forecast_runtime",
                    status="TRAINING_SKIPPED",
                    eventType="TRAINING_SKIPPED",
                    message="Scheduled forecast model training skipped because forecast is disabled in scheduler config.",
                    source="cli.run_scheduler_loop",
                    reason="forecast_disabled_in_scheduler_config",
                )
            )
            return None
        return bootstrap_forecast_runtime(
            args=args,
            schedulerConfig=effectiveSchedulerConfig,
            startupReason="scheduled_forecast_refresh",
        )

    def refresh_cluster_snapshot_job():
        effectiveSchedulerConfig = (
            schedulerRuntimeConfig.get_config()
            if schedulerRuntimeConfig is not None
            else getSchedulerConfig()
        )
        try:
            savedPaths = refreshClusterConfig(
                command=resolve_cluster_refresh_command(effectiveSchedulerConfig)
            )
            logger.info(
                f"Cluster config snapshot refreshed by scheduled job: {savedPaths['backup_file']}"
            )
            return savedPaths
        except Exception as error:
            logger.warning(f"Scheduled cluster config refresh failed: {error}")
            return None

    def job_runner():
        try:
            return execute_scheduler_pass(trigger="scheduled")
        except GracefulInterrupt:
            raise
        except Exception as error:
            logger.exception(
                f"Scheduler pass failed inside background service: {error}"
            )

    if schedulerControlPlane is not None:
        schedulerControlPlane.set_job_runner(execute_scheduler_pass)

    logger.info("Starting scheduler loop in foreground. Stop with Ctrl+C")
    backgroundJobs = []
    effectiveSchedulerConfig = (
        schedulerRuntimeConfig.get_config()
        if schedulerRuntimeConfig is not None
        else getSchedulerConfig()
    )
    timezoneName = effectiveSchedulerConfig.timezone
    refreshHour, refreshMinute = (
        int(part) for part in effectiveSchedulerConfig.cluster_config_refresh_time.split(":")
    )
    if not getattr(args, "without_forecast", False) and effectiveSchedulerConfig.forecast_enabled:
        forecastIntervalHours = getattr(
            effectiveSchedulerConfig,
            "forecast_model_update_interval_hours",
            DEFAULT_MODEL_UPDATE_INTERVAL_HOURS,
        )
        modelDir = resolve_forecast_model_dir(schedulerConfig=effectiveSchedulerConfig)
        nextForecastRunAt = None
        if modelDir:
            artifact = load_artifact(resolve_model_dir(get_default_project_root(), modelDir), includeModel=False)
            nextForecastRunAt = resolve_artifact_refresh_due_at(
                artifact.metadata if artifact is not None else None,
                updateIntervalHours=forecastIntervalHours,
                timezoneName=timezoneName,
            )
        if nextForecastRunAt is None:
            nextForecastRunAt = now_in_timezone(timezoneName)
        backgroundJobs.append(
            {
                "id": "taskshift-forecast-model-training",
                "kind": "interval",
                "runner": train_forecast_model_job,
                "hours": forecastIntervalHours,
                "next_run_time": nextForecastRunAt,
                "misfire_grace_time": 12 * 60 * 60,
            }
        )
    backgroundJobs.append(
        {
            "id": "taskshift-cluster-config-refresh",
            "kind": "cron",
            "runner": refresh_cluster_snapshot_job,
            "day_of_week": "*",
            "hour": refreshHour,
            "minute": refreshMinute,
            "misfire_grace_time": 12 * 60 * 60,
        }
    )
    run_scheduler_service_loop(
        jobRunner=job_runner,
        projectRoot=get_default_project_root(),
        runImmediately=True,
        schedulerController=schedulerControlPlane,
        backgroundJobs=backgroundJobs,
        timezoneName=timezoneName,
    )


def run_export(args):
    logger.debug("Creating MySQL connector for historical utilization export")
    storage = register_resource(slurmStorage().create())

    try:
        outputPath = storage.exportIncrementalHistoricalUtilization(
            outputDir=resolve_export_output_dir(args),
            intervalMinutes=args.interval_minutes,
            historyStart=args.history_start,
            modifiedUntil=args.modified_until,
            nowTimestamp=args.now_timestamp,
        )
        logger.info(f"Historical utilization series exported to '{outputPath}'")
    finally:
        unregister_resource(storage)
        storage.close()


def run_rebuild_series(args):
    storage = slurmStorage()
    outputPath = storage.rebuildHistoricalUtilizationFromCache(
        outputDir=resolve_export_output_dir(args),
        intervalMinutes=args.interval_minutes,
        nowTimestamp=args.now_timestamp,
    )
    logger.info(
        f"Historical utilization series rebuilt from local raw cache in '{outputPath}'"
    )


def run_train_forecast_model(args=None, schedulerConfig=None, refreshData: bool | None = None):
    effectiveSchedulerConfig = schedulerConfig or getSchedulerConfig()
    dataDir = resolve_export_output_dir(args or argparse.Namespace(), effectiveSchedulerConfig)
    modelDir = resolve_forecast_model_dir(args=args, schedulerConfig=effectiveSchedulerConfig)
    if not dataDir:
        raise RuntimeError("Forecast data directory is not configured")
    if not modelDir:
        raise RuntimeError("Forecast model directory is not configured")

    shouldRefreshData = refreshData
    if shouldRefreshData is None:
        shouldRefreshData = not getattr(args, "skip_export", False)

    artifact = train_gradient_boosting_forecast(
        dataDir=dataDir,
        modelDir=modelDir,
        projectRoot=get_default_project_root(),
        refreshData=bool(shouldRefreshData),
        eventWriter=append_forecast_runtime_event,
        timezoneName=effectiveSchedulerConfig.timezone,
        modelUpdateIntervalHours=_get_numeric_config_value(
            effectiveSchedulerConfig,
            "forecast_model_update_interval_hours",
            DEFAULT_MODEL_UPDATE_INTERVAL_HOURS,
        ),
        forecastPredictionHorizonHours=_get_numeric_config_value(
            effectiveSchedulerConfig,
            "forecast_prediction_horizon_hours",
            DEFAULT_FORECAST_PREDICTION_HORIZON_HOURS,
        ),
    )
    logger.info(
        "Forecast model training finished: "
        f"model_kind={artifact.metadata.get('model_kind')} "
        f"target={artifact.metadata.get('target_name')} "
        f"trained_at={artifact.metadata.get('trained_at')} "
        f"next_6h_gpu_mean_forecast={artifact.metadata.get('last_prediction_gpu_percent')}"
    )
    return artifact


def bootstrap_forecast_runtime(args=None, schedulerConfig=None, startupReason: str = "service_startup"):
    effectiveArgs = args or argparse.Namespace()
    effectiveSchedulerConfig = schedulerConfig or getSchedulerConfig()
    eventSource = "cli.bootstrap_forecast_runtime"

    if getattr(effectiveArgs, "without_forecast", False):
        logger.info("Forecast startup bootstrap skipped because forecast is disabled by CLI flag")
        append_forecast_runtime_event(
            build_runtime_log_event(
                category="forecast_runtime",
                status="BOOTSTRAP_SKIPPED",
                eventType="BOOTSTRAP_SKIPPED",
                message="Forecast startup bootstrap skipped because forecast is disabled by CLI flag.",
                source=eventSource,
                reason="forecast_disabled_by_cli_flag",
                startup_reason=startupReason,
            )
        )
        return None

    if not effectiveSchedulerConfig.forecast_enabled:
        logger.info("Forecast startup bootstrap skipped because forecast is disabled in scheduler config")
        append_forecast_runtime_event(
            build_runtime_log_event(
                category="forecast_runtime",
                status="BOOTSTRAP_SKIPPED",
                eventType="BOOTSTRAP_SKIPPED",
                message="Forecast startup bootstrap skipped because forecast is disabled in scheduler config.",
                source=eventSource,
                reason="forecast_disabled_in_scheduler_config",
                startup_reason=startupReason,
            )
        )
        return None

    dataDir = resolve_export_output_dir(effectiveArgs, effectiveSchedulerConfig)
    modelDir = resolve_forecast_model_dir(args=effectiveArgs, schedulerConfig=effectiveSchedulerConfig)
    if not modelDir:
        raise RuntimeError("Forecast model directory is not configured")

    resolvedModelDir = resolve_model_dir(get_default_project_root(), modelDir)
    modelUpdateIntervalHours = _get_numeric_config_value(
        effectiveSchedulerConfig,
        "forecast_model_update_interval_hours",
        DEFAULT_MODEL_UPDATE_INTERVAL_HOURS,
    )
    forecastPredictionHorizonHours = _get_numeric_config_value(
        effectiveSchedulerConfig,
        "forecast_prediction_horizon_hours",
        DEFAULT_FORECAST_PREDICTION_HORIZON_HOURS,
    )
    timezoneName = effectiveSchedulerConfig.timezone
    existingArtifact = load_artifact(resolvedModelDir, includeModel=False)
    if existingArtifact is not None:
        dueAt = resolve_artifact_refresh_due_at(
            existingArtifact.metadata,
            updateIntervalHours=modelUpdateIntervalHours,
            timezoneName=timezoneName,
        )
        now = now_in_timezone(timezoneName)
        if dueAt is not None and now < dueAt:
            logger.info(
                "Forecast startup bootstrap reused a fresh artifact: "
                f"data_dir={dataDir} model_dir={resolvedModelDir} "
                f"trained_at={existingArtifact.metadata.get('trained_at')} "
                f"next_refresh_due_at={dueAt.isoformat(timespec='seconds')}"
            )
            append_forecast_runtime_event(
                build_runtime_log_event(
                    category="forecast_runtime",
                    status="BOOTSTRAP_READY",
                    eventType="BOOTSTRAP_READY",
                    message="Forecast startup bootstrap reused the current fresh model artifact.",
                    source=eventSource,
                    startup_reason=startupReason,
                    startup_training_skipped=True,
                    data_dir=dataDir,
                    model_dir=str(resolvedModelDir),
                    trained_at=existingArtifact.metadata.get("trained_at"),
                    next_refresh_due_at=dueAt.isoformat(timespec="seconds"),
                    cached_prediction_gpu_percent=existingArtifact.metadata.get("last_prediction_gpu_percent"),
                    forecast_prediction_horizon_hours=forecastPredictionHorizonHours,
                )
            )
            return existingArtifact

    append_forecast_runtime_event(
        build_runtime_log_event(
            category="forecast_runtime",
            status="BOOTSTRAP_STARTED",
            eventType="BOOTSTRAP_STARTED",
            message=(
                "Forecast startup bootstrap started: refresh historical utilization, "
                "rebuild the series, and train the model before scheduler work begins."
            ),
            source=eventSource,
            startup_reason=startupReason,
            data_dir=dataDir,
            model_dir=str(resolvedModelDir),
            model_update_interval_hours=modelUpdateIntervalHours,
            forecast_prediction_horizon_hours=forecastPredictionHorizonHours,
            timezone=timezoneName,
        )
    )

    if getattr(effectiveSchedulerConfig, "forecast_skip_startup_training", False):
        artifact = existingArtifact
        if artifact is None:
            raise RuntimeError(
                "Forecast startup training is disabled, but no saved model artifact was found. "
                "Enable startup training or train the model manually before starting the scheduler service."
            )
        logger.warning(
            "Forecast startup bootstrap reused the existing artifact because startup training is disabled: "
            f"data_dir={dataDir} model_dir={resolvedModelDir}"
        )
        append_forecast_runtime_event(
            build_runtime_log_event(
                category="forecast_runtime",
                status="BOOTSTRAP_READY",
                eventType="BOOTSTRAP_READY",
                message="Forecast startup bootstrap loaded the existing model artifact without retraining.",
                source=eventSource,
                startup_reason=startupReason,
                startup_training_skipped=True,
                data_dir=dataDir,
                model_dir=str(resolvedModelDir),
                trained_at=artifact.metadata.get("trained_at"),
                cached_prediction_gpu_percent=artifact.metadata.get("last_prediction_gpu_percent"),
                model_update_interval_hours=modelUpdateIntervalHours,
                forecast_prediction_horizon_hours=forecastPredictionHorizonHours,
                timezone=timezoneName,
            )
        )
        return artifact

    try:
        artifact = train_gradient_boosting_forecast(
            dataDir=dataDir,
            modelDir=str(resolvedModelDir),
            projectRoot=get_default_project_root(),
            refreshData=True,
            eventWriter=append_forecast_runtime_event,
            timezoneName=timezoneName,
            modelUpdateIntervalHours=modelUpdateIntervalHours,
            forecastPredictionHorizonHours=forecastPredictionHorizonHours,
        )
    except Exception as error:
        append_forecast_runtime_event(
            build_runtime_log_event(
                category="forecast_runtime",
                status="BOOTSTRAP_FAILED",
                level="ERROR",
                eventType="BOOTSTRAP_FAILED",
                message=f"Forecast startup bootstrap failed: {error}",
                source=eventSource,
                startup_reason=startupReason,
                data_dir=dataDir,
                model_dir=str(resolvedModelDir),
            model_update_interval_hours=modelUpdateIntervalHours,
            forecast_prediction_horizon_hours=forecastPredictionHorizonHours,
            timezone=timezoneName,
            error=str(error),
            )
        )
        raise

    logger.info(
        "Forecast startup bootstrap finished: "
        f"data_dir={dataDir} "
        f"model_dir={resolvedModelDir} "
        f"trained_at={artifact.metadata.get('trained_at')} "
        f"next_6h_gpu_mean_forecast={artifact.metadata.get('last_prediction_gpu_percent')}"
    )
    append_forecast_runtime_event(
        build_runtime_log_event(
            category="forecast_runtime",
            status="BOOTSTRAP_READY",
            eventType="BOOTSTRAP_READY",
            message="Forecast startup bootstrap finished. Scheduler can use the freshly trained model.",
            source=eventSource,
            startup_reason=startupReason,
            startup_training_skipped=False,
            data_dir=dataDir,
            model_dir=str(resolvedModelDir),
            trained_at=artifact.metadata.get("trained_at"),
            cached_prediction_gpu_percent=artifact.metadata.get("last_prediction_gpu_percent"),
            next_refresh_due_at=artifact.metadata.get("next_refresh_due_at"),
            model_update_interval_hours=modelUpdateIntervalHours,
            forecast_prediction_horizon_hours=forecastPredictionHorizonHours,
            timezone=timezoneName,
        )
    )
    return artifact


def run_refresh_cluster_config(args):
    schedulerConfig = getSchedulerConfig()
    refreshCommand = resolve_cluster_refresh_command(schedulerConfig)
    logger.info(f"Refreshing cluster config using command: {' '.join(refreshCommand)}")
    savedPaths = refreshClusterConfig(command=refreshCommand, filePath=args.output_file)
    logger.info(f"Cluster config was updated: {savedPaths['current_file']}")
    logger.info(f"Cluster config backup was saved: {savedPaths['backup_file']}")


def run_set_forecast_data_dir(args):
    schedulerConfig = setSchedulerForecastDataDir(args.path)
    logger.info(
        f"Scheduler forecast data directory was updated: {schedulerConfig.forecast_data_dir}"
    )


def run_serve_web_panel(args):
    webPanelServer = register_resource(AdminPanelServer(get_default_project_root()))
    logger.info(f"Admin web panel is available at {webPanelServer.base_url}")
    try:
        webPanelServer.serve_forever()
    finally:
        unregister_resource(webPanelServer)
        webPanelServer.close()


def main():
    setup_logger()
    signal.signal(signal.SIGINT, handle_sigint)
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command is None:
            parser.print_help()
            return 2

        if args.command == "schedule":
            run_schedule(args)
            return 0

        if args.command == "run-scheduler-once":
            run_scheduler_once(args)
            return 0

        if args.command == "refresh-cluster-config":
            run_refresh_cluster_config(args)
            return 0

        if args.command == "set-forecast-data-dir":
            run_set_forecast_data_dir(args)
            return 0

        if args.command == "serve-web-panel":
            run_serve_web_panel(args)
            return 0

        if args.command == "export":
            run_export(args)
            return 0

        if args.command == "rebuild-series":
            run_rebuild_series(args)
            return 0

        if args.command == "train-forecast-model":
            run_train_forecast_model(args)
            return 0

        parser.error(f"Unknown command: {args.command}")
        return 2
    except GracefulInterrupt:
        logger.warning("Interrupted by user, shutting down gracefully")
        cleanup_active_resources()
        return 130
    except KeyboardInterrupt:
        logger.warning("Interrupted by user, shutting down gracefully")
        cleanup_active_resources()
        return 130
