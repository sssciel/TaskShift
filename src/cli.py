import argparse
import logging
import signal
from pathlib import Path

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info

from admin_panel import AdminPanelServer
from config import (
    clusterConfigFile,
    getSchedulerConfig,
    refreshClusterConfig,
    refreshClusterConfigIfDue,
    SchedulerRuntimeConfig,
    setSchedulerForecastDataDir,
)
from config.logger import setup_logger
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

    scheduleParser = subparsers.add_parser("schedule", help="Run scheduler loop in foreground every 15 minutes")
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

    runOnceParser = subparsers.add_parser("run-scheduler-once", help="Run one scheduler pass immediately")
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

    return resolve_project_path(effectiveSchedulerConfig.forecast_data_dir)


def resolve_export_output_dir(args) -> str | None:
    if getattr(args, "output_dir", None):
        return resolve_project_path(args.output_dir)

    return resolve_project_path(getSchedulerConfig().forecast_data_dir)


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


def run_scheduler_once(args, schedulerConfig=None, maxLaunchedJobsOverride: int | None = None):
    logger.debug("Creating MySQL connector")
    storage = register_resource(slurmStorage().create())
    effectiveSchedulerConfig = schedulerConfig or getSchedulerConfig()

    try:
        logger.info("Get pending jobs")
        connector = SlurmConnector()
        Scheduler(
            storage,
            connector,
            schedulerConfig=effectiveSchedulerConfig,
            forecastDataDir=resolve_forecast_data_dir(args, effectiveSchedulerConfig),
        ).schedule(
            maxLaunchedJobs=(
                maxLaunchedJobsOverride
                if maxLaunchedJobsOverride is not None
                else getattr(args, "max_launched_jobs", None)
            )
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
    def refresh_cluster_snapshot_if_due():
        schedulerConfig = (
            schedulerRuntimeConfig.get_config()
            if schedulerRuntimeConfig is not None
            else getSchedulerConfig()
        )
        snapshotStatus = refreshClusterConfigIfDue(
            command=resolve_cluster_refresh_command(schedulerConfig),
            snapshotIntervalHours=schedulerConfig.cluster_config_snapshot_interval_hours,
        )
        if snapshotStatus["refreshed"]:
            logger.info(f"Cluster config snapshot refreshed: {snapshotStatus['backup_file']}")
        elif snapshotStatus["reason"] == "window_not_elapsed":
            logger.info(
                "Cluster config snapshot refresh skipped: latest backup "
                f"{snapshotStatus.get('latest_backup_file')} from {snapshotStatus.get('latest_backup_at')} "
                "is still inside the configured refresh window."
            )
        elif snapshotStatus["reason"] == "disabled":
            logger.info("Cluster config snapshot auto-refresh is disabled in scheduler.yaml")
        elif snapshotStatus["reason"] == "refresh_failed_using_latest_backup":
            logger.warning(
                "Cluster live refresh failed; scheduler will continue using the latest backup "
                f"{snapshotStatus['backup_file']}. Error: {snapshotStatus.get('error')}"
            )
        elif snapshotStatus["reason"] == "refresh_failed_seeded_from_current_file":
            logger.warning(
                "Cluster live refresh failed; scheduler created a backup from the existing current config "
                f"{snapshotStatus['current_file']} and will continue using it via {snapshotStatus['backup_file']}. "
                f"Error: {snapshotStatus.get('error')}"
            )

    def execute_scheduler_pass(maxLaunchedJobs=None, trigger="scheduled"):
        logger.info(f"Scheduler tick started | trigger={trigger}")
        effectiveSchedulerConfig = (
            schedulerRuntimeConfig.get_config()
            if schedulerRuntimeConfig is not None
            else getSchedulerConfig()
        )
        refresh_cluster_snapshot_if_due()
        summary = run_scheduler_once(
            args,
            schedulerConfig=effectiveSchedulerConfig,
            maxLaunchedJobsOverride=maxLaunchedJobs,
        )
        logger.info(f"Scheduler tick finished | trigger={trigger}")
        return summary

    def job_runner():
        try:
            return execute_scheduler_pass(trigger="scheduled")
        except GracefulInterrupt:
            raise
        except Exception as error:
            logger.exception(f"Scheduler pass failed inside background service: {error}")

    if schedulerControlPlane is not None:
        schedulerControlPlane.set_job_runner(execute_scheduler_pass)

    logger.info("Starting scheduler loop in foreground. Stop with Ctrl+C")
    run_scheduler_service_loop(
        jobRunner=job_runner,
        projectRoot=get_default_project_root(),
        runImmediately=True,
        schedulerController=schedulerControlPlane,
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
    logger.info(f"Historical utilization series rebuilt from local raw cache in '{outputPath}'")


def run_refresh_cluster_config(args):
    schedulerConfig = getSchedulerConfig()
    refreshCommand = resolve_cluster_refresh_command(schedulerConfig)
    logger.info(f"Refreshing cluster config using command: {' '.join(refreshCommand)}")
    savedPaths = refreshClusterConfig(command=refreshCommand, filePath=args.output_file)
    logger.info(f"Cluster config was updated: {savedPaths['current_file']}")
    logger.info(f"Cluster config backup was saved: {savedPaths['backup_file']}")


def run_set_forecast_data_dir(args):
    schedulerConfig = setSchedulerForecastDataDir(args.path)
    logger.info(f"Scheduler forecast data directory was updated: {schedulerConfig.forecast_data_dir}")


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
