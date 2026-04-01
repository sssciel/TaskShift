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

from config import clusterConfigFile, refreshClusterConfig
from config.logger import setup_logger
from scheduler import Scheduler, SlurmConnector
from scheduler.cron import (
    get_default_project_root,
    run_scheduler_service_loop,
)
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

    runOnceParser = subparsers.add_parser("run-scheduler-once", help="Run one scheduler pass immediately")
    add_scheduler_run_arguments(runOnceParser)

    runServiceParser = subparsers.add_parser("run-scheduler-service", help="Internal scheduler process")
    add_scheduler_run_arguments(runServiceParser)

    refreshClusterParser = subparsers.add_parser(
        "refresh-cluster-config",
        help="Refresh cluster nodes and partitions config from /etc/slurm/slurm.conf",
    )
    refreshClusterParser.add_argument(
        "--output-file",
        default=clusterConfigFile,
        help="Path where the parsed cluster config YAML should be written",
    )

    exportParser = subparsers.add_parser(
        "export",
        help="Incrementally export historical utilization series into separate files",
    )
    exportParser.add_argument(
        "--output-dir",
        default="exports/historical_utilization/current",
        help="Stable export directory with raw cache, state, and series files",
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

    return parser


def add_scheduler_run_arguments(parser):
    parser.add_argument(
        "--forecast-data-dir",
        default=None,
        help="Directory with exported utilization data. Accepts either the export root or its series subdirectory",
    )
    parser.add_argument(
        "--max-launched-jobs",
        type=int,
        default=None,
        help="Maximum number of jobs to launch during one scheduler pass",
    )


def run_scheduler_once(args):
    logger.debug("Creating MySQL connector")
    storage = register_resource(slurmStorage().create())

    try:
        logger.info("Get pending jobs")
        connector = SlurmConnector()
        Scheduler(
            storage,
            connector,
            forecastDataDir=getattr(args, "forecast_data_dir", None),
        ).schedule(maxLaunchedJobs=getattr(args, "max_launched_jobs", None))
    finally:
        unregister_resource(storage)
        storage.close()


def run_schedule(args):
    run_scheduler_service(args)


def run_scheduler_service(args):
    def job_runner():
        logger.info("Scheduler tick started")
        try:
            run_scheduler_once(args)
        except GracefulInterrupt:
            raise
        except Exception as error:
            logger.exception(f"Scheduler pass failed inside background service: {error}")
        else:
            logger.info("Scheduler tick finished")

    logger.info("Starting scheduler loop in foreground. Stop with Ctrl+C")
    run_scheduler_service_loop(jobRunner=job_runner, projectRoot=get_default_project_root(), runImmediately=True)


def run_export(args):
    logger.debug("Creating MySQL connector for historical utilization export")
    storage = register_resource(slurmStorage().create())

    try:
        outputPath = storage.exportIncrementalHistoricalUtilization(
            outputDir=args.output_dir,
            intervalMinutes=args.interval_minutes,
            historyStart=args.history_start,
            modifiedUntil=args.modified_until,
            nowTimestamp=args.now_timestamp,
        )
        logger.info(f"Historical utilization series exported to '{outputPath}'")
    finally:
        unregister_resource(storage)
        storage.close()


def run_refresh_cluster_config(args):
    logger.info("Refreshing cluster config from /etc/slurm/slurm.conf")
    refreshClusterConfig(filePath=args.output_file)
    logger.info(f"Cluster config was updated: {args.output_file}")


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
            run_scheduler_service(args)
            return 0

        if args.command == "run-scheduler-once":
            run_scheduler_once(args)
            return 0

        if args.command == "refresh-cluster-config":
            run_refresh_cluster_config(args)
            return 0

        if args.command == "export":
            run_export(args)
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
