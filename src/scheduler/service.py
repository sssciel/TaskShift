import logging
from datetime import datetime

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info

from config import append_job_launch_event, build_job_launch_event, getClusterConfig, getSchedulerConfig
from forecast import ForecastService
from .attempt_cache import load_launch_attempts, save_launch_attempts
from .resources import ResourceAvailabilityTree


class Scheduler:
    def __init__(self, storage, connector, forecastDataDir: str | None = None):
        self.config = getSchedulerConfig()
        self.clusterConfig = getClusterConfig()
        self.forecastService = ForecastService(dataDir=forecastDataDir)
        self.storage = storage
        self.connector = connector

    def schedule(self, maxLaunchedJobs: int | None = None):
        currentTimestamp = int(datetime.now().timestamp())
        jobs = self.storage.getPendingJobs()
        self._reconcileLaunchAttempts(jobs, currentTimestamp)
        runningJobs = self.storage.getRunningJobs(currentTimestamp)
        resourceTree = ResourceAvailabilityTree.fromClusterAndJobs(
            clusterConfig=self.clusterConfig,
            runningJobs=runningJobs,
            timestamp=currentTimestamp,
        )
        launchedJobsCount = 0
        skippedByTimelimitCount = 0
        skippedByResourcesCount = 0
        effectiveMaxLaunchedJobs = maxLaunchedJobs
        currentLaunchAttempts = []
        if effectiveMaxLaunchedJobs is None:
            effectiveMaxLaunchedJobs = self.config.max_launched_jobs

        for job in jobs:
            if effectiveMaxLaunchedJobs is not None and launchedJobsCount >= effectiveMaxLaunchedJobs:
                logger.info(f"Reached per-pass launch limit: {effectiveMaxLaunchedJobs}")
                break

            if job.getTimelimit() > self.config.timelimit:
                skippedByTimelimitCount += 1
                continue

            placement = self._findRunnablePlacement(job, resourceTree, currentTimestamp)
            if placement is None:
                skippedByResourcesCount += 1
                continue

            resourceTree.reservePlacement(placement)
            launchedJobsCount += 1
            logger.info(
                f"Executing job: {job.getID()} on feature {placement.featureName} "
                f"using nodes {', '.join(placement.nodeNames)}"
            )
            self.connector.executeJob(job)
            launchEvent = build_job_launch_event(
                job=job,
                placement=placement,
                launchTimestamp=currentTimestamp,
            )
            append_job_launch_event(launchEvent)
            currentLaunchAttempts.append(launchEvent)

        save_launch_attempts(currentLaunchAttempts)
        skippedTotalCount = skippedByTimelimitCount + skippedByResourcesCount
        logger.info(
            f"Scheduler pass finished: launched={launchedJobsCount}, skipped_total={skippedTotalCount}, "
            f"skipped_by_timelimit={skippedByTimelimitCount}, skipped_by_resources={skippedByResourcesCount}"
        )

    def _reconcileLaunchAttempts(self, pendingJobs, timestamp: int):
        previousAttempts = load_launch_attempts()
        if not previousAttempts:
            return

        save_launch_attempts([])
        pendingJobIds = {job.getID() for job in pendingJobs}
        for attempt in previousAttempts:
            reconciledAttemptEvent = dict(attempt)
            reconciledAttemptEvent["checked_at_unix"] = timestamp
            reconciledAttemptEvent["checked_at"] = datetime.fromtimestamp(timestamp).isoformat(timespec="seconds")

            if attempt.get("job_id") in pendingJobIds:
                reconciledAttemptEvent["status"] = "FAILED"
                reconciledAttemptEvent["reason"] = "job_still_pending_on_next_scheduler_tick"
            else:
                reconciledAttemptEvent["status"] = "SUCCEEDED"
                reconciledAttemptEvent["reason"] = "job_missing_from_pending_queue_on_next_scheduler_tick"

            append_job_launch_event(reconciledAttemptEvent)

    def _findRunnablePlacement(self, job, resourceTree, timestamp: int):
        partitionConfig = self.clusterConfig.getPartition(job.partition)
        if job.partition and partitionConfig is None:
            return None

        allowedNodeNames = self.clusterConfig.getPartitionNodeNames(job.partition, timestamp)
        partitionFeatures = self.clusterConfig.getPartitionFeatureNames(job.partition, timestamp)
        requestedFeatures = job.getRequestedFeatures(partitionFeatures)
        horizonMinutes = job.getTimelimit()
        requestedCpus = job.getRequestedCpus()
        requestedGpus = job.getRequestedGpus()
        maxCpuPerNode = partitionConfig.max_cpus_per_node if partitionConfig is not None else None
        maxNodesLimit = partitionConfig.max_nodes if partitionConfig is not None else None

        for featureName in requestedFeatures:
            placement = resourceTree.findPlacementOnFeature(
                job,
                featureName,
                allowedNodeNames=allowedNodeNames,
                maxCpuPerNode=maxCpuPerNode,
                maxNodesLimit=maxNodesLimit,
            )
            if placement is None:
                continue

            capacities = resourceTree.getFeatureTotals(
                featureName,
                allowedNodeNames=allowedNodeNames,
                maxCpuPerNode=maxCpuPerNode,
            )
            requestedCpuPercent = self._calculateRequestedPercent(requestedCpus, capacities["cpu"])
            requestedGpuPercent = self._calculateRequestedPercent(requestedGpus, capacities["gpu"])

            if requestedCpuPercent is None or requestedGpuPercent is None:
                continue

            forecast = self.forecastService.buildFeatureForecast(featureName, horizonMinutes)
            if (
                requestedCpuPercent <= forecast.availableCpuPercent
                and requestedGpuPercent <= forecast.availableGpuPercent
            ):
                return placement

        return None

    def _calculateRequestedPercent(self, requestedAmount: int, totalCapacity: int):
        if requestedAmount <= 0:
            return 0.0

        if totalCapacity <= 0:
            return None

        return (requestedAmount / totalCapacity) * 100.0
