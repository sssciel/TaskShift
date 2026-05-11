import logging
from datetime import datetime

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info

from config import (
    JOB_LAUNCH_STATUS_ATTEMPTED,
    JOB_LAUNCH_STATUS_FAILED,
    JOB_LAUNCH_STATUS_LEFT_PENDING_QUEUE,
    append_job_launch_event,
    build_job_launch_event,
    getClusterConfig,
    getSchedulerConfig,
)
from forecast import ForecastService

from .attempt_cache import (
    load_failed_job_pool,
    load_launch_attempts,
    save_failed_job_pool,
    save_launch_attempts,
)
from .resources import ResourceAvailabilityTree


class Scheduler:
    def __init__(
        self,
        storage,
        connector,
        forecastDataDir: str | None = None,
        schedulerConfig=None,
    ):
        self.config = schedulerConfig or getSchedulerConfig()
        self.clusterConfig = getClusterConfig()
        self.forecastService = ForecastService(dataDir=forecastDataDir)
        self.storage = storage
        self.connector = connector

    def schedule(self, maxLaunchedJobs: int | None = None):
        currentTimestamp = int(datetime.now().timestamp())
        jobs = self.storage.getPendingJobs()
        pendingJobsSnapshot = [self._build_pending_job_payload(job) for job in jobs]
        pendingJobsById = {entry["job_id"]: entry for entry in pendingJobsSnapshot}
        failedJobPool = self._reconcileLaunchAttempts(jobs, currentTimestamp)
        skippedByFailedAttemptCount = 0
        if failedJobPool:
            filteredJobs = []
            for job in jobs:
                if job.getID() in failedJobPool:
                    skippedByFailedAttemptCount += 1
                    pendingJobsById[job.getID()]["status"] = "BLOCKED_FAILED_POOL"
                    pendingJobsById[job.getID()]["in_failed_attempt_pool"] = True
                    continue

                filteredJobs.append(job)

            jobs = filteredJobs

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
        attemptedJobIds = []
        if effectiveMaxLaunchedJobs is None:
            effectiveMaxLaunchedJobs = self.config.max_launched_jobs

        for job in jobs:
            if (
                effectiveMaxLaunchedJobs is not None
                and launchedJobsCount >= effectiveMaxLaunchedJobs
            ):
                logger.info(
                    f"Reached per-pass launch limit: {effectiveMaxLaunchedJobs}"
                )
                break

            if job.getTimelimit() > self.config.timelimit:
                skippedByTimelimitCount += 1
                pendingJobsById[job.getID()]["status"] = "SKIPPED_TIMELIMIT"
                continue

            placement = self._findRunnablePlacement(job, resourceTree, currentTimestamp)
            if placement is None:
                skippedByResourcesCount += 1
                pendingJobsById[job.getID()]["status"] = "SKIPPED_RESOURCES"
                continue

            resourceTree.reservePlacement(placement)
            launchedJobsCount += 1
            attemptedJobIds.append(job.getID())
            logger.info(
                f"Executing job: {job.getID()} on feature {placement.featureName} "
                f"using nodes {', '.join(placement.nodeNames)}"
            )
            self.connector.executeJob(job, placement=placement)
            launchEvent = build_job_launch_event(
                job=job,
                placement=placement,
                launchTimestamp=currentTimestamp,
                status=JOB_LAUNCH_STATUS_ATTEMPTED,
            )
            append_job_launch_event(launchEvent)
            currentLaunchAttempts.append(launchEvent)
            pendingJobsById[job.getID()]["status"] = "ATTEMPTED"
            pendingJobsById[job.getID()]["was_attempted"] = True

        save_launch_attempts(currentLaunchAttempts)
        skippedTotalCount = (
            skippedByTimelimitCount
            + skippedByResourcesCount
            + skippedByFailedAttemptCount
        )
        logger.info(
            f"Scheduler pass finished: launched={launchedJobsCount}, skipped_total={skippedTotalCount}, "
            f"skipped_by_timelimit={skippedByTimelimitCount}, skipped_by_resources={skippedByResourcesCount}, "
            f"skipped_by_failed_attempt_pool={skippedByFailedAttemptCount}"
        )
        return {
            "pending_job_count": len(pendingJobsSnapshot),
            "running_job_count": len(runningJobs),
            "launched_count": launchedJobsCount,
            "skipped_by_timelimit": skippedByTimelimitCount,
            "skipped_by_resources": skippedByResourcesCount,
            "skipped_by_failed_attempt_pool": skippedByFailedAttemptCount,
            "failed_job_pool_size": len(failedJobPool),
            "attempted_job_ids": attemptedJobIds,
            "pending_jobs": pendingJobsSnapshot,
            "effective_max_launched_jobs": effectiveMaxLaunchedJobs,
        }

    def _reconcileLaunchAttempts(self, pendingJobs, timestamp: int):
        previousAttempts = load_launch_attempts()
        failedJobPool = load_failed_job_pool()
        if not previousAttempts:
            return failedJobPool

        pendingJobIds = {job.getID() for job in pendingJobs}
        for attempt in previousAttempts:
            reconciledAttemptEvent = dict(attempt)
            reconciledAttemptEvent["checked_at_unix"] = timestamp
            reconciledAttemptEvent["checked_at"] = datetime.fromtimestamp(
                timestamp
            ).isoformat(timespec="seconds")

            jobId = attempt.get("job_id")
            if jobId in pendingJobIds:
                reconciledAttemptEvent["status"] = JOB_LAUNCH_STATUS_FAILED
                reconciledAttemptEvent["reason"] = (
                    "job_still_pending_on_next_scheduler_tick"
                )
                try:
                    failedJobPool.add(int(jobId))
                except (TypeError, ValueError):
                    pass
            else:
                reconciledAttemptEvent["status"] = JOB_LAUNCH_STATUS_LEFT_PENDING_QUEUE
                reconciledAttemptEvent["reason"] = (
                    "job_missing_from_pending_queue_on_next_scheduler_tick"
                )

            append_job_launch_event(reconciledAttemptEvent)

        save_launch_attempts([])
        save_failed_job_pool(failedJobPool)
        return failedJobPool

    def _findRunnablePlacement(self, job, resourceTree, timestamp: int):
        partitionConfig = self.clusterConfig.getPartition(job.partition)
        if job.partition and partitionConfig is None:
            return None

        allowedNodeNames = self.clusterConfig.getPartitionNodeNames(
            job.partition, timestamp
        )
        partitionFeatures = self.clusterConfig.getPartitionFeatureNames(
            job.partition, timestamp
        )
        requestedFeatures = job.getRequestedFeatures(partitionFeatures)
        horizonMinutes = job.getTimelimit()
        requestedCpus = job.getRequestedCpus()
        requestedGpus = job.getRequestedGpus()
        maxCpuPerNode = (
            partitionConfig.max_cpus_per_node if partitionConfig is not None else None
        )
        maxNodesLimit = (
            partitionConfig.max_nodes if partitionConfig is not None else None
        )

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
            requestedCpuPercent = self._calculateRequestedPercent(
                requestedCpus, capacities["cpu"]
            )
            requestedGpuPercent = self._calculateRequestedPercent(
                requestedGpus, capacities["gpu"]
            )

            if requestedCpuPercent is None or requestedGpuPercent is None:
                continue

            forecast = self.forecastService.buildFeatureForecast(
                featureName, horizonMinutes
            )
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

    def _build_pending_job_payload(self, job) -> dict:
        return {
            "job_id": job.getID(),
            "job_name": job.jobName,
            "partition": job.partition,
            "constraints": job.constraints,
            "requested_cpus": job.getRequestedCpus(),
            "requested_gpus": job.getRequestedGpus(),
            "requested_nodes": job.getRequestedNodes(),
            "timelimit_minutes": job.getTimelimit(),
            "status": "PENDING",
            "was_attempted": False,
            "in_failed_attempt_pool": False,
        }
