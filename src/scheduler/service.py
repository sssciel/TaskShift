import logging
import json
from datetime import datetime
from pathlib import Path

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
from config.logger import (
    build_runtime_log_event,
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
        jobRuntimeEventWriter=None,
        schedulerRuntimeEventWriter=None,
    ):
        self.config = schedulerConfig or getSchedulerConfig()
        self.clusterConfig = getClusterConfig()
        self.jobRuntimeEventWriter = jobRuntimeEventWriter
        self.schedulerRuntimeEventWriter = schedulerRuntimeEventWriter
        self.forecastService = None
        if forecastDataDir is not None:
            self.forecastService = ForecastService(
                dataDir=forecastDataDir,
                modelDir=getattr(self.config, "forecast_model_dir", None),
                schedulerConfig=self.config,
                projectRoot=Path(__file__).resolve().parents[2],
            )
        self.storage = storage
        self.connector = connector

    def schedule(
        self,
        maxLaunchedJobs: int | None = None,
        runId: str | None = None,
        trigger: str = "scheduled",
    ):
        currentTimestamp = int(datetime.now().timestamp())
        jobs = self.storage.getPendingJobs()
        self._write_scheduler_event(
            status="PENDING_JOBS_LOADED",
            timestamp=currentTimestamp,
            runId=runId,
            trigger=trigger,
            message=f"Loaded {len(jobs)} pending jobs from Slurm.",
            pending_job_count=len(jobs),
        )
        pendingJobsSnapshot = [self._build_pending_job_payload(job) for job in jobs]
        pendingJobsById = {entry["job_id"]: entry for entry in pendingJobsSnapshot}
        failedJobPool = self._reconcileLaunchAttempts(jobs, currentTimestamp, runId=runId)
        skippedByFailedAttemptCount = 0
        skippedByLaunchLimitCount = 0
        if failedJobPool:
            filteredJobs = []
            for job in jobs:
                if job.getID() in failedJobPool:
                    skippedByFailedAttemptCount += 1
                    pendingJobsById[job.getID()]["status"] = "BLOCKED_FAILED_POOL"
                    pendingJobsById[job.getID()]["in_failed_attempt_pool"] = True
                    self._log_job_decision(
                        job,
                        status="BLOCKED_FAILED_POOL",
                        timestamp=currentTimestamp,
                        runId=runId,
                        details={
                            "reason": "job_remained_in_failed_attempt_pool_after_previous_failed_launch"
                        },
                    )
                    continue

                filteredJobs.append(job)

            jobs = filteredJobs

        runningJobs = self.storage.getRunningJobs(currentTimestamp)
        self._write_scheduler_event(
            status="RUNNING_JOBS_LOADED",
            timestamp=currentTimestamp,
            runId=runId,
            trigger=trigger,
            message=f"Built running-jobs snapshot with {len(runningJobs)} active jobs.",
            running_job_count=len(runningJobs),
        )
        resourceTree = ResourceAvailabilityTree.fromClusterAndJobs(
            clusterConfig=self.clusterConfig,
            runningJobs=runningJobs,
            timestamp=currentTimestamp,
        )
        launchedJobsCount = 0
        skippedByTimelimitCount = 0
        skippedByResourcesCount = 0
        skippedByForecastCount = 0
        effectiveMaxLaunchedJobs = maxLaunchedJobs
        currentLaunchAttempts = []
        attemptedJobIds = []
        if effectiveMaxLaunchedJobs is None:
            effectiveMaxLaunchedJobs = self.config.max_launched_jobs

        for job in jobs:
            if job.getTimelimit() > self.config.timelimit:
                skippedByTimelimitCount += 1
                pendingJobsById[job.getID()]["status"] = "SKIPPED_TIMELIMIT"
                self._log_job_decision(
                    job,
                    status="SKIPPED_TIMELIMIT",
                    timestamp=currentTimestamp,
                    runId=runId,
                    details={
                        "reason": "job_timelimit_exceeds_scheduler_limit",
                        "job_timelimit_minutes": job.getTimelimit(),
                        "scheduler_timelimit_minutes": self.config.timelimit,
                    },
                )
                continue

            placement, featureDiagnostics = self._evaluateRunnablePlacement(
                job, resourceTree, currentTimestamp
            )
            if placement is None:
                blockedByForecast = self._diagnostics_blocked_by_forecast(featureDiagnostics)
                if blockedByForecast:
                    skippedByForecastCount += 1
                    status = "SKIPPED_FORECAST"
                    reason = "forecast_gpu_availability_below_request"
                else:
                    skippedByResourcesCount += 1
                    status = "SKIPPED_RESOURCES"
                    reason = "no_feature_satisfied_current_resource_constraints"
                pendingJobsById[job.getID()]["status"] = status
                self._log_job_decision(
                    job,
                    status=status,
                    timestamp=currentTimestamp,
                    runId=runId,
                    details={
                        "reason": reason,
                        "feature_checks": featureDiagnostics,
                    },
                )
                continue

            selectedFeatureDiagnostic = next(
                (
                    item
                    for item in featureDiagnostics
                    if item.get("feature") == placement.featureName
                ),
                None,
            )
            if (
                effectiveMaxLaunchedJobs is not None
                and launchedJobsCount >= effectiveMaxLaunchedJobs
            ):
                skippedByLaunchLimitCount += 1
                pendingJobsById[job.getID()]["status"] = "SKIPPED_LAUNCH_LIMIT"
                self._log_job_decision(
                    job,
                    status="SKIPPED_LAUNCH_LIMIT",
                    timestamp=currentTimestamp,
                    runId=runId,
                    details={
                        "reason": "runnable_but_per_pass_launch_limit_reached",
                        "effective_max_launched_jobs": effectiveMaxLaunchedJobs,
                        "selected_feature_check": selectedFeatureDiagnostic,
                    },
                )
                continue

            resourceTree.reservePlacement(placement)
            launchedJobsCount += 1
            attemptedJobIds.append(job.getID())
            logger.info(
                f"Executing job: {job.getID()} on feature {placement.featureName} "
                f"using nodes {', '.join(placement.nodeNames)}"
            )
            self._log_job_decision(
                job,
                status="ATTEMPTED",
                timestamp=currentTimestamp,
                runId=runId,
                details={
                    "reason": "placement_selected",
                    "placement_feature": placement.featureName,
                    "placement_nodes": placement.nodeNames,
                    "allocations": [
                        {
                            "node": allocation.nodeName,
                            "cpu": allocation.cpu,
                            "gpu": allocation.gpu,
                        }
                        for allocation in placement.allocations
                    ],
                    "selected_feature_check": selectedFeatureDiagnostic,
                },
            )
            self.connector.executeJob(job, placement=placement, runId=runId)
            launchEvent = build_job_launch_event(
                job=job,
                placement=placement,
                launchTimestamp=currentTimestamp,
                status=JOB_LAUNCH_STATUS_ATTEMPTED,
                runId=runId,
                trigger=trigger,
            )
            append_job_launch_event(launchEvent)
            currentLaunchAttempts.append(launchEvent)
            pendingJobsById[job.getID()]["status"] = "ATTEMPTED"
            pendingJobsById[job.getID()]["was_attempted"] = True

        save_launch_attempts(currentLaunchAttempts)
        skippedTotalCount = (
            skippedByTimelimitCount
            + skippedByResourcesCount
            + skippedByForecastCount
            + skippedByFailedAttemptCount
            + skippedByLaunchLimitCount
        )
        logger.info(
            f"Scheduler pass finished: launched={launchedJobsCount}, skipped_total={skippedTotalCount}, "
            f"skipped_by_timelimit={skippedByTimelimitCount}, skipped_by_resources={skippedByResourcesCount}, "
            f"skipped_by_forecast={skippedByForecastCount}, "
            f"skipped_by_failed_attempt_pool={skippedByFailedAttemptCount}, "
            f"skipped_by_launch_limit={skippedByLaunchLimitCount}"
        )
        self._write_scheduler_event(
            status="RUN_FINISHED",
            timestamp=currentTimestamp,
            runId=runId,
            trigger=trigger,
            message=(
                f"Scheduler pass finished: launched={launchedJobsCount}, skipped_total={skippedTotalCount}, "
                f"failed_pool={len(failedJobPool)}"
            ),
            pending_job_count=len(pendingJobsSnapshot),
            running_job_count=len(runningJobs),
            launched_count=launchedJobsCount,
            skipped_by_timelimit=skippedByTimelimitCount,
            skipped_by_resources=skippedByResourcesCount,
            skipped_by_forecast=skippedByForecastCount,
            skipped_by_failed_attempt_pool=skippedByFailedAttemptCount,
            skipped_by_launch_limit=skippedByLaunchLimitCount,
            failed_job_pool_size=len(failedJobPool),
            attempted_job_ids=attemptedJobIds,
            effective_max_launched_jobs=effectiveMaxLaunchedJobs,
        )
        return {
            "pending_job_count": len(pendingJobsSnapshot),
            "running_job_count": len(runningJobs),
            "launched_count": launchedJobsCount,
            "skipped_by_timelimit": skippedByTimelimitCount,
            "skipped_by_resources": skippedByResourcesCount,
            "skipped_by_forecast": skippedByForecastCount,
            "skipped_by_failed_attempt_pool": skippedByFailedAttemptCount,
            "skipped_by_launch_limit": skippedByLaunchLimitCount,
            "failed_job_pool_size": len(failedJobPool),
            "attempted_job_ids": attemptedJobIds,
            "pending_jobs": pendingJobsSnapshot,
            "effective_max_launched_jobs": effectiveMaxLaunchedJobs,
            "run_id": runId,
        }

    def _reconcileLaunchAttempts(self, pendingJobs, timestamp: int, runId: str | None = None):
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
                self._write_job_runtime_event(
                    build_runtime_log_event(
                        category="job_runtime",
                        status=JOB_LAUNCH_STATUS_FAILED,
                        message=f"Job {jobId} stayed pending on the next scheduler tick.",
                        timestamp=timestamp,
                        eventType="ATTEMPT_RECONCILED",
                        run_id=runId,
                        previous_run_id=attempt.get("run_id"),
                        job_id=jobId,
                        job_name=attempt.get("job_name"),
                        partition=attempt.get("partition"),
                        feature=attempt.get("feature"),
                        nodes=attempt.get("nodes"),
                        reason="job_still_pending_on_next_scheduler_tick",
                    )
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
                self._write_job_runtime_event(
                    build_runtime_log_event(
                        category="job_runtime",
                        status=JOB_LAUNCH_STATUS_LEFT_PENDING_QUEUE,
                        message=f"Job {jobId} left the pending queue before the next scheduler tick.",
                        timestamp=timestamp,
                        eventType="ATTEMPT_RECONCILED",
                        run_id=runId,
                        previous_run_id=attempt.get("run_id"),
                        job_id=jobId,
                        job_name=attempt.get("job_name"),
                        partition=attempt.get("partition"),
                        feature=attempt.get("feature"),
                        nodes=attempt.get("nodes"),
                        reason="job_missing_from_pending_queue_on_next_scheduler_tick",
                    )
                )

            append_job_launch_event(reconciledAttemptEvent)

        save_launch_attempts([])
        save_failed_job_pool(failedJobPool)
        return failedJobPool

    def _findRunnablePlacement(self, job, resourceTree, timestamp: int):
        placement, _ = self._evaluateRunnablePlacement(job, resourceTree, timestamp)
        return placement

    def _evaluateRunnablePlacement(self, job, resourceTree, timestamp: int):
        partitionConfig = self.clusterConfig.getPartition(job.partition)
        if job.partition and partitionConfig is None:
            return None, [
                {
                    "decision": "unknown_partition",
                    "partition": job.partition,
                }
            ]

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
        featureDiagnostics = []

        for featureName in requestedFeatures:
            placement = resourceTree.findPlacementOnFeature(
                job,
                featureName,
                allowedNodeNames=allowedNodeNames,
                maxCpuPerNode=maxCpuPerNode,
                maxNodesLimit=maxNodesLimit,
            )
            capacities = resourceTree.getFeatureTotals(
                featureName,
                allowedNodeNames=allowedNodeNames,
                maxCpuPerNode=maxCpuPerNode,
            )
            snapshot = resourceTree.getFeatureSnapshot(
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
            featureCheck = {
                "feature": featureName,
                "placement_found": placement is not None,
                "current_available_cpu": round(float(snapshot["available_cpu"]), 2),
                "current_available_gpu": round(float(snapshot["available_gpu"]), 2),
                "total_cpu_capacity": round(float(snapshot["total_cpu"]), 2),
                "total_gpu_capacity": round(float(snapshot["total_gpu"]), 2),
                "requested_cpus": requestedCpus,
                "requested_gpus": requestedGpus,
                "requested_nodes": job.getRequestedNodes(),
                "requested_cpu_percent": self._round_optional(requestedCpuPercent),
                "requested_gpu_percent": self._round_optional(requestedGpuPercent),
                "allowed_node_count": len(allowedNodeNames) if allowedNodeNames is not None else None,
                "max_cpu_per_node": maxCpuPerNode,
                "max_nodes_limit": maxNodesLimit,
            }
            if placement is None:
                featureCheck["decision"] = "placement_unavailable"
                featureDiagnostics.append(featureCheck)
                continue

            if requestedCpuPercent is None or requestedGpuPercent is None:
                featureCheck["decision"] = "feature_has_no_capacity_for_requested_resource_type"
                featureDiagnostics.append(featureCheck)
                continue

            if self.forecastService is None:
                featureCheck["decision"] = "selected_without_forecast"
                featureDiagnostics.append(featureCheck)
                return placement, featureDiagnostics

            forecast = self.forecastService.buildFeatureForecast(
                featureName, horizonMinutes
            )
            featureCheck["forecast_available_cpu_percent"] = self._round_optional(
                forecast.availableCpuPercent
            )
            featureCheck["forecast_available_gpu_percent"] = self._round_optional(
                forecast.availableGpuPercent
            )
            featureCheck["forecast_available_cpu_estimate"] = self._estimate_available_capacity(
                snapshot["total_cpu"],
                forecast.availableCpuPercent,
            )
            featureCheck["forecast_available_gpu_estimate"] = self._estimate_available_capacity(
                snapshot["total_gpu"],
                forecast.availableGpuPercent,
            )
            if requestedGpuPercent <= forecast.availableGpuPercent:
                featureCheck["decision"] = "selected_with_forecast"
                featureDiagnostics.append(featureCheck)
                return placement, featureDiagnostics

            featureCheck["decision"] = "blocked_by_forecast_gpu_availability"
            featureDiagnostics.append(featureCheck)

        return None, featureDiagnostics

    def _diagnostics_blocked_by_forecast(self, featureDiagnostics: list[dict]) -> bool:
        return any(
            item.get("decision") == "blocked_by_forecast_gpu_availability"
            for item in featureDiagnostics
        )

    def _calculateRequestedPercent(self, requestedAmount: int, totalCapacity: int):
        if requestedAmount <= 0:
            return 0.0

        if totalCapacity <= 0:
            return None

        return (requestedAmount / totalCapacity) * 100.0

    def _estimate_available_capacity(self, totalCapacity: float, availablePercent: float | None):
        if availablePercent is None:
            return None
        return round(float(totalCapacity) * (float(availablePercent) / 100.0), 2)

    def _round_optional(self, value: float | None):
        if value is None:
            return None
        return round(float(value), 2)

    def _log_job_decision(
        self,
        job,
        *,
        status: str,
        timestamp: int,
        runId: str | None = None,
        details: dict | None = None,
    ):
        payload = {
            "job_id": job.getID(),
            "job_name": job.jobName,
            "status": status,
            "partition": job.partition,
            "constraints": job.constraints,
            "requested_cpus": job.getRequestedCpus(),
            "requested_gpus": job.getRequestedGpus(),
            "requested_nodes": job.getRequestedNodes(),
            "timelimit_minutes": job.getTimelimit(),
            "decision_at_unix": timestamp,
            "decision_at": datetime.fromtimestamp(timestamp).isoformat(timespec="seconds"),
        }
        if runId:
            payload["run_id"] = runId
        if details:
            payload.update(details)
        logger.info(f"Scheduler job decision | {json.dumps(payload, ensure_ascii=False, sort_keys=True)}")
        runtimeFields = dict(payload)
        runtimeFields.pop("status", None)
        runtimeFields.pop("run_id", None)
        self._write_job_runtime_event(
            build_runtime_log_event(
                category="job_runtime",
                status=status,
                message=self._build_job_decision_message(job, status=status, details=details),
                timestamp=timestamp,
                eventType="JOB_DECISION",
                run_id=runId,
                source="scheduler.service",
                **runtimeFields,
            )
        )

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

    def _build_job_decision_message(self, job, *, status: str, details: dict | None = None) -> str:
        reason = (details or {}).get("reason")
        if reason:
            return f"Job {job.getID()} decision={status} reason={reason}"
        return f"Job {job.getID()} decision={status}"

    def _write_scheduler_event(
        self,
        *,
        status: str,
        timestamp: int,
        runId: str | None,
        trigger: str,
        message: str,
        **fields,
    ):
        if self.schedulerRuntimeEventWriter is None:
            return
        event = build_runtime_log_event(
            category="scheduler_runtime",
            status=status,
            message=message,
            timestamp=timestamp,
            eventType=status,
            source="scheduler.service",
            run_id=runId,
            trigger=trigger,
            **fields,
        )
        self.schedulerRuntimeEventWriter(event)

    def _write_job_runtime_event(self, event: dict):
        if self.jobRuntimeEventWriter is None:
            return
        self.jobRuntimeEventWriter(event)
