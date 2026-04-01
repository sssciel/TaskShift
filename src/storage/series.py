import json
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info

from config import getClusterConfig

from .timeutils import ceil_timestamp, floor_timestamp, format_timestamp


def build_historical_utilization_series(
    jobs,
    clusterConfig=None,
    intervalMinutes: int = 15,
    nowTimestamp: int | None = None,
) -> dict[str, list[dict]]:
    if clusterConfig is None:
        clusterConfig = getClusterConfig()

    activeJobs = [job for job in jobs if job.hasStarted()]
    resolvedJobs = [job for job in activeJobs if job.hasAssignedNodes()]
    skippedJobsCount = len(activeJobs) - len(resolvedJobs)
    featureNames = clusterConfig.getFeatureNames()

    if skippedJobsCount > 0:
        logger.warning(
            f"Skipped {skippedJobsCount} started jobs without assigned nodelist during utilization aggregation"
        )

    if not resolvedJobs:
        return {feature: [] for feature in featureNames}

    if nowTimestamp is None:
        unfinishedJobs = [job for job in resolvedJobs if job.timeEnd <= 0]
        if unfinishedJobs:
            nowTimestamp = int(datetime.now().timestamp())
        else:
            nowTimestamp = max(max(job.timeStart, job.timeEnd, job.modTime) for job in resolvedJobs)

    intervalSeconds = intervalMinutes * 60
    rangeStart = floor_timestamp(min(job.timeStart for job in resolvedJobs), intervalSeconds)
    rangeEnd = ceil_timestamp(max(job.getEffectiveEnd(nowTimestamp) for job in resolvedJobs), intervalSeconds)

    featureLoads = _build_feature_events(resolvedJobs, clusterConfig, nowTimestamp)
    allFeatures = sorted(set(featureNames) | set(featureLoads.keys()))

    series = {}
    for feature in allFeatures:
        series[feature] = _build_feature_series(
            feature=feature,
            featureEvents=featureLoads.get(feature, {}),
            clusterConfig=clusterConfig,
            rangeStart=rangeStart,
            rangeEnd=rangeEnd,
            intervalSeconds=intervalSeconds,
        )

    return series


def export_historical_utilization_series(
    outputDir: str | Path,
    jobs,
    clusterConfig=None,
    intervalMinutes: int = 15,
    nowTimestamp: int | None = None,
) -> Path:
    outputPath = Path(outputDir)
    outputPath.mkdir(parents=True, exist_ok=True)

    series = build_historical_utilization_series(
        jobs=jobs,
        clusterConfig=clusterConfig,
        intervalMinutes=intervalMinutes,
        nowTimestamp=nowTimestamp,
    )

    exportedFiles = []
    for feature, featureSeries in series.items():
        featurePath = outputPath / f"{feature}.json"
        with open(featurePath, "w", encoding="utf-8") as file:
            json.dump(featureSeries, file, indent=2, ensure_ascii=False)
        exportedFiles.append(featurePath.name)

    metadataPath = outputPath / "metadata.json"
    with open(metadataPath, "w", encoding="utf-8") as file:
        json.dump(
            {
                "generated_at": datetime.now().isoformat(),
                "interval_minutes": intervalMinutes,
                "features": sorted(series.keys()),
                "files": sorted(exportedFiles),
            },
            file,
            indent=2,
            ensure_ascii=False,
        )

    logger.success(f"Exported {len(exportedFiles)} utilization series files to '{outputPath}'")
    return outputPath


def _build_feature_events(jobs, clusterConfig, nowTimestamp: int):
    featureEvents = defaultdict(lambda: defaultdict(lambda: {"cpu": 0.0, "gpu": 0.0}))

    for job in jobs:
        endTimestamp = job.getEffectiveEnd(nowTimestamp)
        if endTimestamp < job.timeStart:
            continue

        featureShares = _resolve_job_feature_shares(job, clusterConfig)
        if not featureShares:
            continue

        requestedGpus = job.getRequestedGpus()
        for feature, share in featureShares.items():
            cpuDelta = float(job.cpusReq) * share
            gpuDelta = float(requestedGpus) * share

            featureEvents[feature][job.timeStart]["cpu"] += cpuDelta
            featureEvents[feature][job.timeStart]["gpu"] += gpuDelta
            featureEvents[feature][endTimestamp]["cpu"] -= cpuDelta
            featureEvents[feature][endTimestamp]["gpu"] -= gpuDelta

    return featureEvents


def _build_feature_series(feature, featureEvents, clusterConfig, rangeStart, rangeEnd, intervalSeconds):
    sortedEventTimestamps = sorted(featureEvents.keys())
    eventIndex = 0
    currentCpuLoad = 0.0
    currentGpuLoad = 0.0
    featureSeries = []

    for timestamp in range(rangeStart, rangeEnd + intervalSeconds, intervalSeconds):
        while eventIndex < len(sortedEventTimestamps) and sortedEventTimestamps[eventIndex] <= timestamp:
            event = featureEvents[sortedEventTimestamps[eventIndex]]
            currentCpuLoad += event["cpu"]
            currentGpuLoad += event["gpu"]
            eventIndex += 1

        capacities = clusterConfig.getFeatureCapacitiesAt(timestamp).get(feature, {"cpu": 0, "gpu": 0})
        featureSeries.append(
            {
                "time": format_timestamp(timestamp),
                "cpu": _calculate_utilization(currentCpuLoad, capacities["cpu"]),
                "gpu": _calculate_utilization(currentGpuLoad, capacities["gpu"]),
            }
        )

    return featureSeries


def _calculate_utilization(usedCapacity: float, totalCapacity: int) -> float:
    if totalCapacity <= 0:
        return 0.0

    return round((usedCapacity / totalCapacity) * 100, 2)


def _resolve_job_feature_shares(job, clusterConfig) -> dict[str, float]:
    if not job.hasAssignedNodes():
        return {}

    featureNodeCounts = clusterConfig.getFeatureNodeCountsForHostlist(job.nodelist)
    totalNodes = sum(featureNodeCounts.values())
    if totalNodes <= 0:
        return {}

    return {feature: nodeCount / totalNodes for feature, nodeCount in featureNodeCounts.items()}
