import json
import logging
from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import datetime
from pathlib import Path

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info

from config import loadClusterConfigTimelineSnapshots
from config.parsing import parse_timestamp

from .timeutils import ceil_timestamp, floor_timestamp, format_timestamp

ANALYSIS_DISABLED_FEATURES = {
    "type_f",
    "type_h",
}

# Скрытые из анализа типы оставляем в конфигурации на будущее.
ANALYSIS_FORCED_START_TIMESTAMPS = {
    "type_e": parse_timestamp("2022-01-01T00:00:00"),
}


class _ClusterConfigTimeline:
    def __init__(self, snapshots: list[dict]):
        if not snapshots:
            raise ValueError("Cluster config timeline requires at least one snapshot")

        self.snapshots = snapshots
        self.timestamps = [int(snapshot["timestamp"]) for snapshot in snapshots]

    @classmethod
    def from_cluster_config(cls, clusterConfig):
        return cls(
            [
                {
                    "timestamp": 0,
                    "kind": "static",
                    "path": "<provided-cluster-config>",
                    "config": clusterConfig,
                }
            ]
        )

    @classmethod
    def load_default(cls, currentTimestamp: int | None = None):
        return cls(
            loadClusterConfigTimelineSnapshots(currentTimestamp=currentTimestamp)
        )

    def getConfigAt(self, timestamp: int):
        snapshotIndex = bisect_right(self.timestamps, int(timestamp)) - 1
        if snapshotIndex < 0:
            snapshotIndex = 0

        return self.snapshots[snapshotIndex]["config"]

    def iterSegments(self, startTimestamp: int, endTimestamp: int):
        if endTimestamp <= startTimestamp:
            return

        boundaryStart = bisect_right(self.timestamps, int(startTimestamp))
        boundaryEnd = bisect_left(self.timestamps, int(endTimestamp))
        boundaries = [
            int(startTimestamp),
            *self.timestamps[boundaryStart:boundaryEnd],
            int(endTimestamp),
        ]
        for index in range(len(boundaries) - 1):
            segmentStart = boundaries[index]
            segmentEnd = boundaries[index + 1]
            if segmentEnd <= segmentStart:
                continue

            yield segmentStart, segmentEnd, self.getConfigAt(segmentStart)

    def getFeatureNames(self) -> list[str]:
        featureNames = set()
        for snapshot in self.snapshots:
            featureNames.update(snapshot["config"].getFeatureNames())

        return sorted(featureNames)


def build_historical_utilization_series(
    jobs,
    clusterConfig=None,
    intervalMinutes: int = 15,
    nowTimestamp: int | None = None,
) -> dict[str, list[dict]]:
    normalizedJobs = _normalize_historical_jobs(jobs)
    activeJobs = [job for job in normalizedJobs if job.hasStarted()]
    resolvedJobs = [job for job in activeJobs if job.hasAssignedNodes()]
    skippedJobsCount = len(activeJobs) - len(resolvedJobs)

    if nowTimestamp is None:
        if not activeJobs:
            nowTimestamp = int(datetime.now().timestamp())
        else:
            unfinishedJobs = [job for job in activeJobs if job.timeEnd <= 0]
            if unfinishedJobs:
                nowTimestamp = int(datetime.now().timestamp())
            else:
                nowTimestamp = max(
                    max(job.timeStart, job.timeEnd, job.modTime) for job in activeJobs
                )

    clusterConfigTimeline = (
        _ClusterConfigTimeline.from_cluster_config(clusterConfig)
        if clusterConfig is not None
        else _ClusterConfigTimeline.load_default(currentTimestamp=nowTimestamp)
    )
    featureNames = _get_analysis_feature_names(clusterConfigTimeline)

    if skippedJobsCount > 0:
        logger.warning(
            f"Skipped {skippedJobsCount} started jobs without assigned nodelist during utilization aggregation"
        )

    if not activeJobs:
        return {**{feature: [] for feature in featureNames}, "overall": []}

    intervalSeconds = intervalMinutes * 60
    featureLoads, jobDiagnostics = _build_feature_events(
        jobs=resolvedJobs,
        clusterConfigTimeline=clusterConfigTimeline,
        nowTimestamp=nowTimestamp,
        allowedFeatures=set(featureNames),
        forcedFeatureStartTimestamps=ANALYSIS_FORCED_START_TIMESTAMPS,
    )
    overallEvents = _build_overall_events(featureLoads)
    featureCommissionTimestamps = {
        feature: ANALYSIS_FORCED_START_TIMESTAMPS[feature]
        for feature in featureNames
        if feature in ANALYSIS_FORCED_START_TIMESTAMPS
    }

    if overallEvents:
        rangeStart = floor_timestamp(min(overallEvents.keys()), intervalSeconds)
        rangeEnd = ceil_timestamp(max(overallEvents.keys()), intervalSeconds)
    else:
        return {**{feature: [] for feature in featureNames}, "overall": []}

    series = {}
    for feature in featureNames:
        series[feature] = _build_feature_series(
            feature=feature,
            featureEvents=featureLoads.get(feature, {}),
            clusterConfigTimeline=clusterConfigTimeline,
            rangeStart=rangeStart,
            rangeEnd=rangeEnd,
            intervalSeconds=intervalSeconds,
            commissionTimestamp=featureCommissionTimestamps.get(feature),
        )

    series["overall"] = _build_overall_series(
        overallEvents=overallEvents,
        clusterConfigTimeline=clusterConfigTimeline,
        rangeStart=rangeStart,
        rangeEnd=rangeEnd,
        intervalSeconds=intervalSeconds,
        allowedFeatures=set(featureNames),
        forcedFeatureStartTimestamps=ANALYSIS_FORCED_START_TIMESTAMPS,
    )

    _log_job_diagnostics(jobDiagnostics)
    overflowCounts = _count_overflow_points(series)
    totalOverflowCount = sum(overflowCounts.values())
    if totalOverflowCount > 0:
        logger.warning(
            f"Historical utilization series still contains {totalOverflowCount} points above 100%: "
            f"{overflowCounts}"
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

    logger.success(
        f"Exported {len(exportedFiles)} utilization series files to '{outputPath}'"
    )
    return outputPath


def _build_feature_events(
    jobs,
    clusterConfigTimeline,
    nowTimestamp: int,
    allowedFeatures: set[str] | None = None,
    forcedFeatureStartTimestamps: dict[str, int] | None = None,
):
    featureEvents = defaultdict(lambda: defaultdict(lambda: {"cpu": 0.0, "gpu": 0.0}))
    diagnostics = {
        "jobs_with_unknown_nodes": 0,
    }
    forcedFeatureStartTimestamps = forcedFeatureStartTimestamps or {}

    for job in jobs:
        endTimestamp = job.getEffectiveEnd(nowTimestamp)
        if endTimestamp <= job.timeStart:
            continue

        resolvedAnySegment = False
        for (
            segmentStart,
            segmentEnd,
            segmentConfig,
        ) in clusterConfigTimeline.iterSegments(job.timeStart, endTimestamp):
            featureShares = _resolve_job_feature_resource_shares(
                job=job,
                clusterConfig=segmentConfig,
                timestamp=segmentStart,
                allowedFeatures=allowedFeatures,
            )
            if not featureShares:
                continue

            resolvedAnySegment = True
            allocatedCpus = job.getAllocatedCpus()
            allocatedGpus = job.getAllocatedGpus()
            for feature, shares in featureShares.items():
                effectiveStart = max(
                    segmentStart,
                    forcedFeatureStartTimestamps.get(feature, segmentStart),
                )
                if segmentEnd <= effectiveStart:
                    continue

                cpuDelta = float(allocatedCpus) * shares["cpu"]
                gpuDelta = float(allocatedGpus) * shares["gpu"]

                featureEvents[feature][effectiveStart]["cpu"] += cpuDelta
                featureEvents[feature][effectiveStart]["gpu"] += gpuDelta
                featureEvents[feature][segmentEnd]["cpu"] -= cpuDelta
                featureEvents[feature][segmentEnd]["gpu"] -= gpuDelta

        if not resolvedAnySegment:
            diagnostics["jobs_with_unknown_nodes"] += 1

    return featureEvents, diagnostics


def _build_feature_series(
    feature,
    featureEvents,
    clusterConfigTimeline,
    rangeStart,
    rangeEnd,
    intervalSeconds,
    commissionTimestamp=None,
):
    sortedEventTimestamps = sorted(featureEvents.keys())
    eventIndex = 0
    currentCpuLoad = 0.0
    currentGpuLoad = 0.0
    featureSeries = []

    for timestamp in range(rangeStart, rangeEnd + intervalSeconds, intervalSeconds):
        while (
            eventIndex < len(sortedEventTimestamps)
            and sortedEventTimestamps[eventIndex] <= timestamp
        ):
            event = featureEvents[sortedEventTimestamps[eventIndex]]
            currentCpuLoad += event["cpu"]
            currentGpuLoad += event["gpu"]
            eventIndex += 1

        if commissionTimestamp is not None and timestamp < commissionTimestamp:
            capacities = {"cpu": 0, "gpu": 0}
        else:
            capacities = (
                clusterConfigTimeline.getConfigAt(timestamp)
                .getFeatureCapacitiesAt(timestamp)
                .get(
                    feature,
                    {"cpu": 0, "gpu": 0},
                )
            )
        featureSeries.append(
            {
                "time": format_timestamp(timestamp),
                "cpu": _calculate_utilization(currentCpuLoad, capacities["cpu"]),
                "gpu": _calculate_utilization(currentGpuLoad, capacities["gpu"]),
            }
        )

    # Clean up overflow points (trim corrupted data + clip remaining overflows)
    featureSeries = _cleanup_overflow_points(featureSeries)

    return featureSeries


def _build_overall_events(
    featureLoads: dict[str, dict[int, dict[str, float]]],
) -> dict[int, dict[str, float]]:
    overallEvents = defaultdict(lambda: {"cpu": 0.0, "gpu": 0.0})

    for featureEvents in featureLoads.values():
        for timestamp, event in featureEvents.items():
            overallEvents[timestamp]["cpu"] += float(event.get("cpu", 0.0))
            overallEvents[timestamp]["gpu"] += float(event.get("gpu", 0.0))

    return overallEvents


def _build_overall_series(
    overallEvents,
    clusterConfigTimeline,
    rangeStart,
    rangeEnd,
    intervalSeconds,
    allowedFeatures,
    forcedFeatureStartTimestamps,
):
    sortedEventTimestamps = sorted(overallEvents.keys())
    eventIndex = 0
    currentCpuLoad = 0.0
    currentGpuLoad = 0.0
    overallSeries = []

    for timestamp in range(rangeStart, rangeEnd + intervalSeconds, intervalSeconds):
        while (
            eventIndex < len(sortedEventTimestamps)
            and sortedEventTimestamps[eventIndex] <= timestamp
        ):
            event = overallEvents[sortedEventTimestamps[eventIndex]]
            currentCpuLoad += event["cpu"]
            currentGpuLoad += event["gpu"]
            eventIndex += 1

        activeFeatures = {
            feature
            for feature in allowedFeatures
            if forcedFeatureStartTimestamps.get(feature, 0) <= timestamp
        }
        capacities = clusterConfigTimeline.getConfigAt(
            timestamp
        ).getClusterCapacitiesForFeaturesAt(
            timestamp,
            activeFeatures,
        )
        overallSeries.append(
            {
                "time": format_timestamp(timestamp),
                "cpu": _calculate_utilization(currentCpuLoad, capacities["cpu"]),
                "gpu": _calculate_utilization(currentGpuLoad, capacities["gpu"]),
            }
        )

    return overallSeries


def _calculate_utilization(usedCapacity: float, totalCapacity: int) -> float:
    if totalCapacity <= 0:
        return 0.0

    return round((usedCapacity / totalCapacity) * 100, 2)


def _cleanup_overflow_points(series: list[dict]) -> list[dict]:
    """
    Clean up overflow points in utilization series using a two-step strategy:

    1. **Trim corrupted data**: Remove consecutive overflow points at the beginning
       of the series. These are considered corrupted data (e.g., due to missing
       configuration snapshots for old jobs).

    2. **Clip remaining overflows**: For any remaining overflow points, clip to 100%.
       These might be minor artifacts or rounding issues.

    Args:
        series: List of {"time": str, "cpu": float, "gpu": float} points

    Returns:
        Cleaned series with overflow handled
    """
    if not series:
        return series

    # Step 1: Find the last consecutive overflow point from the start
    # Overflow = cpu > 100 OR gpu > 100
    last_consecutive_overflow_index = -1

    for i, point in enumerate(series):
        cpu = point.get("cpu", 0.0)
        gpu = point.get("gpu", 0.0)

        if cpu > 100.0 or gpu > 100.0:
            last_consecutive_overflow_index = i
        else:
            # First non-overflow point - stop looking for consecutive overflows
            break

    # Step 2: Trim the series if we found consecutive overflows at the start
    if last_consecutive_overflow_index >= 0:
        # Remove all points up to and including the last consecutive overflow
        series = series[last_consecutive_overflow_index + 1 :]
        logger.warning(
            f"Trimmed {last_consecutive_overflow_index + 1} consecutive overflow points "
            f"from the beginning of utilization series"
        )

    # Step 3: Clip any remaining overflow points to 100%
    clipped_count = 0
    for point in series:
        cpu = point.get("cpu", 0.0)
        gpu = point.get("gpu", 0.0)

        if cpu > 100.0:
            point["cpu"] = 100.0
            clipped_count += 1

        if gpu > 100.0:
            point["gpu"] = 100.0
            clipped_count += 1

    if clipped_count > 0:
        logger.warning(
            f"Clipped {clipped_count} overflow points to 100% in utilization series"
        )

    return series


def _count_overflow_points(series: dict[str, list[dict]]) -> dict[str, int]:
    overflowCounts = {}
    for feature, points in series.items():
        overflowCount = sum(
            1
            for point in points
            if point.get("cpu", 0.0) > 100.0 or point.get("gpu", 0.0) > 100.0
        )
        if overflowCount > 0:
            overflowCounts[feature] = overflowCount

    return overflowCounts


def _resolve_job_feature_resource_shares(
    job,
    clusterConfig,
    timestamp: int | None = None,
    allowedFeatures: set[str] | None = None,
) -> dict[str, dict[str, float]]:
    if not job.hasAssignedNodes():
        return {}

    featureCapacities = clusterConfig.getFeatureCapacitiesForHostlist(
        job.nodelist, timestamp=timestamp
    )
    if allowedFeatures is not None:
        featureCapacities = {
            feature: capacity
            for feature, capacity in featureCapacities.items()
            if feature in allowedFeatures
        }
    if not featureCapacities:
        return {}

    totalNodes = sum(capacity["nodes"] for capacity in featureCapacities.values())
    totalCpu = sum(capacity["cpu"] for capacity in featureCapacities.values())
    totalGpu = sum(capacity["gpu"] for capacity in featureCapacities.values())
    if totalNodes <= 0:
        return {}

    shares = {}
    for feature, capacity in featureCapacities.items():
        nodeShare = capacity["nodes"] / totalNodes
        shares[feature] = {
            "cpu": (capacity["cpu"] / totalCpu) if totalCpu > 0 else nodeShare,
            "gpu": (capacity["gpu"] / totalGpu) if totalGpu > 0 else nodeShare,
        }

    return shares


def _infer_feature_commission_timestamps(
    featureEvents: dict,
    featureNames: list[str],
    forcedFeatureStartTimestamps: dict[str, int] | None = None,
) -> dict[str, int]:
    commissionTimestamps = {}
    forcedFeatureStartTimestamps = forcedFeatureStartTimestamps or {}

    for feature in featureNames:
        if feature in forcedFeatureStartTimestamps:
            commissionTimestamps[feature] = forcedFeatureStartTimestamps[feature]
            continue

        eventsByTimestamp = featureEvents.get(feature, {})
        positiveTimestamps = [
            timestamp
            for timestamp, event in eventsByTimestamp.items()
            if event.get("cpu", 0.0) > 0.0 or event.get("gpu", 0.0) > 0.0
        ]
        if positiveTimestamps:
            commissionTimestamps[feature] = min(positiveTimestamps)

    return commissionTimestamps


def _get_analysis_feature_names(clusterConfigTimeline) -> list[str]:
    return [
        feature
        for feature in clusterConfigTimeline.getFeatureNames()
        if feature not in ANALYSIS_DISABLED_FEATURES
    ]


def _log_job_diagnostics(diagnostics: dict):
    if not diagnostics:
        return

    problematicJobs = {key: value for key, value in diagnostics.items() if value > 0}
    if not problematicJobs:
        return

    logger.warning(
        f"Historical utilization aggregation adjusted or skipped jobs: {problematicJobs}"
    )


def _normalize_historical_jobs(jobs):
    jobsByLogicalKey = {}
    duplicateVersions = 0

    for job in jobs:
        logicalKey = job.getLogicalKey()
        current = jobsByLogicalKey.get(logicalKey)
        if current is None:
            jobsByLogicalKey[logicalKey] = job
            continue

        duplicateVersions += 1
        jobsByLogicalKey[logicalKey] = _select_preferred_job_version(current, job)

    if duplicateVersions > 0:
        logger.warning(
            f"Collapsed {duplicateVersions} duplicate historical job versions before utilization aggregation"
        )

    return sorted(
        jobsByLogicalKey.values(),
        key=lambda job: (
            job.timeStart,
            job.jobID,
            job.dbIndex if job.dbIndex is not None else -1,
        ),
    )


def _select_preferred_job_version(left, right):
    leftScore = _score_job_version(left)
    rightScore = _score_job_version(right)
    if rightScore > leftScore:
        return right

    if leftScore > rightScore:
        return left

    if (right.modTime or 0) > (left.modTime or 0):
        return right

    return left


def _score_job_version(job) -> tuple:
    return (
        1 if job.hasAssignedNodes() else 0,
        1 if job.timeEnd > 0 else 0,
        1 if job.tresAlloc else 0,
        int(job.modTime or 0),
    )
