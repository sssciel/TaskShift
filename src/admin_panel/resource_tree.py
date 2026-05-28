from datetime import datetime
from pathlib import Path

from config import getClusterConfig
from scheduler.resources import ResourceAvailabilityTree
from storage.service import slurmStorage


def build_resource_tree_payload() -> dict:
    timestamp = int(datetime.now().timestamp())
    storage = slurmStorage().create()
    try:
        runningJobs = storage.getRunningJobs(timestamp)
    finally:
        storage.close()

    clusterConfig = getClusterConfig()
    resourceTree = ResourceAvailabilityTree.fromClusterAndJobs(
        clusterConfig=clusterConfig,
        runningJobs=runningJobs,
        timestamp=timestamp,
    )
    features = []
    seenNodes = {}

    for featureName in sorted(resourceTree.nodesByFeature.keys()):
        nodes = sorted(
            resourceTree.nodesByFeature.get(featureName, []),
            key=lambda node: node.nodeName,
        )
        nodeEntries = [_build_node_payload(node) for node in nodes]
        for node in nodes:
            seenNodes[node.nodeName] = node

        totalCpu = sum(node.totalCpu for node in nodes)
        totalGpu = sum(node.totalGpu for node in nodes)
        usedCpu = sum(node.usedCpu for node in nodes)
        usedGpu = sum(node.usedGpu for node in nodes)
        features.append(
            {
                "name": featureName,
                "node_count": len(nodes),
                "total_cpu": _round(totalCpu),
                "used_cpu": _round(usedCpu),
                "available_cpu": _round(sum(node.availableCpu for node in nodes)),
                "total_gpu": _round(totalGpu),
                "used_gpu": _round(usedGpu),
                "available_gpu": _round(sum(node.availableGpu for node in nodes)),
                "cpu_used_percent": _percent(usedCpu, totalCpu),
                "gpu_used_percent": _percent(usedGpu, totalGpu),
                "nodes": nodeEntries,
            }
        )

    allNodes = list(seenNodes.values())
    totalCpu = sum(node.totalCpu for node in allNodes)
    totalGpu = sum(node.totalGpu for node in allNodes)
    usedCpu = sum(node.usedCpu for node in allNodes)
    usedGpu = sum(node.usedGpu for node in allNodes)

    return {
        "generated_at": datetime.fromtimestamp(timestamp).isoformat(timespec="seconds"),
        "generated_at_unix": timestamp,
        "cluster_config_file": str(Path(getattr(clusterConfig, "config_path", "") or "").resolve())
        if getattr(clusterConfig, "config_path", None)
        else None,
        "running_job_count": len(runningJobs),
        "feature_count": len(features),
        "node_count": len(allNodes),
        "total_cpu": _round(totalCpu),
        "used_cpu": _round(usedCpu),
        "available_cpu": _round(sum(node.availableCpu for node in allNodes)),
        "total_gpu": _round(totalGpu),
        "used_gpu": _round(usedGpu),
        "available_gpu": _round(sum(node.availableGpu for node in allNodes)),
        "cpu_used_percent": _percent(usedCpu, totalCpu),
        "gpu_used_percent": _percent(usedGpu, totalGpu),
        "features": features,
    }


def _build_node_payload(node) -> dict:
    return {
        "name": node.nodeName,
        "feature": node.featureName,
        "total_cpu": _round(node.totalCpu),
        "used_cpu": _round(node.usedCpu),
        "available_cpu": _round(node.availableCpu),
        "total_gpu": _round(node.totalGpu),
        "used_gpu": _round(node.usedGpu),
        "available_gpu": _round(node.availableGpu),
        "cpu_used_percent": _percent(node.usedCpu, node.totalCpu),
        "gpu_used_percent": _percent(node.usedGpu, node.totalGpu),
    }


def _round(value: float | int) -> float:
    return round(float(value), 2)


def _percent(used: float | int, total: float | int) -> float | None:
    if float(total) <= 0:
        return None
    return round((float(used) / float(total)) * 100.0, 2)
