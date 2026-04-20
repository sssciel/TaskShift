from datetime import datetime
from pathlib import Path

from config import ClusterConfig, clusterConfigFile, getLatestClusterConfigBackupFile, getLatestClusterConfigFile
from config.parsing import expand_hostlist


def build_cluster_tree_payload(sourcePath: str | Path | None = None) -> dict:
    selectedPath = resolve_cluster_snapshot_path(sourcePath)
    clusterConfig = ClusterConfig().loadConfig(str(selectedPath))
    activeFile = Path(getLatestClusterConfigFile()).resolve()
    latestBackup = getLatestClusterConfigBackupFile()
    nodeToPartitions = _build_node_partition_map(clusterConfig)
    features = []
    totalCpuCores = 0
    totalGpus = 0
    totalNodes = 0

    for featureName in clusterConfig.getFeatureNames():
        featureGroups = []
        featureNodes = []
        featurePartitions = set()
        featureCpu = 0
        featureGpu = 0

        for nodeGroup in clusterConfig.node_groups:
            if featureName not in nodeGroup.features:
                continue

            expandedNodes = expand_hostlist(nodeGroup.name_pattern)
            nodeEntries = []
            groupPartitions = set()
            for nodeName in expandedNodes:
                partitions = sorted(nodeToPartitions.get(nodeName, []))
                groupPartitions.update(partitions)
                nodeEntries.append(
                    {
                        "name": nodeName,
                        "partitions": partitions,
                        "cpu_cores": nodeGroup.resources.cpu_cores,
                        "gpus": nodeGroup.resources.gpus,
                    }
                )

            featureNodes.extend(nodeEntries)
            featurePartitions.update(groupPartitions)
            featureCpu += nodeGroup.node_count * nodeGroup.resources.cpu_cores
            featureGpu += nodeGroup.node_count * nodeGroup.resources.gpus
            featureGroups.append(
                {
                    "name_pattern": nodeGroup.name_pattern,
                    "node_count": nodeGroup.node_count,
                    "weight": nodeGroup.weight,
                    "features": list(nodeGroup.features),
                    "resources": {
                        "sockets": nodeGroup.resources.sockets,
                        "cores_per_socket": nodeGroup.resources.cores_per_socket,
                        "threads_per_core": nodeGroup.resources.threads_per_core,
                        "cpu_cores": nodeGroup.resources.cpu_cores,
                        "gpus": nodeGroup.resources.gpus,
                    },
                    "partitions": sorted(groupPartitions),
                    "history": [
                        {
                            "node_count": period.node_count,
                            "start": period.start,
                            "end": period.end,
                        }
                        for period in (nodeGroup.history or [])
                    ],
                    "nodes": nodeEntries,
                }
            )

        totalCpuCores += featureCpu
        totalGpus += featureGpu
        totalNodes += sum(group["node_count"] for group in featureGroups)
        features.append(
            {
                "name": featureName,
                "node_group_count": len(featureGroups),
                "nodes": featureNodes,
                "partitions": sorted(featurePartitions),
                "total_cpu_cores": featureCpu,
                "total_gpus": featureGpu,
                "node_groups": featureGroups,
            }
        )

    return {
        "selected_file": str(selectedPath),
        "selected_created_at": _format_timestamp(selectedPath.stat().st_mtime) if selectedPath.exists() else None,
        "scheduler_active_file": str(activeFile),
        "latest_backup_file": str(latestBackup.resolve()) if latestBackup is not None else None,
        "feature_count": len(features),
        "partition_count": len(clusterConfig.partitions),
        "total_nodes": totalNodes,
        "total_cpu_cores": totalCpuCores,
        "total_gpus": totalGpus,
        "features": features,
    }


def get_cluster_snapshot_sources_payload() -> dict:
    currentPath = Path(clusterConfigFile).resolve()
    latestBackup = getLatestClusterConfigBackupFile()
    backupRoot = currentPath.parent / "cluster_backups"
    backupFiles = sorted(
        [
            path.resolve()
            for path in backupRoot.rglob("*.yaml")
            if path.is_file()
        ],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    sources = [
        {
            "path": str(currentPath),
            "label": "Current config file",
            "kind": "current",
            "created_at": _format_timestamp(currentPath.stat().st_mtime) if currentPath.exists() else None,
            "is_scheduler_active": str(currentPath) == str(Path(getLatestClusterConfigFile()).resolve()),
        }
    ]
    for backupPath in backupFiles:
        sources.append(
            {
                "path": str(backupPath),
                "label": f"Backup {backupPath.stem}",
                "kind": "backup",
                "created_at": _format_timestamp(backupPath.stat().st_mtime),
                "is_scheduler_active": latestBackup is not None and backupPath == latestBackup.resolve(),
            }
        )

    return {
        "default_path": str(currentPath),
        "scheduler_active_path": str(Path(getLatestClusterConfigFile()).resolve()),
        "latest_backup_path": str(latestBackup.resolve()) if latestBackup is not None else None,
        "sources": sources,
    }


def resolve_cluster_snapshot_path(pathValue: str | None = None) -> Path:
    catalog = get_cluster_snapshot_sources_payload()
    allowedPaths = {entry["path"] for entry in catalog["sources"]}
    defaultPath = Path(catalog["default_path"]).resolve()
    if not pathValue:
        return defaultPath

    requestedPath = Path(pathValue).resolve()
    if str(requestedPath) not in allowedPaths:
        raise ValueError(f"Cluster snapshot source is not available: {requestedPath}")

    return requestedPath


def _build_node_partition_map(clusterConfig) -> dict[str, list[str]]:
    nodeToPartitions = {}
    for partition in clusterConfig.partitions:
        for nodeName in expand_hostlist(partition.nodes):
            nodeToPartitions.setdefault(nodeName, []).append(partition.name)

    return {
        nodeName: sorted(set(partitions))
        for nodeName, partitions in nodeToPartitions.items()
    }


def _format_timestamp(timestamp: float | int | None) -> str | None:
    if timestamp is None:
        return None

    return datetime.fromtimestamp(float(timestamp)).isoformat(timespec="seconds")
