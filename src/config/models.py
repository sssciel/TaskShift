import os
import re
import shlex
import subprocess
from dataclasses import dataclass

try:
    import yaml
except ModuleNotFoundError:
    yaml = None

from .parsing import expand_hostlist, parse_timestamp


def get_yaml_module():
    if yaml is None:
        raise ModuleNotFoundError(
            "pyyaml is required to load or save YAML configuration files."
        )

    return yaml


@dataclass
class NodeCountPeriod:
    node_count: int
    start: int | None = None
    end: int | None = None

    def contains(self, timestamp: int) -> bool:
        if self.start is not None and timestamp < self.start:
            return False

        if self.end is not None and timestamp >= self.end:
            return False

        return True


@dataclass
class NodeResources:
    sockets: int
    cores_per_socket: int
    threads_per_core: int
    gpus: int = 0

    @property
    def cpu_cores(self) -> int:
        return self.sockets * self.cores_per_socket * self.threads_per_core


@dataclass
class NodeGroupConfig:
    name_pattern: str
    node_count: int
    weight: int
    features: list[str]
    resources: NodeResources
    history: list[NodeCountPeriod] | None = None

    @property
    def total_cpu_cores(self) -> int:
        return self.node_count * self.resources.cpu_cores

    @property
    def total_gpus(self) -> int:
        return self.node_count * self.resources.gpus

    def get_node_count_at(self, timestamp: int) -> int:
        if not self.history:
            return self.node_count

        for period in self.history:
            if period.contains(timestamp):
                return period.node_count

        return 0


@dataclass
class PartitionConfig:
    name: str
    nodes: str
    state: str | None = None
    max_cpus_per_node: int | None = None
    max_nodes: int | None = None


class ClusterConfig:
    def __init__(self):
        self.gres_types = []
        self.node_groups = []
        self.partitions = []
        self._node_features_cache = None
        self._node_capacities_cache = None

    def loadConfig(self, filePath):
        if not os.path.exists(filePath):
            raise FileNotFoundError(
                f"Configuration file '{filePath}' not found. Please create it using cluster.yaml as a template."
            )

        with open(filePath, "r") as file:
            config = get_yaml_module().safe_load(file) or {}

        self.gres_types = config.get("gres_types", [])
        self.node_groups = [
            NodeGroupConfig(
                name_pattern=node_group["name_pattern"],
                node_count=node_group["node_count"],
                weight=node_group["weight"],
                features=node_group.get("features", []),
                resources=NodeResources(
                    sockets=node_group["resources"]["sockets"],
                    cores_per_socket=node_group["resources"]["cores_per_socket"],
                    threads_per_core=node_group["resources"]["threads_per_core"],
                    gpus=node_group["resources"].get("gpus", 0),
                ),
                history=[
                    NodeCountPeriod(
                        node_count=period["node_count"],
                        start=parse_timestamp(period.get("start")),
                        end=parse_timestamp(period.get("end")),
                    )
                    for period in node_group.get("history", [])
                ]
                or None,
            )
            for node_group in config.get("node_groups", [])
        ]
        self.partitions = [
            PartitionConfig(
                name=partition["name"],
                nodes=partition["nodes"],
                state=partition.get("state"),
                max_cpus_per_node=partition.get("max_cpus_per_node"),
                max_nodes=partition.get("max_nodes"),
            )
            for partition in config.get("partitions", [])
        ]
        self._node_features_cache = None
        self._node_capacities_cache = None
        return self

    def loadFromSlurmText(self, slurmConfigText: str):
        nodesSection = self._extractSection(slurmConfigText, "# * NODES * #")
        partitionsSection = self._extractSection(
            slurmConfigText, "# * PARTITIONS * #", required=False
        )

        self.gres_types = []
        self.node_groups = []
        self.partitions = []
        self._node_features_cache = None
        self._node_capacities_cache = None

        for line in nodesSection:
            if line.startswith("GresTypes="):
                self.gres_types = self._parseGresTypes(line)
                continue

            if line.startswith("NodeName="):
                self.node_groups.append(self._parseNodeLine(line))

        for line in partitionsSection:
            if line.startswith("PartitionName="):
                self.partitions.append(self._parsePartitionLine(line))

        return self

    def loadFromCommand(self, command=None):
        if command is None:
            command = ["cat", "/Users/ciel/study/hpc2026/repo/config_tmp.conf"]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
        )

        return self.loadFromSlurmText(result.stdout)

    def saveConfig(self, filePath):
        with open(filePath, "w") as file:
            get_yaml_module().safe_dump(
                self.to_dict(),
                file,
                sort_keys=False,
                allow_unicode=False,
            )

        return self

    def to_dict(self) -> dict:
        return {
            "gres_types": self.gres_types,
            "node_groups": [
                self._node_group_to_dict(node_group) for node_group in self.node_groups
            ],
            "partitions": [
                self._partition_to_dict(partition) for partition in self.partitions
            ],
        }

    def getFeatureNames(self) -> list[str]:
        featureNames = set()
        for node_group in self.node_groups:
            featureNames.update(node_group.features)

        return sorted(featureNames)

    def getFeatureCapacitiesAt(self, timestamp: int) -> dict[str, dict[str, int]]:
        capacities = {
            feature: {"cpu": 0, "gpu": 0} for feature in self.getFeatureNames()
        }

        for node_group in self.node_groups:
            activeNodeCount = node_group.get_node_count_at(timestamp)
            if activeNodeCount <= 0:
                continue

            for feature in node_group.features:
                capacities.setdefault(feature, {"cpu": 0, "gpu": 0})
                capacities[feature]["cpu"] += (
                    activeNodeCount * node_group.resources.cpu_cores
                )
                capacities[feature]["gpu"] += (
                    activeNodeCount * node_group.resources.gpus
                )

        return capacities

    def getClusterCapacitiesAt(self, timestamp: int) -> dict[str, int]:
        capacities = {"cpu": 0, "gpu": 0}

        for node_group in self.node_groups:
            activeNodeCount = node_group.get_node_count_at(timestamp)
            if activeNodeCount <= 0:
                continue

            capacities["cpu"] += activeNodeCount * node_group.resources.cpu_cores
            capacities["gpu"] += activeNodeCount * node_group.resources.gpus

        return capacities

    def getClusterCapacitiesForFeaturesAt(
        self, timestamp: int, featureNames: set[str]
    ) -> dict[str, int]:
        capacities = {"cpu": 0, "gpu": 0}
        if not featureNames:
            return capacities

        for node_group in self.node_groups:
            if not set(node_group.features).intersection(featureNames):
                continue

            activeNodeCount = node_group.get_node_count_at(timestamp)
            if activeNodeCount <= 0:
                continue

            capacities["cpu"] += activeNodeCount * node_group.resources.cpu_cores
            capacities["gpu"] += activeNodeCount * node_group.resources.gpus

        return capacities

    def getNodeCapacitiesAt(self, timestamp: int) -> dict[str, dict]:
        nodeCapacities = {}

        for node_group in self.node_groups:
            expandedNodes = expand_hostlist(node_group.name_pattern)
            activeNodeCount = node_group.get_node_count_at(timestamp)
            activeNodes = expandedNodes[:activeNodeCount]

            for nodeName in activeNodes:
                nodeCapacities[nodeName] = {
                    "features": list(node_group.features),
                    "cpu": node_group.resources.cpu_cores,
                    "gpu": node_group.resources.gpus,
                }

        return nodeCapacities

    def getPartition(self, partitionName: str | None) -> PartitionConfig | None:
        if not partitionName:
            return None

        for partition in self.partitions:
            if partition.name == partitionName:
                return partition

        return None

    def getPartitionNodeNames(
        self, partitionName: str | None, timestamp: int
    ) -> set[str] | None:
        partition = self.getPartition(partitionName)
        if partition is None:
            return None

        if partition.state and partition.state.upper() != "UP":
            return set()

        activeNodeNames = set(self.getNodeCapacitiesAt(timestamp).keys())
        return {
            nodeName
            for nodeName in expand_hostlist(partition.nodes)
            if nodeName in activeNodeNames
        }

    def getPartitionFeatureNames(
        self, partitionName: str | None, timestamp: int
    ) -> list[str]:
        allowedNodeNames = self.getPartitionNodeNames(partitionName, timestamp)
        if allowedNodeNames is None:
            return self.getFeatureNames()

        nodeFeatureMap = self._get_node_features_map()
        featureNames = set()
        for nodeName in allowedNodeNames:
            featureNames.update(nodeFeatureMap.get(nodeName, []))

        return sorted(featureNames)

    def getFeatureNodeCountsForHostlist(self, hostlist: str) -> dict[str, int]:
        featureCounts = {}
        nodeFeatureMap = self._get_node_features_map()

        for nodeName in expand_hostlist(hostlist):
            for feature in nodeFeatureMap.get(nodeName, []):
                featureCounts[feature] = featureCounts.get(feature, 0) + 1

        return featureCounts

    def getFeatureCapacitiesForHostlist(
        self, hostlist: str, timestamp: int | None = None
    ) -> dict[str, dict[str, int]]:
        featureCapacities = {}
        nodeCapacitiesMap = self.getNodeCapacitiesForHostlist(hostlist, timestamp)

        for nodeCapacity in nodeCapacitiesMap.values():
            for feature in nodeCapacity["features"]:
                featureCapacities.setdefault(feature, {"nodes": 0, "cpu": 0, "gpu": 0})
                featureCapacities[feature]["nodes"] += 1
                featureCapacities[feature]["cpu"] += nodeCapacity["cpu"]
                featureCapacities[feature]["gpu"] += nodeCapacity["gpu"]

        return featureCapacities

    def getNodeCapacitiesForHostlist(
        self, hostlist: str, timestamp: int | None = None
    ) -> dict[str, dict]:
        if timestamp is None:
            sourceCapacities = self._get_node_capacities_map()
        else:
            sourceCapacities = self.getNodeCapacitiesAt(timestamp)

        hostlistCapacities = {}
        for nodeName in expand_hostlist(hostlist):
            nodeCapacity = sourceCapacities.get(nodeName)
            if nodeCapacity is None:
                continue

            hostlistCapacities[nodeName] = {
                "features": list(nodeCapacity["features"]),
                "cpu": nodeCapacity["cpu"],
                "gpu": nodeCapacity["gpu"],
            }

        return hostlistCapacities

    def _get_node_features_map(self) -> dict[str, list[str]]:
        if self._node_features_cache is not None:
            return self._node_features_cache

        self._node_features_cache = {}
        for node_group in self.node_groups:
            for nodeName in expand_hostlist(node_group.name_pattern):
                self._node_features_cache[nodeName] = list(node_group.features)

        return self._node_features_cache

    def _get_node_capacities_map(self) -> dict[str, dict]:
        if self._node_capacities_cache is not None:
            return self._node_capacities_cache

        self._node_capacities_cache = {}
        for node_group in self.node_groups:
            for nodeName in expand_hostlist(node_group.name_pattern):
                self._node_capacities_cache[nodeName] = {
                    "features": list(node_group.features),
                    "cpu": node_group.resources.cpu_cores,
                    "gpu": node_group.resources.gpus,
                }

        return self._node_capacities_cache

    def _node_group_to_dict(self, node_group: NodeGroupConfig) -> dict:
        result = {
            "name_pattern": node_group.name_pattern,
            "node_count": node_group.node_count,
            "weight": node_group.weight,
            "features": node_group.features,
            "resources": {
                "sockets": node_group.resources.sockets,
                "cores_per_socket": node_group.resources.cores_per_socket,
                "threads_per_core": node_group.resources.threads_per_core,
                "gpus": node_group.resources.gpus,
            },
        }

        if node_group.history:
            result["history"] = [
                {
                    "node_count": period.node_count,
                    "start": period.start,
                    "end": period.end,
                }
                for period in node_group.history
            ]

        return result

    def _partition_to_dict(self, partition: PartitionConfig) -> dict:
        result = {
            "name": partition.name,
            "nodes": partition.nodes,
        }

        if partition.state is not None:
            result["state"] = partition.state

        if partition.max_cpus_per_node is not None:
            result["max_cpus_per_node"] = partition.max_cpus_per_node

        if partition.max_nodes is not None:
            result["max_nodes"] = partition.max_nodes

        return result

    def _extractSection(
        self, slurmConfigText: str, marker: str, required: bool = True
    ) -> list[str]:
        lines = slurmConfigText.splitlines()
        startIndex = None

        for index, line in enumerate(lines):
            if line.strip() == marker:
                startIndex = index + 1
                break

        if startIndex is None:
            if required:
                raise ValueError(
                    f"Could not find '{marker}' section in slurm.conf output."
                )

            return []

        section = []
        for line in lines[startIndex:]:
            strippedLine = line.strip()
            if not strippedLine:
                continue

            if strippedLine.startswith("# * ") and strippedLine.endswith(" * #"):
                break

            section.append(strippedLine)

        return section

    def _parseGresTypes(self, line: str) -> list[str]:
        _, value = line.split("=", maxsplit=1)
        return [gresType.strip() for gresType in value.split(",") if gresType.strip()]

    def _parseNodeLine(self, line: str) -> NodeGroupConfig:
        parts = {}
        for token in line.split():
            if "=" not in token:
                continue

            key, value = token.split("=", maxsplit=1)
            parts[key] = value

        resources = NodeResources(
            sockets=int(parts["Sockets"]),
            cores_per_socket=int(parts["CoresPerSocket"]),
            threads_per_core=int(parts["ThreadsPerCore"]),
            gpus=self._parseGpus(parts.get("Gres")),
        )

        features = []
        if "Feature" in parts and parts["Feature"]:
            features = [feature for feature in parts["Feature"].split(",") if feature]

        return NodeGroupConfig(
            name_pattern=parts["NodeName"],
            node_count=self._countNodes(parts["NodeName"]),
            weight=int(parts["Weight"]),
            features=features,
            resources=resources,
        )

    def _parsePartitionLine(self, line: str) -> PartitionConfig:
        parts = {}
        for token in line.split():
            if "=" not in token:
                continue

            key, value = token.split("=", maxsplit=1)
            parts[key] = value

        return PartitionConfig(
            name=parts["PartitionName"],
            nodes=parts["Nodes"],
            state=parts.get("State"),
            max_cpus_per_node=int(parts["MaxCPUsPerNode"])
            if "MaxCPUsPerNode" in parts
            else None,
            max_nodes=int(parts["MaxNodes"]) if "MaxNodes" in parts else None,
        )

    def _parseGpus(self, gresValue: str | None) -> int:
        if not gresValue:
            return 0

        # Match both formats: "gpu:4" and "gpu:v100:4"
        # Try format with type first: gpu:TYPE:COUNT
        match = re.search(r"gpu:[^:]*:(\d+)", gresValue)
        if match:
            return int(match.group(1))

        # Try format without type: gpu:COUNT
        match = re.search(r"gpu:(\d+)", gresValue)
        if match:
            return int(match.group(1))

        return 0

    def _countNodes(self, nodePattern: str) -> int:
        match = re.search(r"\[(.+)\]", nodePattern)
        if match is None:
            return 1

        total = 0
        for chunk in match.group(1).split(","):
            chunk = chunk.strip()
            if not chunk:
                continue

            if "-" in chunk:
                start, end = chunk.split("-", maxsplit=1)
                total += int(end) - int(start) + 1
                continue

            total += 1

        return total


class ServerConfig:
    DEFAULT_HOST = "127.0.0.1"
    DEFAULT_PORT = 8000

    def __init__(self):
        self.host = self.DEFAULT_HOST
        self.port = self.DEFAULT_PORT

    def loadConfig(self, filePath):
        if not os.path.exists(filePath):
            config = {}
        else:
            with open(filePath, "r", encoding="utf-8") as file:
                config = get_yaml_module().safe_load(file) or {}

        self.host = os.getenv("TASKSHIFT_SERVER_HOST", config.get("host", self.DEFAULT_HOST))
        self.port = int(os.getenv("TASKSHIFT_SERVER_PORT", config.get("port", self.DEFAULT_PORT)))
        return self

    def saveConfig(self, filePath):
        with open(filePath, "w", encoding="utf-8") as file:
            get_yaml_module().safe_dump(
                self.to_dict(),
                file,
                sort_keys=False,
                allow_unicode=False,
            )

        return self

    def to_dict(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
        }


class AdminPanelAccessConfig:
    def __init__(self):
        self.token = None

    def loadConfig(self, filePath):
        from dotenv import dotenv_values

        if os.path.exists(filePath):
            config = dotenv_values(filePath)
        else:
            config = {}

        token = os.getenv("ADMIN_PANEL_TOKEN") or config.get("ADMIN_PANEL_TOKEN")
        if not token:
            raise FileNotFoundError(
                f"ADMIN_PANEL_TOKEN is not configured. Set it in '{filePath}' or pass it as an environment variable."
            )

        self.token = token.strip() if token else None
        return self

    def requireToken(self) -> str:
        if not self.token:
            raise ValueError(
                "ADMIN_PANEL_TOKEN is not configured in the environment or configs/.env"
            )

        return self.token


class DBConfig:
    DEFAULT_CHARSET = "utf8mb4"
    DEFAULT_COLLATION = "utf8mb4_general_ci"

    def __init__(self):
        self.host = None
        self.user = None
        self.password = None
        self.database = None
        self.charset = self.DEFAULT_CHARSET
        self.collation = self.DEFAULT_COLLATION

    def loadConfig(self, filePath):
        from dotenv import dotenv_values

        if os.path.exists(filePath):
            load_dotenv(filePath)
        elif not any(os.getenv(name) for name in ("DB_HOST", "DB_USER", "DB_PASSWD", "DB_DATABASE")):
            raise FileNotFoundError(
                f"Configuration file '{filePath}' not found. Please create it using .env.example as a template "
                "or pass DB_HOST, DB_USER, DB_PASSWD, and DB_DATABASE as environment variables."
            )

        self.host = os.getenv("DB_HOST")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWD")
        self.database = os.getenv("DB_DATABASE")
        self.charset = os.getenv("DB_CHARSET", self.DEFAULT_CHARSET)
        self.collation = os.getenv("DB_COLLATION", self.DEFAULT_COLLATION)
        return self

    def getParameters(self):
        parameters = {
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "database": self.database,
        }
        if self.charset:
            parameters["charset"] = self.charset
        if self.collation:
            parameters["collation"] = self.collation
        return parameters


class SchedulerConfig:
    DEFAULT_FORECAST_ENABLED = False
    DEFAULT_FORECAST_DATA_DIR = "exports/historical_utilization/current"
    DEFAULT_CLUSTER_CONFIG_SNAPSHOT_INTERVAL_HOURS = 24
    DEFAULT_WEB_PANEL_ENABLED = True
    DEFAULT_HOT_RELOAD_ENABLED = False
    DEFAULT_CLUSTER_CONFIG_REFRESH_COMMAND = ["cat", "configs/slurm.conf"]
    DEFAULT_CONNECTOR_LAUNCH_SCRIPT = "slurm-launch-job.sh"
    DEFAULT_CONNECTOR_TARGET_QOS = None
    
    def __init__(self):
        self.timelimit = None
        self.max_launched_jobs = None
        self.forecast_enabled = self.DEFAULT_FORECAST_ENABLED
        self.forecast_data_dir = self.DEFAULT_FORECAST_DATA_DIR
        self.cluster_config_snapshot_interval_hours = (
            self.DEFAULT_CLUSTER_CONFIG_SNAPSHOT_INTERVAL_HOURS
        )
        self.web_panel_enabled = self.DEFAULT_WEB_PANEL_ENABLED
        self.hot_reload_enabled = self.DEFAULT_HOT_RELOAD_ENABLED
        self.cluster_config_refresh_command = list(
            self.DEFAULT_CLUSTER_CONFIG_REFRESH_COMMAND
        )
        self.connector_launch_script = self.DEFAULT_CONNECTOR_LAUNCH_SCRIPT
        self.connector_target_qos = self.DEFAULT_CONNECTOR_TARGET_QOS

    def loadConfig(self, filePath):
        if not os.path.exists(filePath):
            raise FileNotFoundError(
                f"Configuration file '{filePath}' not found. Please create it using scheduler.yaml as a template."
            )

        with open(filePath, "r") as file:
            config = get_yaml_module().safe_load(file)

        self.timelimit = config["timelimit"]
        self.max_launched_jobs = config.get("max_launched_jobs")
        self.forecast_enabled = config.get(
            "forecast_enabled", self.DEFAULT_FORECAST_ENABLED
        )
        self.forecast_data_dir = config.get(
            "forecast_data_dir", self.DEFAULT_FORECAST_DATA_DIR
        )
        self.cluster_config_snapshot_interval_hours = config.get(
            "cluster_config_snapshot_interval_hours",
            self.DEFAULT_CLUSTER_CONFIG_SNAPSHOT_INTERVAL_HOURS,
        )
        self.web_panel_enabled = config.get(
            "web_panel_enabled", self.DEFAULT_WEB_PANEL_ENABLED
        )
        self.hot_reload_enabled = config.get(
            "hot_reload_enabled", self.DEFAULT_HOT_RELOAD_ENABLED
        )
        self.cluster_config_refresh_command = self._normalize_command(
            config.get(
                "cluster_config_refresh_command",
                self.DEFAULT_CLUSTER_CONFIG_REFRESH_COMMAND,
            )
        )
        connectorConfig = config.get("connector", {})
        self.connector_launch_script = connectorConfig.get(
            "launch_script", self.DEFAULT_CONNECTOR_LAUNCH_SCRIPT
        )
        self.connector_target_qos = connectorConfig.get(
            "target_qos", self.DEFAULT_CONNECTOR_TARGET_QOS
        )
        return self

    def saveConfig(self, filePath):
        with open(filePath, "w", encoding="utf-8") as file:
            get_yaml_module().safe_dump(
                self.to_dict(),
                file,
                sort_keys=False,
                allow_unicode=False,
            )

        return self

    def to_dict(self) -> dict:
        result = {
            "timelimit": self.timelimit,
        }

        if self.max_launched_jobs is not None:
            result["max_launched_jobs"] = self.max_launched_jobs

        if self.forecast_enabled is not None:
            result["forecast_enabled"] = bool(self.forecast_enabled)

        if self.forecast_data_dir is not None:
            result["forecast_data_dir"] = self.forecast_data_dir

        if self.cluster_config_snapshot_interval_hours is not None:
            result["cluster_config_snapshot_interval_hours"] = (
                self.cluster_config_snapshot_interval_hours
            )

        if self.web_panel_enabled is not None:
            result["web_panel_enabled"] = bool(self.web_panel_enabled)

        if self.hot_reload_enabled is not None:
            result["hot_reload_enabled"] = bool(self.hot_reload_enabled)

        if self.cluster_config_refresh_command:
            result["cluster_config_refresh_command"] = list(
                self.cluster_config_refresh_command
            )

        if self.connector_launch_script:
            result["connector"] = result.get("connector", {})
            result["connector"]["launch_script"] = self.connector_launch_script

        if self.connector_target_qos is not None:
            result["connector"] = result.get("connector", {})
            result["connector"]["target_qos"] = self.connector_target_qos

        return result

    def copy(self):
        clone = SchedulerConfig()
        clone.timelimit = self.timelimit
        clone.max_launched_jobs = self.max_launched_jobs
        clone.forecast_enabled = self.forecast_enabled
        clone.forecast_data_dir = self.forecast_data_dir
        clone.cluster_config_snapshot_interval_hours = (
            self.cluster_config_snapshot_interval_hours
        )
        clone.web_panel_enabled = self.web_panel_enabled
        clone.hot_reload_enabled = self.hot_reload_enabled
        clone.cluster_config_refresh_command = list(self.cluster_config_refresh_command)
        clone.connector_launch_script = self.connector_launch_script
        clone.connector_target_qos = self.connector_target_qos
        return clone

    def _normalize_command(self, commandValue):
        if commandValue is None:
            return list(self.DEFAULT_CLUSTER_CONFIG_REFRESH_COMMAND)

        if isinstance(commandValue, str):
            return shlex.split(commandValue)

        if isinstance(commandValue, (list, tuple)):
            normalized = [
                str(part).strip() for part in commandValue if str(part).strip()
            ]
            if not normalized:
                raise ValueError("cluster_config_refresh_command must not be empty")
            return normalized

        raise ValueError(
            "cluster_config_refresh_command must be either a shell string or a YAML list"
        )
