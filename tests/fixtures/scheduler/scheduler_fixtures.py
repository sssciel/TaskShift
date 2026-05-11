"""
Test fixtures for scheduler module
"""

from unittest.mock import MagicMock

from config.models import ClusterConfig, NodeGroupConfig, NodeResources, PartitionConfig
from storage.models import HistoricalJob, Job

# ─── Cluster Config Fixtures ────────────────────────────────────────────────────

# Mini cluster topology:
#   type_a: cn-[001-004] — 2 sockets, 2 cores/socket, 1 thread/core, 2 GPU each
#     => 4 CPUs per node, 2 GPUs per node
#     => 4 nodes * 4 CPU = 16 CPU total, 4 * 2 GPU = 8 GPU total
#   type_b: cn-[005-006] — 2 sockets, 4 cores/socket, 1 thread/core, 0 GPU
#     => 8 CPUs per node, 0 GPUs per node
#     => 2 nodes * 8 CPU = 16 CPU total, 0 GPU total
#   type_d: cn-[007-008] — 2 sockets, 4 cores/socket, 1 thread/core, 4 GPU each
#     => 8 CPUs per node, 4 GPUs per node
#     => 2 nodes * 8 CPU = 16 CPU total, 2 * 4 GPU = 8 GPU total
#
# Cluster totals: 48 CPU, 16 GPU across 8 nodes
#
# Partitions:
#   "normal": cn-[001-008], max_cpus_per_node=None, max_nodes=None
#   "gpu_only": cn-[001-004,007-008], max_cpus_per_node=4, max_nodes=4


MINI_CLUSTER_NODE_GROUPS = [
    NodeGroupConfig(
        name_pattern="cn-[001-004]",
        node_count=4,
        weight=1,
        features=["type_a"],
        resources=NodeResources(
            sockets=2, cores_per_socket=2, threads_per_core=1, gpus=2
        ),
    ),
    NodeGroupConfig(
        name_pattern="cn-[005-006]",
        node_count=2,
        weight=1,
        features=["type_b"],
        resources=NodeResources(
            sockets=2, cores_per_socket=4, threads_per_core=1, gpus=0
        ),
    ),
    NodeGroupConfig(
        name_pattern="cn-[007-008]",
        node_count=2,
        weight=1,
        features=["type_d"],
        resources=NodeResources(
            sockets=2, cores_per_socket=4, threads_per_core=1, gpus=4
        ),
    ),
]

MINI_CLUSTER_PARTITIONS = [
    PartitionConfig(
        name="normal",
        nodes="cn-[001-008]",
        state="UP",
        max_cpus_per_node=None,
        max_nodes=None,
    ),
    PartitionConfig(
        name="gpu_only",
        nodes="cn-[001-004,007-008]",
        state="UP",
        max_cpus_per_node=4,
        max_nodes=4,
    ),
]

TIMESTAMP_NOW = 1700000000


def build_mini_cluster_config() -> ClusterConfig:
    """Build a minimal ClusterConfig for testing"""
    config = ClusterConfig()
    config.node_groups = list(MINI_CLUSTER_NODE_GROUPS)
    config.partitions = list(MINI_CLUSTER_PARTITIONS)
    config._node_features_cache = None
    config._node_capacities_cache = None
    return config


def build_mini_cluster_config_mock() -> MagicMock:
    """Build a MagicMock of ClusterConfig that behaves like the real one"""
    config = build_mini_cluster_config()
    mock = MagicMock(wraps=config)
    # Ensure the real methods are used (MagicMock wraps them)
    mock.getNodeCapacitiesAt.side_effect = config.getNodeCapacitiesAt
    mock.getFeatureNames.side_effect = config.getFeatureNames
    mock.getPartition.side_effect = config.getPartition
    mock.getPartitionNodeNames.side_effect = config.getPartitionNodeNames
    mock.getPartitionFeatureNames.side_effect = config.getPartitionFeatureNames
    return mock


# ─── Expected Cluster Topology ──────────────────────────────────────────────────

EXPECTED_NODE_CAPACITIES = {
    # type_a nodes: 4 CPU, 2 GPU each
    "cn-001": {"features": ["type_a"], "cpu": 4, "gpu": 2},
    "cn-002": {"features": ["type_a"], "cpu": 4, "gpu": 2},
    "cn-003": {"features": ["type_a"], "cpu": 4, "gpu": 2},
    "cn-004": {"features": ["type_a"], "cpu": 4, "gpu": 2},
    # type_b nodes: 8 CPU, 0 GPU each
    "cn-005": {"features": ["type_b"], "cpu": 8, "gpu": 0},
    "cn-006": {"features": ["type_b"], "cpu": 8, "gpu": 0},
    # type_d nodes: 8 CPU, 4 GPU each
    "cn-007": {"features": ["type_d"], "cpu": 8, "gpu": 4},
    "cn-008": {"features": ["type_d"], "cpu": 8, "gpu": 4},
}

EXPECTED_FEATURE_CAPACITIES = {
    "type_a": {"cpu": 16, "gpu": 8},
    "type_b": {"cpu": 16, "gpu": 0},
    "type_d": {"cpu": 16, "gpu": 8},
}

EXPECTED_FEATURE_NAMES = ["type_a", "type_b", "type_d"]

EXPECTED_PARTITION_NODE_NAMES = {
    "normal": {
        "cn-001",
        "cn-002",
        "cn-003",
        "cn-004",
        "cn-005",
        "cn-006",
        "cn-007",
        "cn-008",
    },
    "gpu_only": {"cn-001", "cn-002", "cn-003", "cn-004", "cn-007", "cn-008"},
}


# ─── Job Helpers ────────────────────────────────────────────────────────────────


def create_pending_job(
    jobID: int = 1001,
    jobName: str = "test_job",
    timelimit: int = 60,
    priority: int = 100,
    constraints: str | None = "type_a",
    cpusReq: int = 2,
    tresReq: str | None = None,
    partition: str = "normal",
) -> Job:
    """Create a pending Job (state=0) with sensible defaults"""
    if tresReq is None:
        tresReq = f"1={cpusReq},4=1"
    return Job(
        jobID=jobID,
        jobName=jobName,
        timelimit=timelimit,
        state=0,
        priority=priority,
        constraints=constraints,
        cpusReq=cpusReq,
        tresReq=tresReq,
        partition=partition,
    )


def create_running_job(
    jobID: int = 2001,
    jobName: str = "running_job",
    cpusReq: int = 2,
    nodesAlloc: int = 1,
    timeStart: int = TIMESTAMP_NOW - 3600,
    nodelist: str = "cn-001",
    tresAlloc: str | None = None,
    constraints: str = "type_a",
    partition: str = "normal",
) -> HistoricalJob:
    """Create a running HistoricalJob (state=1) with assigned nodes"""
    if tresAlloc is None:
        tresAlloc = f"1={cpusReq},4={nodesAlloc}"
    return HistoricalJob(
        dbIndex=jobID,
        jobID=jobID,
        jobName=jobName,
        timelimit=1440,
        state=1,
        priority=100,
        constraints=constraints,
        cpusReq=cpusReq,
        nodesAlloc=nodesAlloc,
        timeStart=timeStart,
        timeEnd=0,
        timeSubmit=timeStart - 60,
        timeEligible=timeStart - 60,
        modTime=timeStart,
        tresReq=f"1={cpusReq},4={nodesAlloc}",
        tresAlloc=tresAlloc,
        nodelist=nodelist,
        partition=partition,
    )


def create_running_gpu_job(
    jobID: int = 3001,
    jobName: str = "gpu_job",
    cpusReq: int = 4,
    nodesAlloc: int = 1,
    gpusRequested: int = 2,
    timeStart: int = TIMESTAMP_NOW - 1800,
    nodelist: str = "cn-001",
    constraints: str = "type_a",
    partition: str = "normal",
) -> HistoricalJob:
    """Create a running HistoricalJob with GPU allocation"""
    return HistoricalJob(
        dbIndex=jobID,
        jobID=jobID,
        jobName=jobName,
        timelimit=1440,
        state=1,
        priority=100,
        constraints=constraints,
        cpusReq=cpusReq,
        nodesAlloc=nodesAlloc,
        timeStart=timeStart,
        timeEnd=0,
        timeSubmit=timeStart - 60,
        timeEligible=timeStart - 60,
        modTime=timeStart,
        tresReq=f"1={cpusReq},4={nodesAlloc},1001={gpusRequested}",
        tresAlloc=f"1={cpusReq},4={nodesAlloc},1001={gpusRequested}",
        nodelist=nodelist,
        partition=partition,
    )
