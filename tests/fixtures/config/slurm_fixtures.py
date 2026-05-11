"""
Test fixtures for slurm.conf and cluster configuration
"""

# Valid slurm.conf content for testing
VALID_SLURM_CONF = """# * NODES * #
GresTypes=gpu
NodeName=cn-[001-002] Gres=gpu:v100:4 Sockets=2 CoresPerSocket=22 ThreadsPerCore=1 Weight=2 Feature=type_a
NodeName=cn-003 Gres=gpu:v100:3 Sockets=2 CoresPerSocket=22 ThreadsPerCore=1 Weight=2 Feature=type_a
NodeName=cn-[004-016] Gres=gpu:v100:4 Sockets=2 CoresPerSocket=22 ThreadsPerCore=1 Weight=2 Feature=type_a
NodeName=cn-[017-026] Gres=gpu:v100:4 Sockets=2 CoresPerSocket=22 ThreadsPerCore=1 Weight=4 Feature=type_b
NodeName=cn-[027-029] Gres=gpu:v100:4 Sockets=2 CoresPerSocket=24 ThreadsPerCore=1 Weight=3 Feature=type_c
NodeName=cn-[030-040] Sockets=2 CoresPerSocket=24 ThreadsPerCore=1 Weight=1 Feature=type_d

# * PARTITIONS * #
PartitionName=normal Nodes=cn-[007-040] MaxCPUsPerNode=128 Default=YES DefaultTime=24:00:00 MaxTime=30-00:00:00 State=UP PreemptMode=OFF PriorityTier=10

PartitionName=test Nodes=cn-[007-040,044-046] MaxCPUsPerNode=10 Default=NO DefaultTime=30:00 MaxTime=30:00 State=UP QoS=test PreemptMode=OFF PriorityJobFactor=20000 PriorityTier=100

PartitionName=rocky Nodes=cn-[001-006,041,043-051] MaxCPUsPerNode=128 Default=NO DefaultTime=24:00:00 MaxTime=07-00:00:00 State=UP QOS=rocky AllowQoS=rocky,unlimit PreemptMode=OFF PriorityTier=10
"""

# Minimal slurm.conf with no GPUs
MINIMAL_SLURM_CONF = """# * NODES * #
NodeName=cn-001 Sockets=2 CoresPerSocket=24 ThreadsPerCore=1 Weight=1 Feature=cpu_only

# * PARTITIONS * #
PartitionName=default Nodes=cn-001 State=UP
"""

# slurm.conf with node history (snapshots)
SLURM_CONF_WITH_HISTORY = """# * NODES * #
NodeName=cn-[001-010] Gres=gpu:v100:4 Sockets=2 CoresPerSocket=24 ThreadsPerCore=1 Weight=1 Feature=gpu_node

# * PARTITIONS * #
PartitionName=gpu Nodes=cn-[001-010] State=UP MaxCPUsPerNode=96
"""

# Expected node groups after parsing VALID_SLURM_CONF
EXPECTED_NODE_GROUPS = [
    {
        "name_pattern": "cn-[001-002]",
        "node_count": 2,
        "weight": 2,
        "features": ["type_a"],
        "resources": {
            "sockets": 2,
            "cores_per_socket": 22,
            "threads_per_core": 1,
            "gpus": 4,
        },
    },
    {
        "name_pattern": "cn-003",
        "node_count": 1,
        "weight": 2,
        "features": ["type_a"],
        "resources": {
            "sockets": 2,
            "cores_per_socket": 22,
            "threads_per_core": 1,
            "gpus": 3,
        },
    },
    {
        "name_pattern": "cn-[004-016]",
        "node_count": 13,
        "weight": 2,
        "features": ["type_a"],
        "resources": {
            "sockets": 2,
            "cores_per_socket": 22,
            "threads_per_core": 1,
            "gpus": 4,
        },
    },
    {
        "name_pattern": "cn-[017-026]",
        "node_count": 10,
        "weight": 4,
        "features": ["type_b"],
        "resources": {
            "sockets": 2,
            "cores_per_socket": 22,
            "threads_per_core": 1,
            "gpus": 4,
        },
    },
    {
        "name_pattern": "cn-[027-029]",
        "node_count": 3,
        "weight": 3,
        "features": ["type_c"],
        "resources": {
            "sockets": 2,
            "cores_per_socket": 24,
            "threads_per_core": 1,
            "gpus": 4,
        },
    },
    {
        "name_pattern": "cn-[030-040]",
        "node_count": 11,
        "weight": 1,
        "features": ["type_d"],
        "resources": {
            "sockets": 2,
            "cores_per_socket": 24,
            "threads_per_core": 1,
            "gpus": 0,
        },
    },
]

# Expected partitions after parsing VALID_SLURM_CONF
EXPECTED_PARTITIONS = [
    {
        "name": "normal",
        "nodes": "cn-[007-040]",
        "state": "UP",
        "max_cpus_per_node": 128,
    },
    {
        "name": "test",
        "nodes": "cn-[007-040,044-046]",
        "state": "UP",
        "max_cpus_per_node": 10,
    },
    {
        "name": "rocky",
        "nodes": "cn-[001-006,041,043-051]",
        "state": "UP",
        "max_cpus_per_node": 128,
    },
]
