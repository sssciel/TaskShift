"""
TRES (Trackable Resources) type codes for SLURM

Based on slurm cluster configuration, TRES codes represent different resource types.
See: https://slurm.schedmd.com/tres.html
"""

# Basic TRES codes
TRES_CPU = 1
TRES_MEM = 2
TRES_ENERGY = 3
TRES_NODE = 4
TRES_BILLING = 5
TRES_FS_DISK = 6
TRES_VMEM = 7
TRES_PAGES = 8

# GPU TRES codes (GRES - Generic RESource)
TRES_GPU = 1001
TRES_GPU_TESLA = 1002
TRES_FS_LUSTRE = 1003
TRES_IC_OFED = 1004
TRES_GPUMEM = 1005
TRES_GPUUTIL = 1006
TRES_GPU_A100 = 1007
TRES_GPU_H100 = 1008
TRES_GPU_H200 = 1009
TRES_GPU_V100 = 1010

# GPU aliases for parsing
GPU_ALIASES = {
    str(TRES_GPU),  # "1001"
    "gres/gpu",  # Generic GPU
    "gpu",  # Short form
    f"gres/gpu:tesla",  # Specific GPU types
    f"gres/gpu:a100",
    f"gres/gpu:h100",
    f"gres/gpu:h200",
    f"gres/gpu:v100",
}

# CPU aliases for parsing
CPU_ALIASES = {
    str(TRES_CPU),  # "1"
    "cpu",  # String alias
}

# Node aliases for parsing
NODE_ALIASES = {
    str(TRES_NODE),  # "4"
    "node",  # String alias
    "nodes",  # Plural form
}

# Memory aliases
MEM_ALIASES = {
    str(TRES_MEM),  # "2"
    "mem",  # String alias
}


def get_tres_name(tres_id: int) -> str:
    """Get TRES name by ID"""
    tres_names = {
        TRES_CPU: "cpu",
        TRES_MEM: "mem",
        TRES_ENERGY: "energy",
        TRES_NODE: "node",
        TRES_BILLING: "billing",
        TRES_FS_DISK: "fs/disk",
        TRES_VMEM: "vmem",
        TRES_PAGES: "pages",
        TRES_GPU: "gres/gpu",
        TRES_GPU_TESLA: "gres/gpu:tesla",
        TRES_FS_LUSTRE: "fs/lustre",
        TRES_IC_OFED: "ic/ofed",
        TRES_GPUMEM: "gres/gpumem",
        TRES_GPUUTIL: "gres/gpuutil",
        TRES_GPU_A100: "gres/gpu:a100",
        TRES_GPU_H100: "gres/gpu:h100",
        TRES_GPU_H200: "gres/gpu:h200",
        TRES_GPU_V100: "gres/gpu:v100",
    }
    return tres_names.get(tres_id, f"tres_{tres_id}")
