"""
Unit tests for config.models.ClusterConfig
"""

import pytest

from config.models import ClusterConfig
from tests.fixtures.config.slurm_fixtures import (
    EXPECTED_NODE_GROUPS,
    EXPECTED_PARTITIONS,
    MINIMAL_SLURM_CONF,
    VALID_SLURM_CONF,
)


class TestClusterConfigParsing:
    """Tests for ClusterConfig parsing from slurm.conf text"""

    @pytest.fixture
    def cluster_config(self):
        """Create a ClusterConfig instance"""
        return ClusterConfig()

    def test_load_from_slurm_text_valid(self, cluster_config):
        """Test parsing valid slurm.conf"""
        config = cluster_config.loadFromSlurmText(VALID_SLURM_CONF)

        # Should return self for chaining
        assert config is cluster_config

        # Check gres_types
        assert cluster_config.gres_types == ["gpu"]

        # Check node groups count
        assert len(cluster_config.node_groups) == 6

        # Check partitions count
        assert len(cluster_config.partitions) == 3

    def test_parse_node_groups(self, cluster_config):
        """Test parsing node groups from slurm.conf"""
        cluster_config.loadFromSlurmText(VALID_SLURM_CONF)

        for i, expected in enumerate(EXPECTED_NODE_GROUPS):
            actual = cluster_config.node_groups[i]

            assert actual.name_pattern == expected["name_pattern"]
            assert actual.node_count == expected["node_count"]
            assert actual.weight == expected["weight"]
            assert actual.features == expected["features"]
            assert actual.resources.sockets == expected["resources"]["sockets"]
            assert (
                actual.resources.cores_per_socket
                == expected["resources"]["cores_per_socket"]
            )
            assert (
                actual.resources.threads_per_core
                == expected["resources"]["threads_per_core"]
            )
            assert actual.resources.gpus == expected["resources"]["gpus"]

    def test_parse_partitions(self, cluster_config):
        """Test parsing partitions from slurm.conf"""
        cluster_config.loadFromSlurmText(VALID_SLURM_CONF)

        for i, expected in enumerate(EXPECTED_PARTITIONS):
            actual = cluster_config.partitions[i]

            assert actual.name == expected["name"]
            assert actual.nodes == expected["nodes"]
            assert actual.state == expected["state"]
            assert actual.max_cpus_per_node == expected["max_cpus_per_node"]

    def test_parse_minimal_config(self, cluster_config):
        """Test parsing minimal slurm.conf without GPUs"""
        cluster_config.loadFromSlurmText(MINIMAL_SLURM_CONF)

        assert len(cluster_config.node_groups) == 1
        assert cluster_config.node_groups[0].name_pattern == "cn-001"
        assert cluster_config.node_groups[0].node_count == 1
        assert cluster_config.node_groups[0].resources.gpus == 0

        assert len(cluster_config.partitions) == 1
        assert cluster_config.partitions[0].name == "default"

    def test_parse_gres_types(self, cluster_config):
        """Test parsing GresTypes line"""
        cluster_config.loadFromSlurmText(VALID_SLURM_CONF)
        assert cluster_config.gres_types == ["gpu"]

    def test_parse_node_without_gpus(self, cluster_config):
        """Test parsing node without Gres field"""
        cluster_config.loadFromSlurmText(VALID_SLURM_CONF)

        # cn-[030-040] have no GPUs
        last_group = cluster_config.node_groups[-1]
        assert last_group.name_pattern == "cn-[030-040]"
        assert last_group.resources.gpus == 0

    def test_parse_node_count_single_node(self, cluster_config):
        """Test node count for single node"""
        cluster_config.loadFromSlurmText(VALID_SLURM_CONF)

        # cn-003 is a single node
        single_node_group = cluster_config.node_groups[1]
        assert single_node_group.name_pattern == "cn-003"
        assert single_node_group.node_count == 1

    def test_parse_node_count_range(self, cluster_config):
        """Test node count for range"""
        cluster_config.loadFromSlurmText(VALID_SLURM_CONF)

        # cn-[004-016] is 13 nodes
        range_group = cluster_config.node_groups[2]
        assert range_group.name_pattern == "cn-[004-016]"
        assert range_group.node_count == 13


class TestClusterConfigFeatures:
    """Tests for ClusterConfig feature-related methods"""

    @pytest.fixture
    def cluster_config(self):
        """Create and load a ClusterConfig"""
        config = ClusterConfig()
        config.loadFromSlurmText(VALID_SLURM_CONF)
        return config

    def test_get_feature_names(self, cluster_config):
        """Test getting all feature names"""
        features = cluster_config.getFeatureNames()

        assert features == ["type_a", "type_b", "type_c", "type_d"]

    def test_get_feature_capacities_at(self, cluster_config):
        """Test getting feature capacities at timestamp"""
        timestamp = 1000
        capacities = cluster_config.getFeatureCapacitiesAt(timestamp)

        # type_a: cn-001 to cn-016 = 16 nodes * 44 cores = 704 CPU, 16 nodes * ~3.8 GPU
        # Actually: 2 + 1 + 13 = 16 nodes for type_a
        # CPU: 16 nodes * 44 cores = 704
        # GPU: (2*4 + 1*3 + 13*4) = 8 + 3 + 52 = 63
        assert "type_a" in capacities
        assert capacities["type_a"]["cpu"] == 704  # 16 nodes * 44 cores
        assert capacities["type_a"]["gpu"] == 63  # 8 + 3 + 52 GPUs

        # type_b: cn-017 to cn-026 = 10 nodes
        assert "type_b" in capacities
        assert capacities["type_b"]["cpu"] == 440  # 10 nodes * 44 cores
        assert capacities["type_b"]["gpu"] == 40  # 10 nodes * 4 GPUs

        # type_c: cn-027 to cn-029 = 3 nodes
        assert "type_c" in capacities
        assert capacities["type_c"]["cpu"] == 144  # 3 nodes * 48 cores
        assert capacities["type_c"]["gpu"] == 12  # 3 nodes * 4 GPUs

        # type_d: cn-030 to cn-040 = 11 nodes, no GPUs
        assert "type_d" in capacities
        assert capacities["type_d"]["cpu"] == 528  # 11 nodes * 48 cores
        assert capacities["type_d"]["gpu"] == 0  # No GPUs


class TestClusterConfigNodes:
    """Tests for ClusterConfig node-related methods"""

    @pytest.fixture
    def cluster_config(self):
        """Create and load a ClusterConfig"""
        config = ClusterConfig()
        config.loadFromSlurmText(VALID_SLURM_CONF)
        return config

    def test_get_node_capacities_at(self, cluster_config):
        """Test getting node capacities at timestamp"""
        timestamp = 1000
        capacities = cluster_config.getNodeCapacitiesAt(timestamp)

        # Should have 40 nodes total (cn-001 to cn-040)
        assert len(capacities) == 40

        # Check first node (cn-001)
        assert "cn-001" in capacities
        assert capacities["cn-001"]["features"] == ["type_a"]
        assert capacities["cn-001"]["cpu"] == 44
        assert capacities["cn-001"]["gpu"] == 4

        # Check last node (cn-040)
        assert "cn-040" in capacities
        assert capacities["cn-040"]["features"] == ["type_d"]
        assert capacities["cn-040"]["cpu"] == 48
        assert capacities["cn-040"]["gpu"] == 0

    def test_get_cluster_capacities_at(self, cluster_config):
        """Test getting cluster total capacities"""
        timestamp = 1000
        capacities = cluster_config.getClusterCapacitiesAt(timestamp)

        # Total: 16 nodes type_a + 10 nodes type_b + 3 nodes type_c + 11 nodes type_d = 40 nodes
        # CPU: 16*44 + 10*44 + 3*48 + 11*48 = 704 + 440 + 144 + 528 = 1816
        # GPU: 63 + 40 + 12 + 0 = 115
        assert capacities["cpu"] == 1816
        assert capacities["gpu"] == 115


class TestClusterConfigPartitions:
    """Tests for ClusterConfig partition-related methods"""

    @pytest.fixture
    def cluster_config(self):
        """Create and load a ClusterConfig"""
        config = ClusterConfig()
        config.loadFromSlurmText(VALID_SLURM_CONF)
        return config

    def test_get_partition_existing(self, cluster_config):
        """Test getting existing partition"""
        partition = cluster_config.getPartition("normal")

        assert partition is not None
        assert partition.name == "normal"
        assert partition.nodes == "cn-[007-040]"

    def test_get_partition_nonexistent(self, cluster_config):
        """Test getting non-existent partition"""
        partition = cluster_config.getPartition("nonexistent")
        assert partition is None

    def test_get_partition_none_name(self, cluster_config):
        """Test getting partition with None name"""
        partition = cluster_config.getPartition(None)
        assert partition is None

    def test_get_partition_node_names(self, cluster_config):
        """Test getting partition node names"""
        timestamp = 1000

        # normal partition: cn-[007-040]
        node_names = cluster_config.getPartitionNodeNames("normal", timestamp)

        assert node_names is not None
        assert len(node_names) == 34  # cn-007 to cn-040 = 34 nodes
        assert "cn-007" in node_names
        assert "cn-040" in node_names
        assert "cn-006" not in node_names  # Before partition range

    def test_get_partition_node_names_nonexistent_partition(self, cluster_config):
        """Test getting node names for non-existent partition"""
        timestamp = 1000
        node_names = cluster_config.getPartitionNodeNames("nonexistent", timestamp)
        assert node_names is None

    def test_get_partition_feature_names(self, cluster_config):
        """Test getting partition feature names"""
        timestamp = 1000

        # normal partition: cn-[007-040]
        # Should include type_a (cn-007 to cn-016), type_b, type_c, type_d
        features = cluster_config.getPartitionFeatureNames("normal", timestamp)

        # Should have type_a, type_b, type_c, type_d
        assert "type_a" in features
        assert "type_b" in features
        assert "type_c" in features
        assert "type_d" in features

    def test_get_partition_feature_names_nonexistent_partition(self, cluster_config):
        """Test getting feature names for non-existent partition"""
        timestamp = 1000
        features = cluster_config.getPartitionFeatureNames("nonexistent", timestamp)
        # Should return all features
        assert features == ["type_a", "type_b", "type_c", "type_d"]


class TestClusterConfigSerialization:
    """Tests for ClusterConfig serialization"""

    @pytest.fixture
    def cluster_config(self):
        """Create and load a ClusterConfig"""
        config = ClusterConfig()
        config.loadFromSlurmText(VALID_SLURM_CONF)
        return config

    def test_to_dict(self, cluster_config):
        """Test serializing ClusterConfig to dict"""
        config_dict = cluster_config.to_dict()

        assert "gres_types" in config_dict
        assert "node_groups" in config_dict
        assert "partitions" in config_dict

        assert config_dict["gres_types"] == ["gpu"]
        assert len(config_dict["node_groups"]) == 6
        assert len(config_dict["partitions"]) == 3


class TestClusterConfigHelperMethods:
    """Tests for ClusterConfig helper methods"""

    @pytest.fixture
    def cluster_config(self):
        """Create and load a ClusterConfig"""
        config = ClusterConfig()
        config.loadFromSlurmText(VALID_SLURM_CONF)
        return config

    def test_get_feature_node_counts_for_hostlist(self, cluster_config):
        """Test getting feature node counts for hostlist"""
        hostlist = "cn-[001-003]"
        counts = cluster_config.getFeatureNodeCountsForHostlist(hostlist)

        assert counts == {"type_a": 3}

    def test_get_feature_capacities_for_hostlist(self, cluster_config):
        """Test getting feature capacities for hostlist"""
        hostlist = "cn-[001-003]"
        timestamp = 1000
        capacities = cluster_config.getFeatureCapacitiesForHostlist(hostlist, timestamp)

        assert "type_a" in capacities
        assert capacities["type_a"]["nodes"] == 3
        assert capacities["type_a"]["cpu"] == 132  # 3 nodes * 44 cores
        assert capacities["type_a"]["gpu"] == 11  # 2*4 + 1*3 = 11 GPUs

    def test_get_node_capacities_for_hostlist(self, cluster_config):
        """Test getting node capacities for hostlist"""
        hostlist = "cn-[001-003]"
        timestamp = 1000
        capacities = cluster_config.getNodeCapacitiesForHostlist(hostlist, timestamp)

        assert len(capacities) == 3
        assert "cn-001" in capacities
        assert "cn-002" in capacities
        assert "cn-003" in capacities

        # All should be type_a with 44 cores
        for node in ["cn-001", "cn-002", "cn-003"]:
            assert capacities[node]["features"] == ["type_a"]
            assert capacities[node]["cpu"] == 44

        # cn-001 and cn-002 have 4 GPUs, cn-003 has 3
        assert capacities["cn-001"]["gpu"] == 4
        assert capacities["cn-002"]["gpu"] == 4
        assert capacities["cn-003"]["gpu"] == 3
