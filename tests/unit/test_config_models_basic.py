"""
Unit tests for config.models module - Basic models
"""

import pytest

from config.models import (
    NodeCountPeriod,
    NodeGroupConfig,
    NodeResources,
    PartitionConfig,
    SchedulerConfig,
)


class TestNodeResources:
    """Tests for NodeResources dataclass"""

    def test_cpu_cores_calculation(self):
        """Test that cpu_cores property calculates correctly"""
        resources = NodeResources(
            sockets=2, cores_per_socket=24, threads_per_core=1, gpus=4
        )
        assert resources.cpu_cores == 48  # 2 * 24 * 1

    def test_cpu_cores_with_threads(self):
        """Test cpu_cores calculation with hyperthreading"""
        resources = NodeResources(
            sockets=2, cores_per_socket=24, threads_per_core=2, gpus=4
        )
        assert resources.cpu_cores == 96  # 2 * 24 * 2

    def test_cpu_cores_no_gpus(self):
        """Test cpu_cores calculation with no GPUs"""
        resources = NodeResources(
            sockets=2, cores_per_socket=24, threads_per_core=1, gpus=0
        )
        assert resources.cpu_cores == 48
        assert resources.gpus == 0

    def test_default_gpus(self):
        """Test that gpus defaults to 0"""
        resources = NodeResources(sockets=2, cores_per_socket=24, threads_per_core=1)
        assert resources.gpus == 0


class TestNodeCountPeriod:
    """Tests for NodeCountPeriod dataclass"""

    def test_contains_within_range(self):
        """Test contains() returns True for timestamp within range"""
        period = NodeCountPeriod(node_count=10, start=1000, end=2000)
        assert period.contains(1500) is True

    def test_contains_at_start(self):
        """Test contains() returns True for timestamp at start boundary"""
        period = NodeCountPeriod(node_count=10, start=1000, end=2000)
        assert period.contains(1000) is True

    def test_contains_at_end(self):
        """Test contains() returns False for timestamp at end boundary"""
        period = NodeCountPeriod(node_count=10, start=1000, end=2000)
        assert period.contains(2000) is False

    def test_contains_before_start(self):
        """Test contains() returns False for timestamp before start"""
        period = NodeCountPeriod(node_count=10, start=1000, end=2000)
        assert period.contains(500) is False

    def test_contains_after_end(self):
        """Test contains() returns False for timestamp after end"""
        period = NodeCountPeriod(node_count=10, start=1000, end=2000)
        assert period.contains(2500) is False

    def test_contains_no_start(self):
        """Test contains() with no start boundary"""
        period = NodeCountPeriod(node_count=10, start=None, end=2000)
        assert period.contains(500) is True
        assert period.contains(2500) is False

    def test_contains_no_end(self):
        """Test contains() with no end boundary"""
        period = NodeCountPeriod(node_count=10, start=1000, end=None)
        assert period.contains(500) is False
        assert period.contains(2500) is True

    def test_contains_no_boundaries(self):
        """Test contains() with no boundaries"""
        period = NodeCountPeriod(node_count=10, start=None, end=None)
        assert period.contains(500) is True
        assert period.contains(1500) is True
        assert period.contains(2500) is True


class TestNodeGroupConfig:
    """Tests for NodeGroupConfig dataclass"""

    @pytest.fixture
    def basic_node_group(self):
        """Create a basic node group for testing"""
        return NodeGroupConfig(
            name_pattern="cn-[001-010]",
            node_count=10,
            weight=1,
            features=["gpu_node"],
            resources=NodeResources(
                sockets=2, cores_per_socket=24, threads_per_core=1, gpus=4
            ),
        )

    def test_total_cpu_cores(self, basic_node_group):
        """Test total_cpu_cores property"""
        # 10 nodes * 2 sockets * 24 cores * 1 thread = 480
        assert basic_node_group.total_cpu_cores == 480

    def test_total_gpus(self, basic_node_group):
        """Test total_gpus property"""
        # 10 nodes * 4 GPUs = 40
        assert basic_node_group.total_gpus == 40

    def test_get_node_count_at_no_history(self, basic_node_group):
        """Test get_node_count_at() with no history"""
        result = basic_node_group.get_node_count_at(1000)
        assert result == 10

    def test_get_node_count_at_with_history(self):
        """Test get_node_count_at() with history"""
        node_group = NodeGroupConfig(
            name_pattern="cn-[001-010]",
            node_count=10,
            weight=1,
            features=["gpu_node"],
            resources=NodeResources(
                sockets=2, cores_per_socket=24, threads_per_core=1, gpus=4
            ),
            history=[
                NodeCountPeriod(node_count=5, start=1000, end=2000),
                NodeCountPeriod(node_count=10, start=2000, end=3000),
            ],
        )

        # Before history
        assert node_group.get_node_count_at(500) == 0
        # First period
        assert node_group.get_node_count_at(1500) == 5
        # Second period
        assert node_group.get_node_count_at(2500) == 10
        # After history
        assert node_group.get_node_count_at(3500) == 0

    def test_get_node_count_at_boundary(self):
        """Test get_node_count_at() at period boundaries"""
        node_group = NodeGroupConfig(
            name_pattern="cn-[001-010]",
            node_count=10,
            weight=1,
            features=["gpu_node"],
            resources=NodeResources(
                sockets=2, cores_per_socket=24, threads_per_core=1, gpus=4
            ),
            history=[
                NodeCountPeriod(node_count=5, start=1000, end=2000),
            ],
        )

        # At start (inclusive)
        assert node_group.get_node_count_at(1000) == 5
        # At end (exclusive)
        assert node_group.get_node_count_at(2000) == 0

    def test_multiple_features(self):
        """Test node group with multiple features"""
        node_group = NodeGroupConfig(
            name_pattern="cn-[001-010]",
            node_count=10,
            weight=1,
            features=["gpu_node", "high_mem"],
            resources=NodeResources(
                sockets=2, cores_per_socket=24, threads_per_core=1, gpus=4
            ),
        )

        assert node_group.features == ["gpu_node", "high_mem"]


class TestPartitionConfig:
    """Tests for PartitionConfig dataclass"""

    def test_basic_partition(self):
        """Test basic partition configuration"""
        partition = PartitionConfig(
            name="normal", nodes="cn-[001-010]", state="UP", max_cpus_per_node=128
        )

        assert partition.name == "normal"
        assert partition.nodes == "cn-[001-010]"
        assert partition.state == "UP"
        assert partition.max_cpus_per_node == 128

    def test_partition_optional_fields(self):
        """Test partition with optional fields"""
        partition = PartitionConfig(name="test", nodes="cn-001")

        assert partition.name == "test"
        assert partition.nodes == "cn-001"
        assert partition.state is None
        assert partition.max_cpus_per_node is None
        assert partition.max_nodes is None

    def test_partition_with_max_nodes(self):
        """Test partition with max_nodes limit"""
        partition = PartitionConfig(name="test", nodes="cn-[001-010]", max_nodes=5)

        assert partition.max_nodes == 5


class TestSchedulerConfigForecastFields:
    def test_defaults_include_forecast_model_fields(self):
        config = SchedulerConfig()

        assert config.forecast_model_dir == "artifacts/forecast_model"
        assert config.forecast_skip_startup_training is False

    def test_copy_preserves_forecast_model_fields(self):
        config = SchedulerConfig()
        config.forecast_model_dir = "artifacts/custom_forecast_model"
        config.forecast_skip_startup_training = True

        cloned = config.copy()

        assert cloned.forecast_model_dir == "artifacts/custom_forecast_model"
        assert cloned.forecast_skip_startup_training is True


class TestSchedulerConfigConnectorFields:
    def test_defaults_include_mserver_connector_fields(self):
        config = SchedulerConfig()

        assert config.connector_mserver_url is None
        assert config.connector_api_token is None
        assert config.connector_timeout_seconds == 30
        assert config.connector_target_qos is None

    def test_loads_mserver_connector_fields(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TASKSHIFT_DB_CONFIG_FILE", str(tmp_path / "missing.env"))
        monkeypatch.delenv("TASKSHIFT_MSERVER_API_TOKEN", raising=False)
        monkeypatch.delenv("TASKSHIFT_MSERVER_TIMEOUT_SECONDS", raising=False)
        config_path = tmp_path / "scheduler.yaml"
        config_path.write_text(
            "\n".join(
                [
                    "timelimit: 600",
                    "connector:",
                    "  mserver_url: http://mserver.local/slurm_set_job_qos",
                    "  timeout_seconds: 12",
                    "  target_qos: normal",
                ]
            ),
            encoding="utf-8",
        )

        config = SchedulerConfig().loadConfig(str(config_path))

        assert config.connector_mserver_url == "http://mserver.local/slurm_set_job_qos"
        assert config.connector_api_token is None
        assert config.connector_timeout_seconds == 12
        assert config.connector_target_qos == "normal"

    def test_loads_mserver_token_only_from_taskshift_mserver_api_token(
        self, tmp_path, monkeypatch
    ):
        env_path = tmp_path / ".env"
        env_path.write_text(
            'TASKSHIFT_MSERVER_API_TOKEN="env-secret"\n',
            encoding="utf-8",
        )
        monkeypatch.setenv("TASKSHIFT_DB_CONFIG_FILE", str(env_path))
        monkeypatch.setenv("TASKSHIFT_MSERVER_API_TOKEN", "ignored-process-secret")
        monkeypatch.delenv("TASKSHIFT_MSERVER_TIMEOUT_SECONDS", raising=False)
        config_path = tmp_path / "scheduler.yaml"
        config_path.write_text(
            "\n".join(
                [
                    "timelimit: 600",
                    "connector:",
                    "  mserver_url: http://mserver.local/slurm_set_job_qos",
                    "  target_qos: normal",
                ]
            ),
            encoding="utf-8",
        )

        config = SchedulerConfig().loadConfig(str(config_path))

        assert config.connector_api_token == "env-secret"
        assert "api_token" not in config.to_dict()["connector"]

    def test_ignores_other_mserver_token_names(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TASKSHIFT_DB_CONFIG_FILE", str(tmp_path / "missing.env"))
        monkeypatch.delenv("TASKSHIFT_MSERVER_API_TOKEN", raising=False)
        monkeypatch.setenv("MSERVER_API_TOKEN", "ignored-secret")
        monkeypatch.setenv("API_TOKEN", "also-ignored")
        monkeypatch.delenv("TASKSHIFT_MSERVER_TIMEOUT_SECONDS", raising=False)
        config_path = tmp_path / "scheduler.yaml"
        config_path.write_text(
            "\n".join(
                [
                    "timelimit: 600",
                    "connector:",
                    "  mserver_url: http://mserver.local/slurm_set_job_qos",
                    "  target_qos: normal",
                ]
            ),
            encoding="utf-8",
        )

        config = SchedulerConfig().loadConfig(str(config_path))

        assert config.connector_api_token is None

    def test_copy_preserves_mserver_connector_fields(self):
        config = SchedulerConfig()
        config.connector_mserver_url = "http://mserver.local/slurm_set_job_qos"
        config.connector_api_token = "secret"
        config.connector_timeout_seconds = 5
        config.connector_target_qos = "normal"

        cloned = config.copy()

        assert cloned.connector_mserver_url == "http://mserver.local/slurm_set_job_qos"
        assert cloned.connector_api_token == "secret"
        assert cloned.connector_timeout_seconds == 5
        assert cloned.connector_target_qos == "normal"
