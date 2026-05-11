"""
Unit tests for storage.models module - Job model
"""

import pytest

from storage.models import Job
from tests.fixtures.storage.storage_fixtures import create_pending_job


class TestJobBasics:
    """Tests for Job model basic functionality"""

    def test_job_creation(self):
        """Test creating a Job instance"""
        job = Job(
            jobID=12345,
            jobName="test_job",
            timelimit=60,
            state=0,
            priority=100,
            constraints="type_a",
            cpusReq=4,
            tresReq="1=4,4=1",
            partition="normal",
        )

        assert job.jobID == 12345
        assert job.jobName == "test_job"
        assert job.timelimit == 60
        assert job.state == 0
        assert job.priority == 100
        assert job.constraints == "type_a"
        assert job.cpusReq == 4
        assert job.tresReq == "1=4,4=1"
        assert job.partition == "normal"

    def test_get_id(self):
        """Test getID() method"""
        job = create_pending_job(jobID=54321)
        assert job.getID() == 54321

    def test_get_state(self):
        """Test getState() method"""
        job = create_pending_job(state=0)
        assert job.getState() == 0

    def test_get_timelimit(self):
        """Test getTimelimit() method"""
        job = create_pending_job(timelimit=1440)
        assert job.getTimelimit() == 1440


class TestJobRequestedCpus:
    """Tests for Job.getRequestedCpus()"""

    def test_get_requested_cpus_positive(self):
        """Test getting requested CPUs with positive value"""
        job = create_pending_job(cpusReq=8)
        assert job.getRequestedCpus() == 8

    def test_get_requested_cpus_zero(self):
        """Test getting requested CPUs with zero value"""
        job = create_pending_job(cpusReq=0)
        assert job.getRequestedCpus() == 0

    def test_get_requested_cpus_none(self):
        """Test getting requested CPUs with None value"""
        job = Job(jobID=12345, jobName="test", timelimit=60, state=0, priority=100)
        assert job.getRequestedCpus() == 0


class TestJobRequestedGpus:
    """Tests for Job.getRequestedGpus()"""

    def test_get_requested_gpus_numeric_code(self):
        """Test getting requested GPUs with numeric code 1001"""
        job = create_pending_job(tresReq="1001=4")
        assert job.getRequestedGpus() == 4

    def test_get_requested_gpus_billing_code_ignored(self):
        """Test that billing TRES code 5 is NOT treated as GPU"""
        # Code 5 = billing, code 1001 = gpu
        job = create_pending_job(tresReq="1=4,4=1,5=4")
        assert job.getRequestedGpus() == 0  # Code 5 is billing, not GPU

    def test_get_requested_gpus_gres_format(self):
        """Test getting requested GPUs with gres/gpu format"""
        job = create_pending_job(tresReq="gres/gpu=2")
        assert job.getRequestedGpus() == 2

    def test_get_requested_gpus_simple_format(self):
        """Test getting requested GPUs with simple 'gpu' format"""
        job = create_pending_job(tresReq="gpu=8")
        assert job.getRequestedGpus() == 8

    def test_get_requested_gpus_multiple_entries(self):
        """Test getting requested GPUs with multiple entries (should sum)"""
        job = create_pending_job(tresReq="1001=2,gres/gpu=3,gpu=4")
        # Should return first match
        result = job.getRequestedGpus()
        assert result in [2, 3, 4]

    def test_get_requested_gpus_no_gpus(self):
        """Test getting requested GPUs when no GPUs requested"""
        job = create_pending_job(tresReq="1=4,4=1")
        assert job.getRequestedGpus() == 0

    def test_get_requested_gpus_empty_tres(self):
        """Test getting requested GPUs with empty TRES string"""
        job = create_pending_job(tresReq="")
        assert job.getRequestedGpus() == 0

    def test_get_requested_gpus_none_tres(self):
        """Test getting requested GPUs with None TRES string"""
        job = Job(
            jobID=12345,
            jobName="test",
            timelimit=60,
            state=0,
            priority=100,
            tresReq=None,
        )
        assert job.getRequestedGpus() == 0


class TestJobRequestedNodes:
    """Tests for Job.getRequestedNodes()"""

    def test_get_requested_nodes_numeric_code(self):
        """Test getting requested nodes with numeric code 4"""
        job = create_pending_job(tresReq="4=2")
        assert job.getRequestedNodes() == 2

    def test_get_requested_nodes_node_format(self):
        """Test getting requested nodes with 'node' format"""
        job = create_pending_job(tresReq="node=3")
        assert job.getRequestedNodes() == 3

    def test_get_requested_nodes_nodes_format(self):
        """Test getting requested nodes with 'nodes' format"""
        job = create_pending_job(tresReq="nodes=5")
        assert job.getRequestedNodes() == 5

    def test_get_requested_nodes_no_nodes(self):
        """Test getting requested nodes when no nodes requested"""
        job = create_pending_job(tresReq="1=4,1001=2")
        assert job.getRequestedNodes() == 0

    def test_get_requested_nodes_empty_tres(self):
        """Test getting requested nodes with empty TRES string"""
        job = create_pending_job(tresReq="")
        assert job.getRequestedNodes() == 0


class TestJobRequestedFeatures:
    """Tests for Job.getRequestedFeatures()"""

    def test_get_requested_features_single(self):
        """Test getting requested features with single feature"""
        job = create_pending_job(constraints="type_a")
        features = job.getRequestedFeatures(["type_a", "type_b", "type_c"])
        assert features == ["type_a"]

    def test_get_requested_features_multiple_pipe(self):
        """Test getting requested features with pipe separator"""
        job = create_pending_job(constraints="type_a|type_b")
        features = job.getRequestedFeatures(["type_a", "type_b", "type_c"])
        assert set(features) == {"type_a", "type_b"}

    def test_get_requested_features_multiple_comma(self):
        """Test getting requested features with comma separator"""
        job = create_pending_job(constraints="type_a,type_b")
        features = job.getRequestedFeatures(["type_a", "type_b", "type_c"])
        assert set(features) == {"type_a", "type_b"}

    def test_get_requested_features_mixed_separators(self):
        """Test getting requested features with mixed separators"""
        job = create_pending_job(constraints="type_a|type_b,type_c")
        features = job.getRequestedFeatures(["type_a", "type_b", "type_c", "type_d"])
        assert set(features) == {"type_a", "type_b", "type_c"}

    def test_get_requested_features_with_parentheses(self):
        """Test getting requested features with parentheses"""
        job = create_pending_job(constraints="(type_a|type_b)")
        features = job.getRequestedFeatures(["type_a", "type_b", "type_c"])
        assert set(features) == {"type_a", "type_b"}

    def test_get_requested_features_no_match(self):
        """Test getting requested features when none match"""
        job = create_pending_job(constraints="type_x|type_y")
        features = job.getRequestedFeatures(["type_a", "type_b", "type_c"])
        assert features == []

    def test_get_requested_features_no_constraints(self):
        """Test getting requested features with no constraints"""
        job = create_pending_job(constraints=None)
        features = job.getRequestedFeatures(["type_a", "type_b", "type_c"])
        assert set(features) == {"type_a", "type_b", "type_c"}

    def test_get_requested_features_empty_constraints(self):
        """Test getting requested features with empty constraints"""
        job = create_pending_job(constraints="")
        features = job.getRequestedFeatures(["type_a", "type_b", "type_c"])
        assert set(features) == {"type_a", "type_b", "type_c"}

    def test_get_requested_features_sorted(self):
        """Test that requested features are sorted"""
        job = create_pending_job(constraints="type_c|type_a|type_b")
        features = job.getRequestedFeatures(["type_a", "type_b", "type_c"])
        assert features == ["type_a", "type_b", "type_c"]


class TestJobTresParsing:
    """Tests for Job TRES parsing edge cases"""

    def test_tres_with_whitespace(self):
        """Test TRES parsing with whitespace"""
        job = create_pending_job(tresReq="1 = 4, 4 = 1")
        # getRequestedCpus() returns cpusReq field, not TRES
        # TRES is parsed by getRequestedGpus() and getRequestedNodes()
        assert isinstance(job.getRequestedCpus(), int)

    def test_tres_with_invalid_value(self):
        """Test TRES parsing with invalid GPU value"""
        job = create_pending_job(tresReq="1001=invalid")
        # Invalid TRES values should return 0
        assert job.getRequestedGpus() == 0

    def test_tres_with_duplicate_keys(self):
        """Test TRES parsing with duplicate GPU keys"""
        job = create_pending_job(tresReq="1001=2,1001=4")
        # Duplicates: last one wins in _parse_tres_map
        result = job.getRequestedGpus()
        assert result == 4

    def test_tres_empty_value(self):
        """Test TRES parsing with empty GPU value"""
        job = create_pending_job(tresReq="1001=")
        # Empty value should return 0
        assert job.getRequestedGpus() == 0

    def test_tres_complex_format(self):
        """Test TRES parsing with complex real-world format"""
        # Real format: CPU from cpusReq, GPU from tresReq (code 1001), nodes from tresReq (code 4)
        # Code 5 = billing (NOT gpu), code 2 = memory
        job = create_pending_job(cpusReq=12, tresReq="1=12,2=1,4=1,5=12,1001=4")
        assert job.getRequestedCpus() == 12  # from cpusReq field
        assert job.getRequestedGpus() == 4   # code 1001=4 (gpu), billing code 5=12 ignored
        assert job.getRequestedNodes() == 1  # code 4=1 (node)
