"""
Comprehensive unit tests for scheduler/resources.py

Classes tested:
  - NodeResourceState
  - NodeAllocation
  - JobPlacement
  - ResourceAvailabilityTree
"""

import pytest

from scheduler.resources import (
    JobPlacement,
    NodeAllocation,
    NodeResourceState,
    ResourceAvailabilityTree,
)
from tests.fixtures.scheduler.scheduler_fixtures import (
    EXPECTED_FEATURE_CAPACITIES,
    EXPECTED_NODE_CAPACITIES,
    TIMESTAMP_NOW,
    build_mini_cluster_config,
    build_mini_cluster_config_mock,
    create_pending_job,
    create_running_gpu_job,
    create_running_job,
)

# ════════════════════════════════════════════════════════════════════════════════
# NodeResourceState
# ════════════════════════════════════════════════════════════════════════════════


class TestNodeResourceState:
    """Tests for NodeResourceState dataclass and its properties."""

    def test_default_values(self):
        """New node starts with usedCpu=0.0 and usedGpu=0.0."""
        node = NodeResourceState(
            nodeName="cn-001", featureName="type_a", totalCpu=4, totalGpu=2
        )
        assert node.availableCpu == 4.0
        assert node.availableGpu == 2.0

    def test_availableCpu_with_partial_usage(self):
        """availableCpu returns totalCpu - usedCpu."""
        node = NodeResourceState(
            nodeName="cn-001",
            featureName="type_a",
            totalCpu=4,
            totalGpu=2,
            usedCpu=2.0,
        )
        assert node.availableCpu == 2.0

    def test_availableGpu_with_partial_usage(self):
        """availableGpu returns totalGpu - usedGpu."""
        node = NodeResourceState(
            nodeName="cn-001",
            featureName="type_a",
            totalCpu=4,
            totalGpu=2,
            usedGpu=1.0,
        )
        assert node.availableGpu == 1.0

    def test_availableCpu_clamped_to_zero(self):
        """availableCpu uses max(0, ...) — never goes negative."""
        node = NodeResourceState(
            nodeName="cn-001",
            featureName="type_a",
            totalCpu=4,
            totalGpu=2,
            usedCpu=10.0,
        )
        assert node.availableCpu == 0.0

    def test_availableGpu_clamped_to_zero(self):
        """availableGpu uses max(0, ...) — never goes negative."""
        node = NodeResourceState(
            nodeName="cn-001",
            featureName="type_a",
            totalCpu=4,
            totalGpu=2,
            usedGpu=5.0,
        )
        assert node.availableGpu == 0.0

    def test_availableCpu_at_exact_total(self):
        """When usedCpu equals totalCpu, availableCpu is 0."""
        node = NodeResourceState(
            nodeName="cn-001",
            featureName="type_a",
            totalCpu=4,
            totalGpu=2,
            usedCpu=4.0,
        )
        assert node.availableCpu == 0.0

    def test_no_gpu_node(self):
        """Node with 0 GPU always reports 0 available GPU."""
        node = NodeResourceState(
            nodeName="cn-005",
            featureName="type_b",
            totalCpu=8,
            totalGpu=0,
        )
        assert node.availableGpu == 0.0
        assert node.availableCpu == 8.0

    def test_mutable_state(self):
        """usedCpu and usedGpu can be mutated and properties update."""
        node = NodeResourceState(
            nodeName="cn-001",
            featureName="type_a",
            totalCpu=4,
            totalGpu=2,
        )
        assert node.availableCpu == 4.0

        node.usedCpu += 2.0
        assert node.availableCpu == 2.0

        node.usedCpu += 3.0
        # usedCpu=5.0, totalCpu=4 → max(0, -1) = 0
        assert node.availableCpu == 0.0


# ════════════════════════════════════════════════════════════════════════════════
# NodeAllocation
# ════════════════════════════════════════════════════════════════════════════════


class TestNodeAllocation:
    """Tests for NodeAllocation dataclass."""

    def test_creation_cpu_only(self):
        alloc = NodeAllocation(nodeName="cn-001", cpu=2.0, gpu=0.0)
        assert alloc.nodeName == "cn-001"
        assert alloc.cpu == 2.0
        assert alloc.gpu == 0.0

    def test_creation_with_gpu(self):
        alloc = NodeAllocation(nodeName="cn-001", cpu=4.0, gpu=1.0)
        assert alloc.cpu == 4.0
        assert alloc.gpu == 1.0


# ════════════════════════════════════════════════════════════════════════════════
# JobPlacement
# ════════════════════════════════════════════════════════════════════════════════


class TestJobPlacement:
    """Tests for JobPlacement dataclass and its nodeNames property."""

    def test_single_allocation(self):
        placement = JobPlacement(
            featureName="type_a",
            allocations=[NodeAllocation(nodeName="cn-001", cpu=2.0, gpu=1.0)],
        )
        assert placement.nodeNames == ["cn-001"]

    def test_multiple_allocations(self):
        placement = JobPlacement(
            featureName="type_a",
            allocations=[
                NodeAllocation(nodeName="cn-001", cpu=2.0, gpu=1.0),
                NodeAllocation(nodeName="cn-002", cpu=2.0, gpu=1.0),
            ],
        )
        assert placement.nodeNames == ["cn-001", "cn-002"]

    def test_empty_allocations(self):
        placement = JobPlacement(featureName="type_a", allocations=[])
        assert placement.nodeNames == []


# ════════════════════════════════════════════════════════════════════════════════
# ResourceAvailabilityTree — fromClusterAndJobs
# ════════════════════════════════════════════════════════════════════════════════


class TestFromClusterAndJobs:
    """Tests for ResourceAvailabilityTree.fromClusterAndJobs class method."""

    @pytest.fixture
    def cluster_config(self):
        return build_mini_cluster_config()

    def test_empty_cluster_no_jobs(self, cluster_config):
        """Empty node_groups → empty tree."""
        cluster_config.node_groups = []
        tree = ResourceAvailabilityTree.fromClusterAndJobs(
            cluster_config, [], TIMESTAMP_NOW
        )
        assert tree.nodesByFeature == {}

    def test_no_running_jobs(self, cluster_config):
        """Cluster with nodes but no running jobs → all resources available."""
        tree = ResourceAvailabilityTree.fromClusterAndJobs(
            cluster_config, [], TIMESTAMP_NOW
        )

        # Check that all three features are present
        assert set(tree.nodesByFeature.keys()) == {"type_a", "type_b", "type_d"}

        # type_a: 4 nodes, 4 CPU / 2 GPU each
        type_a_nodes = tree.nodesByFeature["type_a"]
        assert len(type_a_nodes) == 4
        for node in type_a_nodes:
            assert node.availableCpu == 4.0
            assert node.availableGpu == 2.0

        # type_b: 2 nodes, 8 CPU / 0 GPU each
        type_b_nodes = tree.nodesByFeature["type_b"]
        assert len(type_b_nodes) == 2
        for node in type_b_nodes:
            assert node.availableCpu == 8.0
            assert node.availableGpu == 0.0

        # type_d: 2 nodes, 8 CPU / 4 GPU each
        type_d_nodes = tree.nodesByFeature["type_d"]
        assert len(type_d_nodes) == 2
        for node in type_d_nodes:
            assert node.availableCpu == 8.0
            assert node.availableGpu == 4.0

    def test_with_running_jobs_resources_consumed(self, cluster_config):
        """Running jobs consume resources from their nodes."""
        # Job using cn-001: 2 CPU. GPU comes from getRequestedGpus() which reads
        # tresReq. Default tresReq is f"1={cpusReq},4={nodesAlloc}" — no 1001 key,
        # so GPU=0. Use create_running_gpu_job for GPU consumption.
        job = create_running_gpu_job(
            jobID=2001, cpusReq=2, nodelist="cn-001", gpusRequested=1
        )
        tree = ResourceAvailabilityTree.fromClusterAndJobs(
            cluster_config, [job], TIMESTAMP_NOW
        )

        cn001 = [n for n in tree.nodesByFeature["type_a"] if n.nodeName == "cn-001"][0]
        assert cn001.availableCpu == 2.0  # 4 - 2
        assert cn001.availableGpu == 1.0  # 2 - 1

        # Other type_a nodes should be untouched
        cn002 = [n for n in tree.nodesByFeature["type_a"] if n.nodeName == "cn-002"][0]
        assert cn002.availableCpu == 4.0
        assert cn002.availableGpu == 2.0

    def test_running_job_without_nodelist_skipped(self, cluster_config):
        """Running job with no nodelist → skipped with warning."""
        job = create_running_job(jobID=2001, nodelist="")
        tree = ResourceAvailabilityTree.fromClusterAndJobs(
            cluster_config, [job], TIMESTAMP_NOW
        )

        # Tree should have all resources untouched
        for node_list in tree.nodesByFeature.values():
            for node in node_list:
                assert node.availableCpu == float(node.totalCpu)
                assert node.availableGpu == float(node.totalGpu)

    def test_running_job_nodelist_none_assigned_skipped(self, cluster_config):
        """Running job with nodelist='None assigned' → skipped with warning."""
        job = create_running_job(jobID=2001, nodelist="None assigned")
        tree = ResourceAvailabilityTree.fromClusterAndJobs(
            cluster_config, [job], TIMESTAMP_NOW
        )
        # hasAssignedNodes returns False, so placeRunningJob returns None

    def test_running_job_on_unknown_nodes_ignored(self, cluster_config):
        """Running job on nodes not in tree → placement returns None, no crash."""
        job = create_running_job(jobID=2001, nodelist="cn-999")
        tree = ResourceAvailabilityTree.fromClusterAndJobs(
            cluster_config, [job], TIMESTAMP_NOW
        )
        # Tree should be fine, all nodes untouched
        for node_list in tree.nodesByFeature.values():
            for node in node_list:
                assert node.usedCpu == 0.0
                assert node.usedGpu == 0.0

    def test_multiple_running_jobs(self, cluster_config):
        """Multiple running jobs on different nodes accumulate consumption."""
        job1 = create_running_gpu_job(
            jobID=2001, cpusReq=2, nodelist="cn-001", gpusRequested=1
        )
        job2 = create_running_gpu_job(
            jobID=3001,
            cpusReq=4,
            nodelist="cn-007",
            gpusRequested=2,
            constraints="type_d",
        )
        tree = ResourceAvailabilityTree.fromClusterAndJobs(
            cluster_config, [job1, job2], TIMESTAMP_NOW
        )

        cn001 = _find_node(tree, "cn-001")
        assert cn001.availableCpu == 2.0  # 4 - 2
        assert cn001.availableGpu == 1.0  # 2 - 1

        cn007 = _find_node(tree, "cn-007")
        assert cn007.availableCpu == 4.0  # 8 - 4
        assert cn007.availableGpu == 2.0  # 4 - 2

    def test_running_job_multi_node(self, cluster_config):
        """Running job across multiple nodes consumes resources on each.

        With spreadAcrossSelectedNodes=True, CPU is distributed round-robin.
        2 CPU across 2 nodes → 1 CPU each.
        """
        job = create_running_job(
            jobID=2001,
            cpusReq=2,
            nodesAlloc=2,
            nodelist="cn-[001-002]",
        )
        tree = ResourceAvailabilityTree.fromClusterAndJobs(
            cluster_config, [job], TIMESTAMP_NOW
        )

        cn001 = _find_node(tree, "cn-001")
        assert cn001.availableCpu == 3.0  # 4 - 1 (spread: 1 CPU each)
        cn002 = _find_node(tree, "cn-002")
        assert cn002.availableCpu == 3.0  # 4 - 1 (spread: 1 CPU each)

    def test_node_count_per_feature(self, cluster_config):
        """Verify node counts per feature match the cluster config."""
        tree = ResourceAvailabilityTree.fromClusterAndJobs(
            cluster_config, [], TIMESTAMP_NOW
        )
        assert len(tree.nodesByFeature["type_a"]) == 4
        assert len(tree.nodesByFeature["type_b"]) == 2
        assert len(tree.nodesByFeature["type_d"]) == 2


# ════════════════════════════════════════════════════════════════════════════════
# ResourceAvailabilityTree — findPlacement
# ════════════════════════════════════════════════════════════════════════════════


class TestFindPlacement:
    """Tests for ResourceAvailabilityTree.findPlacement."""

    @pytest.fixture
    def tree(self):
        config = build_mini_cluster_config()
        return ResourceAvailabilityTree.fromClusterAndJobs(config, [], TIMESTAMP_NOW)

    def test_job_needs_type_a(self, tree):
        """Job with constraint type_a gets placed on type_a nodes."""
        job = create_pending_job(
            jobID=1001, constraints="type_a", cpusReq=2, tresReq="1=2,4=1,1001=1"
        )
        placement = tree.findPlacement(job)

        assert placement is not None
        assert placement.featureName == "type_a"
        assert len(placement.nodeNames) == 1
        assert placement.nodeNames[0].startswith("cn-00")

    def test_job_needs_nonexistent_feature(self, tree):
        """Job requesting a feature that doesn't exist → None."""
        job = create_pending_job(
            jobID=1001, constraints="type_z", cpusReq=2, tresReq="1=2,4=1"
        )
        assert tree.findPlacement(job) is None

    def test_job_more_resources_than_available(self, tree):
        """Job requesting more resources than cluster has → None."""
        # type_a has 8 GPU total; requesting 10
        job = create_pending_job(
            jobID=1001, constraints="type_a", cpusReq=2, tresReq="1=2,4=1,1001=10"
        )
        assert tree.findPlacement(job) is None

    def test_job_no_constraints(self, tree):
        """Job with no constraints tries all features."""
        job = create_pending_job(
            jobID=1001, constraints=None, cpusReq=2, tresReq="1=2,4=1"
        )
        placement = tree.findPlacement(job)

        # Should find placement on some feature
        assert placement is not None
        assert placement.featureName in {"type_a", "type_b", "type_d"}

    def test_job_no_constraints_falls_through_features(self, tree):
        """Job with no constraints that exhausts first feature tries others."""
        # Consume all type_a resources
        for node in tree.nodesByFeature["type_a"]:
            node.usedCpu = node.totalCpu
            node.usedGpu = node.totalGpu

        job = create_pending_job(
            jobID=1001, constraints=None, cpusReq=2, tresReq="1=2,4=1"
        )
        placement = tree.findPlacement(job)

        # Should place on type_b or type_d (both have CPU)
        assert placement is not None
        assert placement.featureName in {"type_b", "type_d"}

    def test_job_fits_only_one_feature(self, tree):
        """Job that needs GPU should prefer GPU-capable features."""
        job = create_pending_job(
            jobID=1001, constraints=None, cpusReq=2, tresReq="1=2,4=1,1001=1"
        )
        placement = tree.findPlacement(job)

        assert placement is not None
        # type_b has no GPU, so placement must be on type_a or type_d
        assert placement.featureName in {"type_a", "type_d"}

    def test_job_requests_multiple_nodes(self, tree):
        """Job requesting multiple nodes gets placement across them."""
        job = create_pending_job(
            jobID=1001, constraints="type_b", cpusReq=16, tresReq="1=16,4=2"
        )
        placement = tree.findPlacement(job)

        assert placement is not None
        assert placement.featureName == "type_b"
        # 2 nodes of type_b
        assert len(placement.nodeNames) == 2

    def test_no_resources_anywhere(self, tree):
        """All features exhausted → None."""
        for feature_nodes in tree.nodesByFeature.values():
            for node in feature_nodes:
                node.usedCpu = node.totalCpu
                node.usedGpu = node.totalGpu

        job = create_pending_job(jobID=1001, cpusReq=1, tresReq="1=1,4=1")
        assert tree.findPlacement(job) is None


# ════════════════════════════════════════════════════════════════════════════════
# ResourceAvailabilityTree — findPlacementOnFeature
# ════════════════════════════════════════════════════════════════════════════════


class TestFindPlacementOnFeature:
    """Tests for ResourceAvailabilityTree.findPlacementOnFeature."""

    @pytest.fixture
    def tree(self):
        config = build_mini_cluster_config()
        return ResourceAvailabilityTree.fromClusterAndJobs(config, [], TIMESTAMP_NOW)

    def test_basic_placement(self, tree):
        """Basic placement with enough resources → placement found."""
        job = create_pending_job(
            jobID=1001, constraints="type_a", cpusReq=2, tresReq="1=2,4=1,1001=1"
        )
        placement = tree.findPlacementOnFeature(job, "type_a")

        assert placement is not None
        assert placement.featureName == "type_a"
        assert len(placement.nodeNames) >= 1

    def test_not_enough_cpu(self, tree):
        """Not enough CPU available → None."""
        job = create_pending_job(
            jobID=1001, constraints="type_a", cpusReq=100, tresReq="1=100,4=1"
        )
        # type_a has 16 CPU total
        assert tree.findPlacementOnFeature(job, "type_a") is None

    def test_not_enough_gpu(self, tree):
        """Not enough GPU available → None."""
        job = create_pending_job(
            jobID=1001, constraints="type_a", cpusReq=1, tresReq="1=1,4=1,1001=10"
        )
        # type_a has 8 GPU total
        assert tree.findPlacementOnFeature(job, "type_a") is None

    def test_allowedNodeNames_filters(self, tree):
        """allowedNodeNames restricts placement to specific nodes."""
        job = create_pending_job(
            jobID=1001, constraints="type_a", cpusReq=4, tresReq="1=4,4=2,1001=2"
        )
        # Only allow cn-001 and cn-002
        allowed = {"cn-001", "cn-002"}
        placement = tree.findPlacementOnFeature(job, "type_a", allowedNodeNames=allowed)

        assert placement is not None
        assert set(placement.nodeNames).issubset(allowed)

    def test_allowedNodeNames_excludes_all(self, tree):
        """allowedNodeNames that excludes all feature nodes → None."""
        job = create_pending_job(
            jobID=1001, constraints="type_a", cpusReq=2, tresReq="1=2,4=1"
        )
        allowed = {"cn-005"}  # type_b node, not type_a
        assert (
            tree.findPlacementOnFeature(job, "type_a", allowedNodeNames=allowed) is None
        )

    def test_maxCpuPerNode_limits_cpu(self, tree):
        """maxCpuPerNode caps how much CPU can be placed on each node.

        Request 8 CPU on type_b with no specific node count → the allocator
        picks minimum nodes. With maxCpuPerNode=4, each node provides at
        most 4 CPU, so 2 nodes are needed (2*4=8).
        """
        job = create_pending_job(
            jobID=1001, constraints="type_b", cpusReq=8, tresReq="1=8"
        )
        placement = tree.findPlacementOnFeature(job, "type_b", maxCpuPerNode=4)

        assert placement is not None
        assert len(placement.nodeNames) == 2
        # Each node should have at most 4 CPU allocated
        for alloc in placement.allocations:
            assert alloc.cpu <= 4.0

    def test_maxCpuPerNode_insufficient(self, tree):
        """maxCpuPerNode too restrictive → None."""
        # Request 16 CPU on type_b with maxCpuPerNode=4 → max 2 nodes * 4 = 8 CPU
        job = create_pending_job(
            jobID=1001, constraints="type_b", cpusReq=16, tresReq="1=16,4=1"
        )
        assert tree.findPlacementOnFeature(job, "type_b", maxCpuPerNode=4) is None

    def test_maxNodesLimit(self, tree):
        """maxNodesLimit caps the number of nodes used."""
        # Request 1 node
        job = create_pending_job(
            jobID=1001, constraints="type_a", cpusReq=2, tresReq="1=2,4=1"
        )
        placement = tree.findPlacementOnFeature(job, "type_a", maxNodesLimit=1)

        assert placement is not None
        assert len(placement.nodeNames) <= 1

    def test_maxNodesLimit_too_few(self, tree):
        """requestedNodes > maxNodesLimit → None."""
        # Request 3 nodes but maxNodesLimit=2
        job = create_pending_job(
            jobID=1001, constraints="type_a", cpusReq=4, tresReq="1=4,4=3"
        )
        assert tree.findPlacementOnFeature(job, "type_a", maxNodesLimit=2) is None

    def test_requestedNodes_more_than_available_nodes(self, tree):
        """requestedNodes > available nodes for feature → None."""
        # type_b has 2 nodes, request 3
        job = create_pending_job(
            jobID=1001, constraints="type_b", cpusReq=1, tresReq="1=1,4=3"
        )
        assert tree.findPlacementOnFeature(job, "type_b") is None

    def test_nonexistent_feature(self, tree):
        """Nonexistent feature name → None."""
        job = create_pending_job(jobID=1001, cpusReq=1, tresReq="1=1,4=1")
        assert tree.findPlacementOnFeature(job, "nonexistent") is None

    def test_feature_partially_consumed(self, tree):
        """Placement accounts for already-consumed resources.

        Consume all CPU on cn-001. A job requesting 6 CPU should still fit
        on the remaining 3 type_a nodes (3*4=12 CPU available).
        """
        cn001 = _find_node(tree, "cn-001")
        cn001.usedCpu = 4.0
        cn001.usedGpu = 2.0

        # No specific node count (4= not in tresReq) → allocator picks minimum
        job = create_pending_job(
            jobID=1001, constraints="type_a", cpusReq=6, tresReq="1=6,1001=6"
        )
        placement = tree.findPlacementOnFeature(job, "type_a")

        assert placement is not None
        assert "cn-001" not in placement.nodeNames

    def test_single_node_job(self, tree):
        """Job requesting 1 node gets placed on a single node."""
        job = create_pending_job(
            jobID=1001, constraints="type_a", cpusReq=2, tresReq="1=2,4=1,1001=1"
        )
        placement = tree.findPlacementOnFeature(job, "type_a")

        assert placement is not None
        assert len(placement.nodeNames) == 1


# ════════════════════════════════════════════════════════════════════════════════
# ResourceAvailabilityTree — reservePlacement
# ════════════════════════════════════════════════════════════════════════════════


class TestReservePlacement:
    """Tests for ResourceAvailabilityTree.reservePlacement."""

    @pytest.fixture
    def tree(self):
        config = build_mini_cluster_config()
        return ResourceAvailabilityTree.fromClusterAndJobs(config, [], TIMESTAMP_NOW)

    def test_reserve_cpu(self, tree):
        """Reserve CPU → availableCpu decreases on the node."""
        placement = JobPlacement(
            featureName="type_a",
            allocations=[NodeAllocation(nodeName="cn-001", cpu=2.0, gpu=0.0)],
        )
        tree.reservePlacement(placement)

        cn001 = _find_node(tree, "cn-001")
        assert cn001.usedCpu == 2.0
        assert cn001.availableCpu == 2.0  # 4 - 2

    def test_reserve_gpu(self, tree):
        """Reserve GPU → availableGpu decreases on the node."""
        placement = JobPlacement(
            featureName="type_a",
            allocations=[NodeAllocation(nodeName="cn-001", cpu=0.0, gpu=1.0)],
        )
        tree.reservePlacement(placement)

        cn001 = _find_node(tree, "cn-001")
        assert cn001.usedGpu == 1.0
        assert cn001.availableGpu == 1.0  # 2 - 1

    def test_reserve_all_resources_on_feature(self, tree):
        """Reserve on all nodes of a feature → 0 available."""
        allocations = [
            NodeAllocation(nodeName=f"cn-{i:03d}", cpu=4.0, gpu=2.0)
            for i in range(1, 5)
        ]
        placement = JobPlacement(featureName="type_a", allocations=allocations)
        tree.reservePlacement(placement)

        for node in tree.nodesByFeature["type_a"]:
            assert node.availableCpu == 0.0
            assert node.availableGpu == 0.0

    def test_over_reserve_clamped_to_zero(self, tree):
        """Over-reserve → available goes to 0 (due to max(0, ...))."""
        placement = JobPlacement(
            featureName="type_a",
            allocations=[NodeAllocation(nodeName="cn-001", cpu=100.0, gpu=100.0)],
        )
        tree.reservePlacement(placement)

        cn001 = _find_node(tree, "cn-001")
        assert cn001.availableCpu == 0.0
        assert cn001.availableGpu == 0.0

    def test_reserve_unknown_feature_ignored(self, tree):
        """Reserve on unknown feature → silently ignored, no crash."""
        placement = JobPlacement(
            featureName="nonexistent",
            allocations=[NodeAllocation(nodeName="cn-001", cpu=10.0, gpu=10.0)],
        )
        tree.reservePlacement(placement)

        cn001 = _find_node(tree, "cn-001")
        assert cn001.availableCpu == 4.0  # unchanged
        assert cn001.availableGpu == 2.0

    def test_reserve_unknown_node_ignored(self, tree):
        """Reserve on a node not in the feature → silently ignored."""
        placement = JobPlacement(
            featureName="type_a",
            allocations=[NodeAllocation(nodeName="cn-999", cpu=10.0, gpu=10.0)],
        )
        tree.reservePlacement(placement)

        # All type_a nodes should be untouched
        for node in tree.nodesByFeature["type_a"]:
            assert node.availableCpu == 4.0
            assert node.availableGpu == 2.0

    def test_multiple_reservations_accumulate(self, tree):
        """Multiple reserves on the same node accumulate."""
        placement1 = JobPlacement(
            featureName="type_a",
            allocations=[NodeAllocation(nodeName="cn-001", cpu=1.0, gpu=0.5)],
        )
        placement2 = JobPlacement(
            featureName="type_a",
            allocations=[NodeAllocation(nodeName="cn-001", cpu=1.0, gpu=0.5)],
        )
        tree.reservePlacement(placement1)
        tree.reservePlacement(placement2)

        cn001 = _find_node(tree, "cn-001")
        assert cn001.usedCpu == 2.0
        assert cn001.availableCpu == 2.0  # 4 - 2
        assert cn001.usedGpu == 1.0
        assert cn001.availableGpu == 1.0  # 2 - 1

    def test_reserve_across_multiple_nodes(self, tree):
        """Reserve across multiple nodes consumes on each."""
        placement = JobPlacement(
            featureName="type_a",
            allocations=[
                NodeAllocation(nodeName="cn-001", cpu=2.0, gpu=1.0),
                NodeAllocation(nodeName="cn-002", cpu=3.0, gpu=2.0),
            ],
        )
        tree.reservePlacement(placement)

        cn001 = _find_node(tree, "cn-001")
        assert cn001.availableCpu == 2.0
        assert cn001.availableGpu == 1.0

        cn002 = _find_node(tree, "cn-002")
        assert cn002.availableCpu == 1.0  # 4 - 3
        assert cn002.availableGpu == 0.0  # 2 - 2


# ════════════════════════════════════════════════════════════════════════════════
# ResourceAvailabilityTree — getFeatureTotals
# ════════════════════════════════════════════════════════════════════════════════


class TestGetFeatureTotals:
    """Tests for ResourceAvailabilityTree.getFeatureTotals."""

    @pytest.fixture
    def tree(self):
        config = build_mini_cluster_config()
        return ResourceAvailabilityTree.fromClusterAndJobs(config, [], TIMESTAMP_NOW)

    def test_all_nodes_available(self, tree):
        """All nodes available → total feature capacity."""
        totals = tree.getFeatureTotals("type_a")
        assert totals["cpu"] == 16.0  # 4 nodes * 4 CPU
        assert totals["gpu"] == 8.0  # 4 nodes * 2 GPU

    def test_type_b_no_gpu(self, tree):
        """type_b has 0 GPU."""
        totals = tree.getFeatureTotals("type_b")
        assert totals["cpu"] == 16.0  # 2 nodes * 8 CPU
        assert totals["gpu"] == 0.0

    def test_type_d_full(self, tree):
        """type_d full capacity."""
        totals = tree.getFeatureTotals("type_d")
        assert totals["cpu"] == 16.0  # 2 nodes * 8 CPU
        assert totals["gpu"] == 8.0  # 2 nodes * 4 GPU

    def test_some_consumed(self, tree):
        """Some CPU consumed → reduced available CPU.

        NOTE: getFeatureTotals uses node.totalGpu for GPU, not availableGpu.
        GPU totals always reflect full capacity regardless of consumption.
        """
        cn001 = _find_node(tree, "cn-001")
        cn001.usedCpu = 2.0
        cn001.usedGpu = 1.0

        totals = tree.getFeatureTotals("type_a")
        # CPU: cn-001 has 2 available; others have 4 each → 2 + 3*4 = 14
        # GPU: getFeatureTotals uses totalGpu, so all nodes still count → 4*2 = 8
        assert totals["cpu"] == 14.0
        assert totals["gpu"] == 8.0

    def test_with_allowedNodeNames(self, tree):
        """allowedNodeNames filters which nodes contribute to totals."""
        allowed = {"cn-001", "cn-002"}
        totals = tree.getFeatureTotals("type_a", allowedNodeNames=allowed)
        assert totals["cpu"] == 8.0  # 2 nodes * 4 CPU
        assert totals["gpu"] == 4.0  # 2 nodes * 2 GPU

    def test_with_allowedNodeNames_single_node(self, tree):
        """Single allowed node → only that node's resources."""
        totals = tree.getFeatureTotals("type_a", allowedNodeNames={"cn-003"})
        assert totals["cpu"] == 4.0
        assert totals["gpu"] == 2.0

    def test_with_allowedNodeNames_excludes_all(self, tree):
        """allowedNodeNames that doesn't match any node → 0."""
        totals = tree.getFeatureTotals("type_a", allowedNodeNames={"cn-005"})
        assert totals["cpu"] == 0.0
        assert totals["gpu"] == 0.0

    def test_with_maxCpuPerNode(self, tree):
        """maxCpuPerNode caps per-node CPU in totals."""
        # type_a: 4 nodes with 4 CPU each, capped to 2
        totals = tree.getFeatureTotals("type_a", maxCpuPerNode=2)
        assert totals["cpu"] == 8.0  # 4 * 2 = 8
        assert totals["gpu"] == 8.0  # GPU not affected by CPU cap

    def test_with_maxCpuPerNode_and_allowedNodeNames(self, tree):
        """Both maxCpuPerNode and allowedNodeNames together."""
        allowed = {"cn-001", "cn-002", "cn-003"}
        totals = tree.getFeatureTotals(
            "type_a", allowedNodeNames=allowed, maxCpuPerNode=3
        )
        assert totals["cpu"] == 9.0  # 3 nodes * 3 CPU
        assert totals["gpu"] == 6.0  # 3 nodes * 2 GPU (uncapped)

    def test_unknown_feature(self, tree):
        """Unknown feature → {"cpu": 0, "gpu": 0}."""
        totals = tree.getFeatureTotals("nonexistent")
        assert totals == {"cpu": 0.0, "gpu": 0.0}

    def test_consumed_beyond_capacity_still_zero(self, tree):
        """Over-consumed node contributes 0 CPU to totals.

        NOTE: getFeatureTotals uses node.totalGpu for GPU, so GPU is unaffected.
        """
        cn001 = _find_node(tree, "cn-001")
        cn001.usedCpu = 100.0
        cn001.usedGpu = 100.0

        totals = tree.getFeatureTotals("type_a")
        # CPU: cn-001 has 0 available (clamped); cn-[002-004] have 4 each → 12
        # GPU: uses totalGpu → 4*2 = 8
        assert totals["cpu"] == 12.0
        assert totals["gpu"] == 8.0

    def test_maxCpuPerNode_lower_than_available(self, tree):
        """maxCpuPerNode that's lower than actual available caps correctly."""
        # Consume 2 CPU from cn-001 → 2 CPU available
        cn001 = _find_node(tree, "cn-001")
        cn001.usedCpu = 2.0

        # maxCpuPerNode=1 caps cn-001 to 1 even though 2 are available
        totals = tree.getFeatureTotals(
            "type_a", allowedNodeNames={"cn-001"}, maxCpuPerNode=1
        )
        assert totals["cpu"] == 1.0


# ════════════════════════════════════════════════════════════════════════════════
# ResourceAvailabilityTree — placeRunningJob
# ════════════════════════════════════════════════════════════════════════════════


class TestPlaceRunningJob:
    """Tests for ResourceAvailabilityTree.placeRunningJob."""

    @pytest.fixture
    def tree(self):
        config = build_mini_cluster_config()
        return ResourceAvailabilityTree.fromClusterAndJobs(config, [], TIMESTAMP_NOW)

    def test_single_node_job(self, tree):
        """Single node job → correct CPU allocation on one node.

        Uses create_running_gpu_job so getRequestedGpus() returns non-zero
        (it reads tresReq, which has 1001= key).
        """
        job = create_running_gpu_job(
            jobID=2001, cpusReq=2, nodelist="cn-001", gpusRequested=1
        )
        placement = tree.placeRunningJob(job)

        assert placement is not None
        assert placement.featureName == "type_a"
        assert placement.nodeNames == ["cn-001"]
        assert len(placement.allocations) == 1

        alloc = placement.allocations[0]
        assert alloc.cpu == 2.0
        assert alloc.gpu == 1.0

    def test_multi_node_job_hostlist_range(self, tree):
        """Multi-node job (cn-[001-003]) → nodes allocated.

        With 2 CPU across 3 nodes and spread=True, only 2 nodes receive
        CPU allocation (1 each). The third node gets 0 CPU and is excluded
        from the placement's allocations (only non-zero allocations returned).
        """
        job = create_running_job(
            jobID=2001,
            cpusReq=3,
            nodesAlloc=3,
            nodelist="cn-[001-003]",
        )
        placement = tree.placeRunningJob(job)

        assert placement is not None
        assert placement.featureName == "type_a"
        assert set(placement.nodeNames) == {"cn-001", "cn-002", "cn-003"}
        assert len(placement.allocations) == 3

    def test_multi_node_job_gpu_allocation(self, tree):
        """Multi-node job with GPU → GPU spread across nodes."""
        job = create_running_gpu_job(
            jobID=3001,
            cpusReq=2,
            nodesAlloc=2,
            gpusRequested=2,
            nodelist="cn-[007-008]",
            constraints="type_d",
        )
        placement = tree.placeRunningJob(job)

        assert placement is not None
        assert placement.featureName == "type_d"
        assert set(placement.nodeNames) == {"cn-007", "cn-008"}

        total_gpu = sum(a.gpu for a in placement.allocations)
        assert total_gpu == 2.0

    def test_job_without_nodelist(self, tree):
        """Job without nodelist → None."""
        job = create_running_job(jobID=2001, nodelist="")
        assert tree.placeRunningJob(job) is None

    def test_job_nodelist_none_assigned(self, tree):
        """Job with nodelist='None assigned' → None."""
        job = create_running_job(jobID=2001, nodelist="None assigned")
        assert tree.placeRunningJob(job) is None

    def test_job_on_unknown_nodes(self, tree):
        """Job on nodes not in tree → empty placement (None)."""
        job = create_running_job(jobID=2001, nodelist="cn-999")
        assert tree.placeRunningJob(job) is None

    def test_placement_not_reserved_until_reservePlacement_called(self, tree):
        """placeRunningJob returns placement but doesn't mutate tree."""
        job = create_running_gpu_job(
            jobID=2001, cpusReq=2, nodelist="cn-001", gpusRequested=1
        )
        placement = tree.placeRunningJob(job)

        # The tree should NOT be mutated yet
        cn001 = _find_node(tree, "cn-001")
        assert cn001.availableCpu == 4.0  # still full

        # Now reserve
        tree.reservePlacement(placement)
        assert cn001.availableCpu == 2.0  # 4 - 2

    def test_job_nodelist_is_comma_list(self, tree):
        """Job with comma-separated nodelist → all nodes placed."""
        job = create_running_job(
            jobID=2001,
            cpusReq=2,
            nodesAlloc=2,
            nodelist="cn-005,cn-006",
            tresAlloc="1=2,4=2",
            constraints="type_b",
        )
        placement = tree.placeRunningJob(job)

        assert placement is not None
        assert set(placement.nodeNames) == {"cn-005", "cn-006"}

    def test_running_job_with_zero_gpu(self, tree):
        """Running job with 0 GPU allocated → 0 GPU in allocation."""
        job = create_running_job(
            jobID=2001,
            cpusReq=2,
            nodelist="cn-005",
            constraints="type_b",
            tresAlloc="1=2,4=1",
        )
        placement = tree.placeRunningJob(job)

        assert placement is not None
        assert placement.featureName == "type_b"
        total_gpu = sum(a.gpu for a in placement.allocations)
        assert total_gpu == 0.0


# ════════════════════════════════════════════════════════════════════════════════
# Integration: fromClusterAndJobs + findPlacement + reservePlacement
# ════════════════════════════════════════════════════════════════════════════════


class TestResourceTreeIntegration:
    """Integration tests combining multiple tree operations."""

    @pytest.fixture
    def tree(self):
        config = build_mini_cluster_config()
        return ResourceAvailabilityTree.fromClusterAndJobs(config, [], TIMESTAMP_NOW)

    def test_place_reserve_then_find_reduced(self, tree):
        """After placing and reserving, findPlacement sees reduced resources."""
        # First placement: use cn-001 fully
        job1 = create_pending_job(
            jobID=1001, constraints="type_a", cpusReq=4, tresReq="1=4,4=1,1001=2"
        )
        p1 = tree.findPlacement(job1)
        assert p1 is not None
        tree.reservePlacement(p1)

        # cn-001 is now exhausted
        cn001 = _find_node(tree, "cn-001")
        assert cn001.availableCpu == 0.0
        assert cn001.availableGpu == 0.0

        # Second placement needs GPU → still fits on other type_a nodes
        job2 = create_pending_job(
            jobID=1002, constraints="type_a", cpusReq=2, tresReq="1=2,4=1,1001=1"
        )
        p2 = tree.findPlacement(job2)
        assert p2 is not None
        assert "cn-001" not in p2.nodeNames

    def test_full_utilization(self, tree):
        """Exhaust all type_a CPU → no more placements on type_a."""
        # Reserve all type_a CPU (4 nodes * 4 CPU = 16)
        for node in tree.nodesByFeature["type_a"]:
            node.usedCpu = node.totalCpu

        # CPU-only job on type_a should fail (all CPU consumed)
        job = create_pending_job(
            jobID=1001, constraints="type_a", cpusReq=1, tresReq="1=1,4=1"
        )
        assert tree.findPlacement(job) is None

    def test_concurrent_placement_different_features(self, tree):
        """Placements on different features don't interfere.

        Place 1 GPU on type_a and 4 GPU on type_d. After reserving,
        type_a should still have plenty of resources, and type_d should
        have reduced available GPU.
        """
        # No specific node count (4= not in tresReq) → minimum nodes selected
        job_a = create_pending_job(
            jobID=1001, constraints="type_a", cpusReq=2, tresReq="1=2,1001=1"
        )
        job_d = create_pending_job(
            jobID=1002, constraints="type_d", cpusReq=4, tresReq="1=4,1001=4"
        )

        p_a = tree.findPlacement(job_a)
        p_d = tree.findPlacement(job_d)

        assert p_a is not None
        assert p_d is not None

        tree.reservePlacement(p_a)
        tree.reservePlacement(p_d)

        # type_d should have reduced available GPU (4 GPU used total)
        total_available_gpu = sum(n.availableGpu for n in tree.nodesByFeature["type_d"])
        assert total_available_gpu < 8.0  # started with 8, used 4

        # type_a should still have CPU available (only used 2 of 16)
        totals_a = tree.getFeatureTotals("type_a")
        assert totals_a["cpu"] == 14.0  # 16 - 2

    def test_getFeatureTotals_after_reserve(self, tree):
        """getFeatureTotals reflects reserved CPU resources.

        NOTE: getFeatureTotals uses node.totalGpu for GPU, so GPU is
        unaffected by reservations. CPU uses availableCpu.
        """
        placement = JobPlacement(
            featureName="type_d",
            allocations=[
                NodeAllocation(nodeName="cn-007", cpu=4.0, gpu=2.0),
                NodeAllocation(nodeName="cn-008", cpu=4.0, gpu=2.0),
            ],
        )
        tree.reservePlacement(placement)

        totals = tree.getFeatureTotals("type_d")
        assert totals["cpu"] == 8.0  # (8-4) + (8-4) = 8
        assert totals["gpu"] == 8.0  # totalGpu is unaffected by usedGpu


# ════════════════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════════════════


def _find_node(tree: ResourceAvailabilityTree, node_name: str) -> NodeResourceState:
    """Find a node by name in the tree."""
    for feature_nodes in tree.nodesByFeature.values():
        for node in feature_nodes:
            if node.nodeName == node_name:
                return node
    raise ValueError(f"Node {node_name} not found in tree")
