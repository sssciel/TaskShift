import logging
from dataclasses import dataclass

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info


@dataclass
class NodeResourceState:
    nodeName: str
    featureName: str
    totalCpu: int
    totalGpu: int
    usedCpu: float = 0.0
    usedGpu: float = 0.0

    @property
    def availableCpu(self) -> float:
        return max(0.0, self.totalCpu - self.usedCpu)

    @property
    def availableGpu(self) -> float:
        return max(0.0, self.totalGpu - self.usedGpu)


@dataclass
class NodeAllocation:
    nodeName: str
    cpu: float
    gpu: float


@dataclass
class JobPlacement:
    featureName: str
    allocations: list[NodeAllocation]

    @property
    def nodeNames(self) -> list[str]:
        return [allocation.nodeName for allocation in self.allocations]


class ResourceAvailabilityTree:
    def __init__(self, nodesByFeature: dict[str, list[NodeResourceState]]):
        self.nodesByFeature = nodesByFeature

    @classmethod
    def fromClusterAndJobs(cls, clusterConfig, runningJobs, timestamp: int):
        nodeCapacities = clusterConfig.getNodeCapacitiesAt(timestamp)
        nodesByFeature = {}
        nodeStatesByName = {}

        for nodeName, capacity in nodeCapacities.items():
            nodeState = NodeResourceState(
                nodeName=nodeName,
                featureName=capacity["features"][0],
                totalCpu=capacity["cpu"],
                totalGpu=capacity["gpu"],
            )
            nodeStatesByName[nodeName] = nodeState

            for featureName in capacity["features"]:
                nodesByFeature.setdefault(featureName, []).append(
                    nodeState
                )

        tree = cls(nodesByFeature)

        for job in runningJobs:
            if not job.hasAssignedNodes():
                logger.warning(f"Skipping active job {job.jobID} without assigned nodelist in resource tree")
                continue

            placement = tree.placeRunningJob(job)
            if placement is not None:
                tree.reservePlacement(placement)

        return tree

    def findPlacement(self, job):
        requestedFeatures = job.getRequestedFeatures(list(self.nodesByFeature.keys()))

        for featureName in requestedFeatures:
            placement = self.findPlacementOnFeature(job, featureName)
            if placement is not None:
                return placement

        return None

    def findPlacementOnFeature(
        self,
        job,
        featureName: str,
        allowedNodeNames: set[str] | None = None,
        maxCpuPerNode: int | None = None,
        maxNodesLimit: int | None = None,
    ):
        return self._findPlacementOnFeature(job, featureName, allowedNodeNames, maxCpuPerNode, maxNodesLimit)

    def placeRunningJob(self, job):
        assignedNodes = self._getAssignedNodeStates(job)
        if not assignedNodes:
            return None

        return self._buildPlacement(
            featureName=assignedNodes[0].featureName,
            candidateNodes=assignedNodes,
            requestedCpu=job.cpusReq,
            requestedGpu=job.getRequestedGpus(),
            requestedNodes=len(assignedNodes),
            spreadAcrossSelectedNodes=True,
            maxCpuPerNode=None,
            maxNodesLimit=None,
        )

    def reservePlacement(self, placement: JobPlacement):
        featureNodes = {
            nodeState.nodeName: nodeState
            for nodeState in self.nodesByFeature.get(placement.featureName, [])
        }

        for allocation in placement.allocations:
            nodeState = featureNodes.get(allocation.nodeName)
            if nodeState is None:
                continue

            nodeState.usedCpu += allocation.cpu
            nodeState.usedGpu += allocation.gpu

    def getFeatureTotals(
        self,
        featureName: str,
        allowedNodeNames: set[str] | None = None,
        maxCpuPerNode: int | None = None,
    ) -> dict[str, float]:
        nodes = self._filterCandidateNodes(self.nodesByFeature.get(featureName, []), allowedNodeNames)
        return {
            "cpu": sum(self._getAvailableCpu(node, maxCpuPerNode) for node in nodes),
            "gpu": sum(node.totalGpu for node in nodes),
        }

    def getFeatureSnapshot(
        self,
        featureName: str,
        allowedNodeNames: set[str] | None = None,
        maxCpuPerNode: int | None = None,
    ) -> dict[str, float]:
        nodes = self._filterCandidateNodes(
            self.nodesByFeature.get(featureName, []), allowedNodeNames
        )
        return {
            "available_cpu": sum(
                self._getAvailableCpu(node, maxCpuPerNode) for node in nodes
            ),
            "available_gpu": sum(node.availableGpu for node in nodes),
            "total_cpu": sum(node.totalCpu for node in nodes),
            "total_gpu": sum(node.totalGpu for node in nodes),
        }

    def _findPlacementOnFeature(
        self,
        job,
        featureName: str,
        allowedNodeNames: set[str] | None,
        maxCpuPerNode: int | None,
        maxNodesLimit: int | None,
    ):
        candidateNodes = self._filterCandidateNodes(self.nodesByFeature.get(featureName, []), allowedNodeNames)
        if not candidateNodes:
            return None

        requestedNodes = job.getRequestedNodes()
        requestedCpu = job.getRequestedCpus()
        requestedGpu = job.getRequestedGpus()

        if maxNodesLimit is not None and requestedNodes > maxNodesLimit:
            return None

        return self._buildPlacement(
            featureName=featureName,
            candidateNodes=candidateNodes,
            requestedCpu=requestedCpu,
            requestedGpu=requestedGpu,
            requestedNodes=requestedNodes if requestedNodes > 0 else None,
            spreadAcrossSelectedNodes=requestedNodes > 1,
            maxCpuPerNode=maxCpuPerNode,
            maxNodesLimit=maxNodesLimit,
        )

    def _buildPlacement(
        self,
        featureName: str,
        candidateNodes: list[NodeResourceState],
        requestedCpu: int,
        requestedGpu: int,
        requestedNodes: int | None,
        spreadAcrossSelectedNodes: bool,
        maxCpuPerNode: int | None,
        maxNodesLimit: int | None,
    ):
        sortedNodes = sorted(
            candidateNodes,
            key=lambda node: (node.availableGpu, self._getAvailableCpu(node, maxCpuPerNode)),
            reverse=True,
        )

        if requestedNodes is None:
            selectedNodes = self._pickMinimumNodes(sortedNodes, requestedCpu, requestedGpu, maxCpuPerNode, maxNodesLimit)
        else:
            if len(sortedNodes) < requestedNodes:
                return None

            selectedNodes = sortedNodes[:requestedNodes]
            if not self._hasCapacity(selectedNodes, requestedCpu, requestedGpu, maxCpuPerNode):
                return None

        if not selectedNodes:
            return None

        allocations = self._allocateAcrossNodes(
            selectedNodes=selectedNodes,
            requestedCpu=requestedCpu,
            requestedGpu=requestedGpu,
            spreadAcrossSelectedNodes=spreadAcrossSelectedNodes,
            maxCpuPerNode=maxCpuPerNode,
        )
        if allocations is None:
            return None

        return JobPlacement(featureName=featureName, allocations=allocations)

    def _pickMinimumNodes(
        self,
        sortedNodes,
        requestedCpu: int,
        requestedGpu: int,
        maxCpuPerNode: int | None,
        maxNodesLimit: int | None,
    ):
        selectedNodes = []
        totalCpu = 0.0
        totalGpu = 0.0

        for node in sortedNodes:
            if maxNodesLimit is not None and len(selectedNodes) >= maxNodesLimit:
                break

            selectedNodes.append(node)
            totalCpu += self._getAvailableCpu(node, maxCpuPerNode)
            totalGpu += node.availableGpu

            if totalCpu >= requestedCpu and totalGpu >= requestedGpu:
                return selectedNodes

        return []

    def _hasCapacity(self, selectedNodes, requestedCpu: int, requestedGpu: int, maxCpuPerNode: int | None) -> bool:
        return (
            sum(self._getAvailableCpu(node, maxCpuPerNode) for node in selectedNodes) >= requestedCpu
            and sum(node.availableGpu for node in selectedNodes) >= requestedGpu
        )

    def _allocateAcrossNodes(
        self,
        selectedNodes,
        requestedCpu: int,
        requestedGpu: int,
        spreadAcrossSelectedNodes: bool,
        maxCpuPerNode: int | None,
    ):
        allocations = {
            node.nodeName: NodeAllocation(nodeName=node.nodeName, cpu=0.0, gpu=0.0)
            for node in selectedNodes
        }

        remainingGpu = float(requestedGpu)
        if remainingGpu > 0:
            gpuNodes = self._orderNodesForResource(selectedNodes, resource="gpu", spread=spreadAcrossSelectedNodes)
            remainingGpu = self._distributeResource(
                allocations=allocations,
                nodes=gpuNodes,
                remainingAmount=remainingGpu,
                capacityAttr="availableGpu",
                fieldName="gpu",
                spread=spreadAcrossSelectedNodes,
            )
            if remainingGpu > 0:
                return None

        remainingCpu = float(requestedCpu)
        if remainingCpu > 0:
            cpuNodes = self._orderNodesForCpu(selectedNodes, allocations, spreadAcrossSelectedNodes, maxCpuPerNode)
            remainingCpu = self._distributeResource(
                allocations=allocations,
                nodes=cpuNodes,
                remainingAmount=remainingCpu,
                capacityAttr="availableCpu",
                fieldName="cpu",
                spread=spreadAcrossSelectedNodes,
                maxCpuPerNode=maxCpuPerNode,
            )
            if remainingCpu > 0:
                return None

        return [allocation for allocation in allocations.values() if allocation.cpu > 0 or allocation.gpu > 0]

    def _orderNodesForResource(self, nodes, resource: str, spread: bool):
        if spread:
            return sorted(nodes, key=lambda node: getattr(node, f"available{resource.capitalize()}"))

        return sorted(nodes, key=lambda node: getattr(node, f"available{resource.capitalize()}"), reverse=True)

    def _orderNodesForCpu(self, nodes, allocations, spread: bool, maxCpuPerNode: int | None):
        if spread:
            return sorted(
                nodes,
                key=lambda node: (
                    allocations[node.nodeName].gpu == 0,
                    self._getAvailableCpu(node, maxCpuPerNode),
                ),
            )

        return sorted(
            nodes,
            key=lambda node: (
                allocations[node.nodeName].gpu > 0,
                self._getAvailableCpu(node, maxCpuPerNode),
            ),
            reverse=True,
        )

    def _distributeResource(
        self,
        allocations,
        nodes,
        remainingAmount: float,
        capacityAttr: str,
        fieldName: str,
        spread: bool,
        maxCpuPerNode: int | None = None,
    ):
        if remainingAmount <= 0:
            return 0.0

        if spread and nodes:
            while remainingAmount > 0:
                progressed = False
                for node in nodes:
                    allocation = allocations[node.nodeName]
                    capacity = self._getNodeResourceCapacity(node, fieldName, capacityAttr, maxCpuPerNode) - getattr(
                        allocation,
                        fieldName,
                    )
                    if capacity <= 0:
                        continue

                    step = min(1.0, capacity, remainingAmount)
                    setattr(allocation, fieldName, getattr(allocation, fieldName) + step)
                    remainingAmount -= step
                    progressed = True
                    if remainingAmount <= 0:
                        break

                if not progressed:
                    break

            return remainingAmount

        for node in nodes:
            allocation = allocations[node.nodeName]
            capacity = self._getNodeResourceCapacity(node, fieldName, capacityAttr, maxCpuPerNode) - getattr(
                allocation,
                fieldName,
            )
            if capacity <= 0:
                continue

            delta = min(capacity, remainingAmount)
            setattr(allocation, fieldName, getattr(allocation, fieldName) + delta)
            remainingAmount -= delta
            if remainingAmount <= 0:
                break

        return remainingAmount

    def _filterCandidateNodes(self, nodes, allowedNodeNames: set[str] | None):
        if allowedNodeNames is None:
            return list(nodes)

        return [node for node in nodes if node.nodeName in allowedNodeNames]

    def _getAvailableCpu(self, node: NodeResourceState, maxCpuPerNode: int | None):
        availableCpu = node.availableCpu
        if maxCpuPerNode is None:
            return availableCpu

        return min(availableCpu, float(maxCpuPerNode))

    def _getNodeResourceCapacity(
        self,
        node: NodeResourceState,
        fieldName: str,
        capacityAttr: str,
        maxCpuPerNode: int | None,
    ):
        if fieldName == "cpu":
            return self._getAvailableCpu(node, maxCpuPerNode)

        return getattr(node, capacityAttr)

    def _getAssignedNodeStates(self, job):
        from config import expand_hostlist

        assignedNodeNames = set(expand_hostlist(job.nodelist))

        assignedStates = []
        for featureNodes in self.nodesByFeature.values():
            for nodeState in featureNodes:
                if nodeState.nodeName in assignedNodeNames:
                    assignedStates.append(nodeState)

        uniqueStates = {}
        for nodeState in assignedStates:
            uniqueStates[nodeState.nodeName] = nodeState

        return list(uniqueStates.values())
