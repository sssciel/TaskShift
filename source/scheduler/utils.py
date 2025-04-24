from collections import deque
from configs.config import ClusterConfig

class UniqueQueue:
    """
    The UniqueQueue is needed in order to save the 
    queue state between calls to get_sessionid_queue_tasks_test. 
    Without this, the queue would have to be rebuilt with each 
    call, and failed launches would still be at the top, 
    instead of falling to the beginning.
    """

    def __init__(self, initial=None):
        self._dq = deque(initial or [])
        self._set = set(self._dq)

    def add_elements(self, elements):
        for item in (set(elements) - self._set):
            self._dq.append(item)
            self._set.add(item)

    def rebuild(self, elements):
        """
        Adds items that are not in the queue and 
        removes items that are missing from the elements, 
        preserving the order.

        Args:
            elements (List)
        """
        elements_set = set(elements)
        self._dq = deque(x for x in self._dq if x in elements)
        self._set = set(self._dq)

        for item in (elements_set - self._set):
            self._dq.append(item)
            self._set.add(item)

    def put(self, item):
        if item not in self._set:
            self._dq.append(item)
            self._set.add(item)

    def pop(self):
        item = self._dq.popleft()
        self._set.remove(item)
        return item

    def __len__(self):
        return len(self._dq)

    def empty(self):
        return not self._dq

cluster_config = ClusterConfig()

def get_devices_count():
    cpu_count, gpu_count = 0, 0

    for node in cluster_config.get_config()["nodes"]:
        node_count = node["count"]
        cpu_count += node["cpu"]["sockets"] * node["cpu"]["cores_per_cpu"] * node_count

        if node["gpu"] is not None:
            gpu_count += node["gpu"]["count"] * node_count

    return cpu_count, gpu_count
