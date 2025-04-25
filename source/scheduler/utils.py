from collections import deque


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
        for item in set(elements) - self._set:
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

        for item in elements_set - self._set:
            self._dq.append(item)
            self._set.add(item)

    def put(self, item):
        if item not in self._set:
            self._dq.appendleft(item)
            self._set.add(item)

    def pop(self):
        item = self._dq.pop()
        self._set.remove(item)
        return item

    def __len__(self):
        return len(self._dq)

    def empty(self):
        return not self._dq

    def __contains__(self, item):
        return item in self._set
