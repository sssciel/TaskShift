from collections import deque

from configs.logging import log


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

    def rebuild(self, elements):
        """
        Adds items that are not in the queue and
        removes items that are missing from the elements,
        preserving the order.

        Args:
            elements (List)
        """
        log.trace("Rebuilding UniqueQueue...")
        elements_set = set(elements)
        self._dq = deque(x for x in self._dq if x in elements)
        self._set = set(self._dq)

        for item in elements_set - self._set:
            self._dq.append(elements[item])
            self._set.add(item)

            log.trace(f"Element {item} was added to UniqueQueue")

        log.debug(
            f"UniqueQueue rebuilding was finished. Total length = {len(self._set)}"
        )

    def put(self, item):
        # Та же ситуация, что и в pop.
        job_id = item["job_id"]
        if job_id not in self._set:
            self._dq.appendleft(item)
            self._set.add(job_id)

            log.trace(
                f"Element {job_id} was added to UniqueQueue. Length = {len(self._set)}"
            )

    def pop(self):
        item = self._dq.pop()
        # Set не умеет хранить нехэшируемые объекты, то есть словари, поэтому
        # весь сет состоит только из чисел. В то же самое время очередь состоит
        # только из словарей. Поэтому в самих словарях мне пришлось
        # продублировать job_id, что видно в коде integrations. Кроме этого,
        # когда я достаю задачу из очереди, я достаю словарь, в котором есть
        # поле job_id, а сет состоит только из чисел. Поэтому пришлось захардкодить
        # ключ job_id при remove из сета, чтобы достать этот самый ID.

        self._set.remove(item["job_id"])  # TO DO: написать абстракцию класса задачи

        log.trace(
            f"Element {item['job_id']} was popped from UniqueQueue. Length = {len(self._set)}"
        )
        return item

    def __len__(self):
        return len(self._dq)

    def empty(self):
        return not self._dq

    def __contains__(self, item):
        return item in self._set
