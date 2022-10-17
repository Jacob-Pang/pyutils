import heapq
from multiprocessing import Queue
from multiprocessing.managers import SyncManager
from task_scheduler.task import Task

class TaskManagerProxy:
    def __init__(self, _new_task_queue: Queue) -> None:
        self._new_task_queue = _new_task_queue

    def register_task(self, task: Task) -> None:
        self._new_task_queue.put(task)

    def post_task_completion(self, task: Task) -> None:
        if task.get_remaining_runs() > 0: # Requeue
            return self._new_task_queue.put(task)

class TaskManager (TaskManagerProxy):
    def __init__(self, sync_manager: SyncManager) -> None:
        self._waiting_queue = [] # [task ...]
        TaskManagerProxy.__init__(self, sync_manager.Queue())

    # Accessors
    def get_time_to_update(self) -> float:
        if self._waiting_queue:
            return self._waiting_queue[0].get_time_to_ready()
        
        return 5

    def get_proxy(self) -> TaskManagerProxy:
        return TaskManagerProxy(self._new_task_queue)

    # Mutators
    def update(self) -> None:
        while not self._new_task_queue.empty():
            # Process new tasks
            task = self._new_task_queue.get()
            heapq.heappush(self._waiting_queue, task)

    def __iter__(self) -> Task:
        while self._waiting_queue and self._waiting_queue[0].ready():
            task = heapq.heappop(self._waiting_queue)
            yield task

if __name__ == "__main__":
    pass