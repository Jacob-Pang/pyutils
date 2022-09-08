import time

from multiprocessing import Semaphore
from multiprocessing.managers import DictProxy, ListProxy, Namespace, SyncManager
from typing import Tuple

from pyutils.task_scheduler.resource.resource_manager import ResourceManager
from pyutils.task_scheduler.task.base import TaskBase
from pyutils.task_scheduler.task.task_state import TaskState

class TaskManagerProxy:
    def __init__(self, task_states: DictProxy, task_futures: DictProxy, end_of_task_events: DictProxy,
        task_queue: ListProxy, task_manager_state: Namespace, task_queue_sem: Semaphore,
        task_manager_state_sem: Semaphore) -> None:

        self._task_states = task_states
        self._task_futures = task_futures
        self._end_of_task_events = end_of_task_events
        self._task_queue = task_queue
        self._task_manager_state = task_manager_state
        self._task_queue_sem = task_queue_sem
        self._task_manager_state_sem = task_manager_state_sem

    @property
    def active_tasks(self) -> int:
        return self._task_manager_state.active_tasks

    @active_tasks.setter
    def active_tasks(self, count: int) -> None:
        self._task_manager_state.active_tasks = count

    def update_active_tasks(self, change: int) -> None:
        with self._task_manager_state_sem:
            self._task_manager_state.active_tasks += change

    def update_task_state(self, task: TaskBase, state: str, timestamp: float = None) -> None:
        self._task_states[task.key] = TaskState(task.name, state, task._run_count, timestamp)

    def update_end_of_task(self, task: TaskBase, output: any) -> None:
        if task.key in self._end_of_task_events:
            self._task_futures[task.key] = output
            self._end_of_task_events[task.key].set()

        self.update_active_tasks(-1)

    # Heapq adaptations to ensure compatibility with ListProxy
    def _task_comparator(self, task: TaskBase, other: TaskBase) -> bool:
        return task.start_time < other.start_time

    def _siftdown_task_queue(self, start_pos: int, pos: int) -> None:
        new_task = self._task_queue[pos]

        while pos > start_pos:
            parent_pos = (pos - 1) >> 1
            parent_task = self._task_queue[parent_pos]

            if not self._task_comparator(new_task, parent_task):
                break

            self._task_queue[pos] = parent_task
            pos = parent_pos

        self._task_queue[pos] = new_task

    def _siftup_task_queue(self, pos: int) -> None:
        end_pos = len(self._task_queue)
        start_pos = pos
        new_task = self._task_queue[pos]
        child_pos = 2 * pos + 1

        while child_pos < end_pos:
            right_pos = child_pos + 1

            if right_pos < end_pos and not self._task_comparator(self._task_queue[child_pos],
                    self._task_queue[right_pos]):
                child_pos = right_pos

            self._task_queue[pos] = self._task_queue[child_pos]
            pos = child_pos
            child_pos = 2 * pos + 1

        self._task_queue[pos] = new_task
        self._siftdown_task_queue(start_pos, pos)

    def push_task(self, task: TaskBase) -> None:
        if not task.key in self._task_states:
            self.update_active_tasks(1)

        with self._task_queue_sem:
            self.update_task_state(task, TaskState.NEW_STATE, task.start_time)
            self._task_queue.append(task)
            self._siftdown_task_queue(0, len(self._task_queue) - 1)

    def pop_task(self) -> TaskBase:
        with self._task_queue_sem:
            last_task = self._task_queue.pop()

            if self._task_queue:
                next_task = self._task_queue[0]
                self._task_queue[0] = last_task

                self._siftup_task_queue(0)
                return next_task

            return last_task

    def remove_task_state(self, task_key: str) -> None:
        self._task_states.pop(task_key)

class TaskManager (TaskManagerProxy):
    def __init__(self, sync_manager: SyncManager) -> None:
        TaskManagerProxy.__init__(self, sync_manager.dict(), sync_manager.dict(), sync_manager.dict(),
                sync_manager.list(), sync_manager.Namespace(active_tasks=0), sync_manager.Semaphore(1),
                sync_manager.Semaphore(1))

        self.blocked_tasks = dict()
        self.resource_constraints = dict()
        self.active_tasks = 0

    def submit_task(self, sync_manager: SyncManager, task: TaskBase) -> None:
        self._end_of_task_events[task.key] = sync_manager.Event()
        self._task_futures[task.key] = None
        self.push_task(task)

    def block_task(self, task: TaskBase, resource_constraints: set) -> None:
        self.blocked_tasks[task.key] = task
        self.resource_constraints[task.key] = resource_constraints

        self.update_task_state(task, TaskState.BLOCKED_STATE, self._task_states.get(task.key).timestamp)

    def process_next_task(self, resource_manager: ResourceManager) -> Tuple[TaskBase, dict]:
        """ Attempts to allocate resources for the next task and return the task and the resource
        keys allocated to the execution. Blocks any task with resource constraints during processing.

        Parameters:
        :resource_manager (ResourceManager):

        Returns:
        :task (Task): The next task that is ready to run. None if there are no ready tasks.
        :allocated_keys (dict): The mapping of resource_keys to keys generated by the resources upon
                allocation. None if there is no next task.
        """
        if not self._task_queue or self._task_queue[0].start_time > time.time():
            return None, None

        task = self.pop_task()
        resource_constraints = set()
        allocated_keys = resource_manager.use_or_queue_resources(task.resource_usage, resource_constraints)

        if not resource_constraints:
            return (task, allocated_keys)

        self.block_task(task, resource_constraints)
        return self.process_next_task(resource_manager)

    def update(self, resource_manager: ResourceManager, updated_resources: set) -> dict:
        """
        Parameters:
        :resource_manager (ResourceManager):
        :updated_resources (set): The set of resource_keys for which the resources have undergone
                changes in state during update procedure. (see ResourceManager.update)

        Returns:
        :freed_tasks (dict): The mapping of Task: allocated_keys (resource_key: allocated_key), for
                tasks that have been freed from the blocked state.
        """
        freed_tasks = dict()

        for task_key, resource_constraints in self.resource_constraints.items():
            if updated_resources.isdisjoint(resource_constraints):
                continue # No update to resource_constraints

            task = self.blocked_tasks.get(task_key)
            allocated_keys = resource_manager.use_from_queued_resources(task.resource_usage,
                    resource_constraints)

            if not resource_constraints:
                self.blocked_tasks.pop(task_key)
                freed_tasks[task] = allocated_keys

        for task in freed_tasks:
            self.resource_constraints.pop(task.key)

        return freed_tasks

    def get_timeout_to_next_task(self) -> float:
        if not self._task_queue:
            return None
        
        return self._task_states.get(self._task_queue[0].key).timestamp - time.time()

    def get_task_output(self, task_key: str, timeout: float = None) -> any:
        self._end_of_task_events[task_key].wait(timeout)

        return self._task_futures.get(task_key)

    def as_proxy(self) -> TaskManagerProxy:
        return TaskManagerProxy(self._task_states, self._task_futures, self._end_of_task_events,
                self._task_queue, self._task_manager_state, self._task_queue_sem,
                self._task_manager_state_sem)
    
    def __repr__(self) -> str:
        return "\n".join([
            str(task_state) for task_state in self._task_states.values()
        ])

if __name__ == "__main__":
    pass