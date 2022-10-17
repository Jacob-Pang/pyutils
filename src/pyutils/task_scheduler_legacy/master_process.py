from multiprocessing import Event
from multiprocessing.managers import Namespace
from concurrent.futures import Executor, ProcessPoolExecutor

from pyutils.io import erase_stdout
from pyutils.task_scheduler_legacy.resource.resource_manager import ResourceManager
from pyutils.task_scheduler_legacy.task.base import TaskBase
from pyutils.task_scheduler_legacy.task.task_manager import TaskManager

class MasterProcess:
    def __init__(self, resource_manager: ResourceManager, task_manager: TaskManager, update_event: Event,
        no_remaining_tasks_event: Event, master_state: Namespace, shared_namespace: Namespace,
        executor_construct: Executor = ProcessPoolExecutor, max_workers: int = 2,
        verbose: bool = False) -> None:

        self._resource_manager = resource_manager
        self._task_manager = task_manager
        self._update_event = update_event
        self._no_remaining_tasks_event = no_remaining_tasks_event
        self._master_state = master_state
        self._shared_namespace = shared_namespace
        self._executor_construct = executor_construct
        self._max_workers = max_workers
        self._verbose = verbose
        self._state_repr_size = 0

    @property
    def heartbeat(self) -> bool:
        return self._master_state.heartbeat

    @property
    def description(self) -> str:
        return self._master_state.description

    def get_timeout_to_update(self) -> float:
        resource_timeout = self._resource_manager.get_timeout_to_update()
        task_timeout = self._task_manager.get_timeout_to_next_task()

        if not resource_timeout:
            return task_timeout
        
        if not task_timeout:
            return resource_timeout

        return min(resource_timeout, task_timeout)

    def _run_task(self, task: TaskBase, allocated_keys: dict) -> None:
        self._executor.submit(
            task,
            allocated_keys=allocated_keys,
            update_event=self._update_event,
            resource_manager_proxy=self._resource_manager.as_proxy(),
            task_manager_proxy=self._task_manager.as_proxy(),
            shared_namespace=self._shared_namespace
        )

    def _display_state(self) -> None:
        erase_stdout(self._state_repr_size)
        state_repr = f"{self.description}\n" + \
            "==================================================================================\n" + \
            "RESOURCES                                    Usage\n\n" + \
            str(self._resource_manager) + \
            f"\n\nTASKS [{self._task_manager.active_tasks:<4}]                                 State                          Runs\n\n" + \
            str(self._task_manager)

        self._state_repr_size = state_repr.count('\n') + 1
        print(state_repr)

    def _run_master_process(self) -> None:
        while self.heartbeat:
            if not self._task_manager.active_tasks:
                self._no_remaining_tasks_event.set()

            self._update_event.wait(timeout=self.get_timeout_to_update())
            self._update_event.clear()

            updated_resources = self._resource_manager.update()
            freed_tasks = self._task_manager.update(self._resource_manager, updated_resources)

            for task, allocated_keys in freed_tasks.items():
                self._run_task(task, allocated_keys)

            while True:
                task, allocated_keys = self._task_manager.process_next_task(self._resource_manager)
                
                if not task:
                    break

                self._run_task(task, allocated_keys)

            if self._verbose:
                self._display_state() # prob: done + active tasks

    def start(self) -> None:
        self._executor = self._executor_construct(max_workers=self._max_workers)
        self._update_event.set()

        self._run_master_process()
        self._executor.shutdown()

if __name__ == "__main__":
    pass