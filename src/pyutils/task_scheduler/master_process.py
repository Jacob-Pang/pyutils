from multiprocessing import Event
from multiprocessing.managers import Namespace
from concurrent.futures import Executor, ProcessPoolExecutor

from pyutils.io import erase_stdout
from pyutils.task_scheduler.resource.resource_manager import ResourceManager
from pyutils.task_scheduler.task.base import TaskBase
from pyutils.task_scheduler.task.task_manager import TaskManager

class MasterProcess:
    def __init__(self, resource_manager: ResourceManager, task_manager: TaskManager, update_event: Event,
        no_remaining_tasks_event: Event, master_state: Namespace, shared_namespace: Namespace, executor_type:
        Executor = ProcessPoolExecutor, max_workers: int = 2, verbose: bool = False) -> None:

        self.resource_manager = resource_manager
        self.task_manager = task_manager
        self.update_event = update_event
        self.no_remaining_tasks_event = no_remaining_tasks_event
        self.master_state = master_state
        self.shared_namespace = shared_namespace
        self.executor_type = executor_type
        self.max_workers = max_workers

        self.state_repr_size = 0
        self.verbose = verbose

    @property
    def heartbeat(self) -> bool:
        return self.master_state.heartbeat

    @property
    def description(self) -> str:
        return self.master_state.description

    def get_timeout_to_update(self) -> float:
        resource_timeout = self.resource_manager.get_timeout_to_update()
        task_timeout = self.task_manager.get_timeout_to_next_task()

        if not resource_timeout:
            return task_timeout
        
        if not task_timeout:
            return resource_timeout

        return min(resource_timeout, task_timeout)

    def _run_task(self, task: TaskBase, allocated_keys: dict) -> None:
        self.executor.submit(
            task,
            allocated_keys=allocated_keys,
            update_event=self.update_event,
            resource_manager_proxy=self.resource_manager.as_proxy(),
            task_manager_proxy=self.task_manager.as_proxy(),
            shared_namespace=self.shared_namespace
        )

    def _display_state(self) -> None:
        if not self.verbose:
            return

        erase_stdout(self.state_repr_size)
        state_repr = f"{self.description}\n" + \
            "==================================================================================\n" + \
            "RESOURCES                                    Usage\n\n" + \
            str(self.resource_manager) + \
            f"\n\nTASKS [{self.task_manager.active_tasks:<4}]                                 State                          Runs\n\n" + \
            str(self.task_manager)

        self.state_repr_size = state_repr.count('\n') + 1
        print(state_repr)

    def _run_master_process(self) -> None:
        while self.heartbeat:
            if not self.task_manager.active_tasks:
                self.no_remaining_tasks_event.set()

            self.update_event.wait(timeout=self.get_timeout_to_update())
            self.update_event.clear()

            updated_resources = self.resource_manager.update()
            freed_tasks = self.task_manager.update(self.resource_manager, updated_resources)

            for task, allocated_keys in freed_tasks.items():
                self._run_task(task, allocated_keys)

            while True:
                task, allocated_keys = self.task_manager.process_next_task(self.resource_manager)
                
                if not task:
                    break

                self._run_task(task, allocated_keys)

            self._display_state() # prob: done + active tasks

    def start(self) -> None:
        self.executor = self.executor_type(self.max_workers)
        self.update_event.set()
        self._run_master_process()
        self.executor.shutdown()

if __name__ == "__main__":
    pass