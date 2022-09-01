import multiprocessing

from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor

from pyutils.task_scheduler.master_process import MasterProcess
from pyutils.task_scheduler.resource.base import ResourceBase
from pyutils.task_scheduler.resource.resource_manager import ResourceManager
from pyutils.task_scheduler.task.base import TaskBase
from pyutils.task_scheduler.task.task_manager import TaskManager
from concurrent.futures import Executor

class TaskScheduler:
    def __init__(self):
        self.sync_manager = multiprocessing.Manager()
        self.resource_manager = ResourceManager(self.sync_manager)
        self.task_manager = TaskManager(self.sync_manager)
        self.update_event = self.sync_manager.Event()
        self.no_remaining_tasks_event = self.sync_manager.Event()
        self.master_state = self.sync_manager.Namespace(
            heartbeat=True,
            description=""
        )

        self.shared_namespace = self.sync_manager.Namespace()
        self.started = False

    def add_resources(self, *resources: ResourceBase) -> None:
        self.resource_manager.add_resources(*resources)
    
    def set_to_shared_namespace(self, name: str, value: any) -> None:
        self.shared_namespace.__setattr__(name, value)
    
    def set_description(self, description: str) -> None:
        self.master_state.description = description

    def submit_task(self, task: TaskBase) -> None:
        self.task_manager.submit_task(self.sync_manager, task)

    def execute_tasks(self, *tasks: TaskBase) -> list:
        # Executes the tasks using the master_process and waits for the outputs.
        assert self.started

        for task in tasks:
            self.submit_task(task)

        return [
            self.task_manager.get_task_output(task.key)
            for task in tasks
        ]

    def start(self, executor_type: Executor = ProcessPoolExecutor, max_workers: int = 2,
        description: str = "", verbose: bool = True) -> None:

        assert not self.started
        self.set_description(description)

        master_process = MasterProcess(
            self.resource_manager,
            self.task_manager,
            self.update_event,
            self.no_remaining_tasks_event,
            self.master_state,
            self.shared_namespace,
            executor_type=executor_type,
            max_workers=max_workers,
            verbose=verbose
        )

        self.master_process = multiprocessing.Process(target=master_process.start)
        self.master_process.start()
        self.started = True
    
    def start_mprocess(self, max_workers: int = 2, description: str = "", verbose: bool = True) -> None:
        return self.start(ProcessPoolExecutor, max_workers, description, verbose)

    def start_mthread(self, max_workers: int = 2, description: str = "", verbose: bool = True) -> None:
        return self.start(ThreadPoolExecutor, max_workers, description, verbose)

    def stop(self) -> None:
        self.master_state.heartbeat = False
        self.update_event.set()
        self.master_process.join()
        self.started = False

    def join(self) -> None:
        # Waits for all running and waiting tasks to be completed before stopping.
        self.no_remaining_tasks_event.wait()
        self.stop()   

if __name__ == "__main__":
    pass