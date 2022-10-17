import multiprocessing
from collections import deque
from task_scheduler.master_process import MasterProcess
from task_scheduler.task import Task
from task_scheduler.resource import ResourceBase
from task_scheduler.resource_allocator import ResourceAllocatorBase
from task_scheduler.resource_manager import ResourceManager

class TaskScheduler:
    def __init__(self) -> None:
        self._sync_manager = multiprocessing.Manager()
        self._resource_manager = ResourceManager(self._sync_manager)

        self._master_process = None
        self._async_process = None
        self._task_buffer = deque()

    # Accessors
    def get_task_future(self, task_key: str, timeout: float = None) -> any:
        assert self._master_process
        return self._master_process.get_task_future(self._sync_manager, task_key, timeout)

    # Mutators
    def register_allocator(self, resource_allocator: ResourceAllocatorBase) -> None:
        self._resource_manager.register_allocator(resource_allocator)

    def register_resource(self, resource: ResourceBase, alias: str = None) -> None:
        self._resource_manager.register_resource(resource, alias)

    def register_task(self, task: Task) -> None:
        if self._master_process:
            return self._master_process.register_task(task, self._sync_manager)
        
        self._task_buffer.append(task)
    
    def run_sync(self, max_workers: int = 2, use_multiprocessing: bool = True) -> None:
        self._master_process = MasterProcess(self._sync_manager, self._resource_manager)

        while self._task_buffer:
            self._master_process.register_task(self._task_buffer.pop(), self._sync_manager)

        self._master_process.start(max_workers, use_multiprocessing, persist=False)

    def shutdown(self) -> None:
        self._master_process.shutdown()

    def start_async(self, max_workers: int = 2, use_multiprocessing: bool = True, persist: bool = True) -> None:
        self._master_process = MasterProcess(self._sync_manager, self._resource_manager)

        while self._task_buffer:
            self._master_process.register_task(self._task_buffer.pop(), self._sync_manager)

        self._async_process = multiprocessing.Process(
            target=self._master_process.start,
            kwargs={
                "max_workers": max_workers,
                "use_multiprocessing": use_multiprocessing,
                "persist": persist
            }
        )

        self._async_process.start()
    
    def join_async(self, timeout: int = None) -> None:
        assert self._async_process
        self._async_process.join(timeout)
        self._async_process = None

if __name__ == "__main__":
    pass