from multiprocessing import Event, Semaphore, Value
from multiprocessing.managers import DictProxy, SyncManager
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from task_scheduler.task import Task
from task_scheduler.resource_manager import ResourceManager, ResourceManagerProxy
from task_scheduler.task_manager import TaskManager, TaskManagerProxy

class MasterProcessProxy:
    def __init__(self, _resource_manager: ResourceManagerProxy, _task_manager: TaskManagerProxy,
        _task_events: DictProxy, _task_futures: DictProxy, _update_event: Event, _active_tasks: Value,
        _counter_sem: Semaphore):

        self._resource_manager = _resource_manager
        self._task_manager = _task_manager
        self._task_events = _task_events
        self._task_futures = _task_futures
        self._update_event = _update_event
        self._active_tasks = _active_tasks
        self._counter_sem = _counter_sem

    def register_task(self, task: Task, sync_manager: SyncManager = None) -> None:   
        if sync_manager and task.key not in self._task_events:
            self._task_events[task.key] = sync_manager.Event()

        with self._counter_sem:
            self._active_tasks.value += 1

        self._task_manager.register_task(task)
        self._update_event.set()

    def post_task_completion(self, task: Task, output: any) -> None:
        self._task_futures[task.key] = output

        if task.get_remaining_runs() > 0:
            self._task_manager.post_task_completion(task)
        elif task.key in self._task_events: # End of task
            self._task_events[task.key].set()

            with self._counter_sem:
                self._active_tasks.value -= 1

        self._update_event.set()

    def free_resources(self, resource_usage: dict, allocated_resources: dict) -> None:
        self._resource_manager.free_resources(resource_usage, allocated_resources)
        self._update_event.set()

class MasterProcess (MasterProcessProxy):
    """
    Phases of tasks:
        1. new      : not yet processed by the manager
        2. waiting  : processed and waiting for the task start time to elapse
        3. ready    : start time elapsed (temporally ready) - oustanding ready tasks
                at the end of update represent tasks constrained by resources usage
        4. running  : submitted to executor - may not actually be running
    """
    def __init__(self, sync_manager: SyncManager, _resource_manager: ResourceManager) -> None:
        self._resource_manager = _resource_manager
        self._task_manager = TaskManager(sync_manager)

        self._ready_tasks = dict() # {task_key: task}
        self._task_events = sync_manager.dict()
        self._task_futures = sync_manager.dict()
        self._update_event = sync_manager.Event()
        self._heartbeat = sync_manager.Event()

        self._active_tasks = sync_manager.Value(int, 0)
        self._counter_sem = sync_manager.Semaphore(1)

    @property
    def heartbeat(self) -> bool:
        return not self._heartbeat.is_set()

    # Accessors
    def get_time_to_update(self) -> float:
       return min(self._resource_manager.get_time_to_update(), self._task_manager.get_time_to_update())

    def get_proxy(self) -> MasterProcessProxy:
        return MasterProcessProxy(self._resource_manager, self._task_manager, self._task_events,
                self._task_futures, self._update_event, self._active_tasks, self._counter_sem)

    def get_task_future(self, sync_manager: SyncManager, task_key: str, timeout: int = None) -> any:
        if task_key not in self._task_events:
            self._task_events[task_key] = sync_manager.Event()

        self._task_events[task_key].wait(timeout)

        if task_key in self._task_futures:
            return self._task_futures.get(task_key)

        return None

    # Mutators
    def shutdown(self) -> None:
        self._heartbeat.set()
        self._update_event.set() # Force awakening

    def update(self, master_process: MasterProcessProxy) -> None:
        self._task_manager.update()

        for task in self._task_manager:
            self._resource_manager.register_request(task.key, task.resource_usage)
            self._ready_tasks[task.key] = task

        if not self._ready_tasks:
            return # Update to resources not urgent

        self._resource_manager.update()
        running_tasks = []

        for task in self._ready_tasks.values():
            allocated_resources = self._resource_manager.get_allocated_resources(task.key,
                    task.resource_usage)

            if not allocated_resources is None:
                self._resource_manager.use_resources(task.key, allocated_resources)
                running_tasks.append(task.key)
                self._executor.submit(task, master_process=master_process,
                        allocated_resources=allocated_resources)

        for task_key in running_tasks:
            self._ready_tasks.pop(task_key)

    # Process runner
    def start(self, max_workers: int = 2, use_multiprocessing: bool = True, persist: bool = True) -> None:
        # Initialize executor
        if use_multiprocessing:
            self._executor = ProcessPoolExecutor(max_workers)
        else:
            self._executor = ThreadPoolExecutor(max_workers)

        self._heartbeat.clear()
        master_process = self.get_proxy()

        while self.heartbeat and (persist or self._active_tasks.value > 0):
            self._update_event.clear()
            self.update(master_process)
            self._update_event.wait(self.get_time_to_update())
        
        self._executor.shutdown()

if __name__ == "__main__":
    pass