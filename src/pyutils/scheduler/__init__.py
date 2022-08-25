from multiprocessing import Process
from multiprocessing.managers import SyncManager

from pyutils import generate_unique_key
from pyutils.scheduler.task import Task
from pyutils.scheduler.resource import Resource, ResourceProxy
from pyutils.scheduler.task_manager import TaskManager
from pyutils.scheduler.worker import Worker

class MasterProcess (Worker):
    def __init__(self, sync_manager: SyncManager, verbose: bool = True, timeout: int = None,
        max_workers: int = 1, listening_mode: bool = False) -> None:

        self.worker_timeout = timeout
        self.worker_processes = dict()

        master_process_state = sync_manager.Namespace(
            active=True,
            listening_mode=False,
            max_workers=max_workers,
            task_manager=TaskManager(sync_manager, verbose=verbose)
        )

        Worker.__init__(self, "__MASTER__", master_process_state, None)

    def register_task(self, task: Task, timestamp: float = None) -> None:
        task_manager = self.master_process_state.task_manager

        with task_manager.semaphore:
            task_manager.register_task(task, timestamp)

    def register_resource(self, resource: Resource, sync_manager: SyncManager) -> None:
        self.register_resource_proxy(resource.create_proxy(sync_manager))

    def register_resource_proxy(self, resource_proxy: ResourceProxy) -> None:
        task_manager = self.master_process_state.task_manager

        with task_manager.semaphore:
            task_manager.register_resource(resource_proxy)

    def spawn_worker_process(self) -> Process:
        worker_key = generate_unique_key()
        worker = Worker(worker_key, self.master_process_state, self.worker_timeout)

        self.master_process_state.task_manager.register_worker(worker_key)
        worker_process = Process(target=worker)

        self.worker_processes[worker.key] = worker_process
        return worker_process

    def heartbeat(self, start_time: float) -> bool:
        task_manager = self.master_process_state.task_manager
        
        with task_manager.semaphore:
            while task_manager.state.active_workers < self.master_process_state.max_workers and \
                task_manager.state.public_pending_tasks > task_manager.state.active_workers:

                worker_process = self.spawn_worker_process()
                worker_process.start()
        
        return super().heartbeat(start_time)

    def stop(self) -> None:
        self.master_process_state.active = False
        super().stop()

        for worker_process in self.worker_processes.values():
            worker_process.join()

if __name__ == "__main__":
    pass