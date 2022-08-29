import multiprocessing

from multiprocessing import Process

from pyutils import generate_unique_key
from pyutils.scheduler.resource import Resource
from pyutils.scheduler.task import Task
from pyutils.scheduler.worker import Worker
from pyutils.scheduler.task.task_manager import TaskManager

class MasterProcess (Worker):
    def __init__(self, verbose: bool = True, timeout: int = None, max_workers: int = 1) -> None:
        self.sync_manager = multiprocessing.Manager()
        self.worker_timeout = timeout
        self.worker_processes = dict()

        Worker.__init__(
            self, "__MASTER__", TaskManager(self.sync_manager, verbose=verbose),
            master_process_state=self.sync_manager.Namespace(
                active=True,
                listening_mode=False,
                max_workers=max_workers
            ),
            timeout=None,
            remove_worker_state_on_death=False
        )

    def set_listening_mode(self, listening_mode: bool) -> None:
        self.master_process_state.listening_mode = listening_mode

    def register_task(self, task: Task, timestamp: float = None) -> None:
        task_manager = self.task_manager

        with task_manager.semaphore:
            task_manager.register_task(task, timestamp)

    def register_resource(self, resource: Resource) -> None:
        with self.task_manager.semaphore:
            self.task_manager.register_resource(self.sync_manager, resource)

    def spawn_worker_process(self) -> Process:
        worker_key = generate_unique_key(prefix="W_")
        worker = Worker(worker_key, self.task_manager, self.master_process_state, self.worker_timeout)
        self.task_manager.register_worker(worker_key)
        worker_process = Process(target=worker)
        self.worker_processes[worker.key] = worker_process
        return worker_process

    def heartbeat(self, start_time: float) -> bool:
        with self.task_manager.semaphore:
            while self.task_manager.active_workers < self.master_process_state.max_workers and \
                self.task_manager.public_pending_tasks > self.task_manager.active_workers:
                worker_process = self.spawn_worker_process()
                worker_process.start()
        
        if not self.active: # MasterProcess has been shut down.
            return False

        if not self.task_manager.public_active_tasks and not self.listening_mode:
            return False # No public_active_tasks and not on listening_mode.

        return True

    def start(self) -> None:
        return self()

    def stop(self) -> None:
        self.master_process_state.active = False

        for worker_process in self.worker_processes.values():
            worker_process.join()
        
        super().stop()
        self.sync_manager.shutdown()

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        self.stop()

if __name__ == "__main__":
    pass